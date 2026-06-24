import sys
import os

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import duckdb
import glob
import datetime
import argparse
import shutil
import json
import pandas as pd
from utils.hf_manager import HFManager
from utils.qc import QualityControl

def get_stock_list_with_names():
    print("📋 Loading stock list metadata from JSON...")
    if os.path.exists("stock_list_master.json"):
        try:
            with open("stock_list_master.json", "r", encoding="utf-8") as f:
                data = json.load(f)
            if data:
                df = pd.DataFrame(data)
                df['tradeStatus'] = '1'
                return df
        except Exception as e:
            print(f"⚠️ Failed to parse stock JSON: {e}")
    return pd.DataFrame()

def calculate_ttm_net_profit(f10_parquet_path):
    print("🧮 Calculating Trailing Twelve Months (TTM) Net Profit...")
    if not os.path.exists(f10_parquet_path):
        print("⚠️ Warning: F10 raw parquet not found! PE/PB will fall back to 0.0.")
        return pd.DataFrame()

    df = pd.read_parquet(f10_parquet_path)
    if df.empty:
        return pd.DataFrame()

    df = df.sort_values(['code', 'report_date'])
    df['month_day'] = df['report_date'].str[-5:]
    df['year'] = df['report_date'].str[:4].astype(int)

    df_q4 = df[df['month_day'] == '12-31'][['code', 'year', 'parent_netprofit']].copy()
    df_q4['prev_year'] = df_q4['year']
    df_q4.rename(columns={'parent_netprofit': 'q4_prev_profit'}, inplace=True)

    df_prev = df.copy()
    df_prev['prev_year'] = df_prev['year']
    df_prev.rename(columns={'parent_netprofit': 'prev_cum_profit'}, inplace=True)

    df['prev_year'] = df['year'] - 1
    df = pd.merge(df, df_q4[['code', 'prev_year', 'q4_prev_profit']], on=['code', 'prev_year'], how='left')
    df = pd.merge(df, df_prev[['code', 'prev_year', 'month_day', 'prev_cum_profit']], on=['code', 'prev_year', 'month_day'], how='left')

    q_num_map = {'03-31': 1, '06-30': 2, '09-30': 3, '12-31': 4}
    df['q_num'] = df['month_day'].map(q_num_map).fillna(4)

    ttm_vals = []
    for idx, row in df.iterrows():
        md = row['month_day']
        cum = row['parent_netprofit']
        q4_prev = row['q4_prev_profit']
        prev_cum = row['prev_cum_profit']
        q_num = row['q_num']

        if pd.isnull(cum):
            ttm_vals.append(None)
            continue

        if md == '12-31':
            ttm_vals.append(cum)
        elif pd.isnull(q4_prev) or pd.isnull(prev_cum):
            ttm_vals.append(cum * 4.0 / q_num)
        else:
            ttm_vals.append(cum + q4_prev - prev_cum)

    df['ttm_net_profit'] = ttm_vals
    
    f10_clean = df[['code', 'name', 'report_date', 'notice_date', 'bps', 'ttm_net_profit']].copy()
    os.makedirs("temp_parts", exist_ok=True)
    clean_f10_path = "temp_parts/f10_ttm_clean.parquet"
    f10_clean.to_parquet(clean_f10_path, index=False)
    print(f"✅ F10 TTM 预处理完毕。")
    return f10_clean

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--mode", type=str, default="hf", choices=["hf", "release", "local"])
    parser.add_argument("--year", type=int, default=0)
    args = parser.parse_args()
    
    qc = QualityControl()
    
    print("🦆 Initializing DuckDB Engine...")
    con = duckdb.connect()
    con.execute("SET memory_limit='4GB'")
    con.execute("SET temp_directory='duckdb_temp.tmp'")
    
    k_files = glob.glob("all_artifacts/kline_part_*.parquet")
    f_files = glob.glob("all_artifacts/flow_part_*.parquet")
    sec_k_files = glob.glob("all_artifacts/sector_kline_full.parquet")
    
    # 🎯 独立读取高精度指数 Parquet 数据源
    idx_source = glob.glob("all_artifacts/index_kline_all.parquet")

    if k_files:
        con.execute(f"CREATE OR REPLACE VIEW v_kline_raw AS SELECT * FROM read_parquet({k_files}, union_by_name=True)")
    else:
        con.execute("CREATE OR REPLACE VIEW v_kline_raw AS SELECT * FROM (SELECT '' as date, '' as code) WHERE 1=0")

    if idx_source:
        con.execute(f"CREATE OR REPLACE VIEW v_index_raw AS SELECT * FROM read_parquet('{idx_source[0]}')")
    else:
        con.execute("CREATE OR REPLACE VIEW v_index_raw AS SELECT * FROM (SELECT '' as date, '' as code) WHERE 1=0")

    if f_files:
        con.execute(f"CREATE OR REPLACE VIEW v_flow AS SELECT * FROM read_parquet({f_files}, union_by_name=True)")
    else:
        con.execute("CREATE OR REPLACE VIEW v_flow AS SELECT * FROM (SELECT '' as date, '' as code) WHERE 1=0")

    if sec_k_files:
        con.execute(f"CREATE OR REPLACE VIEW v_sec_k AS SELECT * FROM read_parquet('{sec_k_files[0]}')")
    else:
        con.execute("CREATE OR REPLACE VIEW v_sec_k AS SELECT * FROM (SELECT '' as date, '' as code) WHERE 1=0")

    # 3. 载入 F10 财务指标计算 valuation
    f10_raw_path = "output/all_stocks_f10_raw.parquet"
    f10_ttm_df = calculate_ttm_net_profit(f10_raw_path)
    
    if not f10_ttm_df.empty:
        con.execute("CREATE OR REPLACE VIEW v_f10 AS SELECT * FROM read_parquet('temp_parts/f10_ttm_clean.parquet')")
        
        print("⚡ Performing Look-ahead-bias-free ASOF JOIN via DuckDB...")
        con.execute("""
            CREATE OR REPLACE VIEW v_kline AS
            SELECT 
                k.date,
                k.code,
                k.open,
                k.high,
                k.low,
                k.close,
                CASE 
                    WHEN k.amount IS NULL OR k.amount <= 0 THEN CAST(k.close * k.volume * 100 AS DOUBLE)
                    ELSE k.amount
                END as amount,
                k.volume,
                k.turn,
                k.pctChg,
                CASE 
                    WHEN f.ttm_net_profit IS NULL OR f.ttm_net_profit <= 0 OR k.total_mv IS NULL OR k.total_mv <= 0 THEN 0.0
                    ELSE CAST(k.total_mv / f.ttm_net_profit AS FLOAT)
                END as peTTM,
                CASE 
                    WHEN f.bps IS NULL OR f.bps <= 0 THEN 0.0
                    ELSE CAST(k.close / f.bps AS FLOAT)
                END as pbMRQ,
                k.adjustFactor,
                k.isST,
                k.total_shares,
                k.float_shares,
                k.total_mv,
                k.float_mv
            FROM v_kline_raw k
            ASOF LEFT JOIN v_f10 f
                ON SUBSTR(k.code, 4) = f.code
               AND k.date >= f.notice_date;
        """)
    else:
        print("⚠️ Warning: F10 TTM View could not be created. PE/PB remains 0.0.")
        con.execute("CREATE OR REPLACE VIEW v_kline AS SELECT * FROM v_kline_raw")

    os.makedirs("output", exist_ok=True)
    targets = {}

    df_stocks = get_stock_list_with_names()
    if not df_stocks.empty:
        p = "output/stock_list.parquet"
        df_stocks.to_parquet(p, index=False)
        targets[p] = "stock_list.parquet"
        qc.check_dataframe(df_stocks, "stock_list.parquet", ["code_name"], file_path=p)

    if sec_k_files:
        p = 'output/sector_list.parquet'
        con.execute(f"COPY (SELECT DISTINCT code, name, type FROM v_sec_k ORDER BY type, code) TO '{p}' (FORMAT 'PARQUET')")
        targets[p] = "sector_list.parquet"
        qc.check_dataframe(pd.read_parquet(p), "sector_list.parquet", ["name"], file_path=p)

    # 导出 55 只量化矩阵指数名称元数据表 index_list.parquet
    index_meta = [
        {"code": "sh.000001", "name": "上证指数"}, {"code": "sz.399001", "name": "深证成指"},
        {"code": "sz.399006", "name": "创业板指"}, {"code": "sh.000688", "name": "科创50"},
        {"code": "bj.899050", "name": "北证50"},   {"code": "sh.000016", "name": "上证50"},
        {"code": "sh.000300", "name": "沪深300"},  {"code": "sh.000905", "name": "中证500"},
        {"code": "sh.000852", "name": "中证1000"}, {"code": "sh.000851", "name": "中证2000"},
        {"code": "sz.399303", "name": "国证2000"}, {"code": "sh.000985", "name": "中证全指"},
        {"code": "sz.399330", "name": "深证100"},  {"code": "sh.000090", "name": "上证180"},
        {"code": "sh.000922", "name": "中证红利"}, {"code": "sz.399324", "name": "深证红利"},
        {"code": "sh.000015", "name": "红利指数"}, {"code": "sh.000918", "name": "沪深300成长"},
        {"code": "sh.000919", "name": "沪深300价值"}, {"code": "sh.000807", "name": "中证超大盘"},
        {"code": "sh.000827", "name": "中证中盘"}, {"code": "sh.000925", "name": "中证基本面50"},
        {"code": "sh.000978", "name": "中证大盘价值"}, {"code": "sz.399317", "name": "国证1000"},
        {"code": "sz.399807", "name": "中证人工智能"}, {"code": "sz.399977", "name": "中证机器人"},
        {"code": "sh.932252", "name": "中证低空经济主题"}, {"code": "sz.399812", "name": "国证芯片"},
        {"code": "sh.000973", "name": "中证半导体"}, {"code": "sh.931160", "name": "中证数据要素"},
        {"code": "sz.399354", "name": "国证人工智能"}, {"code": "sz.399673", "name": "创业板50"},
        {"code": "sz.399979", "name": "中证空天安全"}, {"code": "sz.399285", "name": "国证新能源"},
        {"code": "sh.931151", "name": "中证光伏产业"}, {"code": "sz.399008", "name": "中小100"},
        {"code": "sh.931494", "name": "中证消费电子主题"}, {"code": "sh.931409", "name": "中证电网设备"},
        {"code": "sh.931152", "name": "中证创新药产业"}, {"code": "sz.399993", "name": "中证信息安全"},
        {"code": "sz.399975", "name": "证券公司"}, {"code": "sz.399986", "name": "中证银行"},
        {"code": "sz.399932", "name": "中证消费"}, {"code": "sz.399933", "name": "中证医药"},
        {"code": "sz.399967", "name": "中证军工"}, {"code": "sz.399989", "name": "中证医疗"},
        {"code": "sz.399971", "name": "中证传媒"}, {"code": "sz.399997", "name": "中证白酒"},
        {"code": "sh.000934", "name": "中证能源"}, {"code": "sh.000935", "name": "中证原材料"},
        {"code": "sz.399990", "name": "煤炭等权"}, {"code": "sz.399998", "name": "中证有色"},
        {"code": "sz.399974", "name": "国证国企"}, {"code": "sh.000157", "name": "中证央企"},
        {"code": "sh.930606", "name": "中证黄金产业"}
    ]
    df_idx_meta = pd.DataFrame(index_meta)
    idx_p = "output/index_list.parquet"
    df_idx_meta.to_parquet(idx_p, index=False)
    targets[idx_p] = "index_list.parquet"
    qc.check_dataframe(df_idx_meta, "index_list.parquet", ["name"], file_path=idx_p)

    if args.year == 9999:
        years = range(2005, datetime.datetime.now().year + 1)
    elif args.year > 0:
        years = [args.year]
    else:
        years = [datetime.datetime.now().year]

    for y in years:
        print(f"🔪 Merging & Splitting Data for Year {y}...")
        start_date, end_date = f"{y}-01-01", f"{y}-12-31"
        
        tasks = [
            ("v_kline", f"stock_kline_{y}.parquet", ["close", "volume", "peTTM", "pbMRQ", "total_mv", "turn"]),
            ("v_flow", f"stock_money_flow_{y}.parquet", ["net_amount"]),
            ("v_sec_k", f"sector_kline_{y}.parquet", ["close"]),
            # 🎯 高精度、零错位、日线完整的指数 parquet 切片物理生成任务
            ("v_index_raw", f"index_kline_{y}.parquet", ["close", "volume"])
        ]
        
        for view_name, out_name, check_cols in tasks:
            out_path = f"output/{out_name}"
            try:
                count = con.execute(f"SELECT count(*) FROM {view_name} WHERE date >= '{start_date}' AND date <= '{end_date}'").fetchone()[0]
                if count == 0:
                    continue

                con.execute(f"""
                    COPY (
                        SELECT * FROM {view_name} 
                        WHERE date >= '{start_date}' AND date <= '{end_date}' 
                        ORDER BY code, date
                    ) TO '{out_path}' (FORMAT 'PARQUET', COMPRESSION 'ZSTD')
                """)
                
                if os.path.exists(out_path):
                    df_check = pd.read_parquet(out_path)
                    qc.check_dataframe(df_check, out_name, check_cols, file_path=out_path)
                    targets[out_path] = out_name
            except Exception as e:
                print(f"❌ Error merging {out_name}: {e}")

        # 板块成分股关系复制
        sec_c_files = glob.glob("all_artifacts/sector_constituents_latest.parquet")
        if sec_c_files:
            c_out = f"output/sector_constituents_{y}.parquet"
            try:
                shutil.copy(sec_c_files[0], c_out)
                targets[c_out] = f"sector_constituents_{y}.parquet"
            except:
                pass

    print("📝 Finalizing QC Reports...")
    qc.save_report("output/qc_report.json")
    with open("output/qc_summary.md", "w", encoding="utf-8") as f:
        f.write(qc.get_summary_md())

    # 汇总复权异常等监控指标
    factor_stat_files = glob.glob("all_artifacts/factor_stats_*.json")
    all_retried_codes = set()
    for f_path in factor_stat_files:
        try:
            with open(f_path, 'r', encoding="utf-8") as f:
                all_retried_codes.update(json.load(f))
        except:
            pass

    with open("output/qc_summary.md", "a", encoding="utf-8") as f:
        f.write("\n## 🛡️ 数据源监控与自愈报告\n")
        f.write(f"- **复权因子异常拦截：** 今日拦截并强制重试了 **{len(all_retried_codes)}** 只存在复权因子错乱的股票。\n")

    if args.mode == "hf" and os.getenv("HF_TOKEN"):
        print("🚀 Uploading Consolidations to Hugging Face...")
        hf = HFManager(os.getenv("HF_TOKEN"), os.getenv("HF_REPO"))
        for local, remote in targets.items():
            hf.upload_file(local, remote)
    else:
        print(f"\n{'='*50}")
        print(f"✅ Data Preparation Success!")
        print(f"📂 Location: {os.path.abspath('output/')}")
        print(f"📊 Total Files: {len(targets)}")
        print(f"{'='*50}\n")

if __name__ == "__main__":
    main()
