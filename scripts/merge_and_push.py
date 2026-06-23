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
                df['tradeStatus'] = '1' # 默认占位
                return df
        except Exception as e:
            print(f"⚠️ Failed to parse stock JSON: {e}")
    return pd.DataFrame()

def calculate_ttm_net_profit(f10_parquet_path):
    """
    🧮 滚动 TTM 归母净利润计算模块 (Pandas 离线无损计算)
    原理：
    - Q1: TTM = Q1_Cum + Q4_prev - Q1_prev
    - Q2: TTM = Q2_Cum + Q4_prev - Q2_prev
    - Q3: TTM = Q3_Cum + Q4_prev - Q3_prev
    - Q4: TTM = Q4_Cum
    - 内置首发上市无 comparative historical 记录时的“季节乘数安全自愈估算”，防 PE 坠入 NaN
    """
    print("🧮 Calculating Trailing Twelve Months (TTM) Net Profit...")
    if not os.path.exists(f10_parquet_path):
        print("⚠️ Warning: F10 raw parquet not found! PE/PB will fall back to 0.0.")
        return pd.DataFrame()

    df = pd.read_parquet(f10_parquet_path)
    if df.empty:
        return pd.DataFrame()

    # 1. 建立时间结构
    df = df.sort_values(['code', 'report_date'])
    df['month_day'] = df['report_date'].str[-5:]
    df['year'] = df['report_date'].str[:4].astype(int)

    # 2. 构造去年的 Q4 年报查找字典 (作为 Q4_prev)
    df_q4 = df[df['month_day'] == '12-31'][['code', 'year', 'parent_netprofit']].copy()
    df_q4['prev_year'] = df_q4['year']  # 映射到下一年的关联键
    df_q4.rename(columns={'parent_netprofit': 'q4_prev_profit'}, inplace=True)

    # 3. 构造去年的同比季度累计查找字典 (作为 Q1_prev / Q2_prev / Q3_prev)
    df_prev = df.copy()
    df_prev['prev_year'] = df_prev['year']
    df_prev.rename(columns={'parent_netprofit': 'prev_cum_profit'}, inplace=True)

    # 4. 广播拼合
    df['prev_year'] = df['year'] - 1
    df = pd.merge(df, df_q4[['code', 'prev_year', 'q4_prev_profit']], on=['code', 'prev_year'], how='left')
    df = pd.merge(df, df_prev[['code', 'prev_year', 'month_day', 'prev_cum_profit']], on=['code', 'prev_year', 'month_day'], how='left')

    # 5. 执行 TTM 滚动算法
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
            # 💡 新股/借壳重组首年自愈估算：累计净利润 * (4 / 季度数)
            ttm_vals.append(cum * 4.0 / q_num)
        else:
            # 🎯 经典滚动季度抵消算法
            ttm_vals.append(cum + q4_prev - prev_cum)

    df['ttm_net_profit'] = ttm_vals
    
    # 瘦身并输出，作为 DuckDB 关联的数据源
    f10_clean = df[['code', 'name', 'report_date', 'notice_date', 'bps', 'ttm_net_profit']].copy()
    os.makedirs("temp_parts", exist_ok=True)
    clean_f10_path = "temp_parts/f10_ttm_clean.parquet"
    f10_clean.to_parquet(clean_f10_path, index=False)
    print(f"✅ F10 TTM 预处理完毕。生成清洗库: {clean_f10_path}")
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
    sec_c_files = glob.glob("all_artifacts/sector_constituents_latest.parquet")

    # 1. 创建 K 线、资金流向、行业板块的视图
    if k_files:
        con.execute(f"CREATE OR REPLACE VIEW v_kline_raw AS SELECT * FROM read_parquet({k_files}, union_by_name=True)")
    else:
        con.execute("CREATE OR REPLACE VIEW v_kline_raw AS SELECT * FROM (SELECT '' as date, '' as code) WHERE 1=0")

    if f_files:
        con.execute(f"CREATE OR REPLACE VIEW v_flow AS SELECT * FROM read_parquet({f_files}, union_by_name=True)")
    else:
        con.execute("CREATE OR REPLACE VIEW v_flow AS SELECT * FROM (SELECT '' as date, '' as code) WHERE 1=0")

    if sec_k_files:
        con.execute(f"CREATE OR REPLACE VIEW v_sec_k AS SELECT * FROM read_parquet('{sec_k_files[0]}')")
    else:
        con.execute("CREATE OR REPLACE VIEW v_sec_k AS SELECT * FROM (SELECT '' as date, '' as code) WHERE 1=0")

    # 2. 核心：载入东财 F10 并执行 ASOF JOIN 计算滚动估值指标
    f10_raw_path = "output/all_stocks_f10_raw.parquet"
    f10_ttm_df = calculate_ttm_net_profit(f10_raw_path)
    
    if not f10_ttm_df.empty:
        con.execute("CREATE OR REPLACE VIEW v_f10 AS SELECT * FROM read_parquet('temp_parts/f10_ttm_clean.parquet')")
        
        # 🚀 极速时序非等值关联：使用 SUBSTR(k.code, 4) 剔除前缀进行 ASOF JOIN
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
                -- 🛡️ 兜底异常 amount：若缺失，可用 价格*成交量(手)*100 粗略估算，避免 NULL 值出现
                CASE 
                    WHEN k.amount IS NULL OR k.amount <= 0 THEN CAST(k.close * k.volume * 100 AS DOUBLE)
                    ELSE k.amount
                END as amount,
                k.volume,
                k.turn,
                k.pctChg,
                -- 滚动市盈率 = 总市值 / TTM归母净利润
                CASE 
                    WHEN f.ttm_net_profit IS NULL OR f.ttm_net_profit <= 0 OR k.total_mv IS NULL OR k.total_mv <= 0 THEN 0.0
                    ELSE CAST(k.total_mv / f.ttm_net_profit AS FLOAT)
                END as peTTM,
                -- 滚动市净率 = 收盘价 / 每股净资产
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
                ON SUBSTR(k.code, 4) = f.code  -- 🎯 修复：剥离 "sh." 等前缀，使其与 "603730" 精准匹配
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
            ("v_sec_k", f"sector_kline_{y}.parquet", ["close"])
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

    # =========================================================
    # 🛡️ 数据源自愈监控报告汇总
    # =========================================================
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
        f.write(f"- **复权因子异常拦截：** 今日拦截并强制重试了 **{len(all_retried_codes)}** 只存在复权因子错乱(非正数/断层)的股票。\n")
        if all_retried_codes:
            sample = ", ".join(list(all_retried_codes)[:15])
            f.write(f"- **受影响股票示例：** {sample}{'...' if len(all_retried_codes)>15 else ''}\n")

    try:
        with open("output/qc_report.json", "r", encoding="utf-8") as f:
            report_data = json.load(f)
        report_data["Data_Healing_Stats"] = {
            "factor_retried_count": len(all_retried_codes),
            "retried_codes": list(all_retried_codes)
        }
        with open("output/qc_report.json", "w", encoding="utf-8") as f:
            json.dump(report_data, f, ensure_ascii=False, indent=2)
    except:
        pass
    
    targets["output/qc_report.json"] = "qc_report.json"
    targets["output/qc_summary.md"] = "qc_summary.md"

    if args.mode == "hf" and os.getenv("HF_TOKEN"):
        print("🚀 Using Legacy HF API Upload (commits per file)...")
        hf = HFManager(os.getenv("HF_TOKEN"), os.getenv("HF_REPO"))
        for local, remote in targets.items():
            hf.upload_file(local, remote)
    else:
        print(f"\n{'='*50}")
        print(f"✅ Data Preparation Success!")
        print(f"📂 Location: {os.path.abspath('output/')}")
        print(f"📊 Total Files: {len(targets)}")
        print(f"🚀 Action will now execute 'git push --force' to sync.")
        print(f"{'='*50}\n")

if __name__ == "__main__":
    main()
