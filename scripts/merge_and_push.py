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
import baostock as bs
from utils.hf_manager import HFManager
from utils.qc import QualityControl

def get_stock_list_with_names():
    print("📋 Fetching stock list metadata from Baostock...")
    try:
        bs.login()
        data = []
        for i in range(10):
            d = (datetime.datetime.now() - datetime.timedelta(days=i)).strftime("%Y-%m-%d")
            rs = bs.query_all_stock(day=d)
            if rs.error_code == '0' and len(rs.data) > 0:
                while rs.next():
                    data.append(rs.get_row_data())
                break
        bs.logout()
        
        if data:
            df = pd.DataFrame(data, columns=["code", "tradeStatus", "code_name"])
            df = df[df['code_name'].notna() & (df['code_name'].str.strip() != "")]
            df = df[df['code'].str.startswith(('sh.', 'sz.', 'bj.'))]
            return df
    except Exception as e:
        print(f"⚠️ Failed to fetch stock list: {e}")
    return pd.DataFrame()

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

    if k_files:
        con.execute(f"CREATE OR REPLACE VIEW v_kline AS SELECT * FROM read_parquet({k_files}, union_by_name=True)")
    else:
        con.execute("CREATE OR REPLACE VIEW v_kline AS SELECT * FROM (SELECT '' as date, '' as code) WHERE 1=0")

    if f_files:
        con.execute(f"CREATE OR REPLACE VIEW v_flow AS SELECT * FROM read_parquet({f_files}, union_by_name=True)")
    else:
        con.execute("CREATE OR REPLACE VIEW v_flow AS SELECT * FROM (SELECT '' as date, '' as code) WHERE 1=0")

    if sec_k_files:
        con.execute(f"CREATE OR REPLACE VIEW v_sec_k AS SELECT * FROM read_parquet('{sec_k_files[0]}')")
    else:
        con.execute("CREATE OR REPLACE VIEW v_sec_k AS SELECT * FROM (SELECT '' as date, '' as code) WHERE 1=0")

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
            ("v_kline", f"stock_kline_{y}.parquet", ["close", "volume"]),
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
    # 💥 新增：自愈系统跨节点数据统计与报告注入
    # =========================================================
    factor_stat_files = glob.glob("all_artifacts/factor_stats_*.json")
    all_retried_codes = set()
    for f_path in factor_stat_files:
        try:
            with open(f_path, 'r', encoding="utf-8") as f:
                all_retried_codes.update(json.load(f))
        except:
            pass

    # 1. 追加到 Markdown 供 GitHub Actions UI 展示
    with open("output/qc_summary.md", "a", encoding="utf-8") as f:
        f.write("\n## 🛡️ 数据源监控与自愈报告\n")
        f.write(f"- **复权因子异常拦截：** 今日拦截并强制重试了 **{len(all_retried_codes)}** 只存在复权因子错乱(非正数/断层)的股票。\n")
        if all_retried_codes:
            sample = ", ".join(list(all_retried_codes)[:15])
            f.write(f"- **受影响股票示例：** {sample}{'...' if len(all_retried_codes)>15 else ''}\n")

    # 2. 注入到 JSON 提供给未来的 API 调用读取
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
    # =========================================================
    
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
