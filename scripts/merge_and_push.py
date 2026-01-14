import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import duckdb
import glob
import datetime
import argparse
import shutil
import pandas as pd
import baostock as bs # 用于获取股票名称列表
from utils.hf_manager import HFManager
from utils.qc import QualityControl

def get_stock_list_with_names():
    """获取带名称的股票列表"""
    print("📋 Fetching stock list metadata...")
    try:
        bs.login()
        # 获取最近交易日的股票列表
        date = datetime.datetime.now().strftime("%Y-%m-%d")
        # 简单回溯查找有数据的一天
        for i in range(10):
            d = (datetime.datetime.now() - datetime.timedelta(days=i)).strftime("%Y-%m-%d")
            rs = bs.query_all_stock(day=d)
            if rs.error_code == '0' and len(rs.data) > 0:
                break
        
        data = []
        while rs.next():
            data.append(rs.get_row_data())
        bs.logout()
        
        if data:
            df = pd.DataFrame(data, columns=["code", "tradeStatus", "code_name"])
            # 过滤只留个股
            df = df[df['code'].str.startswith(('sh.', 'sz.', 'bj.'))]
            return df
    except Exception as e:
        print(f"⚠️ Failed to fetch stock list: {e}")
    
    return pd.DataFrame()

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--mode", type=str, default="hf", choices=["hf", "release"])
    parser.add_argument("--year", type=int, default=0)
    args = parser.parse_args()
    
    # 1. 初始化 QC
    qc = QualityControl()
    
    # 2. 初始化 DuckDB
    print("🦆 Initializing DuckDB...")
    con = duckdb.connect()
    con.execute("SET memory_limit='5GB'")
    con.execute("SET temp_directory='duckdb_temp.tmp'")
    
    # 3. 注册视图
    k_files = glob.glob("all_artifacts/kline_part_*.parquet")
    f_files = glob.glob("all_artifacts/flow_part_*.parquet")
    sec_k_files = glob.glob("all_artifacts/sector_kline_full.parquet")
    
    if k_files:
        con.execute(f"CREATE OR REPLACE VIEW v_kline AS SELECT * FROM read_parquet({k_files}, union_by_name=True)")
    else:
        con.execute("CREATE OR REPLACE VIEW v_kline AS SELECT * FROM read_parquet([], schema={'date': 'VARCHAR', 'code': 'VARCHAR'})")

    if f_files:
        con.execute(f"CREATE OR REPLACE VIEW v_flow AS SELECT * FROM read_parquet({f_files}, union_by_name=True)")
    else:
        con.execute("CREATE OR REPLACE VIEW v_flow AS SELECT * FROM read_parquet([], schema={'date': 'VARCHAR', 'code': 'VARCHAR'})")

    if sec_k_files:
        con.execute(f"CREATE OR REPLACE VIEW v_sec_k AS SELECT * FROM read_parquet('{sec_k_files[0]}')")
    else:
        con.execute("CREATE OR REPLACE VIEW v_sec_k AS SELECT * FROM read_parquet([], schema={'date': 'VARCHAR', 'code': 'VARCHAR'})")

    os.makedirs("output", exist_ok=True)
    targets = {}

    # === 新增：生成元数据列表文件 ===
    
    # A. 股票列表 (stock_list.parquet)
    df_stocks = get_stock_list_with_names()
    if not df_stocks.empty:
        p = "output/stock_list.parquet"
        df_stocks.to_parquet(p, index=False)
        targets[p] = "stock_list.parquet"
        print("✅ Generated: stock_list.parquet")

    # B. 板块列表 (sector_list.parquet)
    if sec_k_files:
        # 从板块K线中提取去重后的板块信息
        con.execute("COPY (SELECT DISTINCT code, name, type FROM v_sec_k ORDER BY type, code) TO 'output/sector_list.parquet' (FORMAT 'PARQUET')")
        targets['output/sector_list.parquet'] = "sector_list.parquet"
        print("✅ Generated: sector_list.parquet")

    # 4. 确定年份进行切分
    if args.year == 9999:
        current_year = datetime.datetime.now().year
        years = range(2005, current_year)
    elif args.year > 0:
        years = [args.year]
    else:
        years = [datetime.datetime.now().year]

    # 5. 循环切分 + 逐个质检
    for y in years:
        print(f"🔪 Processing Year {y}...")
        start_date = f"{y}-01-01"
        end_date = f"{y}-12-31"
        
        tasks = [
            ("v_kline", f"stock_kline_{y}.parquet", ["close", "volume"]),
            ("v_flow", f"stock_money_flow_{y}.parquet", ["net_amount"]),
            ("v_sec_k", f"sector_kline_{y}.parquet", ["close"])
        ]
        
        for view_name, out_name, check_cols in tasks:
            out_path = f"output/{out_name}"
            
            # 只有当视图有数据时才执行 COPY
            # (简单的判空逻辑：count(*))
            try:
                count = con.execute(f"SELECT count(*) FROM {view_name} WHERE date >= '{start_date}' AND date <= '{end_date}'").fetchone()[0]
                if count == 0:
                    continue
            except: continue

            query = f"""
            COPY (
                SELECT * FROM {view_name}
                WHERE date >= '{start_date}' AND date <= '{end_date}'
                ORDER BY code, date
            ) TO '{out_path}' (FORMAT 'PARQUET', COMPRESSION 'ZSTD');
            """
            
            try:
                con.execute(query)
                if os.path.exists(out_path):
                    df_check = pd.read_parquet(out_path)
                    if not df_check.empty:
                        qc.check_dataframe(df_check, out_name, check_cols)
                        targets[out_path] = out_name
            except Exception as e:
                print(f"❌ Error processing {out_name}: {e}")

        # C. 板块成分表 (sector_constituents_YYYY.parquet)
        sec_c_files = glob.glob("all_artifacts/sector_constituents_latest.parquet")
        if sec_c_files:
            c_out = f"output/sector_constituents_{y}.parquet"
            try:
                shutil.copy(sec_c_files[0], c_out)
                targets[c_out] = f"sector_constituents_{y}.parquet"
            except: pass

    # 6. 保存汇总报告
    print("📝 Generating QC Report...")
    qc.save_report("output/qc_report.json")
    with open("output/qc_summary.md", "w") as f:
        f.write(qc.get_summary_md())
        
    targets["output/qc_report.json"] = "qc_report.json"
    targets["output/qc_summary.md"] = "qc_summary.md"

    # 7. 上传 HF
    if args.mode == "hf":
        if os.getenv("HF_TOKEN"):
            print(f"🚀 Uploading {len(targets)} files to HuggingFace...")
            hf = HFManager(os.getenv("HF_TOKEN"), os.getenv("HF_REPO"))
            for local, remote in targets.items():
                hf.upload_file(local, remote)
        else:
            print("⚠️ HF_TOKEN not set, skipping upload.")

if __name__ == "__main__":
    main()
