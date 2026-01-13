import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import duckdb
import glob
import datetime
import argparse
import shutil
import pandas as pd # 用于质检
from utils.hf_manager import HFManager
from utils.qc import QualityControl

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
    print("📦 Registering views...")
    k_files = glob.glob("all_artifacts/kline_part_*.parquet")
    f_files = glob.glob("all_artifacts/flow_part_*.parquet")
    sec_k_files = glob.glob("all_artifacts/sector_kline_full.parquet")
    
    # 注册逻辑同前...
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

    # 4. 确定年份
    if args.year == 9999:
        current_year = datetime.datetime.now().year
        years = range(2005, current_year)
    elif args.year > 0:
        years = [args.year]
    else:
        years = [datetime.datetime.now().year]

    os.makedirs("output", exist_ok=True)
    targets = {}

    # 5. 循环切分 + 逐个质检
    for y in years:
        print(f"🔪 Processing Year {y}...")
        start_date = f"{y}-01-01"
        end_date = f"{y}-12-31"
        
        # 定义任务
        tasks = [
            ("v_kline", f"stock_kline_{y}.parquet", ["close", "volume"]),
            ("v_flow", f"stock_money_flow_{y}.parquet", ["net_amount"]),
            ("v_sec_k", f"sector_kline_{y}.parquet", ["close"])
        ]
        
        for view_name, out_name, check_cols in tasks:
            out_path = f"output/{out_name}"
            
            # DuckDB 切分写入
            query = f"""
            COPY (
                SELECT * FROM {view_name}
                WHERE date >= '{start_date}' AND date <= '{end_date}'
                ORDER BY code, date
            ) TO '{out_path}' (FORMAT 'PARQUET', COMPRESSION 'ZSTD');
            """
            
            try:
                con.execute(query)
                
                # === 新增：单文件质检 ===
                if os.path.exists(out_path):
                    # 读回 Pandas 进行轻量级质检 (一年数据通常 <200MB，内存安全)
                    df_check = pd.read_parquet(out_path)
                    if not df_check.empty:
                        qc.check_dataframe(df_check, out_name, check_cols)
                        targets[out_path] = out_name
            except Exception as e:
                print(f"❌ Error processing {out_name}: {e}")

        # 板块成分股 (快照)
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
        
    # 将报告也加入上传列表
    targets["output/qc_report.json"] = "qc_report.json"
    targets["output/qc_summary.md"] = "qc_summary.md"

    # 7. 上传 HF
    if args.mode == "hf":
        if os.getenv("HF_TOKEN"):
            hf = HFManager(os.getenv("HF_TOKEN"), os.getenv("HF_REPO"))
            for local, remote in targets.items():
                hf.upload_file(local, remote)
        else:
            print("⚠️ HF_TOKEN not set, skipping upload.")

if __name__ == "__main__":
    main()
