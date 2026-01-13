import sys
import os
# 注入根目录路径，防止 ModuleNotFoundError
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import duckdb
import glob
import datetime
import argparse
import shutil
from utils.hf_manager import HFManager

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--mode", type=str, default="hf", choices=["hf", "release"])
    parser.add_argument("--year", type=int, default=0)
    args = parser.parse_args()
    
    # 1. 初始化 DuckDB
    print("🦆 Initializing DuckDB...")
    con = duckdb.connect()
    # 限制内存 5GB (Runner 通常有 7GB)
    con.execute("SET memory_limit='5GB'")
    # 允许临时文件溢出到磁盘，防止 OOM
    con.execute("SET temp_directory='duckdb_temp.tmp'")
    
    # 2. 注册视图 (View) - 零内存消耗加载
    print("📦 Registering views...")
    
    # K 线
    k_files = glob.glob("all_artifacts/kline_part_*.parquet")
    if k_files:
        # Python list 转 SQL list 字符串
        files_sql = str(k_files).replace('[', '[').replace(']', ']') # 兼容 list 格式
        con.execute(f"CREATE OR REPLACE VIEW v_kline AS SELECT * FROM read_parquet({k_files}, union_by_name=True)")
    else:
        print("⚠️ No K-Line files found!")
        con.execute("CREATE OR REPLACE VIEW v_kline AS SELECT * FROM read_parquet([], schema={'date': 'VARCHAR', 'code': 'VARCHAR'})")

    # 资金流
    f_files = glob.glob("all_artifacts/flow_part_*.parquet")
    if f_files:
        con.execute(f"CREATE OR REPLACE VIEW v_flow AS SELECT * FROM read_parquet({f_files}, union_by_name=True)")
    else:
        con.execute("CREATE OR REPLACE VIEW v_flow AS SELECT * FROM read_parquet([], schema={'date': 'VARCHAR', 'code': 'VARCHAR'})")

    # 板块 K 线
    sec_k_files = glob.glob("all_artifacts/sector_kline_full.parquet")
    if sec_k_files:
        con.execute(f"CREATE OR REPLACE VIEW v_sec_k AS SELECT * FROM read_parquet('{sec_k_files[0]}')")
    else:
        con.execute("CREATE OR REPLACE VIEW v_sec_k AS SELECT * FROM read_parquet([], schema={'date': 'VARCHAR', 'code': 'VARCHAR'})")

    # 3. 确定要处理的年份
    if args.year == 9999:
        # 全量模式：2005 到 去年
        current_year = datetime.datetime.now().year
        years = range(2005, current_year)
    elif args.year > 0:
        years = [args.year]
    else:
        years = [datetime.datetime.now().year]

    os.makedirs("output", exist_ok=True)
    targets = {}

    # 4. 循环切分 (DuckDB SQL)
    for y in years:
        print(f"🔪 Processing Year {y}...")
        start_date = f"{y}-01-01"
        end_date = f"{y}-12-31"
        
        # 定义输出任务
        tasks = [
            ("v_kline", f"stock_kline_{y}.parquet"),
            ("v_flow", f"stock_money_flow_{y}.parquet"),
            ("v_sec_k", f"sector_kline_{y}.parquet")
        ]
        
        for view_name, out_name in tasks:
            out_path = f"output/{out_name}"
            
            # 使用 COPY 命令进行流式写入 + ZSTD 压缩
            query = f"""
            COPY (
                SELECT * FROM {view_name}
                WHERE date >= '{start_date}' AND date <= '{end_date}'
                ORDER BY code, date
            ) TO '{out_path}' (FORMAT 'PARQUET', COMPRESSION 'ZSTD');
            """
            
            try:
                con.execute(query)
                # 只有生成了文件且不为空才记录
                if os.path.exists(out_path):
                    targets[out_path] = out_name
            except Exception as e:
                print(f"❌ Error dumping {out_name}: {e}")

        # 板块成分股 (处理方式：复制最新快照)
        sec_c_files = glob.glob("all_artifacts/sector_constituents_latest.parquet")
        if sec_c_files:
            c_out = f"output/sector_constituents_{y}.parquet"
            try:
                shutil.copy(sec_c_files[0], c_out)
                targets[c_out] = f"sector_constituents_{y}.parquet"
            except: pass

    # 5. 上传 HF
    if args.mode == "hf":
        if os.getenv("HF_TOKEN"):
            print("🚀 Uploading to HuggingFace...")
            hf = HFManager(os.getenv("HF_TOKEN"), os.getenv("HF_REPO"))
            for local, remote in targets.items():
                hf.upload_file(local, remote)
        else:
            print("⚠️ HF_TOKEN not set, skipping upload.")

if __name__ == "__main__":
    main()
