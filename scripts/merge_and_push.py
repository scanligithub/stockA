import sys
import os
import duckdb
import glob
import datetime
import argparse
import shutil
import pandas as pd
import baostock as bs

# 确保能引用到项目根目录的 utils
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils.hf_manager import HFManager
from utils.qc import QualityControl

def get_stock_list_with_names():
    """获取带名称的股票基准名册"""
    print("📋 Fetching stock list metadata as baseline...")
    try:
        bs.login()
        # 尝试获取最近一个交易日的名册
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
            # 核心过滤：必须有代码名称，且属于 A 股范围
            df = df[df['code_name'].notna() & (df['code_name'].str.strip() != "")]
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
    
    qc = QualityControl()
    
    print("🦆 Initializing DuckDB...")
    con = duckdb.connect()
    con.execute("SET memory_limit='5GB'")
    
    # 1. 注册碎片视图
    k_files = glob.glob("all_artifacts/kline_part_*.parquet")
    f_files = glob.glob("all_artifacts/flow_part_*.parquet")
    sec_k_files = glob.glob("all_artifacts/sector_kline_full.parquet")
    
    # 即使没文件也创建空视图防止 SQL 崩溃
    if k_files: con.execute(f"CREATE OR REPLACE VIEW v_kline AS SELECT * FROM read_parquet({k_files}, union_by_name=True)")
    else: con.execute("CREATE OR REPLACE VIEW v_kline AS SELECT * FROM read_parquet([], schema={'date': 'VARCHAR', 'code': 'VARCHAR'})")
    
    if f_files: con.execute(f"CREATE OR REPLACE VIEW v_flow AS SELECT * FROM read_parquet({f_files}, union_by_name=True)")
    else: con.execute("CREATE OR REPLACE VIEW v_flow AS SELECT * FROM read_parquet([], schema={'date': 'VARCHAR', 'code': 'VARCHAR'})")
    
    if sec_k_files: con.execute(f"CREATE OR REPLACE VIEW v_sec_k AS SELECT * FROM read_parquet('{sec_k_files[0]}')")
    else: con.execute("CREATE OR REPLACE VIEW v_sec_k AS SELECT * FROM read_parquet([], schema={'date': 'VARCHAR', 'code': 'VARCHAR'})")

    os.makedirs("output", exist_ok=True)
    targets = {}

    # 2. 差异诊断：找出那 47 只股票
    df_baseline = get_stock_list_with_names()
    if not df_baseline.empty:
        # 获取实际抓取到的唯一代码
        actual_codes = con.execute("SELECT DISTINCT code FROM v_kline").df()['code'].tolist()
        actual_set = set(actual_codes)
        intended_set = set(df_baseline['code'].tolist())
        
        missing_codes = list(intended_set - actual_set)
        df_missing = df_baseline[df_baseline['code'].isin(missing_codes)]
        
        # 将诊断结果注入 QC 系统
        qc.set_missing_analysis(df_missing[['code', 'code_name']].to_dict(orient='records'))
        
        # 保存基准列表
        p = "output/stock_list.parquet"
        df_baseline.to_parquet(p, index=False)
        targets[p] = "stock_list.parquet"
        qc.check_dataframe(df_baseline, "stock_list.parquet", file_path=p)

    # 3. 数据切分与保存逻辑 (按年份)
    curr_year = datetime.datetime.now().year
    years = [args.year] if args.year > 0 and args.year != 9999 else [curr_year]
    if args.year == 9999:
        years = range(2005, curr_year + 1)

    for y in years:
        start_d, end_d = f"{y}-01-01", f"{y}-12-31"
        
        # 定义需要处理的任务
        task_list = [
            ("v_kline", f"stock_kline_{y}.parquet", ["close", "volume"]),
            ("v_flow", f"stock_money_flow_{y}.parquet", ["net_amount"]),
            ("v_sec_k", f"sector_kline_{y}.parquet", ["close"])
        ]
        
        for view, out_name, check_cols in task_list:
            out_path = f"output/{out_name}"
            # 检查该年份是否有数据
            cnt = con.execute(f"SELECT count(*) FROM {view} WHERE date >= '{start_d}' AND date <= '{end_d}'").fetchone()[0]
            if cnt == 0: continue
            
            con.execute(f"COPY (SELECT * FROM {view} WHERE date >= '{start_d}' AND date <= '{end_d}' ORDER BY code, date) TO '{out_path}' (FORMAT 'PARQUET', COMPRESSION 'ZSTD')")
            
            if os.path.exists(out_path):
                qc.check_dataframe(pd.read_parquet(out_path), out_name, check_cols=check_cols, file_path=out_path)
                targets[out_path] = out_name

    # 4. 报告生成与上传
    qc.save_report("output/qc_report.json")
    with open("output/qc_summary.md", "w", encoding="utf-8") as f:
        f.write(qc.get_summary_md())
    
    targets["output/qc_report.json"] = "qc_report.json"
    targets["output/qc_summary.md"] = "qc_summary.md"

    if args.mode == "hf" and os.getenv("HF_TOKEN"):
        hf = HFManager(os.getenv("HF_TOKEN"), os.getenv("HF_REPO"))
        for local, remote in targets.items():
            hf.upload_file(local, remote)

if __name__ == "__main__":
    main()
