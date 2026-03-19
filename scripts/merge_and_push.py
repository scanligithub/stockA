import sys
import os

# 确保能正确加载项目本地模块
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import duckdb
import glob
import datetime
import argparse
import shutil
import pandas as pd
import baostock as bs
from utils.hf_manager import HFManager
from utils.qc import QualityControl

def get_stock_list_with_names():
    """获取带名称的股票列表 (增加名称为空的过滤)"""
    print("📋 Fetching stock list metadata from Baostock...")
    try:
        bs.login()
        # 尝试回溯 10 天找到最近一个有数据的交易日
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
            # 过滤：code_name 不能为空且必须是 A 股代码格式
            df = df[df['code_name'].notna() & (df['code_name'].str.strip() != "")]
            df = df[df['code'].str.startswith(('sh.', 'sz.', 'bj.'))]
            return df
    except Exception as e:
        print(f"⚠️ Failed to fetch stock list: {e}")
    return pd.DataFrame()

def main():
    parser = argparse.ArgumentParser()
    # mode=hf: 内部调用 API 上传 (产生历史)
    # mode=release: 仅生成本地文件 (配合 Action 强推抹除历史)
    parser.add_argument("--mode", type=str, default="hf", choices=["hf", "release", "local"])
    parser.add_argument("--year", type=int, default=0)
    args = parser.parse_args()
    
    qc = QualityControl()
    
    print("🦆 Initializing DuckDB Engine...")
    con = duckdb.connect()
    # 限制内存使用，防止 GitHub Actions 环境崩溃
    con.execute("SET memory_limit='4GB'")
    con.execute("SET temp_directory='duckdb_temp.tmp'")
    
    # 搜索分片文件
    k_files = glob.glob("all_artifacts/kline_part_*.parquet")
    f_files = glob.glob("all_artifacts/flow_part_*.parquet")
    sec_k_files = glob.glob("all_artifacts/sector_kline_full.parquet")
    sec_c_files = glob.glob("all_artifacts/sector_constituents_latest.parquet")

    # 注册 DuckDB 视图
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

    # 1. 生成股票列表元数据
    df_stocks = get_stock_list_with_names()
    if not df_stocks.empty:
        p = "output/stock_list.parquet"
        df_stocks.to_parquet(p, index=False)
        targets[p] = "stock_list.parquet"
        qc.check_dataframe(df_stocks, "stock_list.parquet", ["code_name"], file_path=p)

    # 2. 生成板块列表元数据 (从板块 K 线中提取唯一列表)
    if sec_k_files:
        p = 'output/sector_list.parquet'
        con.execute(f"COPY (SELECT DISTINCT code, name, type FROM v_sec_k ORDER BY type, code) TO '{p}' (FORMAT 'PARQUET')")
        targets[p] = "sector_list.parquet"
        qc.check_dataframe(pd.read_parquet(p), "sector_list.parquet", ["name"], file_path=p)

    # 3. 确定处理年份并执行切分
    if args.year == 9999:
        years = range(2005, datetime.datetime.now().year + 1)
    elif args.year > 0:
        years = [args.year]
    else:
        years = [datetime.datetime.now().year]

    for y in years:
        print(f"🔪 Merging & Splitting Data for Year {y}...")
        start_date, end_date = f"{y}-01-01", f"{y}-12-31"
        
        # 配置各年份对应的输出任务
        tasks = [
            ("v_kline", f"stock_kline_{y}.parquet", ["close", "volume"]),
            ("v_flow", f"stock_money_flow_{y}.parquet", ["net_amount"]),
            ("v_sec_k", f"sector_kline_{y}.parquet", ["close"])
        ]
        
        for view_name, out_name, check_cols in tasks:
            out_path = f"output/{out_name}"
            try:
                # 预检查该年份是否有数据
                count = con.execute(f"SELECT count(*) FROM {view_name} WHERE date >= '{start_date}' AND date <= '{end_date}'").fetchone()[0]
                if count == 0:
                    continue

                # 使用 DuckDB 的极速 COPY 功能生成 ZSTD 压缩的 Parquet
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

        # 4. 处理板块成分股快照 (复制到对应的年份)
        if sec_c_files:
            c_out = f"output/sector_constituents_{y}.parquet"
            try:
                shutil.copy(sec_c_files[0], c_out)
                targets[c_out] = f"sector_constituents_{y}.parquet"
            except:
                pass

    # 5. 生成汇总及质检报告
    print("📝 Finalizing QC Reports...")
    qc.save_report("output/qc_report.json")
    with open("output/qc_summary.md", "w") as f:
        f.write(qc.get_summary_md())
    
    targets["output/qc_report.json"] = "qc_report.json"
    targets["output/qc_summary.md"] = "qc_summary.md"

    # 6. 处理上传/导出
    if args.mode == "hf" and os.getenv("HF_TOKEN"):
        # 旧模式：逐文件 API 上传 (不推荐，会导致 2.88G 膨胀)
        print("🚀 Using Legacy HF API Upload (commits per file)...")
        hf = HFManager(os.getenv("HF_TOKEN"), os.getenv("HF_REPO"))
        for local, remote in targets.items():
            hf.upload_file(local, remote)
    else:
        # 新模式 (release/local)：仅准备好 output/ 目录
        print(f"\n{'='*50}")
        print(f"✅ Data Preparation Success!")
        print(f"📂 Location: {os.path.abspath('output/')}")
        print(f"📊 Total Files: {len(targets)}")
        print(f"🚀 Action will now execute 'git push --force' to sync.")
        print(f"{'='*50}\n")

if __name__ == "__main__":
    main()
