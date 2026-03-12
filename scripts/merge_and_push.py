import sys
import os
import duckdb
import glob
import datetime
import argparse
import pandas as pd
import baostock as bs
import time

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from utils.hf_manager import HFManager
from utils.qc import QualityControl

def get_stock_list_with_names():
    try:
        bs.login()
        d = (datetime.datetime.now() - datetime.timedelta(days=1)).strftime("%Y-%m-%d")
        rs = bs.query_all_stock(day=d)
        data = []
        while rs.next(): data.append(rs.get_row_data())
        bs.logout()
        df = pd.DataFrame(data, columns=["code", "tradeStatus", "code_name"])
        df = df[df['code_name'].notna() & (df['code_name'].str.strip() != "")]
        return df[df['code'].str.startswith(('sh.', 'sz.', 'bj.'))]
    except: return pd.DataFrame()

def repair_missing_stocks(missing_df, start_date, end_date):
    """
    针对缺失标的，在单机环境下进行 5 次暴力重试补录
    """
    if missing_df.empty: return pd.DataFrame()
    
    print(f"🛠️ Starting Last-Mile Repair for {len(missing_df)} stocks...")
    repaired_data = []
    fields = "date,code,open,high,low,close,volume,amount,turn,pctChg,peTTM,pbMRQ,isST"
    
    bs.login()
    for _, row in missing_df.iterrows():
        code = row['code']
        success = False
        for attempt in range(1, 6): # 5次重试
            try:
                rs = bs.query_history_k_data_plus(code, fields, start_date=start_date, end_date=end_date, frequency="d", adjustflag="3")
                if rs.error_code == '0' and rs.next():
                    repaired_data.append(rs.get_row_data())
                    while rs.next(): repaired_data.append(rs.get_row_data())
                    print(f"✅ Repaired {code} on attempt {attempt}")
                    success = True
                    break
            except: pass
            time.sleep(0.5 * attempt)
        if not success: print(f"❌ Failed to repair {code} after 5 attempts.")
    bs.logout()
    
    if repaired_data:
        df = pd.DataFrame(repaired_data, columns=fields.split(","))
        df['adjustFactor'] = 1.0
        return df
    return pd.DataFrame()

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--mode", type=str, default="hf")
    parser.add_argument("--year", type=int, default=0)
    args = parser.parse_args()
    
    qc = QualityControl()
    con = duckdb.connect()
    os.makedirs("output", exist_ok=True)
    
    # 1. 基准准备与缺失分析
    df_baseline = get_stock_list_with_names()
    
    # 2. 首次扫描 Matrix 产物
    k_files = glob.glob("all_artifacts/kline_part_*.parquet")
    if k_files: con.execute(f"CREATE OR REPLACE VIEW v_initial AS SELECT DISTINCT code FROM read_parquet({k_files})")
    else: con.execute("CREATE OR REPLACE VIEW v_initial AS SELECT 'NONE' as code WHERE 1=0")
    
    actual_codes = set(con.execute("SELECT code FROM v_initial").df()['code'].tolist())
    intended_codes = set(df_baseline['code'].tolist())
    missing_codes = list(intended_codes - actual_codes)
    df_missing = df_baseline[df_baseline['code'].isin(missing_codes)]

    # 3. 补录逻辑 (The Last Mile)
    curr_year = datetime.datetime.now().year
    y_target = args.year if args.year > 0 and args.year != 9999 else curr_year
    
    df_repaired = repair_missing_stocks(df_missing, f"{y_target}-01-01", "2099-12-31")
    if not df_repaired.empty:
        repair_path = "all_artifacts/repaired_kline.parquet"
        df_repaired.to_parquet(repair_path, index=False)
        k_files.append(repair_path)
        print(f"💎 Repair complete. Added {df_repaired['code'].nunique()} stocks.")

    # 4. 重新构建最终视图
    if k_files: con.execute(f"CREATE OR REPLACE VIEW v_kline AS SELECT * FROM read_parquet({k_files}, union_by_name=True)")
    else: con.execute("CREATE OR REPLACE VIEW v_kline AS SELECT * FROM read_parquet([], schema={'date':'VARCHAR','code':'VARCHAR'})")
    
    sec_k_files = glob.glob("all_artifacts/sector_kline_full.parquet")
    if sec_k_files: con.execute(f"CREATE OR REPLACE VIEW v_sec_k AS SELECT * FROM read_parquet('{sec_k_files[0]}')")
    else: con.execute("CREATE OR REPLACE VIEW v_sec_k AS SELECT * FROM read_parquet([], schema={'date':'VARCHAR','code':'VARCHAR'})")

    # 5. 生成产物与统计 (修复统计行缺失)
    # A. stock_list
    p_sl = "output/stock_list.parquet"
    df_baseline.to_parquet(p_sl, index=False)
    qc.check_dataframe(df_baseline, "stock_list.parquet", file_path=p_sl)
    
    # B. sector_list
    if sec_k_files:
        p_secl = 'output/sector_list.parquet'
        con.execute(f"COPY (SELECT DISTINCT code, name, type FROM v_sec_k ORDER BY type, code) TO '{p_secl}' (FORMAT 'PARQUET')")
        qc.check_dataframe(pd.read_parquet(p_secl), "sector_list.parquet", file_path=p_secl)

    # C. 行情切分
    y_list = range(2005, curr_year+1) if args.year == 9999 else [y_target]
    for y in y_list:
        start_d, end_d = f"{y}-01-01", f"{y}-12-31"
        for view, prefix in [("v_kline", "stock_kline"), ("v_sec_k", "sector_kline")]:
            out_n = f"{prefix}_{y}.parquet"
            out_p = f"output/{out_n}"
            if con.execute(f"SELECT count(*) FROM {view} WHERE date >= '{start_d}' AND date <= '{end_d}'").fetchone()[0] > 0:
                con.execute(f"COPY (SELECT * FROM {view} WHERE date >= '{start_d}' AND date <= '{end_d}' ORDER BY code, date) TO '{out_p}' (FORMAT 'PARQUET', COMPRESSION 'ZSTD')")
                qc.check_dataframe(pd.read_parquet(out_p), out_n, file_path=out_p)

    # 6. 最终诊断更新
    final_codes = set(con.execute("SELECT DISTINCT code FROM v_kline").df()['code'].tolist())
    still_missing = df_baseline[~df_baseline['code'].isin(final_codes)]
    qc.set_missing_analysis(still_missing[['code', 'code_name']].to_dict(orient='records'))

    # 7. 上传
    qc.save_report("output/qc_report.json")
    with open("output/qc_summary.md", "w", encoding="utf-8") as f: f.write(qc.get_summary_md())
    
    if args.mode == "hf" and os.getenv("HF_TOKEN"):
        hf = HFManager(os.getenv("HF_TOKEN"), os.getenv("HF_REPO"))
        all_outs = glob.glob("output/*.parquet") + glob.glob("output/*.json") + glob.glob("output/*.md")
        for loc in all_outs: hf.upload_file(loc, os.path.basename(loc))

if __name__ == "__main__":
    main()
