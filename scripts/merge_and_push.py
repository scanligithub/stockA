import sys
import os

# === 关键修复：将项目根目录加入 python path ===
# 获取当前脚本所在目录 (scripts) 的上一级目录 (项目根目录)
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import pandas as pd
import glob
import datetime
import argparse
from utils.cleaner import DataCleaner
from utils.qc import QualityControl
from utils.hf_manager import HFManager

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--mode", type=str, default="hf", choices=["hf", "release"])
    parser.add_argument("--year", type=int, default=0)
    args = parser.parse_args()
    
    year = args.year if args.year > 0 else datetime.datetime.now().year
    
    # 1. 合并个股分片
    print("📦 Merging parts...")
    k_files = glob.glob("all_artifacts/kline_part_*.parquet")
    f_files = glob.glob("all_artifacts/flow_part_*.parquet")
    
    df_k = pd.concat([pd.read_parquet(f) for f in k_files]) if k_files else pd.DataFrame()
    df_f = pd.concat([pd.read_parquet(f) for f in f_files]) if f_files else pd.DataFrame()
    
    # 2. 读取板块数据
    # 板块数据由 fetch_sector 生成，放在 artifacts 里
    sec_k_file = glob.glob("all_artifacts/sector_kline_full.parquet")
    sec_c_file = glob.glob("all_artifacts/sector_constituents_latest.parquet")
    
    # 注意：fetch_sector 可能没生成文件（如果没配代理），所以要判空
    df_sec_k = pd.read_parquet(sec_k_file[0]) if sec_k_file else pd.DataFrame()
    df_sec_c = pd.read_parquet(sec_c_file[0]) if sec_c_file else pd.DataFrame()
    
    # 3. 按年份过滤 (对于 Sector，下载的是全量，需要切分)
    start_date = f"{year}-01-01"
    end_date = f"{year}-12-31"
    
    # 个股数据已经是按年份下载的，不需要再 filter
    # 仅对 Sector 数据进行年份过滤
    if not df_sec_k.empty:
        df_sec_k = df_sec_k[(df_sec_k['date'] >= start_date) & (df_sec_k['date'] <= end_date)]

    # 4. 清洗
    print("🧹 Cleaning data...")
    cleaner = DataCleaner()
    df_k = cleaner.clean_stock_kline(df_k)
    df_f = cleaner.clean_money_flow(df_f)
    # Sector数据在 fetch 阶段已清洗
    
    # 5. 质检
    print("🔍 Quality check...")
    qc = QualityControl()
    qc.check_dataframe(df_k, "stock_kline", ["close", "volume"])
    qc.check_dataframe(df_f, "money_flow", ["net_amount"])
    qc.save_report("qc_report.json")
    with open("qc_summary.md", "w") as f: f.write(qc.get_summary_md())

    # 6. 保存最终文件
    os.makedirs("output", exist_ok=True)
    targets = {}
    
    if not df_k.empty:
        p = f"output/stock_kline_{year}.parquet"
        df_k.to_parquet(p, index=False)
        targets[p] = f"stock_kline_{year}.parquet"
        print(f"✅ Generated: {p} ({len(df_k)} rows)")
        
    if not df_f.empty:
        p = f"output/stock_money_flow_{year}.parquet"
        df_f.to_parquet(p, index=False)
        targets[p] = f"stock_money_flow_{year}.parquet"
        print(f"✅ Generated: {p} ({len(df_f)} rows)")
        
    if not df_sec_k.empty:
        p = f"output/sector_kline_{year}.parquet"
        df_sec_k.to_parquet(p, index=False)
        targets[p] = f"sector_kline_{year}.parquet"
        print(f"✅ Generated: {p} ({len(df_sec_k)} rows)")
        
    if not df_sec_c.empty:
        p = f"output/sector_constituents_{year}.parquet"
        df_sec_c.to_parquet(p, index=False)
        targets[p] = f"sector_constituents_{year}.parquet"
        print(f"✅ Generated: {p} ({len(df_sec_c)} rows)")

    # 7. 上传 HF
    if args.mode == "hf":
        if os.getenv("HF_TOKEN"):
            print("🚀 Uploading to HuggingFace...")
            hf = HFManager(os.getenv("HF_TOKEN"), os.getenv("HF_REPO"))
            for local, remote in targets.items():
                hf.upload_file(local, remote)
        else:
            print("⚠️ HF_TOKEN not found, skipping upload.")

if __name__ == "__main__":
    main()
