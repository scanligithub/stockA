# FILE: scripts/merge_events.py
import polars as pl
import glob
import os

def main():
    print("🧩 正在合并分布式抓取的业绩预告事件分片...")
    part_files = glob.glob("all_artifacts/event_forecast_part_*.parquet")
    
    if not part_files:
        print("⚠️ 未找到任何事件分片，任务结束。")
        return
        
    dfs = []
    for f in part_files:
        try:
            dfs.append(pl.read_parquet(f))
        except:
            pass
            
    if not dfs: return
    
    # 纵向拼接并全局去重、排序
    final_df = pl.concat(dfs).unique(subset=["code", "notice_date"], keep="last").sort(["code", "notice_date"])
    
    os.makedirs("output", exist_ok=True)
    out_path = "output/event_earnings_forecast.parquet"
    final_df.write_parquet(out_path, compression="zstd")
    
    print(f"✅ 合并成功！总数据量: {len(final_df):,} 条。已落盘至: {out_path}")

if __name__ == "__main__":
    main()
