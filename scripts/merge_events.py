# FILE: scripts/merge_events.py
import polars as pl
import glob
import os

def main():
    print("\n" + "="*70)
    print("🧩 [事件库合并端] 正在合并分布式抓取的业绩预告事件分片...")
    print("="*70)
    
    part_files = glob.glob("all_artifacts/event_forecast_part_*.parquet")
    print(f"📂 检索到分布式分片文件共计: {len(part_files)} 个")
    
    if not part_files:
        print("⚠️ 未找到任何有效的事件分片，合并终止。")
        return
        
    dfs = []
    for f in part_files:
        try:
            dfs.append(pl.read_parquet(f))
        except Exception as e:
            print(f"⚠️ 读取分片失败: {f}，原因: {e}")
            
    if not dfs:
        print("❌ 错误：分片解析结果为空集！")
        return
    
    # 纵向合并、全局去重并按个股代码与披露日排序
    final_df = pl.concat(dfs).unique(subset=["code", "notice_date"], keep="last").sort(["code", "notice_date"])
    
    os.makedirs("output", exist_ok=True)
    out_path = "output/event_earnings_forecast.parquet"
    final_df.write_parquet(out_path, compression="zstd")
    
    print("\n" + "="*70)
    print(f"🎉 [事件库构建成功] 物理合并已完成！")
    print(f"    - 累计有效历史公告行: {len(final_df):,} 行")
    print(f"    - 最终 Parquet 文件大小: {os.path.getsize(out_path)/(1024*1024):.2f} MB")
    print("="*70 + "\n")

if __name__ == "__main__":
    main()
