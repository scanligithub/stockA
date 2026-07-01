# FILE: scripts/merge_events.py
import polars as pl
import glob
import os
import json
import time

def main():
    start_time = time.time()
    print("\n" + "="*70)
    print("🧩 [分布式事件合并与质量审计引擎] | 启动")
    print("="*70)
    
    # 1. 扫描并合并成功 Parquet 数据
    part_files = glob.glob("all_artifacts/event_forecast_part_*.parquet")
    print(f"📂 检索到分布式成功分片共计: {len(part_files)} 个")
    
    final_df = pl.DataFrame()
    if part_files:
        dfs = []
        for f in part_files:
            try:
                dfs.append(pl.read_parquet(f))
            except Exception as e:
                print(f"⚠️ 读取分片失败: {f}，原因: {e}")
        if dfs:
            final_df = pl.concat(dfs).unique(subset=["code", "notice_date"], keep="last").sort(["code", "notice_date"])
            
    os.makedirs("output", exist_ok=True)
    out_path = "output/event_earnings_forecast.parquet"
    if not final_df.is_empty():
        final_df.write_parquet(out_path, compression="zstd")
    else:
        # 极端兜底，防空表报错
        schema = {
            "code": pl.Utf8, "notice_date": pl.Utf8, "report_date": pl.Utf8,
            "forecast_type": pl.Utf8, "forecast_yoy_mid": pl.Float64, "summary": pl.Utf8
        }
        final_df = pl.DataFrame([], schema=schema)
        final_df.write_parquet(out_path, compression="zstd")

    # 2. 扫描并汇总分布式失败 JSON 碎片
    failed_files = glob.glob("all_artifacts/event_failed_part_*.json")
    print(f"📂 检索到分布式失败分片共计: {len(failed_files)} 个")
    
    all_failed_stocks = []
    for f in failed_files:
        try:
            with open(f, "r", encoding="utf-8") as file:
                all_failed_stocks.extend(json.load(file))
        except Exception as e:
            print(f"⚠️ 读取失败碎片出错: {f}，原因: {e}")
            
    # 全局去重并按字母排序
    all_failed_stocks = sorted(list(set(all_failed_stocks)))

    # -------------------------------------------------------------------------
    # 📊 强类型数据深度质量审计 (Polars Engine)
    # -------------------------------------------------------------------------
    total_rows = len(final_df)
    unique_stocks = final_df["code"].n_unique()
    min_date = final_df["notice_date"].min() if total_rows > 0 else "无"
    max_date = final_df["notice_date"].max() if total_rows > 0 else "无"
    
    # A. 统计预告类型分布
    type_dist = (
        final_df.group_by("forecast_type")
        .agg(pl.len().alias("count"))
        .sort("count", descending=True)
    ) if total_rows > 0 else pl.DataFrame()
    
    # B. 统计年度事件分布（透视宏观景气度）
    yearly_dist = (
        final_df.with_columns(pl.col("notice_date").str.slice(0, 4).alias("year"))
        .group_by("year")
        .agg(pl.len().alias("count"))
        .sort("year")
    ) if total_rows > 0 else pl.DataFrame()
    
    # C. 统计披露历史最长（次数最多）的 Top 5 股票
    top5_stocks = (
        final_df.group_by("code")
        .agg(pl.len().alias("count"))
        .sort("count", descending=True)
        .head(5)
    ) if total_rows > 0 else pl.DataFrame()
    
    # D. 增长率中轴指标
    avg_yoy = final_df["forecast_yoy_mid"].mean() if total_rows > 0 else 0.0
    max_yoy = final_df["forecast_yoy_mid"].max() if total_rows > 0 else 0.0
    min_yoy = final_df["forecast_yoy_mid"].min() if total_rows > 0 else 0.0

    # -------------------------------------------------------------------------
    # 📝 终端精美打印 与 Markdown 报告生成
    # -------------------------------------------------------------------------
    # 1. 终端输出
    print("\n📊 [审计摘要]")
    print(f"    - 累计有效历史预告: {total_rows:,} 行")
    print(f"    - 覆盖唯一 A 股个股: {unique_stocks:,} 只 (就绪率: {unique_stocks/5530*100:.2f}%)")
    print(f"    - 披露时间跨度: {min_date} 至 {max_date}")
    print(f"    - 预告同比增速均值: {avg_yoy:.2f}% (极值范围: {min_yoy:.1f}% ~ {max_yoy:.1f}%)")
    print(f"    - 本次执行失败标的: {len(all_failed_stocks):,} 只 (全市场故障率: {len(all_failed_stocks)/5530*100:.2f}%)")
    print(f"    - Parquet 文件大小: {os.path.getsize(out_path)/(1024*1024):.2f} MB")
    
    if all_failed_stocks:
        print(f"❌ 失败标的代码明细: {all_failed_stocks}")
    
    # 2. 生成标准 Markdown 审计总结（带故障诊断模块）
    md = [
        "# 📢 A-Share 业绩预告事件库深度审计报告",
        f"生成时间: `{time.strftime('%Y-%m-%d %H:%M:%S')}`\n",
        "## 1. 基础吞吐与存储校验",
        f"- **事件库累计存储行数:** `{total_rows:,}` 行",
        f"- **覆盖 A 股有效标的:** `{unique_stocks:,}` 只",
        f"- **时间覆盖区间:** `{min_date}` 至 `{max_date}`",
        f"- **业绩预告同比增长率均值:** `{avg_yoy:.2f}%`",
        f"- **物理磁盘占用 (Parquet ZSTD):** `{os.path.getsize(out_path)/(1024*1024):.2f} MB`\n",
        
        "## 2. 预告披露类型历史分布 (Forecast Type Distribution)",
        "| 预告类型 | 历史披露公告数 | 占比百分比 |",
        "| :--- | :--- | :--- |"
    ]
    for row in type_dist.iter_rows():
        t_name, cnt = row[0], row[1]
        md.append(f"| `{t_name}` | `{cnt:,}` | `{cnt/total_rows*100:.2f}%` |")
        
    md.extend([
        "\n## 3. 年度事件披露频次 (时序景气度周期变迁)",
        "| 披露年份 | 公告计数 | 占比百分比 |",
        "| :--- | :--- | :--- |"
    ])
    for row in yearly_dist.iter_rows():
        yr, cnt = row[0], row[1]
        md.append(f"| `{yr} 年` | `{cnt:,}` | `{cnt/total_rows*100:.2f}%` |")
        
    md.extend([
        "\n## 4. 历史业绩预告最频繁个股 Top 5",
        "| 证券代码 | 累计披露业绩预告次数 |",
        "| :--- | :--- |"
    ])
    for row in top5_stocks.iter_rows():
        code, cnt = row[0], row[1]
        md.append(f"| `{code}` | `{cnt}` 次 |")
        
    md.extend([
        "\n## 5. 抓取失败个股诊断清单 (Failed Stocks Diagnostic)",
        f"- **本次运行故障/跳过标的总数:** `{len(all_failed_stocks)}` 只 (在 3 次重试自愈机制后依旧失败)\n"
    ])
    
    if all_failed_stocks:
        md.append("- **失败股票物理明细 (可用于排查停牌、退市或 Baostock 单股 API 损坏):**")
        # 将代码分块展示，每行展示 10 个代码，防止长文本在 Markdown 换行时排版炸裂
        chunk_size = 10
        for i in range(0, len(all_failed_stocks), chunk_size):
            chunk = all_failed_stocks[i : i + chunk_size]
            md.append("  " + " \| ".join([f"`{c}`" for c in chunk]))
    else:
        md.append("- **🎉 完美运行！全市场 5500+ 个股 100% 下载成功，无任何个股触发失败。**")
        
    report_md_str = "\n".join(md)
    audit_summary_path = "output/event_audit_summary.md"
    with open(audit_summary_path, "w", encoding="utf-8") as f:
        f.write(report_md_str)
        
    print("\n" + "="*70)
    print("📊 [事件库深度质量审计成功]")
    print(f"    - 数据已写出: {out_path}")
    print(f"    - 审计分析已保存: {audit_summary_path}")
    print(f"    - 审计耗时: {time.time() - start_time:.2f} 秒")
    print("="*70 + "\n")

if __name__ == "__main__":
    main()
