# FILE: scripts/extract_top500_products.py
import os
import polars as pl

def main():
    print("\n" + "="*60)
    print("🌟 A股主营业务产品节点 Top 500 提取器 🌟")
    print("="*60)

    input_path = "output/all_stocks_mainbus_raw.parquet"
    if not os.path.exists(input_path):
        print(f"❌ 严重错误: 找不到底层数据文件 {input_path}")
        print("请确保已先运行 fetch_f10_mainbus.py。")
        return

    print(f"📂 正在加载全市场历史主营数据集: {input_path}...")
    df = pl.read_parquet(input_path)

    print("🧮 正在清洗并统计全局最高频的 500 个核心产品...")
    # 核心降维逻辑：
    # 1. 过滤出纯正的“产品级”分类 (item_type == 2)
    # 2. 剔除空字符串或无效名称
    # 3. 按产品名聚合计数，降序排列，取前 500
    top500 = (
        df.filter((pl.col("item_type") == 2) & (pl.col("item_name").str.len_chars() > 0))
        .group_by("item_name")
        .agg(pl.len().alias("history_appearance_count"))
        .sort("history_appearance_count", descending=True)
        .head(500)
    )

    out_dir = "output"
    os.makedirs(out_dir, exist_ok=True)
    out_path = os.path.join(out_dir, "top500_main_products.csv")
    
    top500.write_csv(out_path)
    
    print("\n✅ 提取成功！")
    print(f"💾 文件已导出至: {out_path}")
    print("\n📊 预览 Top 10 核心产品节点:")
    print(top500.head(10))
    print("\n💡 下一步行动指南:")
    print("请下载生成的 top500_main_products.csv，发给 DeepSeek/ChatGPT 并附带以下 Prompt：")
    print("『这是一份A股最高频的500个主营产品名单，请作为行业基本面专家，将它们归类到 10 个最核心的宏观产业链中（如：新能源、半导体、医药等），输出 JSON 格式。』")

if __name__ == "__main__":
    main()
