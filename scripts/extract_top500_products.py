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
        return

    print(f"📂 正在加载全市场历史主营数据集: {input_path}...")
    df = pl.read_parquet(input_path)

    print("🧮 正在清洗并统计全局最高频的 500 个核心产品...")
    top500 = (
        df.filter((pl.col("item_type") == 2) & (pl.col("item_name").str.len_chars() > 0))
        .group_by("item_name")
        .agg(pl.len().alias("history_appearance_count"))
        .sort("history_appearance_count", descending=True)
        .head(500)
    )

    out_dir = "output"
    os.makedirs(out_dir, exist_ok=True)
    
    # 1. 导出标准 CSV 供本地数据分析和存档
    csv_path = os.path.join(out_dir, "top500_main_products.csv")
    top500.write_csv(csv_path)
    
    # 2. 🤖 为大模型专门生成无缝复制的 Prompt 文本
    # 提取纯产品名称，用顿号拼接，极度节省 Token
    product_list = top500["item_name"].to_list()
    compact_text = "、".join(product_list)
    
    llm_prompt = f"""请你作为 A 股量化基本面专家，帮我完成产业链映射字典的构建。
以下是 A 股出现频次最高的 500 个主营产品名称。请将它们归类到 10 个最核心的宏观产业链中（如：新能源、半导体、消费电子、医药生物、人工智能/算力等）。
如果某些产品属于非核心的杂项（如“其他”、“加工费”、“贸易”），请将其丢弃，不要纳入字典。

请必须严格以 JSON 格式输出，不要输出任何 Markdown 标记或多余的解释文字，格式范例如下：
{{
  "新能源链": ["动力电池系统", "碳酸锂", "太阳能组件"],
  "半导体链": ["集成电路", "晶圆制造"]
}}

以下是待分类的 500 个产品名单：
{compact_text}
"""
    
    txt_path = os.path.join(out_dir, "llm_prompt_ready.txt")
    with open(txt_path, "w", encoding="utf-8") as f:
        f.write(llm_prompt)

    print("\n✅ 提取成功！")
    print(f"💾 原始数据已导出: {csv_path}")
    print(f"🤖 大模型专用 Prompt 已生成: {txt_path}")
    print("\n💡 操作指南:")
    print("下载 llm_prompt_ready.txt，全选里面的文字，直接粘贴给 DeepSeek 或 ChatGPT 即可！")

if __name__ == "__main__":
    main()
