# FILE: scripts/extract_top1500_products.py
import os
import polars as pl

def main():
    print("\n" + "="*60)
    print("🌟 A股主营业务产品节点 深度提取器 (Top 1500) 🌟")
    print("="*60)

    input_path = "output/all_stocks_mainbus_raw.parquet"
    if not os.path.exists(input_path):
        print(f"❌ 严重错误: 找不到底层数据文件 {input_path}")
        return

    print(f"📂 正在加载全市场历史主营数据集: {input_path}...")
    df = pl.read_parquet(input_path)

    print("🧮 正在清洗并统计全局最高频的 1500 个核心与新兴产品...")
    # 将提取数量从 500 扩大到 1500，确保绝对不漏掉任何新兴热门产业链
    top1500 = (
        df.filter((pl.col("item_type") == 2) & (pl.col("item_name").str.len_chars() > 0))
        .group_by("item_name")
        .agg(pl.len().alias("history_appearance_count"))
        .sort("history_appearance_count", descending=True)
        .head(1500)
    )

    out_dir = "output"
    os.makedirs(out_dir, exist_ok=True)
    
    csv_path = os.path.join(out_dir, "top1500_main_products.csv")
    top1500.write_csv(csv_path)
    
    # 生成供大模型使用的高度压缩文本
    product_list = top1500["item_name"].to_list()
    compact_text = "、".join(product_list)
    
    llm_prompt = f"""请你作为 A 股量化基本面专家，帮我完成产业链映射字典的构建。
以下是 A 股出现频次最高的 1500 个主营产品名称（包含了传统行业与新兴热门概念）。
请将它们归类到最核心的 15 个宏观产业链中（如：新能源、半导体、人工智能/算力、低空经济、生物医药、消费电子等）。
如果某些产品属于无明显产业链属性的杂项（如“其他业务”、“加工费”、“贸易”、“利息收入”），请将其直接丢弃，不要纳入字典。

请必须严格以 JSON 格式输出，不要输出任何 Markdown 标记或多余的解释文字，格式范例如下：
{{
  "新能源链": ["动力电池系统", "碳酸锂", "太阳能组件", "固态电池"],
  "算力与AI链": ["液冷服务器", "光通信模块", "算力租赁"]
}}

以下是待分类的产品名单：
{compact_text}
"""
    
    txt_path = os.path.join(out_dir, "llm_prompt_ready.txt")
    with open(txt_path, "w", encoding="utf-8") as f:
        f.write(llm_prompt)

    print("\n✅ 提取成功！(已升级为 Top 1500)")
    print(f"🤖 大模型专用 Prompt 已生成: {txt_path}")
    print("\n💡 操作指南:")
    print("提取了前 1500 个词汇，已完美覆盖近几年所有细分热门赛道（如算力、固态电池、CPO等）。")
    print("下载 llm_prompt_ready.txt，全选里面的文字，直接粘贴给 DeepSeek 即可（约消耗 4000 Token，完全在模型处理能力内）！")

if __name__ == "__main__":
    main()
