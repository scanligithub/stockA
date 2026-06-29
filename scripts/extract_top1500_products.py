# FILE: scripts/extract_top1500_products.py
import os
import polars as pl

def main():
    print("\n" + "="*70)
    print("🌟 A股主营业务产品节点 深度提取器 (核心基数升级至 Top 1500) 🌟")
    print("="*70)

    input_path = "output/all_stocks_mainbus_raw.parquet"
    if not os.path.exists(input_path):
        print(f"❌ 严重错误: 找不到底层数据文件 {input_path}")
        print("请确保已先运行 fetch_f10_mainbus.py。")
        return

    print(f"📂 正在加载全市场历史主营数据集: {input_path}...")
    df = pl.read_parquet(input_path)

    print("🧮 正在清洗并统计全局最高频的 1500 个核心与新兴产品...")
    # 扩大至 1500，确保绝对囊括如“液冷服务器”、“固态电池”等最新热门景气度词汇
    top1500 = (
        df.filter((pl.col("item_type") == 2) & (pl.col("item_name").str.len_chars() > 0))
        .group_by("item_name")
        .agg(pl.len().alias("history_appearance_count"))
        .sort("history_appearance_count", descending=True)
        .head(1500)
    )

    out_dir = "output"
    os.makedirs(out_dir, exist_ok=True)
    
    # 1. 导出标准 CSV 供本地数据分析和存档
    csv_path = os.path.join(out_dir, "top1500_main_products.csv")
    top1500.write_csv(csv_path)
    
    # 2. 🤖 为大模型生成具有“二级嵌套拓扑”的超级 Prompt
    product_list = top1500["item_name"].to_list()
    compact_text = "、".join(product_list)
    
    llm_prompt = f"""请你作为中国 A 股顶尖的量化基本面专家，帮我完成一份具有“上下游节点关系”的产业链映射字典。
以下是 A 股出现频次最高的 1500 个主营产品名称。请为它们构建一个“两级嵌套”的分类体系：

1. 第一级（宏观产业链，建议 25~30 个）：如 新能源、半导体、人工智能与算力、消费电子、生物医药、国防军工、基础化工、汽车与零部件 等。
2. 第二级（微观节点/细分赛道，建议 100~150 个）：在每个宏观链下，细分出具体的上下游环节或热门概念。如 新能源 下细分为“上游锂电材料”、“中游动力电池”、“下游整车”、“光伏设备”等。
3. 清洗要求：如果某些产品属于无产业链属性的杂项（如“其他业务”、“租金收入”、“加工费”、“利息净收入”、“内部抵销”），请将其直接丢弃！不要出现在字典中。

请必须严格以 JSON 格式输出，绝对不要输出任何 Markdown 标记 (如 ```json)、不要做任何解释，格式范例如下：
{{
  "新能源产业链": {{
    "上游锂矿与材料": ["碳酸锂", "氢氧化锂", "三元前驱体"],
    "中游动力电池": ["动力电池系统", "储能电池", "固态电池"],
    "光伏风电设备": ["单晶硅片", "光伏组件", "风机"]
  }},
  "半导体产业链": {{
    "半导体设备与材料": ["刻蚀机", "光刻胶", "硅片"],
    "芯片设计与制造": ["集成电路", "晶圆代工", "MCU"]
  }},
  "人工智能与算力": {{
    "算力硬件": ["液冷服务器", "光通信模块", "AI芯片"],
    "大模型与应用": ["自然语言处理", "算力租赁"]
  }}
}}

以下是待分类的 1500 个产品名单：
{compact_text}
"""
    
    txt_path = os.path.join(out_dir, "llm_prompt_ready.txt")
    with open(txt_path, "w", encoding="utf-8") as f:
        f.write(llm_prompt)

    print("\n✅ 提取成功！")
    print(f"💾 原始数据已导出: {csv_path}")
    print(f"🤖 大模型专用 Prompt 已生成: {txt_path} (文件大小约 23KB)")

if __name__ == "__main__":
    main()
