import json
import glob
import pandas as pd

def main():
    files = glob.glob("sector_*.json")
    all_data = []
    totals = {}

    for f in files:
        with open(f, 'r', encoding='utf-8') as j:
            content = json.load(j)
            cat = content['category']
            totals[cat] = content['total_expected']
            # 给每条数据打上分类标签
            for item in content['data']:
                item['type'] = cat
                all_data.append(item)
    
    df = pd.DataFrame(all_data)
    # 核心：根据代码 f12 全局去重
    df_clean = df.drop_duplicates(subset=['f12'])
    
    print("📊 [汇总对账单]")
    print("-" * 30)
    for cat, expected in totals.items():
        actual = len(df_clean[df_clean['type'] == cat])
        status = "✅ 完整" if actual >= expected else f"❌ 缺失 ({actual}/{expected})"
        print(f"分类 {cat:10}: 期望 {expected:3} | 实际 {actual:3} | 状态 {status}")
    
    print("-" * 30)
    print(f"🏆 最终聚合去重后总条数: {len(df_clean)}")
    df_clean.to_json("final_sector_list.json", orient='records', force_ascii=False)

if __name__ == "__main__":
    main()
