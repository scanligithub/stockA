import json
import math
import random
import subprocess
import os

NUM_CHUNKS = 19

# 🎯 55 只核心量化矩阵指数列表
INDEX_LIST = [
    # 1. 宽基与规模 (14只)
    "sh.000001", "sz.399001", "sz.399006", "sh.000688", "bj.899050",
    "sh.000016", "sh.000300", "sh.000905", "sh.000852", "sh.000851",
    "sz.399303", "sh.000985", "sz.399330", "sh.000090",
    # 2. 风格与 Smart Beta (10只)
    "sh.000922", "sz.399324", "sh.000015", "sh.000918", "sh.000919",
    "sh.000807", "sh.000827", "sh.000925", "sh.000978", "sz.399317",
    # 3. 前沿科技与新质生产力 (16只)
    "sz.399807", "sz.399977", "sh.932252", "sz.399812", "sh.000973",
    "sh.931160", "sz.399354", "sz.399673", "sz.399979", "sz.399285",
    "sh.931151", "sz.399008", "sh.931494", "sh.931409", "sh.931152",
    "sz.399993",
    # 4. 传统行业与大宗周期 (15只)
    "sz.399975", "sz.399986", "sz.399932", "sz.399933", "sz.399967",
    "sz.399989", "sz.399971", "sz.399997", "sh.000934", "sh.000935",
    "sz.399990", "sz.399998", "sz.399974", "sh.000157", "sh.930606"
]

def main():
    print("🚀 Invoking Go Engine to fetch Master Stock List via TDX...")
    subprocess.run(["./tdx_fetcher", "-mode=list"], check=True)
    
    if not os.path.exists("stock_list_master.json"):
        print("❌ Go Engine failed to produce stock_list_master.json")
        exit(1)

    with open("stock_list_master.json", "r", encoding="utf-8") as f:
        master_list = json.load(f)

    # 提取纯 A 股个股代码（此时已不含指数）
    valid_stocks = [x['code'] for x in master_list]
    print(f"✅ Total valid A-shares from TDX: {len(valid_stocks)}")

    # 随机打乱个股以均衡分布式负载
    random.shuffle(valid_stocks)
    chunk_size = math.ceil(len(valid_stocks) / NUM_CHUNKS)
    chunks = []
    
    for i in range(NUM_CHUNKS):
        subset = valid_stocks[i * chunk_size : (i + 1) * chunk_size]
        # 🎯 核心修改：在第 0 分片中合并 55 只前瞻性指数
        if i == 0:
            subset.extend(INDEX_LIST)
        if subset:
            chunks.append({"index": i, "codes": subset})

    with open("stock_matrix.json", "w", encoding="utf-8") as f:
        json.dump(chunks, f, ensure_ascii=False)
        
    print(f"✅ Successfully prepared matrix with {NUM_CHUNKS} parts. Indices (55) bound to Job 0.")

if __name__ == "__main__":
    main()
