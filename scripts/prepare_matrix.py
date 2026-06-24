import json
import math
import random
import subprocess
import os

NUM_CHUNKS = 19

def main():
    print("🚀 Invoking Go Engine to fetch Master Stock List via TDX...")
    subprocess.run(["./tdx_fetcher", "-mode=list"], check=True)
    
    if not os.path.exists("stock_list_master.json"):
        print("❌ Go Engine failed to produce stock_list_master.json")
        exit(1)

    with open("stock_list_master.json", "r", encoding="utf-8") as f:
        master_list = json.load(f)

    # 提取纯 A 股个股代码
    valid_stocks = [x['code'] for x in master_list]
    print(f"✅ Total valid A-shares from TDX: {len(valid_stocks)}")

    random.shuffle(valid_stocks)
    chunk_size = math.ceil(len(valid_stocks) / NUM_CHUNKS)
    chunks = []
    
    for i in range(NUM_CHUNKS):
        subset = valid_stocks[i * chunk_size : (i + 1) * chunk_size]
        if subset:
            chunks.append({"index": i, "codes": subset})

    with open("stock_matrix.json", "w", encoding="utf-8") as f:
        json.dump(chunks, f, ensure_ascii=False)

if __name__ == "__main__":
    main()
