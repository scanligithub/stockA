import requests
import json
import math
import random

NUM_CHUNKS = 19
# 东方财富轻量全量行情接口 (含个股代码与名称)
EM_ALL_API = "https://push2.eastmoney.com/api/qt/clist/get?pn=1&pz=6000&po=1&np=1&ut=bd1d9ddb04089700cf9c27f6f7426281&fltt=2&invt=2&fid=f3&fs=m:0+t:6,m:0+t:80,m:1+t:2,m:1+t:23,m:0+t:81+s:2048&fields=f12,f14"

def main():
    print("🚀 Fetching Master Stock List from EastMoney...")
    try:
        resp = requests.get(EM_ALL_API, timeout=15)
        data = resp.json()
        items = data.get("data", {}).get("diff", [])
    except Exception as e:
        print(f"❌ Failed to fetch from EastMoney: {e}")
        return

    valid_stocks = []
    master_list = []
    
    for item in items:
        c = item.get("f12", "")
        n = item.get("f14", "").strip()
        if not c or not n: continue
        
        prefix = "sh" if c.startswith(("60", "68")) else "sz" if c.startswith(("00", "30")) else "bj"
        full_code = f"{prefix}.{c}"
        
        valid_stocks.append(full_code)
        master_list.append({"code": full_code, "code_name": n})

    # 保存 Master List 供后续合并与名称匹配使用
    with open("stock_list_master.json", "w", encoding="utf-8") as f:
        json.dump(master_list, f, ensure_ascii=False, indent=2)

    random.shuffle(valid_stocks)
    print(f"✅ Total valid A-shares: {len(valid_stocks)}")

    chunk_size = math.ceil(len(valid_stocks) / NUM_CHUNKS)
    chunks = []
    for i in range(NUM_CHUNKS):
        subset = valid_stocks[i * chunk_size : (i + 1) * chunk_size]
        if subset:
            chunks.append({"index": i, "codes": subset})

    with open("stock_matrix.json", "w") as f:
        json.dump(chunks, f)

if __name__ == "__main__":
    main()
