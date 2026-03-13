import baostock as bs
import pandas as pd
import json
import math
import random
import datetime
import os
import sys
import time

# 确保能引用到 utils
sys.path.append(os.getcwd())
from utils.cf_proxy import EastMoneyProxy

NUM_CHUNKS = 19

def main():
    # --- Part 1: 获取股票 Matrix (Baostock) ---
    bs.login()
    data = []
    for i in range(10):
        d = (datetime.datetime.now() - datetime.timedelta(days=i)).strftime("%Y-%m-%d")
        rs = bs.query_all_stock(day=d)
        if rs.error_code == '0' and len(rs.data) > 0:
            while rs.next():
                row = rs.get_row_data()
                if row[2] and row[2].strip(): data.append(row)
            break
    bs.logout()
    
    valid_codes = [x[0] for x in data if x[0].startswith(('sh.', 'sz.', 'bj.'))]
    random.shuffle(valid_codes)
    
    chunk_size = math.ceil(len(valid_codes) / NUM_CHUNKS)
    chunks = []
    for i in range(NUM_CHUNKS):
        subset = valid_codes[i * chunk_size : (i + 1) * chunk_size]
        if subset: chunks.append({"index": i, "codes": subset})

    with open("stock_matrix.json", "w") as f:
        json.dump(chunks, f)
    print(f"✅ Stock Matrix ready: {len(valid_codes)} stocks.")

    # --- Part 2: 获取板块名册 (EastMoney via Proxy) ---
    proxy = EastMoneyProxy()
    targets = {"Industry": "m:90 t:2", "Concept": "m:90 t:3", "Region": "m:90 t:1"}
    master_sectors = {}
    
    cf_url = os.getenv("CF_WORKER_URL")
    if not cf_url:
        print("⚠️ CF_WORKER_URL not found, skipping sector discovery.")
    else:
        # 核心优化：去除了 for pass_idx in range(1, 4) 的三重智商税循环
        print("📡 Fast Phase: Single-pass sector discovery (using pz=3000)...")
        for label, fs in targets.items():
            try:
                print(f"   -> Fetching {label} sectors...")
                lst = proxy.get_sector_list(fs)
                for x in lst:
                    code = x['f12']
                    if code not in master_sectors:
                        master_sectors[code] = {"code": code, "market": x['f13'], "name": x['f14'], "type": label}
            except Exception as e: 
                print(f"   ❌ Failed to fetch {label}: {e}")
            time.sleep(1)
        
        with open("sector_list.json", "w", encoding='utf-8') as f:
            json.dump(list(master_sectors.values()), f, ensure_ascii=False)
        print(f"✅ Sector List ready: {len(master_sectors)} sectors.")

if __name__ == "__main__":
    main()
