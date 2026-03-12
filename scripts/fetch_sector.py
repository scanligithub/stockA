import sys
import os
import pandas as pd
import concurrent.futures
from tqdm import tqdm
import time
import random

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from utils.cf_proxy import EastMoneyProxy
from utils.cleaner import DataCleaner

OUTPUT_DIR = "temp_parts"
os.makedirs(OUTPUT_DIR, exist_ok=True)
proxy = EastMoneyProxy()

def fetch_one_sector(info):
    code, name, mkt = info['code'], info['name'], info['market']
    secid = f"90.{code}" if str(mkt) == '90' else f"{mkt}.{code}"
    
    # 增加随机抖动，避免 10 个线程同时发起请求
    time.sleep(random.uniform(0, 2.0))
    
    res_k = proxy.get_sector_kline(secid)
    df_k = pd.DataFrame()
    if res_k and res_k.get('data') and res_k['data'].get('klines'):
        rows = [x.split(',') for x in res_k['data']['klines']]
        df_k = pd.DataFrame(rows, columns=['date','open','close','high','low','volume','amount','turn'])
        df_k['code'] = code
        df_k['name'] = name
        df_k['type'] = info['type']

    res_c = proxy.get_sector_constituents(code)
    consts = []
    if res_c and res_c.get('data') and res_c['data'].get('diff'):
        diff_data = res_c['data']['diff']
        items = diff_data.values() if isinstance(diff_data, dict) else diff_data
        for item in items:
            consts.append({"sector_code": code, "stock_code": item['f12'], "sector_name": name})
    return df_k, consts

def main():
    targets = {"Industry": "m:90 t:2", "Concept": "m:90 t:3", "Region": "m:90 t:1"}
    master_sectors = {}
    
    print("📡 Starting stabilized triple-pass sector discovery...")
    for pass_idx in range(1, 4):
        pass_count = 0
        for label, fs in targets.items():
            try:
                # 核心改进：板块分类查询之间增加长休眠
                time.sleep(random.uniform(2, 5))
                lst = proxy.get_sector_list(fs)
                for x in lst:
                    code = x['f12']
                    if code not in master_sectors:
                        master_sectors[code] = {"code": code, "market": x['f13'], "name": x['f14'], "type": label}
                        pass_count += 1
            except Exception as e:
                print(f"⚠️ Pass {pass_idx} error on {label}: {e}")
        
        print(f"✅ Pass {pass_idx} complete. New: {pass_count}. Total: {len(master_sectors)}")
        if pass_idx < 3:
            # 核心改进：每一轮探测之间大幅增加休息时间，给服务器和 CF 喘息机会
            time.sleep(10) 

    df_list = pd.DataFrame(list(master_sectors.values()))
    if df_list.empty:
        print("❌ Critical Error: No sectors detected. Exiting.")
        return

    print(f"🚀 Fetching data for {len(df_list)} sectors with jitter...")
    all_k, all_c = [], []
    
    # 核心改进：全量抓取时将并发限制在 5，减缓对 CF Worker 的总冲击
    with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
        futures = {executor.submit(fetch_one_sector, row): row['name'] for _, row in df_list.iterrows()}
        for fut in tqdm(concurrent.futures.as_completed(futures), total=len(futures), desc="Sectors"):
            try:
                k, c = fut.result()
                if not k.empty: all_k.append(k)
                if c: all_c.extend(c)
            except: pass

    cleaner = DataCleaner()
    if all_k:
        full_k = pd.concat(all_k)
        full_k = cleaner.clean_sector_kline(full_k)
        full_k.to_parquet(f"{OUTPUT_DIR}/sector_kline_full.parquet", index=False)
        
    if all_c:
        full_c = pd.DataFrame(all_c)
        full_c['date'] = pd.Timestamp.now().strftime('%Y-%m-%d')
        full_c.to_parquet(f"{OUTPUT_DIR}/sector_constituents_latest.parquet", index=False)

if __name__ == "__main__":
    main()
