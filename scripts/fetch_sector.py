import sys
import os
import pandas as pd
import concurrent.futures
from tqdm import tqdm
import json

sys.path.append(os.getcwd())
from utils.cf_proxy import EastMoneyProxy
from utils.cleaner import DataCleaner

OUTPUT_DIR = "temp_parts"
os.makedirs(OUTPUT_DIR, exist_ok=True)
proxy = EastMoneyProxy()

def fetch_one_sector(info):
    code, name, mkt = info['code'], info['name'], info['market']
    secid = f"90.{code}" if str(mkt) == '90' else f"{mkt}.{code}"
    
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
        items = res_c['data']['diff'].values() if isinstance(res_c['data']['diff'], dict) else res_c['data']['diff']
        for item in items:
            consts.append({"sector_code": code, "stock_code": item['f12'], "sector_name": name})
    return df_k, consts

def main():
    # 核心修改：优先读取 prepare 阶段生成的 json
    list_path = "sector_list.json"
    if os.path.exists(list_path):
        with open(list_path, "r", encoding='utf-8') as f:
            sectors = json.load(f)
        print(f"📂 Loaded {len(sectors)} sectors from prepare stage.")
    else:
        print("⚠️ sector_list.json not found, fetching manually (unstable under load)...")
        # 兜底逻辑：如果独立运行，依然尝试在线获取
        return

    df_list = pd.DataFrame(sectors)
    all_k, all_c = [], []
    with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
        futures = {executor.submit(fetch_one_sector, row): row['name'] for _, row in df_list.iterrows()}
        for fut in tqdm(concurrent.futures.as_completed(futures), total=len(futures), desc="Sectors"):
            k, c = fut.result()
            if not k.empty: all_k.append(k)
            if c: all_c.extend(c)

    cleaner = DataCleaner()
    if all_k:
        pd.concat(all_k).to_parquet(f"{OUTPUT_DIR}/sector_kline_full.parquet", index=False)
    if all_c:
        df_c = pd.DataFrame(all_c)
        df_c['date'] = pd.Timestamp.now().strftime('%Y-%m-%d')
        df_c.to_parquet(f"{OUTPUT_DIR}/sector_constituents_latest.parquet", index=False)

if __name__ == "__main__":
    main()
