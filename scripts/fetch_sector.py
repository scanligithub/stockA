import sys
import os
import pandas as pd
import concurrent.futures
from tqdm import tqdm

sys.path.append(os.getcwd())
from utils.cf_proxy import EastMoneyProxy
from utils.cleaner import DataCleaner

OUTPUT_DIR = "temp_parts"
os.makedirs(OUTPUT_DIR, exist_ok=True)
proxy = EastMoneyProxy()

def fetch_one_sector(info):
    code, name, mkt = info['code'], info['name'], info['market']
    secid = f"90.{code}" if str(mkt)=='90' else f"{mkt}.{code}"
    
    # 1. Kline
    # 注意：这里我们下载全量历史，后续再按日期切分或只取当年
    res_k = proxy.get_sector_kline(secid)
    df_k = pd.DataFrame()
    if res_k and res_k.get('data') and res_k['data'].get('klines'):
        rows = [x.split(',') for x in res_k['data']['klines']]
        # 东财返回: date, open, close, high, low, vol, amount, turn
        df_k = pd.DataFrame(rows, columns=['date','open','close','high','low','volume','amount','turn'])
        df_k['code'] = code
        df_k['name'] = name
        df_k['type'] = info['type']

    # 2. Constituents
    res_c = proxy.get_sector_constituents(code)
    consts = []
    if res_c and res_c.get('data') and res_c['data'].get('diff'):
        for item in res_c['data']['diff']:
            consts.append({"sector_code": code, "stock_code": item['f12'], "sector_name": name})
            
    return df_k, consts

def main():
    # 1. 获取列表
    targets = {"Industry": "m:90 t:2", "Concept": "m:90 t:3", "Region": "m:90 t:1"}
    sectors = []
    for label, fs in targets.items():
        lst = proxy.get_sector_list(fs)
        for x in lst:
            sectors.append({"code": x['f12'], "market": x['f13'], "name": x['f14'], "type": label})
            
    df_list = pd.DataFrame(sectors).drop_duplicates('code')
    print(f"Fetching {len(df_list)} sectors...")

    all_k, all_c = [], []
    with concurrent.futures.ThreadPoolExecutor(max_workers=20) as executor:
        futures = {executor.submit(fetch_one_sector, row): row['name'] for _, row in df_list.iterrows()}
        for fut in tqdm(concurrent.futures.as_completed(futures), total=len(futures)):
            k, c = fut.result()
            if not k.empty: all_k.append(k)
            if c: all_c.extend(c)

    # 清洗
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
