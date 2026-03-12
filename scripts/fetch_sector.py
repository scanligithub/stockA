import sys
import os
import pandas as pd
import concurrent.futures
from tqdm import tqdm
import time

# 确保能引用到项目根目录的 utils
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils.cf_proxy import EastMoneyProxy
from utils.cleaner import DataCleaner

OUTPUT_DIR = "temp_parts"
os.makedirs(OUTPUT_DIR, exist_ok=True)
proxy = EastMoneyProxy()

def fetch_one_sector(info):
    """
    抓取单个板块的 K 线和成分股
    """
    code, name, mkt = info['code'], info['name'], info['market']
    # 构造东财 secid: 行业/概念通常是 90. 或 0.
    secid = f"90.{code}" if str(mkt) == '90' else f"{mkt}.{code}"
    
    # 1. 抓取 K 线
    res_k = proxy.get_sector_kline(secid)
    df_k = pd.DataFrame()
    if res_k and res_k.get('data') and res_k['data'].get('klines'):
        rows = [x.split(',') for x in res_k['data']['klines']]
        df_k = pd.DataFrame(rows, columns=['date','open','close','high','low','volume','amount','turn'])
        df_k['code'] = code
        df_k['name'] = name
        df_k['type'] = info['type']

    # 2. 抓取成分股快照
    res_c = proxy.get_sector_constituents(code)
    consts = []
    if res_c and res_c.get('data') and res_c['data'].get('diff'):
        diff_data = res_c['data']['diff']
        # 兼容字典或列表格式
        items = diff_data.values() if isinstance(diff_data, dict) else diff_data
        for item in items:
            consts.append({
                "sector_code": code, 
                "stock_code": item['f12'], 
                "sector_name": name
            })
            
    return df_k, consts

def main():
    # 定义抓取目标：行业、概念、地域
    targets = {
        "Industry": "m:90 t:2", 
        "Concept": "m:90 t:3", 
        "Region": "m:90 t:1"
    }
    
    # --- [核心修改：3次重复探测逻辑] ---
    master_sectors = {} # 使用字典以 code 为键进行去重并集
    
    print("📡 Starting triple-pass sector discovery...")
    for pass_idx in range(1, 4):
        pass_count = 0
        for label, fs in targets.items():
            try:
                lst = proxy.get_sector_list(fs)
                for x in lst:
                    code = x['f12']
                    if code not in master_sectors:
                        master_sectors[code] = {
                            "code": code, 
                            "market": x['f13'], 
                            "name": x['f14'], 
                            "type": label
                        }
                        pass_count += 1
            except Exception as e:
                print(f"⚠️ Pass {pass_idx} error on {label}: {e}")
        
        print(f"✅ Pass {pass_idx} complete. New sectors found: {pass_count}. Total unique: {len(master_sectors)}")
        if pass_idx < 3:
            time.sleep(2) # 探测间歇

    # 转换为 DataFrame 处理
    df_list = pd.DataFrame(list(master_sectors.values()))
    if df_list.empty:
        print("❌ Critical: No sectors found after 3 passes!")
        return

    print(f"🚀 Proceeding to fetch data for {len(df_list)} unique sectors...")

    all_k, all_c = [], []
    # 使用较小的并发数以确保稳定性
    with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
        futures = {executor.submit(fetch_one_sector, row): row['name'] for _, row in df_list.iterrows()}
        for fut in tqdm(concurrent.futures.as_completed(futures), total=len(futures), desc="Fetching Sectors"):
            try:
                k, c = fut.result()
                if not k.empty: all_k.append(k)
                if c: all_c.extend(c)
            except Exception as e:
                print(f"❌ Error fetching sector data: {e}")

    # 3. 清洗与保存
    cleaner = DataCleaner()
    
    if all_k:
        full_k = pd.concat(all_k)
        full_k = cleaner.clean_sector_kline(full_k)
        k_path = f"{OUTPUT_DIR}/sector_kline_full.parquet"
        full_k.to_parquet(k_path, index=False)
        print(f"💾 Saved Sector Kline: {len(full_k)} rows.")
        
    if all_c:
        full_c = pd.DataFrame(all_c)
        full_c['date'] = pd.Timestamp.now().strftime('%Y-%m-%d')
        c_path = f"{OUTPUT_DIR}/sector_constituents_latest.parquet"
        full_c.to_parquet(c_path, index=False)
        print(f"💾 Saved Sector Constituents: {len(full_c)} rows.")

if __name__ == "__main__":
    main()
