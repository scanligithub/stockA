import sys
import os
import time
import pandas as pd
import concurrent.futures
from tqdm import tqdm
from datetime import datetime

sys.path.append(os.getcwd())
from utils.cf_proxy import EastMoneyProxy
from utils.cleaner import DataCleaner
from utils.sector_catalog_builder import build_sector_catalog

OUTPUT_DIR = "temp_parts"
os.makedirs(OUTPUT_DIR, exist_ok=True)
proxy = EastMoneyProxy()

def fetch_one_sector(info):
    code, name = info['code'], info['name']
    secid = f"90.{code}"
    
    k_success, c_success = False, False
    k_data, consts = [], []
    err_msgs = []
    
    try:
        # --- 1. 获取 K 线 ---
        res_k = proxy.get_sector_kline(secid)
        if res_k is None:
            err_msgs.append("K线网络超时")
        elif res_k.get('data') and res_k['data'].get('klines'):
            k_success = True
            for row_str in res_k['data']['klines']:
                r = row_str.split(',')
                if len(r) >= 8:
                    k_data.append({
                        'date': r[0], 'open': r[1], 'close': r[2], 'high': r[3], 'low': r[4], 
                        'volume': r[5], 'amount': r[6], 'amplitude': r[7],
                        'code': code, 'name': name, 'type': info['type']
                    })
        else:
            err_msgs.append(f"K线空数据")

        # --- 2. 获取成份股 ---
        res_c = proxy.get_sector_constituents(code)
        if res_c is None:
            err_msgs.append("成份股网络超时")
        elif res_c.get('data') and res_c['data'].get('diff'):
            c_success = True
            items_list = res_c['data']['diff']
            for item in items_list:
                consts.append({"sector_code": code, "stock_code": item['f12'], "sector_name": name})
        else:
            err_msgs.append(f"成份股空数据")
            
        return k_success, c_success, k_data, consts, " | ".join(err_msgs)
        
    except Exception as e:
        return False, False, [], [], f"异常: {str(e)}"

def main():
    start_time = datetime.now()
    print(f"\n{'='*70}\n[*] 东方财富极速全量引擎 (参数解毒稳定版) | {start_time}\n{'='*70}\n")
    
    all_sectors = build_sector_catalog(proxy)
    df_list = pd.DataFrame(all_sectors).drop_duplicates('code')
    total_found = len(df_list)
    print(f"\n[*] 审计报告：去重后唯一板块总数: {total_found}")

    if total_found < 950:
        print(f"[🔥] 抓取总数 {total_found} 偏低，请检查代理网络。")
        sys.exit(1)

    print(f"[*] 风控冷却：休眠 5 秒...")
    time.sleep(5)

    sectors_to_fetch = df_list.to_dict('records')
    all_k_flat, all_c_flat = [], []
    MAX_RETRIES = 2
    
    # 💥 核心：经过参数解毒后，12 并发是最完美的甜点位 (Sweet Spot)
    CONCURRENT_WORKERS = 12 
    
    for attempt in range(MAX_RETRIES):
        if not sectors_to_fetch:
            break
            
        print(f"\n[*] 开始第 {attempt + 1} 轮采集，等待任务: {len(sectors_to_fetch)} 个 (并发: {CONCURRENT_WORKERS})...")
        failed_sectors = []
        
        with concurrent.futures.ThreadPoolExecutor(max_workers=CONCURRENT_WORKERS) as executor:
            future_map = {executor.submit(fetch_one_sector, row): row for row in sectors_to_fetch}
            
            for future in tqdm(concurrent.futures.as_completed(future_map), total=len(future_map), desc=f"第 {attempt + 1} 轮"):
                row = future_map[future]
                try:
                    k_succ, c_succ, k_list, c_list, err_msg = future.result()
                    
                    # 只要 K 线成功，就直接入库，成份股丢了也不陪葬重试
                    if k_succ:
                        all_k_flat.extend(k_list)
                        if c_succ:
                            all_c_flat.extend(c_list)
                    else:
                        row['err_msg'] = err_msg
                        failed_sectors.append(row)
                except Exception as e:
                    row['err_msg'] = str(e)
                    failed_sectors.append(row)
                    
        sectors_to_fetch = failed_sectors
        if sectors_to_fetch and attempt < MAX_RETRIES - 1:
            time.sleep(3)

    if sectors_to_fetch:
        print(f"\n[!] 仍有 {len(sectors_to_fetch)} 个顽固板块，启动单线低速补抓...")
        final_failed_sectors = []
        for row in tqdm(sectors_to_fetch, desc="单线补抓"):
            time.sleep(0.5) 
            k_succ, c_succ, k_list, c_list, err_msg = fetch_one_sector(row)
            if k_succ:
                all_k_flat.extend(k_list)
                if c_succ: all_c_flat.extend(c_list)
            else:
                row['err_msg'] = err_msg
                final_failed_sectors.append(row)
        sectors_to_fetch = final_failed_sectors

    if sectors_to_fetch:
        print(f"\n[!] 警告: {len(sectors_to_fetch)} 个板块的K线彻底失败:")
        for s in sectors_to_fetch[:10]:
            print(f"    - {s['name']} ({s['code']}) | 失败原因: {s.get('err_msg')}")
    else:
        print("\n[+] 完美！所有板块的 K线 数据均已成功采集。")

    print(f"\n[*] 正在清洗落库...")
    cleaner = DataCleaner()
    today_dt = pd.Timestamp.now().normalize()
    
    if all_k_flat:
        full_k = pd.DataFrame(all_k_flat)
        full_k['date_dt'] = pd.to_datetime(full_k['date'].astype(str).str.strip(), errors='coerce')
        full_k = full_k[(full_k['date_dt'] <= today_dt) & (full_k['date_dt'].notnull())]
        full_k = full_k.drop(columns=['date_dt'])
        
        full_k = cleaner.clean_sector_kline(full_k)
        full_k.to_parquet(f"{OUTPUT_DIR}/sector_kline_full.parquet", index=False)
        print(f"[+] K线数据存储成功: {len(full_k)} 行")

    if all_c_flat:
        full_c = pd.DataFrame(all_c_flat)
        full_c['date'] = today_dt.strftime('%Y-%m-%d')
        full_c = full_c.drop_duplicates(subset=['sector_code', 'stock_code'])
        full_c.to_parquet(f"{OUTPUT_DIR}/sector_constituents_latest.parquet", index=False)
        print(f"[+] 成份股关系存储成功: {len(full_c)} 条映射对")

    print(f"\n{'='*70}\n[*] 任务完成 | 耗时: {datetime.now() - start_time}\n{'='*70}\n")

if __name__ == "__main__":
    main()
