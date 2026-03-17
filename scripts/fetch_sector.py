import sys
import os
import pandas as pd
import concurrent.futures
from tqdm import tqdm
from datetime import datetime

# 加载项目本地模块
sys.path.append(os.getcwd())
from utils.cf_proxy import EastMoneyProxy
from utils.cleaner import DataCleaner

OUTPUT_DIR = "temp_parts"
os.makedirs(OUTPUT_DIR, exist_ok=True)
proxy = EastMoneyProxy()

def fetch_one_sector(info):
    """
    抓取单个板块的 K 线和成份股
    """
    code, name, mkt = info['code'], info['name'], info['market']
    secid = f"90.{code}"
    
    res_k = proxy.get_sector_kline(secid)
    df_k = pd.DataFrame()
    
    if res_k and res_k.get('data') and res_k['data'].get('klines'):
        rows = [x.split(',') for x in res_k['data']['klines']]
        # 你的 Proxy 定义了 8 个字段 (f51~f58)
        cols = ['date','open','close','high','low','volume','amount','amplitude']
        df_k = pd.DataFrame([r[:8] for r in rows], columns=cols)
        df_k['code'], df_k['name'], df_k['type'] = code, name, info['type']

    res_c = proxy.get_sector_constituents(code)
    consts = []
    if res_c and res_c.get('data') and res_c['data'].get('diff'):
        diff_data = res_c['data']['diff']
        items = diff_data.values() if isinstance(diff_data, dict) else diff_data
        for item in items:
            consts.append({"sector_code": code, "stock_code": item['f12'], "sector_name": name})
            
    return df_k, consts

def main():
    print(f"\n{'='*70}")
    print(f"[*] 量化级板块全量对冲采集引擎 | {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"{'='*70}\n")

    targets = {"Industry": "m:90 t:2", "Concept": "m:90 t:3", "Region": "m:90 t:1"}
    all_sectors_data = []

    print("[1/3] 正在执行正反双向对冲抓取...")
    for label, fs in targets.items():
        # 1. 正向抓取前 500 (实际会被截断到 400 左右)
        top_list = proxy.get_sector_list(fs, po=1)
        # 2. 反向抓取后 500 (拿到长尾数据)
        bottom_list = proxy.get_sector_list(fs, po=0)
        
        combined = top_list + bottom_list
        # 去重
        seen = set()
        unique_cat = []
        for x in combined:
            if x['f12'] not in seen:
                unique_cat.append(x)
                seen.add(x['f12'])
        
        print(f"    - {label}: 正向({len(top_list)}) + 反向({len(bottom_list)}) -> 聚合去重后: {len(unique_cat)}")
        
        for x in unique_cat:
            all_sectors_data.append({
                "code": x['f12'], "market": x['f13'], "name": x['f14'], "type": label
            })

    df_list = pd.DataFrame(all_sectors_data).drop_duplicates('code')
    total_found = len(df_list)
    print(f"\n[*] 审计报告：最终唯一板块总数: {total_found}")

    if total_found < 900:
        print("[🔥 严重警告] 板块总数仍不足 900，请检查 Proxy 的 pz 是否设置正确！")
        sys.exit(1)

    # 2. 并发抓取
    print(f"\n[2/3] 开始并发抓取 {total_found} 个板块详情...")
    all_k, all_c = [], []
    with concurrent.futures.ThreadPoolExecutor(max_workers=20) as executor:
        future_to_name = {executor.submit(fetch_one_sector, row): row['name'] for _, row in df_list.iterrows()}
        for future in tqdm(concurrent.futures.as_completed(future_to_name), total=len(future_to_name), desc="采集进度"):
            try:
                k, c = future.result()
                if not k.empty: all_k.append(k)
                if c: all_c.extend(c)
            except: pass

    # 3. 清洗落地
    cleaner = DataCleaner()
    today_str = datetime.now().strftime('%Y-%m-%d')
    if all_k:
        full_k = pd.concat(all_k)
        full_k = full_k[full_k['date'] <= today_str]
        full_k = cleaner.clean_sector_kline(full_k)
        full_k.to_parquet(f"{OUTPUT_DIR}/sector_kline_full.parquet", index=False)
        print(f"[+] K线存储成功: {len(full_k)} 行")
        
    if all_c:
        full_c = pd.DataFrame(all_c)
        full_c['date'] = today_str
        full_c.to_parquet(f"{OUTPUT_DIR}/sector_constituents_latest.parquet", index=False)
        print(f"[+] 成份股存储成功: {len(full_c)} 条关系")

    print(f"\n{'='*70}\n[*] 任务圆满完成\n{'='*70}\n")

if __name__ == "__main__":
    main()
