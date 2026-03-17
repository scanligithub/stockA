import sys
import os
import pandas as pd
import concurrent.futures
from tqdm import tqdm
from datetime import datetime
import time

sys.path.append(os.getcwd())
from utils.cf_proxy import EastMoneyProxy
from utils.cleaner import DataCleaner

OUTPUT_DIR = "temp_parts"
os.makedirs(OUTPUT_DIR, exist_ok=True)
proxy = EastMoneyProxy()

def fetch_one_sector(info):
    code, name = info['code'], info['name']
    res_k = proxy.get_sector_kline(f"90.{code}")
    df_k = pd.DataFrame()
    if res_k and res_k.get('data') and res_k['data'].get('klines'):
        rows = [x.split(',') for x in res_k['data']['klines']]
        cols = ['date','open','close','high','low','volume','amount','amplitude']
        df_k = pd.DataFrame([r[:8] for r in rows], columns=cols)
        df_k['code'], df_k['name'], df_k['type'] = code, name, info['type']
    
    res_c = proxy.get_sector_constituents(code)
    consts = []
    if res_c and res_c.get('data') and res_c['data'].get('diff'):
        items = res_c['data']['diff']
        items_list = items.values() if isinstance(items, dict) else items
        for item in items_list:
            consts.append({"sector_code": code, "stock_code": item['f12'], "sector_name": name})
    return df_k, consts

def get_category_full_data_v2(fs_code, label):
    """
    【首屏包抄策略】不再翻页，通过变换排序维度(fid)获取全量
    """
    print(f"[*] 正在全维度扫描 {label} 分类...")
    seen_codes = {}
    
    # 维度组合: (排序字段fid, 排序方向po)
    # 通过 4 个不同的物理属性排序，确保所有板块都能出现在某一次的前100名中
    dimensions = [
        ("f3", 1), ("f3", 0),  # 涨跌幅 顶/底
        ("f2", 1), ("f2", 0),  # 最新价 顶/底
        ("f12", 1), ("f12", 0), # 代码 顶/底
        ("f6", 1), ("f6", 0)   # 成交额 顶/底
    ]

    for fid, po in dimensions:
        try:
            items = proxy.get_sector_list(fs_code, fid=fid, po=po, pn=1, pz=100)
            if not items: continue
            
            new_count = 0
            for x in items:
                c = x['f12']
                if c not in seen_codes:
                    seen_codes[c] = {"code": c, "market": x['f13'], "name": x['f14'], "type": label}
                    new_count += 1
            if new_count > 0:
                print(f"    - [维度 {fid} po={po}] 新获: {new_count} 条 | 累计: {len(seen_codes)}")
            time.sleep(0.2)
        except: pass
    
    return list(seen_codes.values())

def main():
    print(f"\n{'='*70}\n[*] 维度包抄采集引擎 V2 | {datetime.now()}\n{'='*70}\n")
    targets = {"Industry": "m:90 t:2", "Concept": "m:90 t:3", "Region": "m:90 t:1"}
    all_sectors = []
    for label, fs in targets.items():
        all_sectors.extend(get_category_full_data_v2(fs, label))

    df_list = pd.DataFrame(all_sectors).drop_duplicates('code')
    total_found = len(df_list)
    print(f"\n[*] 审计报告：去重后唯一板块总数: {total_found}")

    # 现在的逻辑非常稳，阈值可以设为 950
    if total_found < 900:
        print(f"[🔥 严重错误] 总数仍不足，停止任务。")
        sys.exit(1)

    print(f"\n[*] 开始并发采集详情...")
    all_k, all_c = [], []
    with concurrent.futures.ThreadPoolExecutor(max_workers=20) as executor:
        f_map = {executor.submit(fetch_one_sector, r): r['name'] for _, r in df_list.iterrows()}
        for f in tqdm(concurrent.futures.as_completed(f_map), total=len(f_map), desc="进度"):
            k, c = f.result()
            if not k.empty: all_k.append(k)
            if c: all_c.extend(c)

    cleaner = DataCleaner()
    today = datetime.now().strftime('%Y-%m-%d')
    if all_k:
        df_k = pd.concat(all_k)
        df_k = cleaner.clean_sector_kline(df_k[df_k['date'] <= today])
        df_k.to_parquet(f"{OUTPUT_DIR}/sector_kline_full.parquet", index=False)
    if all_c:
        pd.DataFrame(all_c).to_parquet(f"{OUTPUT_DIR}/sector_constituents_latest.parquet", index=False)
    print(f"\n{'='*70}\n[*] 任务圆满完成\n{'='*70}\n")

if __name__ == "__main__":
    main()
