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

def get_category_full_data_v3(fs_code, label):
    """
    【250对冲策略】利用 pz=250 的正反两次请求实现全量覆盖
    """
    print(f"[*] 正在执行 250-对冲扫描 {label} 分类...")
    seen_codes = {}
    
    # 只需要 2 个不同的维度，每个维度正反各一次，总计 4 次请求
    # 4 * 250 = 1000 个槽位，覆盖 500 个目标绰绰有余
    dimensions = [
        ("f3", 1), ("f3", 0),  # 涨跌幅 顶/底 (最常用)
        ("f12", 1), ("f12", 0) # 代码 顶/底 (最稳定)
    ]

    for fid, po in dimensions:
        try:
            # 强制 pz=250
            items = proxy.get_sector_list(fs_code, fid=fid, po=po, pn=1, pz=250)
            if not items: continue
            
            actual_len = len(items)
            new_count = 0
            for x in items:
                c = x['f12']
                if c not in seen_codes:
                    seen_codes[c] = {"code": c, "market": x['f13'], "name": x['f14'], "type": label}
                    new_count += 1
            
            print(f"    - [维度 {fid} po={po}] 吐出: {actual_len} | 新增: {new_count} | 累计: {len(seen_codes)}")
            time.sleep(0.3)
        except: pass
    
    return list(seen_codes.values())

def main():
    start_time = datetime.now()
    print(f"\n{'='*70}\n[*] 250-对冲采集引擎 V3 | {start_time}\n{'='*70}\n")
    
    targets = {"Industry": "m:90 t:2", "Concept": "m:90 t:3", "Region": "m:90 t:1"}
    all_sectors = []
    for label, fs in targets.items():
        all_sectors.extend(get_category_full_data_v3(fs, label))

    df_list = pd.DataFrame(all_sectors).drop_duplicates('code')
    total_found = len(df_list)
    print(f"\n[*] 审计报告：去重后唯一板块总数: {total_found}")

    # 现在的逻辑如果跑通，总数应该在 1005 左右
    if total_found < 980:
        print(f"[🔥 严重错误] 板块总数 {total_found} 仍低于 980，怀疑 pz=250 被降级为 100 了。")
        sys.exit(1)

    print(f"\n[*] 开始并发采集详情...")
    all_k, all_c = [], []
    with concurrent.futures.ThreadPoolExecutor(max_workers=20) as executor:
        f_map = {executor.submit(fetch_one_sector, r): r['name'] for _, r in df_list.iterrows()}
        for f in tqdm(concurrent.futures.as_completed(f_map), total=len(f_map), desc="总进度"):
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
    
    print(f"\n{'='*70}\n[*] 任务圆满完成 | 最终板块数: {total_found}\n{'='*70}\n")

if __name__ == "__main__":
    main()
