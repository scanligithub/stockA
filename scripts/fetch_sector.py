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

def get_category_full_data_brute_force(fs_code, label):
    """
    【暴力包抄 V4】10维度正反扫描，强行挤出全量数据
    """
    print(f"[*] 正在对 {label} 分类执行 10 维度暴力包抄...")
    seen_codes = {}
    
    # 10 个完全不同的排序物理属性 (f12:代码, f3:涨跌幅, f2:价格, f6:成交额, f5:最高, f4:最低, f17:开盘, f18:昨收, f8:换手, f10:振幅)
    fids = ["f12", "f3", "f2", "f6", "f5", "f4", "f17", "f18", "f8", "f10"]
    
    for fid in fids:
        for po in [1, 0]: # 每个维度都扫正反两次
            try:
                items = proxy.get_sector_list(fs_code, fid=fid, po=po, pn=1, pz=100)
                if not items: continue
                
                new_in_this_round = 0
                for x in items:
                    c = x['f12']
                    if c not in seen_codes:
                        seen_codes[c] = {"code": c, "market": x['f13'], "name": x['f14'], "type": label}
                        new_in_this_round += 1
                
                if new_in_this_round > 0:
                    print(f"    - [维度 {fid:<3} po={po}] 吐出: 100 | 新增: {new_in_this_round:<3} | 累计: {len(seen_codes)}")
                
                # 如果累计数量已经达到一个合理的饱和值（比如 480+），可以提前结束该分类
                if len(seen_codes) >= 550: break # 板块分类通常不会超过 550 个
                time.sleep(0.1) # 极短休眠
            except: pass
        if len(seen_codes) >= 550: break
            
    return list(seen_codes.values())

def main():
    start_time = datetime.now()
    print(f"\n{'='*70}\n[*] 暴力包抄采集引擎 V4 | {start_time}\n{'='*70}\n")
    
    targets = {"Industry": "m:90 t:2", "Concept": "m:90 t:3", "Region": "m:90 t:1"}
    all_sectors = []
    for label, fs in targets.items():
        all_sectors.extend(get_category_full_data_brute_force(fs, label))

    df_list = pd.DataFrame(all_sectors).drop_duplicates('code')
    total_found = len(df_list)
    print(f"\n[*] 审计报告：去重后唯一板块总数: {total_found}")

    # 这套逻辑下，1005 个板块应该是手到擒来
    if total_found < 950:
        print(f"[🔥 严重错误] 暴力包抄后总数仍只有 {total_found}，检查 API 或 fs 过滤码。")
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
    
    print(f"\n{'='*70}\n[*] 任务圆满完成 | 最终板块数: {total_found} | 耗时: {datetime.now()-start_time}\n{'='*70}\n")

if __name__ == "__main__":
    main()
