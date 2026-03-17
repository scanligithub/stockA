import sys
import os
import pandas as pd
import concurrent.futures
from tqdm import tqdm
from datetime import datetime
import time

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
    code, name = info['code'], info['name']
    secid = f"90.{code}"
    
    # 1. 抓取 K 线
    res_k = proxy.get_sector_kline(secid)
    df_k = pd.DataFrame()
    if res_k and res_k.get('data') and res_k['data'].get('klines'):
        rows = [x.split(',') for x in res_k['data']['klines']]
        # 映射 Proxy fields2 的 8 个字段
        cols = ['date','open','close','high','low','volume','amount','amplitude']
        df_k = pd.DataFrame([r[:8] for r in rows], columns=cols)
        df_k['code'], df_k['name'], df_k['type'] = code, name, info['type']

    # 2. 抓取成份股
    res_c = proxy.get_sector_constituents(code)
    consts = []
    if res_c and res_c.get('data') and res_c['data'].get('diff'):
        diff_data = res_c['data']['diff']
        items_list = diff_data.values() if isinstance(diff_data, dict) else diff_data
        for item in items_list:
            consts.append({
                "sector_code": code, 
                "stock_code": item['f12'], 
                "sector_name": name
            })
            
    return df_k, consts

def get_category_full_data_brute_force(fs_code, label):
    """
    【暴力包抄 V4】10维度正反扫描，强行挤出全量数据
    """
    print(f"[*] 正在对 {label} 分类执行 10 维度暴力包抄...")
    seen_codes = {}
    
    # 使用 10 个互不相关的排序物理属性
    fids = ["f12", "f3", "f2", "f6", "f5", "f4", "f17", "f18", "f8", "f10"]
    
    for fid in fids:
        for po in [1, 0]:
            try:
                # 严格使用 pz=100
                items = proxy.get_sector_list(fs_code, fid=fid, po=po, pn=1, pz=100)
                if not items: continue
                
                new_in_round = 0
                for x in items:
                    c = x['f12']
                    if c not in seen_codes:
                        seen_codes[c] = {
                            "code": c, "market": x['f13'], 
                            "name": x['f14'], "type": label
                        }
                        new_in_round += 1
                
                if new_in_round > 0:
                    print(f"    - [维度 {fid:<3} po={po}] 吐出: 100 | 新增: {new_in_round:<3} | 累计: {len(seen_codes)}")
                
                # 行业/概念板块单分类通常在 550 以内，达到此数可提前结束
                if len(seen_codes) >= 550: break
                time.sleep(0.1)
            except: 
                pass
        if len(seen_codes) >= 550: break
            
    return list(seen_codes.values())

def main():
    start_time = datetime.now()
    print(f"\n{'='*70}\n[*] 东方财富全量板块引擎 (954版) | {start_time}\n{'='*70}\n")
    
    # 1. 维度包抄获取列表
    targets = {"Industry": "m:90 t:2", "Concept": "m:90 t:3", "Region": "m:90 t:1"}
    all_sectors = []
    for label, fs in targets.items():
        all_sectors.extend(get_category_full_data_brute_force(fs, label))

    df_list = pd.DataFrame(all_sectors).drop_duplicates('code')
    total_found = len(df_list)
    print(f"\n[*] 审计报告：去重后唯一板块总数: {total_found}")

    if total_found < 900:
        print(f"[🔥 严重错误] 抓取总数 {total_found} 异常，停止后续采集。")
        sys.exit(1)

    # 2. 并发采集详情
    print(f"\n[*] 开始并发采集 {total_found} 个板块的历史 K 线与成份股 (并发: 20)...")
    all_k, all_c = [], []
    with concurrent.futures.ThreadPoolExecutor(max_workers=20) as executor:
        future_to_name = {
            executor.submit(fetch_one_sector, row): row['name'] 
            for _, row in df_list.iterrows()
        }
        
        for future in tqdm(concurrent.futures.as_completed(future_to_name), 
                          total=len(future_to_name), desc="总进度"):
            try:
                k, c = future.result()
                if not k.empty: all_k.append(k)
                if c: all_c.extend(c)
            except:
                pass

    # 3. 数据清洗与落地
    print(f"\n[*] 正在清洗数据并生成 Parquet 文件...")
    cleaner = DataCleaner()
    today_str = datetime.now().strftime('%Y-%m-%d')
    
    if all_k:
        full_k = pd.concat(all_k)
        # 关键：过滤掉未来日期的脏数据
        full_k = full_k[full_k['date'] <= today_str]
        # 调用清洗逻辑（转换类型等）
        full_k = cleaner.clean_sector_kline(full_k)
        
        k_path = f"{OUTPUT_DIR}/sector_kline_full.parquet"
        full_k.to_parquet(k_path, index=False)
        print(f"[+] K线存储成功: {len(full_k)} 行 | 覆盖日期至 {full_k['date'].max()}")

    if all_c:
        full_c = pd.DataFrame(all_c)
        full_c['date'] = today_str
        c_path = f"{OUTPUT_DIR}/sector_constituents_latest.parquet"
        full_c.to_parquet(c_path, index=False)
        print(f"[+] 成份股存储成功: {len(full_c)} 条映射关系")

    print(f"\n{'='*70}\n[*] 任务圆满完成 | 耗时: {datetime.now() - start_time}\n{'='*70}\n")

if __name__ == "__main__":
    main()
