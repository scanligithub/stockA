import sys
import os
import pandas as pd
import concurrent.futures
from tqdm import tqdm
from datetime import datetime


# 确保能正确加载项目本地模块
sys.path.append(os.getcwd())
from utils.cf_proxy import EastMoneyProxy
from utils.cleaner import DataCleaner
from utils.sector_catalog_builder import build_sector_catalog

OUTPUT_DIR = "temp_parts"
os.makedirs(OUTPUT_DIR, exist_ok=True)
proxy = EastMoneyProxy()

# ==========================================
# 第一阶段：三步式获取完整板块列表和分类
# ==========================================


# ==========================================
# 第二阶段：并发抓取详细 K 线与成份股 (内存级优化)
# ==========================================

def fetch_one_sector(info):
    """
    内存优化：直接返回原生字典列表，避免在多线程中频繁实例化 DataFrame
    """
    code, name = info['code'], info['name']
    secid = f"90.{code}"
    
    res_k = proxy.get_sector_kline(secid)
    k_data = []
    if res_k and res_k.get('data') and res_k['data'].get('klines'):
        for row_str in res_k['data']['klines']:
            r = row_str.split(',')
            # 扁平化字典，极大降低内存碎片
            k_data.append({
                'date': r[0], 'open': r[1], 'close': r[2], 'high': r[3], 'low': r[4], 
                'volume': r[5], 'amount': r[6], 'amplitude': r[7],
                'code': code, 'name': name, 'type': info['type']
            })

    res_c = proxy.get_sector_constituents(code)
    consts = []
    if res_c and res_c.get('data') and res_c['data'].get('diff'):
        diff_data = res_c['data']['diff']
        items_list = diff_data.values() if isinstance(diff_data, dict) else diff_data
        for item in items_list:
            consts.append({"sector_code": code, "stock_code": item['f12'], "sector_name": name})
            
    return k_data, consts

def main():
    start_time = datetime.now()
    print(f"\n{'='*70}\n[*] 东方财富极速全量引擎 (终极定稿版) | {start_time}\n{'='*70}\n")
    
    # 1. 三步式获取完整板块列表和分类
    all_sectors = build_sector_catalog(proxy)


    df_list = pd.DataFrame(all_sectors).drop_duplicates('code')
    total_found = len(df_list)
    print(f"\n[*] 审计报告：去重后唯一板块总数: {total_found}")

    if total_found < 950:
        print(f"[🔥 严重警告] 抓取总数 {total_found} 偏低，停止后续采集防污染。")
        sys.exit(1)

    # 2. 并发采集详情
    print(f"\n[*] 开始并发采集 {total_found} 个板块的历史数据 (并发线程: 20)...")
    all_k_flat, all_c_flat = [], []
    with concurrent.futures.ThreadPoolExecutor(max_workers=20) as executor:
        future_map = {executor.submit(fetch_one_sector, row): row['name'] for _, row in df_list.iterrows()}
        for future in tqdm(concurrent.futures.as_completed(future_map), total=len(future_map), desc="下载进度"):
            try:
                k_list, c_list = future.result()
                if k_list: all_k_flat.extend(k_list)
                if c_list: all_c_flat.extend(c_list)
            except Exception:
                pass


    # 3. 清洗、时间线封锁与落库
    print(f"\n[*] 正在清洗合并数百万行数据并生成 Parquet 压缩文件...")
    cleaner = DataCleaner()
    today_dt = pd.Timestamp.now().normalize()
    
    if all_k_flat:
        # 一次性建表，速度极快
        full_k = pd.DataFrame(all_k_flat)
        
        # [核心防线]：物理切断所有未来日期数据 (例如 2026 年)
        full_k['date_dt'] = pd.to_datetime(full_k['date'].astype(str).str.strip(), errors='coerce')
        full_k = full_k[(full_k['date_dt'] <= today_dt) & (full_k['date_dt'].notnull())]
        full_k = full_k.drop(columns=['date_dt'])
        
        full_k = cleaner.clean_sector_kline(full_k)
        full_k.to_parquet(f"{OUTPUT_DIR}/sector_kline_full.parquet", index=False)
        print(f"[+] K线数据存储成功: {len(full_k)} 行 | 真实覆盖日期至 {full_k['date'].max()}")

    if all_c_flat:
        full_c = pd.DataFrame(all_c_flat)
        full_c['date'] = today_dt.strftime('%Y-%m-%d')
        full_c.to_parquet(f"{OUTPUT_DIR}/sector_constituents_latest.parquet", index=False)
        print(f"[+] 成份股关系存储成功: {len(full_c)} 条映射对")

    print(f"\n{'='*70}\n[*] 任务圆满完成 | 入库板块数: {total_found} | 耗时: {datetime.now() - start_time}\n{'='*70}\n")

if __name__ == "__main__":
    main()
