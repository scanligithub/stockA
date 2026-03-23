import sys
import os
import pandas as pd
import concurrent.futures
from tqdm import tqdm
from datetime import datetime
import time

# 确保能正确加载项目本地模块
sys.path.append(os.getcwd())
# 注意：确保你的 utils.cf_proxy 和 utils.cleaner 已经就绪
from utils.cf_proxy import EastMoneyProxy
from utils.cleaner import DataCleaner

OUTPUT_DIR = "temp_parts"
os.makedirs(OUTPUT_DIR, exist_ok=True)
proxy = EastMoneyProxy()

# ==========================================
# 第一阶段：极速并发扫描板块目录 (20维度全空间覆盖)
# ==========================================

def fetch_single_dimension(fs_code, fid, po):
    """单维探针：只取 100 条，绝对安全不截断"""
    # 模拟东财 API 请求
    return proxy.get_sector_list(fs_code, fid=fid, po=po, pn=1, pz=100)

def get_category_full_data_brute_force(fs_code, label):
    """
    【并发包抄】20 维度正反扫描，强行榨干最后一滴数据
    """
    print(f"[*] 正在对 [{label}] 执行 20 维度并发探测...")
    seen_codes = {}
    
    # 20 个不同视角的物理属性，彻底打碎“死水板块”
    fids = [
        "f12", "f3", "f2", "f6", "f5", "f4", "f17", "f18", "f8", "f10", 
        "f15", "f16", "f11", "f9", "f23", "f20", "f21", "f22", "f24", "f25"
    ]
    tasks = [(fid, po) for fid in fids for po in [1, 0]]
            
    with concurrent.futures.ThreadPoolExecutor(max_workers=15) as executor:
        future_map = {executor.submit(fetch_single_dimension, fs_code, f, p): (f, p) for f, p in tasks}
        for future in concurrent.futures.as_completed(future_map):
            try:
                items = future.result()
                if not items: continue
                for x in items:
                    c = x['f12']
                    if c not in seen_codes:
                        seen_codes[c] = {"code": c, "market": x['f13'], "name": x['f14'], "type": label}
            except: 
                pass

    print(f"    [✓] [{label}] 扫描完成，捕获: {len(seen_codes)} 个唯一板块")
    return list(seen_codes.values())

# ==========================================
# 第二阶段：分类特征分析日志 (这是你需要的核心输出)
# ==========================================

def log_classification_rules(all_sectors):
    """
    输出三个维度的完整代码清单，用于人工分析分类规律
    """
    df = pd.DataFrame(all_sectors)
    
    print(f"\n{'#'*30} 分类规则分析报告 {'#'*30}")
    
    categories = {
        "Industry (行业)": "Industry",
        "Concept (概念)": "Concept",
        "Region (地域)": "Region"
    }
    
    for label, type_name in categories.items():
        subset = df[df['type'] == type_name].sort_values('code')
        codes = subset['code'].tolist()
        
        print(f"\n>>> [{label}] 原始代码清单 (共 {len(codes)} 个):")
        # 每 10 个换一行打印，方便肉眼识别区间
        for i in range(0, len(codes), 10):
            print(", ".join(codes[i:i+10]))
        
        if len(codes) > 0:
            print(f"--- 区间特征: Min={min(codes)}, Max={max(codes)}")
            
    print(f"\n{'#'*77}\n")

# ==========================================
# 第三阶段：成份股与详情 (保持原逻辑)
# ==========================================

def fetch_one_sector(info):
    code, name = info['code'], info['name']
    
    # 抓取 K 线
    res_k = proxy.get_sector_kline(f"90.{code}")
    k_data = []
    if res_k and res_k.get('data') and res_k['data'].get('klines'):
        for row_str in res_k['data']['klines']:
            r = row_str.split(',')
            k_data.append({
                'date': r[0], 'open': r[1], 'close': r[2], 'high': r[3], 'low': r[4], 
                'volume': r[5], 'amount': r[6], 'code': code, 'name': name, 'type': info['type']
            })

    # 抓取成份股
    res_c = proxy.get_sector_constituents(code)
    consts = []
    if res_c and res_c.get('data') and res_c['data'].get('diff'):
        items_list = res_c['data']['diff']
        # 兼容字典或列表格式
        if isinstance(items_list, dict): items_list = items_list.values()
        for item in items_list:
            consts.append({"sector_code": code, "stock_code": item['f12'], "sector_name": name})
            
    return k_data, consts

def main():
    start_time = datetime.now()
    print(f"\n{'='*70}\n[*] 东方财富极速全量引擎 (规则分析版) | {start_time}\n{'='*70}\n")
    
    # 1. 扫描分类目录
    targets = {"Industry": "m:90 t:2", "Concept": "m:90 t:3", "Region": "m:90 t:1"}
    all_sectors_list = []
    for label, fs in targets.items():
        all_sectors_list.extend(get_category_full_data_brute_force(fs, label))

    # 2. 输出分析日志 (核心新增)
    log_classification_rules(all_sectors_list)

    df_list = pd.DataFrame(all_sectors_list).drop_duplicates('code')
    total_found = len(df_list)

    # 3. 采集详情
    print(f"[*] 开始并发采集 {total_found} 个板块的详细数据...")
    all_k_flat, all_c_flat = [], []
    with concurrent.futures.ThreadPoolExecutor(max_workers=20) as executor:
        future_map = {executor.submit(fetch_one_sector, row): row['name'] for _, row in df_list.iterrows()}
        for future in tqdm(concurrent.futures.as_completed(future_map), total=len(future_map), desc="下载进度"):
            try:
                k_list, c_list = future.result()
                if k_list: all_k_flat.extend(k_list)
                if c_list: all_c_flat.extend(c_list)
            except: pass

    # 4. 落库
    cleaner = DataCleaner()
    today_str = pd.Timestamp.now().strftime('%Y-%m-%d')
    
    if all_k_flat:
        df_k = pd.DataFrame(all_k_flat)
        df_k = cleaner.clean_sector_kline(df_k) # 假设 cleaner 有此方法
        df_k.to_parquet(f"{OUTPUT_DIR}/sector_kline_full.parquet", index=False)
        print(f"[+] K线入库: {len(df_k)} 行")

    if all_c_flat:
        df_c = pd.DataFrame(all_c_flat)
        df_c['sync_date'] = today_str
        df_c.to_parquet(f"{OUTPUT_DIR}/sector_constituents_latest.parquet", index=False)
        print(f"[+] 映射对入库: {len(df_c)} 条")

    print(f"\n{'='*70}\n[*] 任务完成 | 耗时: {datetime.now() - start_time}\n{'='*70}\n")

if __name__ == "__main__":
    main()
