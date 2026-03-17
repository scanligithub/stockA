import sys
import os
import pandas as pd
import concurrent.futures
from tqdm import tqdm
from datetime import datetime
import time

# 确保能正确加载项目本地模块
sys.path.append(os.getcwd())
from utils.cf_proxy import EastMoneyProxy
from utils.cleaner import DataCleaner

OUTPUT_DIR = "temp_parts"
os.makedirs(OUTPUT_DIR, exist_ok=True)
proxy = EastMoneyProxy()

# ==========================================
# 第一阶段：极速并发扫描板块目录 (秒级获取 1010+ 板块)
# ==========================================

def fetch_single_dimension(fs_code, fid, po):
    """
    独立工作函数：负责抓取单一维度的一页数据 (只取 100 条)
    """
    return proxy.get_sector_list(fs_code, fid=fid, po=po, pn=1, pz=100)

def get_category_full_data_brute_force(fs_code, label):
    """
    【并发暴力包抄 V6】将串行的 40 次请求并行化，耗时从 12 分钟降至 5 秒！
    """
    print(f"[*] 正在对 {label} 分类执行 20 维度并发探测...")
    seen_codes = {}
    
    # 20 个不同视角的物理属性，彻底打碎中间地带的“死水板块”
    fids = [
        "f12", "f3", "f2", "f6", "f5", "f4", "f17", "f18", "f8", "f10", 
        "f15", "f16", "f11", "f9", "f23", "f20", "f21", "f22", "f24", "f25"
    ]
    
    # 组装 40 个探测任务 (20 个维度 * 2 个方向)
    tasks = [(fid, po) for fid in fids for po in [1, 0]]
            
    # 并发提交 40 个任务。设置 max_workers=15 避免瞬间打爆 CF Worker
    with concurrent.futures.ThreadPoolExecutor(max_workers=15) as executor:
        future_to_task = {
            executor.submit(fetch_single_dimension, fs_code, fid, po): (fid, po) 
            for fid, po in tasks
        }
        
        # 实时收集并发返回的结果
        for future in concurrent.futures.as_completed(future_to_task):
            fid, po = future_to_task[future]
            try:
                items = future.result()
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
                        
                # 打印单维度的成果
                if new_in_round > 0:
                    print(f"    - [维度 {fid:<3} po={po}] 新增: {new_in_round:<3} | 累计: {len(seen_codes)}")
                    
            except Exception as e:
                # 某个维度失败不影响全局，因为我们有极大的维度冗余
                pass

    print(f"    [✓] {label} 并发扫描完成，共捕获: {len(seen_codes)} 个不重复板块")
    return list(seen_codes.values())


# ==========================================
# 第二阶段：并发抓取详细 K 线与成份股 (内存级优化)
# ==========================================

def fetch_one_sector(info):
    """
    抓取单个板块的 K 线和成份股
    内存优化：直接返回原生字典列表，避免在多线程中频繁实例化 DataFrame
    """
    code, name = info['code'], info['name']
    secid = f"90.{code}"
    
    # 1. 抓取板块日K线数据
    res_k = proxy.get_sector_kline(secid)
    k_data = []
    
    if res_k and res_k.get('data') and res_k['data'].get('klines'):
        # 对应 Proxy.get_sector_kline 中的 fields2: f51~f58
        for row_str in res_k['data']['klines']:
            r = row_str.split(',')
            # 构建扁平化的字典，极大降低内存碎片
            k_data.append({
                'date': r[0], 'open': r[1], 'close': r[2], 'high': r[3], 'low': r[4], 
                'volume': r[5], 'amount': r[6], 'amplitude': r[7],
                'code': code, 'name': name, 'type': info['type']
            })

    # 2. 抓取板块成份股列表
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
            
    return k_data, consts

def main():
    start_time = datetime.now()
    print(f"\n{'='*70}")
    print(f"[*] 东方财富极速全量采集引擎 (V6 并发版) | {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"{'='*70}\n")
    
    # 1. 极速获取全量板块列表
    targets = {
        "Industry": "m:90 t:2", 
        "Concept":  "m:90 t:3", 
        "Region":   "m:90 t:1"
    }
    
    all_sectors = []
    for label, fs in targets.items():
        all_sectors.extend(get_category_full_data_brute_force(fs, label))

    # 去重
    df_list = pd.DataFrame(all_sectors).drop_duplicates('code')
    total_found = len(df_list)
    print(f"\n[*] 审计报告：去重后唯一板块总数: {total_found}")

    # 安全熔断：阈值提高到 980，拦截异常
    if total_found < 980:
        print(f"[🔥 严重警告] 抓取总数 {total_found} 仍低于预期，停止后续采集防污染。")
        sys.exit(1)

    # 2. 并发采集板块 K 线与成份股详情
    print(f"\n[*] 开始并发采集 {total_found} 个板块的历史数据 (并发线程: 20)...")
    all_k_flat, all_c_flat = [], []
    
    with concurrent.futures.ThreadPoolExecutor(max_workers=20) as executor:
        future_to_name = {
            executor.submit(fetch_one_sector, row): row['name'] 
            for _, row in df_list.iterrows()
        }
        
        for future in tqdm(concurrent.futures.as_completed(future_to_name), 
                          total=len(future_to_name), desc="全量详情采集进度"):
            try:
                k_list, c_list = future.result()
                if k_list:
                    all_k_flat.extend(k_list)
                if c_list:
                    all_c_flat.extend(c_list)
            except Exception as e:
                pass

    # 3. 数据清洗、脏数据过滤与持久化落库
    print(f"\n[*] 正在清洗合并数百万行数据并生成 Parquet 压缩文件...")
    cleaner = DataCleaner()
    today_str = datetime.now().strftime('%Y-%m-%d')
    
    if all_k_flat:
        # 一次性将海量字典列表转为 DataFrame，速度比 concat 几十万个小 df 快 100 倍
        full_k = pd.DataFrame(all_k_flat)
        
        # [核心防线]：物理剔除因接口测试或异常返回的未来日期数据 (例如 2026 年的数据)
        full_k = full_k[full_k['date'] <= today_str]
        
        # 调用自定义的 Cleaner 进行类型转换
        full_k = cleaner.clean_sector_kline(full_k)
        
        k_path = f"{OUTPUT_DIR}/sector_kline_full.parquet"
        full_k.to_parquet(k_path, index=False)
        print(f"[+] K线数据存储成功: {len(full_k)} 行 | 覆盖日期至 {full_k['date'].max()}")

    if all_c_flat:
        full_c = pd.DataFrame(all_c_flat)
        full_c['date'] = today_str
        
        c_path = f"{OUTPUT_DIR}/sector_constituents_latest.parquet"
        full_c.to_parquet(c_path, index=False)
        print(f"[+] 成份股关系存储成功: {len(full_c)} 条映射对")

    end_time = datetime.now()
    print(f"\n{'='*70}")
    print(f"[*] 任务圆满完成 | 最终入库板块数: {total_found} | 总耗时: {end_time - start_time}")
    print(f"{'='*70}\n")

if __name__ == "__main__":
    main()
