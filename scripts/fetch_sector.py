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

# ==========================================
# 辅助函数
# ==========================================
def extract_price(item):
    """安全提取价格，过滤停牌或新上市无价格的板块"""
    p = item.get('f2')
    if p == '-' or p is None:
        return None
    try:
        return float(p)
    except:
        return None

# ==========================================
# 核心算法：双指针向中逼近 + 递归二分法
# ==========================================
def fallback_brute_force(fs_code, label, existing_seen):
    """【API降级兜底方案】如果API不支持价格过滤，无缝切换到并发多维包抄"""
    print(f"    [!] 触发降级机制：执行 20 维度并发扫描补全 [{label}]...")
    fids = [
        "f12", "f3", "f2", "f6", "f5", "f4", "f17", "f18", "f8", "f10", 
        "f15", "f16", "f11", "f9", "f23", "f20", "f21", "f22", "f24", "f25"
    ]
    tasks = [(fid, po) for fid in fids for po in [1, 0]]
    
    def fetch_dim(fid, po):
        return proxy.get_sector_list(fs_code, fid=fid, po=po, pn=1, pz=100)
        
    with concurrent.futures.ThreadPoolExecutor(max_workers=15) as executor:
        future_map = {executor.submit(fetch_dim, f, p): (f, p) for f, p in tasks}
        for future in concurrent.futures.as_completed(future_map):
            try:
                items = future.result()
                if not items: continue
                for x in items:
                    c = x['f12']
                    if c not in existing_seen:
                        existing_seen[c] = {"code": c, "market": x['f13'], "name": x['f14'], "type": label}
            except: pass
    return list(existing_seen.values())

def get_category_smart_pincer(fs_code, label):
    """
    【双指针向中逼近】你的核心算法实现
    """
    print(f"\n[*] 启动高级算法：双指针逼近 + 递归切片 [{label}]...")
    seen_codes = {}

    def add_items(items):
        added = 0
        for x in items:
            c = x['f12']
            if c not in seen_codes:
                seen_codes[c] = {"code": c, "market": x['f13'], "name": x['f14'], "type": label}
                added += 1
        return added

    # --- 第一步：获取下界 (价格升序 100条) ---
    low_items = proxy.get_sector_list(fs_code, fid="f2", po=0, pn=1, pz=100)
    add_items(low_items)

    # --- 第二步：获取上界 (价格降序 100条) ---
    high_items = proxy.get_sector_list(fs_code, fid="f2", po=1, pn=1, pz=100)
    add_items(high_items)

    if len(low_items) < 100:
        print(f"    [+] {label} 总数不足100，双指针直接合拢，已全量获取。")
        return list(seen_codes.values())

    # 提取有效价格计算边界
    low_prices = [extract_price(x) for x in low_items if extract_price(x) is not None]
    high_prices = [extract_price(x) for x in high_items if extract_price(x) is not None]

    if not low_prices or not high_prices:
        return fallback_brute_force(fs_code, label, seen_codes)

    min_bound = max(low_prices)  # 升序的最高价（下界指针）
    max_bound = min(high_prices) # 降序的最低价（上界指针）
    print(f"    - 指针已锁定盲区：下界 {min_bound} | 上界 {max_bound}")

    # --- 第三步：交叉重叠判断 ---
    if min_bound >= max_bound:
        print(f"    [+] {label} 价格区间已闭合（无盲区），完成抓取！")
        return list(seen_codes.values())

    # --- 第四步：盲区递归切片 ---
    queue = [(min_bound, max_bound)]
    loop_safeguard = 0

    while queue and loop_safeguard < 20: # 安全阀，防死循环
        loop_safeguard += 1
        curr_min, curr_max = queue.pop(0)
        
        # 尝试构造价格过滤的 fs 参数
        test_fs = f"{fs_code} f2>={curr_min} f2<={curr_max}"
        gap_items = proxy.get_sector_list(test_fs, fid="f2", po=1, pn=1, pz=100)
        
        if not gap_items: continue
        
        # [API 哨兵防线]：检查 API 是否忽略了我们的价格过滤器
        test_p = extract_price(gap_items[0])
        if test_p is not None and (test_p > curr_max * 1.5 or test_p < curr_min * 0.5):
            print(f"    [🔥 API 拒绝] 服务器拦截了未授权的过滤语法。")
            return fallback_brute_force(fs_code, label, seen_codes)

        added = add_items(gap_items)
        
        # 二分法分裂
        if len(gap_items) == 100:
            print(f"    - [盲区 {curr_min:.2f}~{curr_max:.2f}] 触碰上限，执行二分切片...")
            mid = (curr_min + curr_max) / 2.0
            # 闭区间分裂防碰撞
            queue.append((curr_min, mid))
            queue.append((mid, curr_max))
        else:
            print(f"    - [盲区 {curr_min:.2f}~{curr_max:.2f}] 击穿！补齐 {added} 条。")

    return list(seen_codes.values())


# ==========================================
# 抓取详情与落库 (保持极限并发优化与脏数据隔离)
# ==========================================
def fetch_one_sector(info):
    code, name = info['code'], info['name']
    secid = f"90.{code}"
    res_k = proxy.get_sector_kline(secid)
    k_data = []
    if res_k and res_k.get('data') and res_k['data'].get('klines'):
        for row_str in res_k['data']['klines']:
            r = row_str.split(',')
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
    print(f"\n{'='*70}\n[*] 东方财富全量引擎 (双指针算法版) | {start_time}\n{'='*70}\n")
    
    targets = {"Industry": "m:90 t:2", "Concept": "m:90 t:3", "Region": "m:90 t:1"}
    all_sectors = []
    for label, fs in targets.items():
        all_sectors.extend(get_category_smart_pincer(fs, label))

    df_list = pd.DataFrame(all_sectors).drop_duplicates('code')
    total_found = len(df_list)
    print(f"\n[*] 审计报告：去重后唯一板块总数: {total_found}")

    if total_found < 980:
        print(f"[🔥 严重警告] 总数仍低于预期，停止后续采集防污染。")
        sys.exit(1)

    print(f"\n[*] 开始并发采集 {total_found} 个板块详情 (并发: 20)...")
    all_k_flat, all_c_flat = [], []
    with concurrent.futures.ThreadPoolExecutor(max_workers=20) as executor:
        f_map = {executor.submit(fetch_one_sector, row): row['name'] for _, row in df_list.iterrows()}
        for future in tqdm(concurrent.futures.as_completed(f_map), total=len(f_map), desc="总进度"):
            try:
                k_list, c_list = future.result()
                if k_list: all_k_flat.extend(k_list)
                if c_list: all_c_flat.extend(c_list)
            except: pass

    print(f"\n[*] 正在清洗千万级合并数据并生成 Parquet...")
    cleaner = DataCleaner()
    today_dt = pd.Timestamp.now().normalize()
    
    if all_k_flat:
        full_k = pd.DataFrame(all_k_flat)
        # [严密封锁]：物理切断所有未来日期数据
        full_k['date_dt'] = pd.to_datetime(full_k['date'].astype(str).str.strip(), errors='coerce')
        full_k = full_k[(full_k['date_dt'] <= today_dt) & (full_k['date_dt'].notnull())]
        full_k = full_k.drop(columns=['date_dt'])
        
        full_k = cleaner.clean_sector_kline(full_k)
        full_k.to_parquet(f"{OUTPUT_DIR}/sector_kline_full.parquet", index=False)
        print(f"[+] K线存储成功: {len(full_k)} 行 | 覆盖日期至 {full_k['date'].max()}")

    if all_c_flat:
        full_c = pd.DataFrame(all_c_flat)
        full_c['date'] = today_dt.strftime('%Y-%m-%d')
        full_c.to_parquet(f"{OUTPUT_DIR}/sector_constituents_latest.parquet", index=False)
        print(f"[+] 成份股存储成功: {len(full_c)} 条映射关系")

    print(f"\n{'='*70}\n[*] 任务圆满完成 | 总耗时: {datetime.now() - start_time}\n{'='*70}\n")

if __name__ == "__main__":
    main()
