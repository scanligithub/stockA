import baostock as bs
import pandas as pd
import json
import math
import random
import datetime
import os
import sys
import time
import requests

# 确保能引用到 utils
sys.path.append(os.getcwd())

NUM_CHUNKS = 19
CF_WORKER_URL = os.getenv("CF_WORKER_URL", "").strip()

def fetch_em_sector_burst(fs_code, label):
    """
    东财单分类抓取：采用 pz=100 分页，直到抓完该分类所有数据
    """
    if not CF_WORKER_URL: return []
    worker_url = f"https://{CF_WORKER_URL}" if not CF_WORKER_URL.startswith("http") else CF_WORKER_URL
    
    items_all = []
    page = 1
    pz = 100
    
    print(f"   🔎 正在采集 [{label}] 分类...")
    
    while True:
        params = {
            "target_func": "list", "pn": page, "pz": pz, "po": 1, "np": 1,
            "ut": "bd1d9ddb04089700cf9c27f6f7426281", "fltt": 2, "invt": 2,
            "fid": "f3", "fs": fs_code, "fields": "f12,f13,f14",
            "_": int(time.time() * 1000)
        }
        headers = {
            "User-Agent": f"Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/{random.randint(118, 122)}.0.0.0 Safari/537.36",
            "Connection": "close"
        }
        
        try:
            # 分页内部的小休眠 (2-3秒)，防止单分类内 520
            if page > 1: time.sleep(random.uniform(2, 3))
            
            r = requests.get(worker_url, params=params, headers=headers, timeout=20)
            if r.status_code == 200:
                data = r.json().get('data', {})
                diff = data.get('diff', [])
                batch = list(diff.values()) if isinstance(diff, dict) else diff
                
                for x in batch:
                    items_all.append({
                        "code": x['f12'], "market": x['f13'], 
                        "name": x['f14'], "type": label
                    })
                
                total = data.get('total', 0)
                print(f"      ✅ Page {page}: +{len(batch)} 条 | 进度: {len(items_all)}/{total}")
                
                if len(batch) < pz or len(items_all) >= total: break
                page += 1
            else:
                print(f"      ❌ Page {page} 失败 (HTTP {r.status_code})")
                break
        except Exception as e:
            print(f"      💥 异常: {e}")
            break
            
    return items_all

def get_baostock_matrix():
    """Baostock 名册获取逻辑：独立 Socket 通信，不消耗东财/CF 配额"""
    print("🔍 [Baostock] 正在同步全市场股票名册...")
    start_t = time.time()
    bs.login()
    data = []
    for i in range(10):
        d = (datetime.datetime.now() - datetime.timedelta(days=i)).strftime("%Y-%m-%d")
        rs = bs.query_all_stock(day=d)
        if rs.error_code == '0' and len(rs.data) > 0:
            while rs.next():
                row = rs.get_row_data()
                if row[2] and row[2].strip(): data.append(row)
            break
    bs.logout()
    
    valid_codes = [x[0] for x in data if x[0].startswith(('sh.', 'sz.', 'bj.'))]
    random.shuffle(valid_codes)
    
    chunk_size = math.ceil(len(valid_codes) / NUM_CHUNKS)
    chunks = [{"index": i, "codes": valid_codes[i*chunk_size : (i+1)*chunk_size]} for i in range(NUM_CHUNKS)]
    chunks = [c for c in chunks if c['codes']]

    with open("stock_matrix.json", "w") as f:
        json.dump(chunks, f)
    
    cost = time.time() - start_t
    print(f"✅ [Baostock] 同步完成! 耗时 {cost:.1f}s | 股票总数: {len(valid_codes)}")
    return cost

def main():
    start_all = time.time()
    master_sectors = []
    
    # --- BURST 1: 行业板块 ---
    master_sectors.extend(fetch_em_sector_burst("m:90 t:2", "Industry"))
    
    # --- COOL DOWN 1 + BAO STOCK (利用这段时间抓股票名册) ---
    print("☕ 进入第一段冷却期 (35s)... 正在后台同步 Baostock 任务...")
    cooldown_start = time.time()
    
    # 核心优化点：在冷却期内同步 Baostock
    bs_cost = get_baostock_matrix()
    
    # 补齐剩余冷却时间，确保东财风控完全重置
    remaining = 35 - (time.time() - cooldown_start)
    if remaining > 0:
        print(f"   ⏱️  Baostock 任务已完成，继续冷却剩余 {remaining:.1f} 秒...")
        time.sleep(remaining)
    
    # --- BURST 2: 概念板块 ---
    master_sectors.extend(fetch_em_sector_burst("m:90 t:3", "Concept"))
    
    # --- COOL DOWN 2 (强制休息) ---
    print("☕ 进入第二段冷却期 (35s)...")
    time.sleep(35)
    
    # --- BURST 3: 地域板块 ---
    master_sectors.extend(fetch_em_sector_burst("m:90 t:1", "Region"))
    
    # --- 保存结果 ---
    if master_sectors:
        # 去重保护
        df_sec = pd.DataFrame(master_sectors).drop_duplicates(subset=['code'])
        with open("sector_list.json", "w", encoding='utf-8') as f:
            json.dump(df_sec.to_dict(orient='records'), f, ensure_ascii=False, indent=2)
        print(f"🏁 所有任务完成! 累计板块: {len(df_sec)} 条 | 总耗时: {time.time()-start_all:.1f}s")
    else:
        print("❌ 严重错误: 未能获取到任何板块数据!")
        sys.exit(1)

if __name__ == "__main__":
    main()
