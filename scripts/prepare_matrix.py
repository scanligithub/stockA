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

sys.path.append(os.getcwd())

NUM_CHUNKS = 19
CF_WORKER_URL = os.getenv("CF_WORKER_URL", "").strip()

def fetch_em_with_retry(params):
    """
    单页抓取核心：具备 520 自动原地愈合功能
    """
    worker_url = f"https://{CF_WORKER_URL}" if not CF_WORKER_URL.startswith("http") else CF_WORKER_URL
    headers = {
        "User-Agent": f"Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/{random.randint(118, 122)}.0.0.0 Safari/537.36",
        "Connection": "close"
    }
    
    # 针对 520 错误进行原地重试
    for attempt in range(1, 4):
        try:
            r = requests.get(worker_url, params=params, headers=headers, timeout=25)
            if r.status_code == 200:
                return r.json()
            elif r.status_code == 520:
                wait = 60 * attempt # 520 了就歇 60s, 120s...
                print(f"      ⚠️ 触发 520 风控，原地进入‘冬眠模式’ {wait} 秒后重试...")
                time.sleep(wait)
            else:
                print(f"      ❌ HTTP {r.status_code}，歇 10 秒重试...")
                time.sleep(10)
        except Exception as e:
            print(f"      💥 网络异常: {e}，歇 10 秒重试...")
            time.sleep(10)
    return None

def fetch_category_stealth(fs_code, label, bs_task_done):
    """
    分类抓取引擎：每 2 页强制进入长冷却，且可触发 Baostock 任务
    """
    items_all = []
    page = 1
    pz = 100
    is_bs_executed = bs_task_done
    
    print(f"   🔎 正在潜行采集 [{label}] ...")
    
    while True:
        # 每抓 2 页，强制强制强制休息！这是避开东财 WAF 累积得分的关键
        if page > 1 and page % 2 == 1:
            wait_time = 65 
            print(f"   ☕ 已连续请求 2 次，强制冷却 {wait_time} 秒以重置防火墙计数器...")
            
            # 💡 只有在第一次长休眠时，顺便把 Baostock 跑了
            if not is_bs_executed:
                get_baostock_matrix()
                is_bs_executed = True
                # 检查一下跑完 BS 还剩多少秒
                # 这里简化处理，直接确保总休息时间够长即可
                time.sleep(30) 
            else:
                time.sleep(wait_time)

        params = {
            "target_func": "list", "pn": page, "pz": pz, "po": 1, "np": 1,
            "ut": "bd1d9ddb04089700cf9c27f6f7426281", "fltt": 2, "invt": 2,
            "fid": "f3", "fs": fs_code, "fields": "f12,f13,f14",
            "_": int(time.time() * 1000)
        }
        
        res = fetch_em_with_retry(params)
        if res and res.get('data') and res['data'].get('diff'):
            data = res['data']
            diff = data['diff']
            batch = list(diff.values()) if isinstance(diff, dict) else diff
            
            for x in batch:
                items_all.append({"code": x['f12'], "market": x['f13'], "name": x['f14'], "type": label})
            
            total = data.get('total', 0)
            print(f"      ✅ Page {page}: +{len(batch)} 条 | 进度: {len(items_all)}/{total}")
            
            if len(batch) < pz or len(items_all) >= total: break
            page += 1
            time.sleep(random.uniform(3, 5)) # 页间普通小碎步
        else:
            print(f"      🛑 [{label}] 采集受阻，跳过剩余页面。")
            break
            
    return items_all, is_bs_executed

def get_baostock_matrix():
    print("🔍 [Baostock] 正在冷却期内同步股票名册...")
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
    with open("stock_matrix.json", "w") as f: json.dump(chunks, f)
    print(f"✅ [Baostock] 名册就绪，股票数: {len(valid_codes)}")

def main():
    start_all = time.time()
    master_sectors = []
    bs_done = False
    
    # --- 阶段 1: 行业板块 (5页) ---
    # 这会触发一次长休眠（在第3页前），顺便跑完 BS
    items, bs_done = fetch_category_stealth("m:90 t:2", "Industry", bs_done)
    master_sectors.extend(items)
    
    # 类别切换大冷却
    print("☕ 类别切换，深度冷却 60s...")
    time.sleep(60)
    
    # --- 阶段 2: 概念板块 (5页) ---
    items, bs_done = fetch_category_stealth("m:90 t:3", "Concept", bs_done)
    master_sectors.extend(items)
    
    print("☕ 类别切换，深度冷却 60s...")
    time.sleep(60)
    
    # --- 阶段 3: 地域板块 (1页) ---
    items, bs_done = fetch_category_stealth("m:90 t:1", "Region", bs_done)
    master_sectors.extend(items)
    
    # --- 兜底: 如果此时 BS 还没跑 (万一板块很少)，则强行跑一次 ---
    if not bs_done: get_baostock_matrix()

    if master_sectors:
        df_sec = pd.DataFrame(master_sectors).drop_duplicates(subset=['code'])
        with open("sector_list.json", "w", encoding='utf-8') as f:
            json.dump(df_sec.to_dict(orient='records'), f, ensure_ascii=False, indent=2)
        print(f"🏁 采集完成! 最终板块: {len(df_sec)} 条 | 总耗时: {time.time()-start_all:.1f}s")
    else:
        print("❌ 致命错误: 未能获取到任何板块数据!")
        sys.exit(1)

if __name__ == "__main__":
    main()
