import baostock as bs
import pandas as pd
import json
import math
import random
import datetime
import os
import sys
import time

# 确保能引用到 utils
sys.path.append(os.getcwd())
from utils.cf_proxy import EastMoneyProxy

# 分片数量保持 19
NUM_CHUNKS = 19

def fetch_all_sectors_robust(proxy):
    """
    深度稳健的板块发现引擎
    通过 pz=100 翻页模式，彻底解决 131 条截断与 520 报错问题
    """
    targets = {
        "Industry": "m:90 t:2", 
        "Concept": "m:90 t:3", 
        "Region": "m:90 t:1"
    }
    master_sectors = {}
    
    print("📡 [Sector Discovery] 启动稳健翻页模式...")
    
    for label, fs_code in targets.items():
        page = 1
        pz = 100 # 经过验证的 Action 环境安全上限
        category_count = 0
        
        print(f"   🔎 正在扫描 [{label}] 分类...")
        
        while True:
            params = {
                "pn": page,
                "pz": pz,
                "po": 1,
                "np": 1,
                "ut": "bd1d9ddb04089700cf9c27f6f7426281",
                "fltt": 2,
                "invt": 2,
                "fid": "f3",
                "fs": fs_code,
                "fields": "f12,f13,f14",
                "_": int(time.time() * 1000)
            }
            
            # 翻页间歇，防止被东财 WAF 识别为连击
            if page > 1:
                time.sleep(random.uniform(2.5, 5.0))
            
            try:
                # 调用 proxy 的底层 _request
                res = proxy._request("list", params)
                
                if res and res.get('data') and res['data'].get('diff'):
                    data_node = res['data']
                    total = data_node.get('total', 0)
                    diff = data_node['diff']
                    
                    # 兼容处理 dict/list 格式
                    items = list(diff.values()) if isinstance(diff, dict) else diff
                    
                    for x in items:
                        code = x['f12']
                        if code not in master_sectors:
                            master_sectors[code] = {
                                "code": code, 
                                "market": x['f13'], 
                                "name": x['f14'], 
                                "type": label
                            }
                    
                    batch_size = len(items)
                    category_count += batch_size
                    print(f"      ✅ Page {page}: 抓取 {batch_size} 条 | 累计: {category_count}/{total}")
                    
                    # 判断是否翻页结束
                    if batch_size < pz or category_count >= total:
                        break
                    page += 1
                else:
                    print(f"      ⚠️ Page {page}: 未获取到有效数据，停止该分类扫描。")
                    break
            except Exception as e:
                print(f"      💥 Page {page}: 发生异常 {e}，尝试跳过...")
                break
                
        # 分类间增加冷却时间
        time.sleep(random.uniform(3, 6))
        
    return list(master_sectors.values())

def main():
    # --- Part 1: 获取股票 Matrix (Baostock 保持原样) ---
    print("🔍 [Stock Matrix] 正在从 Baostock 获取名册...")
    bs.login()
    data = []
    # 尝试回溯10天找到最近一个交易日
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
    random.shuffle(valid_codes) # 随机打乱，让 20 个 Job 负载更均衡
    
    chunk_size = math.ceil(len(valid_codes) / NUM_CHUNKS)
    chunks = []
    for i in range(NUM_CHUNKS):
        subset = valid_codes[i * chunk_size : (i + 1) * chunk_size]
        if subset: chunks.append({"index": i, "codes": subset})

    with open("stock_matrix.json", "w") as f:
        json.dump(chunks, f)
    print(f"✅ Stock Matrix ready: {len(valid_codes)} stocks divided into {len(chunks)} chunks.")

    # --- Part 2: 获取板块名册 (EastMoney 升级版) ---
    cf_url = os.getenv("CF_WORKER_URL")
    if not cf_url:
        print("⚠️ CF_WORKER_URL not found, skipping sector discovery.")
    else:
        proxy = EastMoneyProxy()
        all_sectors = fetch_all_sectors_robust(proxy)
        
        with open("sector_list.json", "w", encoding='utf-8') as f:
            json.dump(all_sectors, f, ensure_ascii=False, indent=2)
        print(f"✅ Sector List ready: {len(all_sectors)} sectors saved to sector_list.json.")

if __name__ == "__main__":
    main()
