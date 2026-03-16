import requests
import json
import os
import argparse
import time

def fetch_page(category, page, cf_url):
    # 映射东财 fs 代码
    fs_map = {"Industry": "m:90 t:2", "Concept": "m:90 t:3", "Region": "m:90 t:1"}
    fs_code = fs_map.get(category)
    
    params = {
        "target_func": "list", "pn": page, "pz": 100, "po": 1, "np": 1,
        "ut": "bd1d9ddb04089700cf9c27f6f7426281", "fltt": 2, "invt": 2,
        "fid": "f3", "fs": fs_code, "fields": "f12,f13,f14", "_": int(time.time()*1000)
    }
    headers = {"User-Agent": "Mozilla/5.0", "Connection": "close"}
    
    try:
        r = requests.get(f"https://{cf_url}", params=params, headers=headers, timeout=20)
        if r.status_code == 200:
            data = r.json().get('data', {})
            items = data.get('diff', [])
            return list(items.values()) if isinstance(items, dict) else items, data.get('total', 0)
    except Exception as e:
        print(f"❌ Page {page} Error: {e}")
    return [], 0

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--category", type=str)
    parser.add_argument("--page", type=int)
    args = parser.parse_args()
    
    cf_url = os.getenv("CF_WORKER_URL")
    results = []
    
    # 1. 必定抓取 Page 1 (基准)
    print(f"🚀 Job [{args.category}]: 正在抓取 Page 1...")
    p1_data, total = fetch_page(args.category, 1, cf_url)
    results.extend(p1_data)
    
    # 2. 如果指定页码 > 1，则抓取特定页
    if args.page > 1:
        print(f"🚀 Job [{args.category}]: 正在抓取特定 Page {args.page}...")
        # 稍微间隔 2 秒，模拟极轻量的人为操作
        time.sleep(2)
        px_data, _ = fetch_page(args.category, args.page, cf_url)
        results.extend(px_data)
        
    # 保存结果
    output = {
        "category": args.category,
        "target_page": args.page,
        "total_expected": total,
        "data": results
    }
    
    filename = f"sector_{args.category}_p{args.page}.json"
    with open(filename, "w", encoding='utf-8') as f:
        json.dump(output, f, ensure_ascii=False)
    print(f"✅ 保存完成: {filename} (包含 {len(results)} 条记录)")
