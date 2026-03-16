import requests
import time
import json
import os
from concurrent.futures import ThreadPoolExecutor, as_completed

def fetch_sector_data(info):
    worker_url = os.getenv("CF_WORKER_URL", "").strip()
    url = f"https://{worker_url}" if not worker_url.startswith("http") else worker_url
    
    code = info['code']
    name = info['name']
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
        "Connection": "keep-alive" # 高速模式尝试保持连接
    }

    # 结果标志
    res_k = False
    res_c = False

    # 1. 抓取 K 线
    try:
        k_params = {
            "target_func": "kline", "secid": f"90.{code}",
            "fields1": "f1,f2", "fields2": "f51,f52", "lmt": "5",
            "_": int(time.time() * 1000)
        }
        r1 = requests.get(url, params=k_params, headers=headers, timeout=10)
        res_k = (r1.status_code == 200)
    except: pass

    # 2. 抓取成分股 (极简)
    try:
        c_params = {
            "target_func": "constituents", "fs": f"b:{code}",
            "pz": "500", "fields": "f12",
            "_": int(time.time() * 1000)
        }
        r2 = requests.get(url, params=c_params, headers=headers, timeout=10)
        res_c = (r2.status_code == 200)
    except: pass

    return code, name, res_k, res_c

def main():
    # 构造 100 个测试板块
    test_list = [{"code": f"BK{1000+i}", "name": f"Sector_{i}"} for i in range(100)]
    
    print(f"🚀 [极限全速测试] 启动! 目标: 100 个板块 | 并发线程: 10 | 延时: 0")
    print("-" * 60)

    start_time = time.time()
    results = []

    # 使用线程池全速推进
    with ThreadPoolExecutor(max_workers=10) as executor:
        futures = {executor.submit(fetch_sector_data, info): info for info in test_list}
        
        for future in as_completed(futures):
            code, name, k, c = future.result()
            results.append((k, c))
            status = f"K:{'✅' if k else '❌'} C:{'✅' if c else '❌'}"
            print(f"📡 {code} ({name}) -> {status}")

    end_time = time.time()
    
    # 统计数据
    k_success = sum(1 for k, c in results if k)
    c_success = sum(1 for k, c in results if c)
    both_success = sum(1 for k, c in results if k and c)

    print("-" * 60)
    print(f"📊 [全速测试报告]")
    print(f"⏱️  总耗时: {end_all := end_time - start_time:.2f} 秒")
    print(f"📈 吞吐量: {100 / end_all:.2f} 板块/秒")
    print(f"✅ K线成功: {k_success}/100")
    print(f"✅ 成分股成功: {c_success}/100")
    print(f"🏆 完全成功: {both_success}/100")
    print("-" * 60)

if __name__ == "__main__":
    main()
