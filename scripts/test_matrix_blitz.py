import requests
import time
import json
import os
import argparse
from concurrent.futures import ThreadPoolExecutor, as_completed

def fetch_sector_unit(info):
    worker_url = os.getenv("CF_WORKER_URL", "").strip()
    url = f"https://{worker_url}" if not worker_url.startswith("http") else worker_url
    
    code = info['code']
    headers = {"User-Agent": "Mozilla/5.0", "Connection": "keep-alive"}

    try:
        # 1. K线抓取
        k_params = {"target_func": "kline", "secid": f"90.{code}", "lmt": "5"}
        r1 = requests.get(url, params=k_params, headers=headers, timeout=8)
        k_ok = (r1.status_code == 200)
        
        # 2. 成分股抓取 (极简)
        c_params = {"target_func": "constituents", "fs": f"b:{code}", "pz": "500", "fields": "f12"}
        r2 = requests.get(url, params=c_params, headers=headers, timeout=8)
        c_ok = (r2.status_code == 200)
        
        return code, k_ok and c_ok
    except:
        return code, False

def run_blitz_round(sector_list, round_num):
    print(f"🚀 [Round {round_num}] 正在尝试抓取 {len(sector_list)} 个板块...")
    results = {}
    
    # 使用 5 线程并发（20个Job合起来就是100并发，这是东财的红线边界）
    with ThreadPoolExecutor(max_workers=5) as executor:
        futures = {executor.submit(fetch_sector_unit, info): info for info in sector_list}
        for future in as_completed(futures):
            code, success = future.result()
            results[code] = success
            
    success_list = [c for c, s in results.items() if s]
    failed_list = [c for c, s in results.items() if not s]
    print(f"✅ Round {round_num} 完成: 成功 {len(success_list)} | 失败 {len(failed_list)}")
    return success_list, failed_list

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--index", type=str, default="0")
    parser.add_argument("--sectors", type=str, default="[]")
    args = parser.parse_args()
    
    initial_sectors = json.loads(args.sectors)
    if not initial_sectors:
        # 兜底测试数据
        initial_sectors = [{"code": f"BK{1000 + int(args.index)*50 + i}"} for i in range(50)]

    print(f"🔥 Job {args.index} 启动闪电战模式...")
    
    # 第一轮
    s1, f1 = run_blitz_round(initial_sectors, 1)
    
    # 第二轮 (仅重试失败的)
    s2, f2 = ([], f1)
    if f1:
        time.sleep(2) # 轮次间稍微喘息
        f1_infos = [s for s in initial_sectors if s['code'] in f1]
        s2, f2 = run_blitz_round(f1_infos, 2)
        
    # 第三轮 (仅重试仍然失败的)
    s3, f3 = ([], f2)
    if f2:
        time.sleep(2)
        f2_infos = [s for s in initial_sectors if s['code'] in f2]
        s3, f3 = run_blitz_round(f2_infos, 3)

    print("-" * 30)
    final_success_count = len(s1) + len(s2) + len(s3)
    print(f"🏁 Job {args.index} 结果: 最终成功 {final_success_count}/{len(initial_sectors)}")
    if f3:
        print(f"💀 依然失败的代码: {f3}")

if __name__ == "__main__":
    main()
