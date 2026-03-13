import requests
import time
import random
import os

def probe_sector_stealth(sector_code):
    worker_url = os.getenv("CF_WORKER_URL", "").strip()
    url = f"https://{worker_url}" if not worker_url.startswith("http") else worker_url
    headers = {"User-Agent": "Mozilla/5.0", "Connection": "close"}
    
    # 模拟串行抓取
    # 1. 抓 K 线
    try:
        r1 = requests.get(url, params={"target_func": "kline", "secid": f"90.{sector_code}", "lmt": 10}, headers=headers, timeout=10)
        s1 = r1.status_code
    except: s1 = "Err"
    
    # 强制页间休眠（这是关键！）
    time.sleep(5) 
    
    # 2. 抓成分股
    try:
        r2 = requests.get(url, params={"target_func": "constituents", "fs": f"b:{sector_code}", "pz": 10}, headers=headers, timeout=10)
        s2 = r2.status_code
    except: s2 = "Err"
    
    return s1, s2

if __name__ == "__main__":
    # 尝试连续抓取 5 个，看能不能突破之前的“2个死循环”
    test_sectors = ["BK1000", "BK1001", "BK1002", "BK1003", "BK1004"]
    
    print("🚀 [板块潜行验证] 目标：单线程+强制间隔，验证是否能突破 2 板块限制")
    
    for i, code in enumerate(test_sectors):
        print(f"📡 正在抓取第 {i+1} 个 ({code})...")
        s1, s2 = probe_sector_stealth(code)
        print(f"   结果: K线[{s1}] | 成分股[{s2}]")
        
        if s1 == 520 or s2 == 520:
            print(f"🚩 [失败] 依然在第 {i+1} 个板块被封锁。")
            break
        
        # 板块间大休眠
        time.sleep(8)

    print("\n🏁 验证结束。")
