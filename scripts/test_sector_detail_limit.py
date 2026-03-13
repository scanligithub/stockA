import requests
import time
import random
import os

def probe_sector_detail(sector_code):
    worker_url = os.getenv("CF_WORKER_URL", "").strip()
    url = f"https://{worker_url}" if not worker_url.startswith("http") else worker_url
    
    headers = {"User-Agent": "Mozilla/5.0", "Connection": "close"}
    
    # 模拟一次完整的板块采集动作：1次K线 + 1次成分股
    results = []
    for func in ["kline", "constituents"]:
        params = {
            "target_func": func,
            "secid": f"90.{sector_code}",
            "fields1": "f1,f2,f3,f4,f5,f6",
            "fields2": "f51,f52,f53,f54,f55,f56,f57,f58",
            "klt": "101", "fqt": "1", "beg": "20240101", "end": "20991231", "lmt": "10",
            "fs": f"b:{sector_code}", "pz": "10", "pn": "1", # constituents 的参数
            "_": int(time.time()*1000)
        }
        try:
            r = requests.get(url, params=params, headers=headers, timeout=15)
            results.append(r.status_code)
        except:
            results.append("Timeout/Error")
    return results

if __name__ == "__main__":
    # 测试样本：模拟连续抓取 10 个板块
    test_sectors = ["BK1000", "BK1001", "BK1002", "BK1003", "BK1004", 
                    "BK1005", "BK1006", "BK1007", "BK1008", "BK1009"]
    
    print("🚀 [板块详情采集压测] 目标：找出连续采集的封锁临界点")
    print("-" * 50)

    for i, code in enumerate(test_sectors):
        print(f"📡 正在尝试抓取第 {i+1} 个板块 ({code})...")
        res = probe_sector_detail(code)
        print(f"   结果: K线[{res[0]}] | 成分股[{res[1]}]")
        
        if "520" in str(res) or 520 in res:
            print(f"🚩 [发现封锁] 在第 {i+1} 个板块处触发了 520 错误！")
            break
        
        # 模拟生产中的小休眠
        time.sleep(2)

    print("-" * 50)
    print("🏁 压测结束。")
