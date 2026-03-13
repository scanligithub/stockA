import requests
import time
import os

def test_disguise(sector_code):
    worker_url = os.getenv("CF_WORKER_URL", "").strip()
    url = f"https://{worker_url}" if not worker_url.startswith("http") else worker_url
    
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
        "Connection": "close"
    }
    
    # 模拟 prepare_matrix 成功时的全套参数，仅仅修改 fs 为板块成分股模式
    params = {
        "target_func": "constituents", # 对应 JS 里的 constituents 映射
        "pn": 1,
        "pz": 50, # 减小每页数量，降低包体大小
        "po": 1,
        "np": 1,
        "ut": "bd1d9ddb04089700cf9c27f6f7426281",
        "fltt": 2,
        "invt": 2,
        "fid": "f3",
        "fs": f"b:{sector_code}", 
        "fields": "f12", # 【关键】只拿代码，拒绝其他几十个字段，确保包体极小
        "_": int(time.time() * 1000)
    }

    print(f"📡 尝试伪装抓取 [{sector_code}] 成分股...")
    try:
        start = time.time()
        r = requests.get(url, params=params, headers=headers, timeout=15)
        print(f"   结果: HTTP {r.status_code} | 耗时: {time.time()-start:.2f}s")
        if r.status_code == 200:
            data = r.json()
            count = len(data.get('data', {}).get('diff', []))
            print(f"   ✅ 成功拿到 {count} 只成分股代码")
            return True
        return False
    except Exception as e:
        print(f"   💥 异常: {e}")
        return False

if __name__ == "__main__":
    # 连续测试 3 个，看是否依然“一秒死”
    for code in ["BK1000", "BK1001", "BK1002"]:
        if not test_disguise(code):
            print("🚩 伪装失败，依然触发风控。")
            break
        time.sleep(5)
