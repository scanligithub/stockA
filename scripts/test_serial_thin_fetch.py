import requests
import time
import random
import os
import json

def test_sector_stealth(sector_info):
    worker_url = os.getenv("CF_WORKER_URL", "").strip()
    url = f"https://{worker_url}" if not worker_url.startswith("http") else worker_url
    
    code = sector_info['code']
    name = sector_info['name']
    headers = {
        "User-Agent": f"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/{random.randint(118, 122)}.0.0.0 Safari/537.36",
        "Connection": "close"
    }

    print(f"📡 正在处理 [{name}] ({code})...")
    
    # --- 1. 抓取 K 线 ---
    k_success = False
    try:
        # K线参数相对固定，包体也不大
        k_params = {
            "target_func": "kline",
            "secid": f"90.{code}",
            "fields1": "f1,f2,f3,f4,f5,f6",
            "fields2": "f51,f52,f53,f54,f55,f56,f57,f58",
            "klt": "101", "fqt": "1", "beg": "20240101", "lmt": "10",
            "_": int(time.time() * 1000)
        }
        r1 = requests.get(url, params=k_params, headers=headers, timeout=15)
        k_success = (r1.status_code == 200)
        print(f"   📊 K线: {r1.status_code}", end=" | ")
    except Exception as e:
        print(f"   📊 K线异常: {e}", end=" | ")

    # 板块内小休眠
    time.sleep(random.uniform(1.5, 3.0))

    # --- 2. 抓取成分股 (极简模式) ---
    c_success = False
    try:
        c_params = {
            "target_func": "constituents",
            "pn": 1, "pz": 500, "po": 1, "np": 1,
            "ut": "bd1d9ddb04089700cf9c27f6f7426281",
            "fltt": 2, "invt": 2, "fid": "f3",
            "fs": f"b:{code}",
            "fields": "f12", # 【核心核心】瘦身字段
            "_": int(time.time() * 1000)
        }
        r2 = requests.get(url, params=c_params, headers=headers, timeout=15)
        c_success = (r2.status_code == 200)
        
        count = 0
        if c_success:
            data = r2.json()
            count = len(data.get('data', {}).get('diff', []))
        
        print(f"👥 成分股: {r2.status_code} (拿到 {count} 只)")
    except Exception as e:
        print(f"👥 成分股异常: {e}")

    return k_success and c_success

if __name__ == "__main__":
    # 模拟从 prepare_matrix 生成的 sector_list.json 中提取前 50 个
    # 如果文件不存在，我们就用预设的几个测试
    test_list = []
    if os.path.exists("sector_list.json"):
        with open("sector_list.json", "r", encoding='utf-8') as f:
            full_list = json.load(f)
            test_list = full_list[:50] # 测试前 50 个
    else:
        # 兜底测试数据
        test_list = [{"code": f"BK{1000+i}", "name": f"Test_{i}", "market": 90, "type": "Industry"} for i in range(50)]

    print(f"🚀 [串行瘦身压测] 启动! 目标: 连续抓取 {len(test_list)} 个板块...")
    print("-" * 60)

    success_count = 0
    start_time = time.time()

    for i, info in enumerate(test_list):
        if test_sector_stealth(info):
            success_count += 1
        else:
            print(f"🚩 [警告] 第 {i+1} 个板块出现失败，可能已触发 520")
            # 如果连续失败 3 次，可能说明 IP 已死，建议直接中断
            
        # 板块间休眠：模拟真实节奏
        time.sleep(random.uniform(2, 4))

    end_time = time.time()
    print("-" * 60)
    print(f"🏁 压测结束! 成功率: {success_count}/{len(test_list)}")
    print(f"⏱️  总耗时: {end_time - start_time:.2f} 秒")
