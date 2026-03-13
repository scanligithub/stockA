import requests
import time
import random

def probe(label):
    url = "https://stock.scanli.de5.net/"
    params = {
        "target_func": "list", "pn": 1, "pz": 1, "po": 1, "np": 1,
        "ut": "bd1d9ddb04089700cf9c27f6f7426281", "fltt": 2, "invt": 2,
        "fid": "f3", "fs": "m:90 t:2", "fields": "f12", "_": int(time.time()*1000)
    }
    try:
        r = requests.get(url, params=params, timeout=10)
        return r.status_code == 200
    except:
        return False

if __name__ == "__main__":
    print("🚀 [刑期探测] 开始测试东财 WAF 的冷却时间...")
    
    # 1. 快速消耗配额（连发 6 次，确保入狱）
    print("⚡ 正在触发封锁...")
    for i in range(6):
        success = probe(f"Trigger_{i}")
        print(f"   请求 {i+1}: {'✅' if success else '❌ (已触发520)'}")
        time.sleep(1)

    # 2. 梯度冷却探测
    # 我们分别测试休息 30s, 60s, 90s，看哪档能“出狱”
    for wait in [30, 60, 90]:
        print(f"\n⏱️  强制休眠 {wait} 秒中...")
        time.sleep(wait)
        if probe("Test"):
            print(f"✨ [突破成功]！东财的冷却窗口大约是 {wait} 秒。")
            break
        else:
            print(f"💀 [依然在狱中] {wait} 秒不足以重置计数器。")

    print("\n🏁 测试结束。")
