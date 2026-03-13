import requests
import time
import random

def test_fixed_delay(delay_seconds):
    base_url = "https://stock.scanli.de5.net/"
    fs_code = "m:90 t:2" 
    pz = 100
    
    print(f"\n🧪 [测试组] 固定延时: {delay_seconds}s")
    
    for page in range(1, 4): # 每组测 3 页，足以触发连击风控
        headers = {
            "User-Agent": f"Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/122.0.0.0 Safari/537.36",
            "Connection": "close"
        }
        params = {
            "target_func": "list", "pn": page, "pz": pz, "po": 1, "np": 1,
            "ut": "bd1d9ddb04089700cf9c27f6f7426281", "fltt": 2, "invt": 2,
            "fid": "f3", "fs": fs_code, "fields": "f12,f13,f14", "_": int(time.time() * 1000)
        }
        
        if page > 1 and delay_seconds > 0:
            time.sleep(delay_seconds)
            
        try:
            resp = requests.get(base_url, params=params, headers=headers, timeout=15)
            if resp.status_code == 200:
                print(f"   ✅ Page {page} 成功")
            else:
                print(f"   ❌ Page {page} 失败 (HTTP {resp.status_code})")
                return False
        except Exception as e:
            print(f"   💥 Page {page} 异常: {e}")
            return False
    return True

if __name__ == "__main__":
    # 延时梯度：从 3秒 压测到 0秒
    delay_steps = [3.0, 2.0, 1.5, 1.0, 0.5, 0.1]
    
    for d in delay_steps:
        success = test_fixed_delay(d)
        if not success:
            print(f"\n🚩 [极限发现] 延时 {d}s 时触发风控或异常！")
            break
        else:
            print(f"   ✨ 延时 {d}s 通过测试")
            # 组间休息，重置防火墙计数器
            time.sleep(5) 

    print("\n" + "="*40)
    print("🏁 极限压测结束")
