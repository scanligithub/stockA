import requests
import time
import os
import json

def test_limit(pz_value):
    # 您的 Worker URL
    base_url = "https://stock.scanli.de5.net/"
    params = {
        "target_func": "list",
        "pn": 1,
        "pz": pz_value,
        "po": 1,
        "np": 1,
        "ut": "bd1d9ddb04089700cf9c27f6f7426281",
        "fltt": 2,
        "invt": 2,
        "fid": "f3",
        "fs": "m:90 t:2",  # 行业板块
        "fields": "f12,f13,f14"
    }
    
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Referer": "https://quote.eastmoney.com/"
    }

    print(f"🚀 [测试开始] 档位 pz={pz_value} ...")
    start_time = time.time()
    
    try:
        # 增加超时容忍度
        resp = requests.get(base_url, params=params, headers=headers, timeout=30)
        cost = time.time() - start_time
        
        if resp.status_code == 200:
            try:
                data = resp.json()
                total = data.get('data', {}).get('total', 0)
                # 处理东财可能返回 dict 或 list 的情况
                items = data.get('data', {}).get('diff', [])
                count = len(items.values()) if isinstance(items, dict) else len(items)
                
                print(f"   ✅ 成功! 耗时: {cost:.2f}s | HTTP: {resp.status_code}")
                print(f"   📊 数据详情: Total={total}, 实际抓取条数={count}")
                if count > 0:
                    # 打印前2个做验证
                    sample = list(items.values())[0:2] if isinstance(items, dict) else items[0:2]
                    print(f"   🧪 样例: {sample}")
            except Exception as json_err:
                print(f"   ⚠️ 解析JSON失败: {json_err}")
                print(f"   📄 原始返回片段: {resp.text[:200]}")
        else:
            print(f"   ❌ 失败! 耗时: {cost:.2f}s | HTTP状态码: {resp.status_code}")
            if resp.status_code == 520:
                print("   💡 诊断: 触发了 Cloudflare 520 错误（东财重置了连接）")
                
    except Exception as e:
        cost = time.time() - start_time
        print(f"   💥 异常! 耗时: {cost:.2f}s | 错误信息: {str(e)}")
    
    print("-" * 50)

if __name__ == "__main__":
    # 梯度测试
    limits = [10, 500, 1000, 2000, 3000]
    for pz in limits:
        test_limit(pz)
        time.sleep(2) # 档位间休息，防止干扰
