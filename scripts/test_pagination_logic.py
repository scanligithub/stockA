import requests
import time
import random
import sys

def test_pagination_v2():
    # 你的接口 URL
    base_url = "https://stock.scanli.de5.net/"
    # 目标分类: 行业板块
    fs_code = "m:90 t:2" 
    
    all_items = []
    page = 1
    pz = 100 
    
    print(f"🚀 [分页验证 V2 - 慢速伪装模式] 启动...")
    print(f"📡 目标: 突破 100 条截断限制")

    while page <= 5: # 我们先尝试抓 5 页，看看能不能突破 100
        # 1. 模拟浏览器请求头
        # 增加 Connection: close 强制断开 Socket，防止会话指纹追踪
        headers = {
            "User-Agent": f"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/{random.randint(110, 122)}.0.0.0 Safari/537.36",
            "Referer": "https://quote.eastmoney.com/",
            "Connection": "close" 
        }
        
        # 2. 增加随机噪声参数 (_) 模拟真实浏览器
        params = {
            "target_func": "list",
            "pn": page,
            "pz": pz,
            "po": 1,
            "np": 1,
            "ut": "bd1d9ddb04089700cf9c27f6f7426281",
            "fltt": 2,
            "invt": 2,
            "fid": "f3",
            "fs": fs_code,
            "fields": "f12,f13,f14",
            "_": int(time.time() * 1000)
        }
        
        print(f"📡 正在尝试抓取第 {page} 页...")
        
        # 3. 核心策略：翻页间增加一个较大的随机休眠，跨过防火墙的检测窗口
        if page > 1:
            wait = random.uniform(5, 10)
            print(f"   ⏱️  为了避开 520 风险，休眠 {wait:.2f} 秒...")
            time.sleep(wait)

        try:
            # 增加超时限制
            resp = requests.get(base_url, params=params, headers=headers, timeout=30)
            
            if resp.status_code == 200:
                res_json = resp.json()
                data_node = res_json.get('data', {})
                total = data_node.get('total', 497)
                diff = data_node.get('diff', [])
                
                # 兼容东财 list/dict 返回格式
                items = list(diff.values()) if isinstance(diff, dict) else diff
                
                batch_size = len(items)
                all_items.extend(items)
                
                print(f"   ✅ 第 {page} 页抓取成功! 获得 {batch_size} 条 | 累计: {len(all_items)}/{total}")
                
                if batch_size < pz or len(all_items) >= total:
                    print(f"   🏁 已经抓完所有数据。")
                    break
                page += 1
            else:
                print(f"   ❌ 第 {page} 页失败! HTTP: {resp.status_code}")
                # 如果是 520，打印一下响应内容（如果是大白页，可能打不出来）
                print(f"   📄 内容片段: {resp.text[:100]}")
                break
                
        except Exception as e:
            print(f"   💥 捕获异常: {str(e)}")
            break

    print("\n" + "="*50)
    print("📊 [V2 验证结论]")
    print(f"✅ 最终实测抓取总数: {len(all_items)}")
    if len(all_items) > 100:
        print(f"🔥 突破成功！已成功拿到第 2 页之后的数据。")
    elif len(all_items) == 100:
        print(f"⚠️ 依然停留在 100 条。说明第 2 页还是被拦截了。")
    print("="*50)

if __name__ == "__main__":
    test_pagination_v2()
