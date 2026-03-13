import requests
import time
import json

def test_fetch(name, url):
    print(f"🚀 开始测试: {name}")
    start = time.time()
    try:
        # 增加 headers 模拟浏览器，防止被简单反爬拦截
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Referer": "https://quote.eastmoney.com/"
        }
        resp = requests.get(url, headers=headers, timeout=20)
        cost = time.time() - start
        
        if resp.status_code == 200:
            data = resp.json()
            total = data.get('data', {}).get('total', 0)
            items = data.get('data', {}).get('diff', [])
            print(f"   ✅ 成功！耗时: {cost:.2f}秒, 数量: {total}")
            # 打印前 3 个板块名作为验证
            sample = [x['f14'] for x in (items.values() if isinstance(items, dict) else items)[:3]]
            print(f"   📊 样例数据: {sample}")
            return True, cost
        else:
            print(f"   ❌ 失败！状态码: {resp.status_code}")
            return False, cost
    except Exception as e:
        print(f"   💥 异常！错误信息: {str(e)}")
        return False, time.time() - start

if __name__ == "__main__":
    urls = {
        "地域板块 (Region)": "http://17.push2.eastmoney.com/api/qt/clist/get?pn=1&pz=3000&po=1&np=1&ut=bd1d9ddb04089700cf9c27f6f7426281&fltt=2&invt=2&fid=f3&fs=m:90+t:1&fields=f12,f13,f14",
        "概念板块 (Concept)": "http://17.push2.eastmoney.com/api/qt/clist/get?pn=1&pz=3000&po=1&np=1&ut=bd1d9ddb04089700cf9c27f6f7426281&fltt=2&invt=2&fid=f3&fs=m:90+t:3&fields=f12,f13,f14",
        "行业板块 (Industry)": "http://17.push2.eastmoney.com/api/qt/clist/get?pn=1&pz=3000&po=1&np=1&ut=bd1d9ddb04089700cf9c27f6f7426281&fltt=2&invt=2&fid=f3&fs=m:90+t:2&fields=f12,f13,f14"
    }

    results = []
    for name, url in urls.items():
        results.append(test_fetch(name, url))
        time.sleep(1) # 稍微间隔一下

    print("\n" + "="*30)
    print("📈 测试总结:")
    total_cost = sum(r[1] for r in results)
    print(f"总耗时: {total_cost:.2f} 秒")
    print("="*30)
