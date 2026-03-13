import requests
import time
import os
import json

def test_pagination():
    base_url = "https://stock.scanli.de5.net/"
    # 行业板块总数约 497
    fs_code = "m:90 t:2" 
    
    all_items = []
    page = 1
    pz = 100 # 经过刚才测试，这是 Action 环境允许的稳定上限
    
    print(f"🚀 [分页验证启动] 目标分类: {fs_code} | 单页大小: {pz}")
    start_all = time.time()

    while True:
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
            "fields": "f12,f13,f14"
        }
        
        print(f"📡 正在抓取第 {page} 页...")
        try:
            resp = requests.get(base_url, params=params, timeout=20)
            if resp.status_code == 200:
                res_data = resp.json()
                data_node = res_data.get('data', {})
                total = data_node.get('total', 0)
                diff = data_node.get('diff', [])
                
                # 兼容处理：东财返回可能是 list 也可能是 dict
                items = list(diff.values()) if isinstance(diff, dict) else diff
                
                batch_size = len(items)
                all_items.extend(items)
                
                print(f"   ✅ 第 {page} 页抓取成功: 获得 {batch_size} 条 | 累计: {len(all_items)}/{total}")
                
                # 判断是否抓完
                if batch_size < pz or len(all_items) >= total:
                    print(f"   🏁 抓取任务完成！")
                    break
                
                page += 1
                time.sleep(0.5) # 稳健策略：翻页间歇休眠
            else:
                print(f"   ❌ 第 {page} 页请求失败，HTTP 状态码: {resp.status_code}")
                break
        except Exception as e:
            print(f"   💥 发生异常: {str(e)}")
            break

    end_all = time.time()
    print("\n" + "="*50)
    print("📊 [验证结论]")
    print(f"⏱️  总耗时: {end_all - start_all:.2f} 秒")
    print(f"🔢  应有总数: {total if 'total' in locals() else 'Unknown'}")
    print(f"✅  实测总数: {len(all_items)}")
    
    if len(all_items) > 131:
        print("🔥 突破成功！已超越之前的 131 条天花板。")
    if len(all_items) == total:
        print("🏆 100% 完整覆盖！证明分页逻辑完全可行。")
    print("="*50)

if __name__ == "__main__":
    test_pagination()
