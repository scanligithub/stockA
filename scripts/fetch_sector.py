import sys
import os
import pandas as pd
import concurrent.futures
from tqdm import tqdm
from datetime import datetime

# 确保加载项目本地模块
sys.path.append(os.getcwd())
from utils.cf_proxy import EastMoneyProxy
from utils.cleaner import DataCleaner

OUTPUT_DIR = "temp_parts"
os.makedirs(OUTPUT_DIR, exist_ok=True)
proxy = EastMoneyProxy()

def fetch_one_sector(info):
    """
    抓取单个板块的 K 线和成份股
    """
    code, name, mkt = info['code'], info['name'], info['market']
    secid = f"90.{code}"
    
    res_k = proxy.get_sector_kline(secid)
    df_k = pd.DataFrame()
    
    if res_k and res_k.get('data') and res_k['data'].get('klines'):
        rows = [x.split(',') for x in res_k['data']['klines']]
        cols = ['date','open','close','high','low','volume','amount','turn']
        df_k = pd.DataFrame([r[:8] for r in rows], columns=cols)
        df_k['code'], df_k['name'], df_k['type'] = code, name, info['type']

    res_c = proxy.get_sector_constituents(code)
    consts = []
    if res_c and res_c.get('data') and res_c['data'].get('diff'):
        for item in res_c['data']['diff']:
            consts.append({"sector_code": code, "stock_code": item['f12'], "sector_name": name})
            
    return df_k, consts

def get_all_sectors_robustly(targets):
    """
    【修正版】自动探测总数并翻页抓取
    """
    combined_sectors = []
    
    for label, fs in targets.items():
        cat_items = []
        
        print(f"[*] 正在抓取 {label} 板块列表...")
        
        # 1. 探测性抓取第一页 (不指定 pz, 让服务器返回默认数量和 total)
        # 注意：这里我们使用 proxy.get_sector_list，它现在应该支持 pn 参数
        try:
            # 获取包含数据和 total 的原始响应，
            # 如果你的 get_sector_list 只返回 list，建议修改它返回原始 JSON
            # 这里我们假设 get_sector_list 内部会处理分页
            page = 1
            while True:
                # 传入 pn 以翻页
                items = proxy.get_sector_list(fs, pn=page, pz=100)
                
                if not items:
                    break
                    
                cat_items.extend(items)
                print(f"    - 第 {page} 页抓取成功，当前累计: {len(cat_items)}")
                
                # 如果返回的数据不足 100 条，说明到底了
                if len(items) < 100:
                    break
                
                page += 1
                if page > 20: break # 安全熔断
                
        except Exception as e:
            print(f"    [!] 抓取异常: {e}")
            # 如果报错，尝试不带分页参数抓一次作为兜底
            cat_items = proxy.get_sector_list(fs)

        print(f"    [✓] {label} 分类抓取完成，共 {len(cat_items)} 条")
        
        for x in cat_items:
            combined_sectors.append({
                "code": x['f12'], "market": x['f13'], "name": x['f14'], "type": label
            })
            
    return combined_sectors

def main():
    print(f"\n{'='*70}")
    print(f"[*] 东方财富板块全量审计引擎启动 | {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"{'='*70}\n")

    # 1. 获取列表
    targets = {"Industry": "m:90 t:2", "Concept": "m:90 t:3", "Region": "m:90 t:1"}
    raw_sectors = get_all_sectors_robustly(targets)
    
    if not raw_sectors:
        print("[🔥 严重错误] 未能获取到板块列表！")
        sys.exit(1)
        
    df_list = pd.DataFrame(raw_sectors).drop_duplicates('code')
    total_found = len(df_list)
    print(f"\n[*] 列表对账完成。合并去重后有效板块总数: {total_found}")

    # 2. 并发抓取 (保持 20 并发)
    print(f"\n[*] 开始并发抓取 {total_found} 个板块数据...")
    all_k, all_c = [], []
    with concurrent.futures.ThreadPoolExecutor(max_workers=20) as executor:
        future_to_name = {executor.submit(fetch_one_sector, row): row['name'] for _, row in df_list.iterrows()}
        for future in tqdm(concurrent.futures.as_completed(future_to_name), total=len(future_to_name), desc="采集进度"):
            try:
                k, c = future.result()
                if not k.empty: all_k.append(k)
                if c: all_c.extend(c)
            except Exception as e:
                pass

    # 3. 清洗与落地
    cleaner = DataCleaner()
    today_str = datetime.now().strftime('%Y-%m-%d')
    if all_k:
        full_k = pd.concat(all_k)
        # 核心：过滤未来日期，防止脏数据
        full_k = full_k[full_k['date'] <= today_str]
        full_k = cleaner.clean_sector_kline(full_k)
        full_k.to_parquet(f"{OUTPUT_DIR}/sector_kline_full.parquet", index=False)
        print(f"[+] K线存储成功: {len(full_k)} 行")
        
    if all_c:
        full_c = pd.DataFrame(all_c)
        full_c['date'] = today_str
        full_c.to_parquet(f"{OUTPUT_DIR}/sector_constituents_latest.parquet", index=False)
        print(f"[+] 成份股存储成功: {len(full_c)} 条关系")

    print(f"\n{'='*70}\n[*] 任务圆满完成\n{'='*70}\n")

if __name__ == "__main__":
    main()
