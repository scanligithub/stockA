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
    # 统一转换 secid，板块标识符固定为 90.
    secid = f"90.{code}"
    
    # 1. 抓取 K 线数据
    res_k = proxy.get_sector_kline(secid)
    df_k = pd.DataFrame()
    
    if res_k and res_k.get('data') and res_k['data'].get('klines'):
        rows = [x.split(',') for x in res_k['data']['klines']]
        # 核心字段映射
        cols = ['date','open','close','high','low','volume','amount','turn']
        df_k = pd.DataFrame([r[:8] for r in rows], columns=cols)
        
        df_k['code'] = code
        df_k['name'] = name
        df_k['type'] = info['type']

    # 2. 抓取成份股
    res_c = proxy.get_sector_constituents(code)
    consts = []
    if res_c and res_c.get('data') and res_c['data'].get('diff'):
        for item in res_c['data']['diff']:
            consts.append({
                "sector_code": code, 
                "stock_code": item['f12'], 
                "sector_name": name
            })
            
    return df_k, consts

def get_all_sectors_robustly(targets):
    """
    【修正版】使用现有的 get_sector_list 进行盲翻页抓取
    """
    combined_sectors = []
    
    for label, fs in targets.items():
        cat_items = []
        page = 1
        
        print(f"[*] 正在抓取 {label} 板块列表...")
        
        while True:
            # 使用你 Proxy 中现有的方法，通过 pn 和 pz 进行翻页
            # 即使你的方法没有显示处理这些参数，根据东财 API 规律，直接传参通常有效
            try:
                # 尝试调用，假设 get_sector_list 接受 fs, pn, pz
                items = proxy.get_sector_list(fs, pn=page, pz=100)
            except TypeError:
                # 如果你的 get_sector_list 不接受 pn, pz，则说明 Proxy 需要微调
                # 这种情况下我们打印警告并只抓第一页
                print(f"    [!] 警告: Proxy.get_sector_list 不支持分页参数，仅抓取首屏数据")
                items = proxy.get_sector_list(fs)
                cat_items.extend(items)
                break

            if not items:
                break
                
            cat_items.extend(items)
            print(f"    - 第 {page} 页抓取成功，当前累计: {len(cat_items)}")
            
            # 判定逻辑：如果返回的数据少于 100 条，说明已经是最后一页了
            if len(items) < 100:
                break
            
            page += 1
            # 安全熔断，防止无限循环
            if page > 20: break
            
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
    targets = {
        "Industry": "m:90 t:2", 
        "Concept":  "m:90 t:3", 
        "Region":   "m:90 t:1"
    }
    
    raw_sectors = get_all_sectors_robustly(targets)
    if not raw_sectors:
        print("[🔥 严重错误] 未能获取到任何板块列表！")
        sys.exit(1)
        
    df_list = pd.DataFrame(raw_sectors).drop_duplicates('code')
    total_found = len(df_list)
    print(f"\n[*] 列表对账完成。合并去重后有效板块总数: {total_found}")
    
    # 2. 并发抓取
    print(f"\n[*] 开始并发抓取 {total_found} 个板块数据 (线程池: 20)...")
    all_k, all_c = [], []
    
    with concurrent.futures.ThreadPoolExecutor(max_workers=20) as executor:
        future_to_name = {
            executor.submit(fetch_one_sector, row): row['name'] 
            for _, row in df_list.iterrows()
        }
        
        for future in tqdm(concurrent.futures.as_completed(future_to_name), 
                          total=len(future_to_name), 
                          desc="全量采集进度"):
            sector_name = future_to_name[future]
            try:
                k, c = future.result()
                if not k.empty: all_k.append(k)
                if c: all_c.extend(c)
            except Exception as e:
                print(f"\n[-] 板块 [{sector_name}] 异常: {str(e)}")

    # 3. 清洗与持久化
    print(f"\n[*] 正在进行数据清洗与落库...")
    cleaner = DataCleaner()
    today_str = datetime.now().strftime('%Y-%m-%d')
    
    if all_k:
        full_k = pd.concat(all_k)
        # 剔除未来日期
        full_k = full_k[full_k['date'] <= today_str]
        full_k = cleaner.clean_sector_kline(full_k)
        
        k_path = f"{OUTPUT_DIR}/sector_kline_full.parquet"
        full_k.to_parquet(k_path, index=False)
        print(f"[+] K线存储成功: {len(full_k)} 行")
        
    if all_c:
        full_c = pd.DataFrame(all_c)
        full_c['date'] = today_str
        c_path = f"{OUTPUT_DIR}/sector_constituents_latest.parquet"
        full_c.to_parquet(c_path, index=False)
        print(f"[+] 成份股存储成功: {len(full_c)} 条关系对")

    print(f"\n{'='*70}")
    print(f"[*] 任务圆满完成")
    print(f"{'='*70}\n")

if __name__ == "__main__":
    main()
