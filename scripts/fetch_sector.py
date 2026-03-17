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
        # 字段映射: 根据你的 Proxy fields2 (f51~f58) 共有 8 个字段
        # f51(日期), f52(开盘), f53(收盘), f54(最高), f55(最低), f56(量), f57(额), f58(振幅)
        cols = ['date','open','close','high','low','volume','amount','amplitude']
        df_k = pd.DataFrame([r[:8] for r in rows], columns=cols)
        
        df_k['code'] = code
        df_k['name'] = name
        df_k['type'] = info['type']

    # 2. 抓取成份股
    # 你的 Proxy 内部 get_sector_constituents 已经设置了 pz=3000，通常能一次拿完
    res_c = proxy.get_sector_constituents(code)
    consts = []
    if res_c and res_c.get('data') and res_c['data'].get('diff'):
        # 兼容 dict 和 list 格式的返回
        diff_data = res_c['data']['diff']
        items = diff_data.values() if isinstance(diff_data, dict) else diff_data
        for item in items:
            consts.append({
                "sector_code": code, 
                "stock_code": item['f12'], 
                "sector_name": name
            })
            
    return df_k, consts

def main():
    print(f"\n{'='*70}")
    print(f"[*] 东方财富全量板块抓取引擎启动 | {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"{'='*70}\n")

    # 1. 获取列表 (利用 Proxy 内部的自动翻页逻辑)
    targets = {
        "Industry": "m:90 t:2", 
        "Concept":  "m:90 t:3", 
        "Region":   "m:90 t:1"
    }
    
    all_sectors_data = []
    print("[1/3] 正在拉取板块分类列表 (已开启 Proxy 自动翻页)...")
    
    for label, fs in targets.items():
        # 直接调用，Proxy 内部会处理 while True
        lst = proxy.get_sector_list(fs)
        print(f"    - {label}: 成功获取 {len(lst)} 个板块")
        for x in lst:
            all_sectors_data.append({
                "code": x['f12'], "market": x['f13'], "name": x['f14'], "type": label
            })
            
    df_list = pd.DataFrame(all_sectors_data).drop_duplicates('code')
    total_found = len(df_list)
    print(f"\n[*] 列表审计完成。合并去重后总数: {total_found}")
    
    if total_found < 800:
        print("[🔥 异常] 板块总数过低，可能存在网络截断，脚本强制熔断。")
        sys.exit(1)

    # 2. 并发抓取 K 线与成份股
    print(f"\n[2/3] 开始并发抓取 {total_found} 个板块详情 (线程池: 20)...")
    all_k, all_c = [], []
    
    with concurrent.futures.ThreadPoolExecutor(max_workers=20) as executor:
        future_to_name = {
            executor.submit(fetch_one_sector, row): row['name'] 
            for _, row in df_list.iterrows()
        }
        
        for future in tqdm(concurrent.futures.as_completed(future_to_name), 
                          total=len(future_to_name), 
                          desc="采集进度"):
            try:
                k, c = future.result()
                if not k.empty: all_k.append(k)
                if c: all_c.extend(c)
            except Exception as e:
                # 记录但不中断
                pass

    # 3. 清洗、过滤未来日期并落地
    print(f"\n[3/3] 正在执行数据清洗与持久化...")
    cleaner = DataCleaner()
    today_str = datetime.now().strftime('%Y-%m-%d')
    
    if all_k:
        full_k = pd.concat(all_k)
        
        # --- 核心：剔除服务器返回的未来脏数据 ---
        full_k = full_k[full_k['date'] <= today_str]
        
        # 调用你的 DataCleaner 转换数据类型
        full_k = cleaner.clean_sector_kline(full_k)
        
        k_path = f"{OUTPUT_DIR}/sector_kline_full.parquet"
        full_k.to_parquet(k_path, index=False)
        print(f"[+] K线存储成功: {len(full_k)} 行 | 覆盖日期至 {full_k['date'].max()}")
        
    if all_c:
        full_c = pd.DataFrame(all_c)
        full_c['date'] = today_str
        c_path = f"{OUTPUT_DIR}/sector_constituents_latest.parquet"
        full_c.to_parquet(c_path, index=False)
        print(f"[+] 成份股存储成功: {len(full_c)} 条映射关系")

    print(f"\n{'='*70}")
    print(f"[*] 任务圆满完成")
    print(f"{'='*70}\n")

if __name__ == "__main__":
    main()
