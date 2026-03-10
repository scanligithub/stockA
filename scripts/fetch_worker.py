import sys
import os
import datetime
import requests
import time
import json
import argparse
import baostock as bs
import pandas as pd
from concurrent.futures import ProcessPoolExecutor
from tqdm import tqdm

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if BASE_DIR not in sys.path:
    sys.path.append(BASE_DIR)

from utils.cleaner import DataCleaner

HEADERS = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/120.0.0.0 Safari/537.36'}

def fetch_sina_flow(code, start, end, cf_url):
    """
    通过传入的 cf_url 代理拉取新浪资金流数据
    """
    symbol = code.replace(".", "")
    
    # 显式判断传入的代理 URL 是否有效
    if not cf_url:
        print(f"⚠️ 跳过 {code} 资金流: 代理未配置")
        return pd.DataFrame()
        
    worker_url = f"https://{cf_url}" if not cf_url.startswith("http") else cf_url
    
    params = {
        "target_func": "sina_flow",
        "page": 1,
        "num": 10000,
        "sort": "opendate",
        "asc": 0,
        "daima": symbol
    }
    
    for attempt in range(3):
        try:
            r = requests.get(worker_url, params=params, headers=HEADERS, timeout=15)
            if r.status_code == 200:
                data = r.json()
                if not data: return pd.DataFrame()
                
                df = pd.DataFrame(data)
                rename_map = {
                    'opendate': 'date', 'netamount': 'net_amount',
                    'r0_net': 'main_net', 'r1_net': 'super_net',
                    'r2_net': 'large_net', 'r3_net': 'medium_net',
                    'r4_net': 'small_net'
                }
                df.rename(columns=rename_map, inplace=True)
                
                mask = (df['date'] >= start) & (df['date'] <= end)
                df = df.loc[mask].copy()
                if not df.empty: 
                    df['code'] = code
                return df
            else:
                time.sleep(1)
        except Exception as e:
            time.sleep(1)
            
    return pd.DataFrame()

def process_single_stock(args):
    """
    单进程执行函数。接收包含 4 个元素的元组。
    """
    # 【核心修复】：显式接收 cf_url 参数
    code, start, end, cf_url = args
    bs.login()
    
    df_k = pd.DataFrame()
    df_f = pd.DataFrame()
    
    fields = "date,code,open,high,low,close,volume,amount,turn,pctChg,peTTM,pbMRQ,isST" 
    try:
        rs = bs.query_history_k_data_plus(code, fields, start_date=start, end_date=end, frequency="d", adjustflag="3")
        k_data = []
        if rs.error_code == '0':
            while rs.next(): k_data.append(rs.get_row_data())
            
        df_k = pd.DataFrame(k_data, columns=fields.split(",")) if k_data else pd.DataFrame()
        
        if not df_k.empty:
            rs_fac = bs.query_adjust_factor(code, start_date="1990-01-01", end_date="2099-12-31")
            fac_data = []
            if rs_fac.error_code == '0':
                while rs_fac.next(): fac_data.append(rs_fac.get_row_data())
            
            if fac_data:
                df_fac = pd.DataFrame(fac_data, columns=["code", "date", "fore", "back", "ratio"])
                df_fac = df_fac[['date', 'back']].rename(columns={'back': 'adjustFactor'})
                
                df_k['date'] = pd.to_datetime(df_k['date'])
                df_fac['date'] = pd.to_datetime(df_fac['date'])
                df_fac['adjustFactor'] = pd.to_numeric(df_fac['adjustFactor'], errors='coerce')
                
                df_k = df_k.sort_values('date')
                df_fac = df_fac.sort_values('date')
                
                df_k = pd.merge_asof(df_k, df_fac, on='date', direction='backward')
                df_k['date'] = df_k['date'].dt.strftime('%Y-%m-%d')
                df_k['adjustFactor'] = df_k['adjustFactor'].fillna(1.0)
            else:
                df_k['adjustFactor'] = 1.0
    except Exception as e:
        pass 

    try:
        # 【核心修复】：将 cf_url 传递给新浪资金流抓取函数
        df_f = fetch_sina_flow(code, start, end, cf_url)
    except:
        pass

    bs.logout()
    time.sleep(0.1) 
    return df_k, df_f

def run_stock_pipeline(year=0, codes=None, part_index="all"):
    future_date = "2099-12-31"
    
    if year == 9999:
        start, end = "2005-01-01", future_date
    elif year > 0:
        start, end = f"{year}-01-01", f"{year}-12-31"
    else:
        curr_year = datetime.datetime.now().year
        start, end = f"{curr_year}-01-01", future_date

    valid_codes = []

    if codes is not None and len(codes) > 0:
        valid_codes = codes
        print(f"Job {part_index}: Using {len(valid_codes)} provided codes (from {start} to {end}).")
    else:
        bs.login()
        data = []
        for i in range(10):
            d = (datetime.datetime.now() - datetime.timedelta(days=i)).strftime("%Y-%m-%d")
            rs = bs.query_all_stock(day=d)
            if rs.error_code == '0' and len(rs.data) > 0:
                while rs.next():
                    data.append(rs.get_row_data())
                break
        bs.logout()

        valid_codes = [x[0] for x in data if x[0].startswith(('sh.', 'sz.', 'bj.')) and x[2].strip()]
        print(f"Total {len(valid_codes)} valid A-shares to fetch (from {start} to {end}).")

    if not valid_codes:
        print("⚠️ 未获取到股票列表，跳过任务！")
        return

    # 【核心修复】：在主进程统一获取一次环境变量，并打包塞入每一个任务元组中
    cf_url_global = os.getenv("CF_WORKER_URL", "").strip()
    if not cf_url_global:
        print("⚠️ 警告：主进程未检测到 CF_WORKER_URL 环境变量！资金流抓取将被跳过。")
        
    tasks = [(code, start, end, cf_url_global) for code in valid_codes]
    res_k, res_f = [], []
    cleaner = DataCleaner()

    os.makedirs("temp_parts", exist_ok=True)
    
    with ProcessPoolExecutor(max_workers=5) as executor:
        for k, f in tqdm(executor.map(process_single_stock, tasks), total=len(tasks), desc=f"Job {part_index}"):
            if not k.empty: res_k.append(k)
            if not f.empty: res_f.append(f)

    if res_k: 
        df_k_all = pd.concat(res_k)
        df_k_all = cleaner.clean_stock_kline(df_k_all)
        df_k_all.to_parquet(f"temp_parts/kline_part_{part_index}.parquet", index=False)
        print(f"✅ Job {part_index} Saved K-Line: {len(df_k_all)} rows.")
    
    if res_f: 
        df_f_all = pd.concat(res_f)
        df_f_all = cleaner.clean_money_flow(df_f_all)
        df_f_all.to_parquet(f"temp_parts/flow_part_{part_index}.parquet", index=False)
        print(f"✅ Job {part_index} Saved Flow: {len(df_f_all)} rows.")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--index", type=str, default="all", help="分片索引")
    parser.add_argument("--codes", type=str, default=None, help="JSON格式的股票代码列表")
    parser.add_argument("--year", type=int, default=0, help="年份 (0=YTD, 9999=全部)")
    args = parser.parse_args()
    
    codes_list = json.loads(args.codes) if args.codes else None
    run_stock_pipeline(year=args.year, codes=codes_list, part_index=args.index)
