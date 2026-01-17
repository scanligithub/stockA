import sys
import os
# 1. 解决导入路径问题
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import argparse
import json
import pandas as pd
import datetime
import requests
import time
import baostock as bs
from tqdm import tqdm
# 2. 引入清洗器
from utils.cleaner import DataCleaner

HEADERS = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/120.0.0.0 Safari/537.36'}

def fetch_sina_flow(code, start, end):
    symbol = code.replace(".", "")
    # num=10000 确保拉取全量历史
    url = f"https://vip.stock.finance.sina.com.cn/quotes_service/api/json_v2.php/MoneyFlow.ssl_qsfx_lscjfb?page=1&num=10000&sort=opendate&asc=0&daima={symbol}"
    for _ in range(3):
        try:
            r = requests.get(url, headers=HEADERS, timeout=10)
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
            # 根据请求的 start/end 过滤
            mask = (df['date'] >= start) & (df['date'] <= end)
            df = df.loc[mask].copy()
            if not df.empty: 
                df['code'] = code
            return df
        except: time.sleep(1)
    return pd.DataFrame()

def process_one(code, start, end):
    # 1. Baostock KLine
    fields = "date,code,open,high,low,close,volume,amount,turn,pctChg,peTTM,pbMRQ,isST" 
    try:
        rs = bs.query_history_k_data_plus(code, fields, start_date=start, end_date=end, frequency="d", adjustflag="3")
        k_data = []
        if rs.error_code == '0':
            while rs.next():
                k_data.append(rs.get_row_data())
        
        df_k = pd.DataFrame(k_data, columns=fields.split(",")) if k_data else pd.DataFrame()
        
        # 复权因子
        rs_fac = bs.query_adjust_factor(code, start_date=start, end_date=end)
        fac_data = []
        if rs_fac.error_code == '0':
            while rs_fac.next():
                fac_data.append(rs_fac.get_row_data())
        
        if fac_data and not df_k.empty:
            df_fac = pd.DataFrame(fac_data, columns=["code","date","fore","back","adjustFactor"])
            df_k = pd.merge(df_k, df_fac[['date','adjustFactor']], on='date', how='left')
            df_k['adjustFactor'] = df_k['adjustFactor'].ffill().fillna(1.0)
        elif not df_k.empty:
            df_k['adjustFactor'] = 1.0
            
    except Exception as e:
        print(f"⚠️ Error fetching {code}: {e}")
        df_k = pd.DataFrame()

    # 2. Sina Flow
    try:
        df_f = fetch_sina_flow(code, start, end)
    except:
        df_f = pd.DataFrame()
    
    return df_k, df_f

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--index", type=int, required=True)
    parser.add_argument("--codes", type=str, required=True)
    parser.add_argument("--year", type=int, default=0, help="Year to fetch (0=YTD, 9999=Full History)")
    args = parser.parse_args()
    
    codes = json.loads(args.codes)
    
    # === 极简逻辑：直接请求到最新时间 (2099年) ===
    # 这样无论是时区差异还是服务器结算延迟，只要数据源有了，就一定能下到
    future_date = "2099-12-31"
    
    if args.year == 9999:
        # 全量模式
        start, end = "2005-01-01", future_date
    elif args.year > 0:
        # 历史指定年份模式 (保持精确)
        start, end = f"{args.year}-01-01", f"{args.year}-12-31"
    else:
        # 默认模式 (YTD)：从今年1月1日到最新
        curr_year = datetime.datetime.now().year
        start, end = f"{curr_year}-01-01", future_date

    print(f"Job {args.index}: Fetching {len(codes)} stocks ({start}~{end})...")
    
    lg = bs.login()
    if lg.error_code != '0':
        print(f"Login failed: {lg.error_msg}")
        return

    res_k, res_f = [], []
    cleaner = DataCleaner()
    
    for code in tqdm(codes):
        k, f = process_one(code, start, end)
        if not k.empty: res_k.append(k)
        if not f.empty: res_f.append(f)
        time.sleep(0.02)
            
    bs.logout()
    
    os.makedirs("temp_parts", exist_ok=True)
    
    if res_k: 
        df_k_all = pd.concat(res_k)
        df_k_all = cleaner.clean_stock_kline(df_k_all)
        df_k_all.to_parquet(f"temp_parts/kline_part_{args.index}.parquet", index=False)
        print(f"✅ Saved K-Line part {args.index}: {len(df_k_all)} rows")
    
    if res_f: 
        df_f_all = pd.concat(res_f)
        df_f_all = cleaner.clean_money_flow(df_f_all)
        df_f_all.to_parquet(f"temp_parts/flow_part_{args.index}.parquet", index=False)
        print(f"✅ Saved Flow part {args.index}: {len(df_f_all)} rows")

if __name__ == "__main__":
    main()
