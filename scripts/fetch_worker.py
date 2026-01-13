import argparse
import json
import pandas as pd
import os
import datetime
import requests
import time
import baostock as bs
import concurrent.futures

# === 复用你原始代码中的下载逻辑 ===
HEADERS = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/120.0.0.0 Safari/537.36'}

def fetch_sina_flow(code, start, end):
    symbol = code.replace(".", "")
    url = f"https://vip.stock.finance.sina.com.cn/quotes_service/api/json_v2.php/MoneyFlow.ssl_qsfx_lscjfb?page=1&num=500&sort=opendate&asc=0&daima={symbol}"
    for _ in range(3):
        try:
            r = requests.get(url, headers=HEADERS, timeout=5)
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
            if not df.empty: df['code'] = code
            return df
        except: time.sleep(1)
    return pd.DataFrame()

def process_one(code, start, end):
    # 1. Baostock KLine
    # 获取 isST 状态 (tradeStatus=1 正常) - Baostock K线不带 isST，需另想办法或忽略，此处暂时只取基本字段
    fields = "date,code,open,high,low,close,volume,amount,turn,pctChg,peTTM,pbMRQ,isST" 
    # 注: Baostock isST 字段支持较新版本，若不支持可移除
    try:
        rs = bs.query_history_k_data_plus(code, fields, start_date=start, end_date=end, frequency="d", adjustflag="3")
        k_data = [rs.get_row_data() for _ in range(1000) if rs.next()] # 简写循环
        if not k_data: return pd.DataFrame(), pd.DataFrame()
        df_k = pd.DataFrame(k_data, columns=fields.split(","))
        
        # 获取因子
        rs_fac = bs.query_adjust_factor(code, start_date=start, end_date=end)
        fac_data = [rs_fac.get_row_data() for _ in range(1000) if rs_fac.next()]
        if fac_data:
            df_fac = pd.DataFrame(fac_data, columns=["code","date","fore","back","adjustFactor"])
            df_k = pd.merge(df_k, df_fac[['date','adjustFactor']], on='date', how='left')
            df_k['adjustFactor'] = df_k['adjustFactor'].ffill().fillna(1.0)
        else:
            df_k['adjustFactor'] = 1.0
    except:
        return pd.DataFrame(), pd.DataFrame()

    # 2. Sina Flow
    df_f = fetch_sina_flow(code, start, end)
    
    return df_k, df_f

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--index", type=int, required=True)
    parser.add_argument("--codes", type=str, required=True)
    parser.add_argument("--year", type=int, default=0, help="Year to fetch (0 for YTD)")
    args = parser.parse_args()
    
    codes = json.loads(args.codes)
    
    if args.year > 0:
        start = f"{args.year}-01-01"
        end = f"{args.year}-12-31"
    else:
        curr_year = datetime.datetime.now().year
        start = f"{curr_year}-01-01"
        end = (datetime.datetime.now() - datetime.timedelta(days=1)).strftime("%Y-%m-%d")

    print(f"Job {args.index}: Fetching {len(codes)} stocks ({start}~{end})...")
    
    bs.login()
    res_k, res_f = [], []
    
    with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
        futures = {executor.submit(process_one, c, start, end): c for c in codes}
        for fut in concurrent.futures.as_completed(futures):
            k, f = fut.result()
            if not k.empty: res_k.append(k)
            if not f.empty: res_f.append(f)
            
    bs.logout()
    
    os.makedirs("temp_parts", exist_ok=True)
    if res_k: pd.concat(res_k).to_parquet(f"temp_parts/kline_part_{args.index}.parquet", index=False)
    if res_f: pd.concat(res_f).to_parquet(f"temp_parts/flow_part_{args.index}.parquet", index=False)

if __name__ == "__main__":
    main()
