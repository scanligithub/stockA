import argparse
import json
import pandas as pd
import os
import datetime
import requests
import time
import baostock as bs
from tqdm import tqdm

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
            # 过滤日期
            mask = (df['date'] >= start) & (df['date'] <= end)
            df = df.loc[mask].copy()
            if not df.empty: df['code'] = code
            return df
        except: time.sleep(1)
    return pd.DataFrame()

def process_one(code, start, end):
    # 1. Baostock KLine
    fields = "date,code,open,high,low,close,volume,amount,turn,pctChg,peTTM,pbMRQ,isST" 
    try:
        # Baostock 是瓶颈，这里必须串行且加重试，否则容易断连
        rs = bs.query_history_k_data_plus(code, fields, start_date=start, end_date=end, frequency="d", adjustflag="3")
        k_data = []
        if rs.error_code == '0':
            while rs.next():
                k_data.append(rs.get_row_data())
        
        if not k_data: return pd.DataFrame(), pd.DataFrame()
        df_k = pd.DataFrame(k_data, columns=fields.split(","))
        
        # 获取复权因子
        rs_fac = bs.query_adjust_factor(code, start_date=start, end_date=end)
        fac_data = []
        if rs_fac.error_code == '0':
            while rs_fac.next():
                fac_data.append(rs_fac.get_row_data())
        
        if fac_data:
            df_fac = pd.DataFrame(fac_data, columns=["code","date","fore","back","adjustFactor"])
            df_k = pd.merge(df_k, df_fac[['date','adjustFactor']], on='date', how='left')
            df_k['adjustFactor'] = df_k['adjustFactor'].ffill().fillna(1.0)
        else:
            df_k['adjustFactor'] = 1.0
            
    except Exception as e:
        print(f"⚠️ Error fetching {code}: {e}")
        return pd.DataFrame(), pd.DataFrame()

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
    
    # 登录 Baostock
    lg = bs.login()
    if lg.error_code != '0':
        print(f"Login failed: {lg.error_msg}")
        return

    res_k, res_f = [], []
    
    # === 关键修改：移除线程池，回归串行循环 ===
    for code in tqdm(codes):
        k, f = process_one(code, start, end)
        if not k.empty: res_k.append(k)
        if not f.empty: res_f.append(f)
        
        # === 关键修改：增加微小延时，保护 Baostock 连接 ===
        time.sleep(0.02)
            
    bs.logout()
    
    os.makedirs("temp_parts", exist_ok=True)
    
    # 只有当有数据时才保存，防止保存空 dataframe 报错
    if res_k: 
        pd.concat(res_k).to_parquet(f"temp_parts/kline_part_{args.index}.parquet", index=False)
        print(f"✅ Saved K-Line part {args.index}: {len(res_k)} stocks")
    
    if res_f: 
        pd.concat(res_f).to_parquet(f"temp_parts/flow_part_{args.index}.parquet", index=False)
        print(f"✅ Saved Flow part {args.index}: {len(res_f)} stocks")

if __name__ == "__main__":
    main()
