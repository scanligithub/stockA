import sys
import os
import argparse
import json
import datetime
import requests
import time
import subprocess
import pandas as pd
import numpy as np
from tqdm import tqdm
from concurrent.futures import ProcessPoolExecutor, as_completed
from utils.cleaner import DataCleaner

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
HEADERS = {'User-Agent': 'Mozilla/5.0'}

def fetch_sina_flow(code, start, end):
    symbol = code.replace(".", "")
    url = f"https://vip.stock.finance.sina.com.cn/quotes_service/api/json_v2.php/MoneyFlow.ssl_qsfx_lscjfb?page=1&num=10000&sort=opendate&asc=0&daima={symbol}"
    for attempt in range(3): 
        try:
            r = requests.get(url, headers=HEADERS, timeout=10)
            if r.status_code != 200: continue
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
            df = df[(df['date'] >= start) & (df['date'] <= end)].copy()
            if not df.empty: df['code'] = code
            return df
        except:
            time.sleep(1)
    return pd.DataFrame()

def process_flow(code, start, end):
    try:
        df_f = fetch_sina_flow(code, start, end)
        return code, df_f, ""
    except Exception as e:
        return code, None, str(e)

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--index", type=int, required=True)
    parser.add_argument("--codes", type=str, required=True)
    parser.add_argument("--year", type=int, default=0)
    args = parser.parse_args()

    codes = json.loads(args.codes)
    future_date = "2099-12-31"
    if args.year == 9999:
        start, end = "2005-01-01", future_date
    elif args.year > 0:
        start, end = f"{args.year}-01-01", f"{args.year}-12-31"
    else:
        curr_year = datetime.datetime.now().year
        start, end = f"{curr_year}-01-01", future_date

    print(f"Job {args.index}: {len(codes)} stocks ({start}~{end})")

    csv_out = f"temp_kline_{args.index}.csv"
    print("🚀 Invoking TDX Go Engine for K-lines & GBBQ Shares...")
    go_cmd = ["./tdx_fetcher", f"-codes={','.join(codes)}", f"-out={csv_out}"]
    subprocess.run(go_cmd, check=True)

    df_k_all = pd.DataFrame()
    if os.path.exists(csv_out):
        df_k_all = pd.read_csv(csv_out)
        if not df_k_all.empty:
            df_k_all['date_dt'] = pd.to_datetime(df_k_all['date'], errors='coerce')
            df_k_all = df_k_all.dropna(subset=['date_dt'])
            
            num_cols = ['open', 'high', 'low', 'close', 'volume', 'amount', 'adjustFactor', 'totalShares', 'floatShares']
            for col in num_cols:
                if col in df_k_all.columns:
                    df_k_all[col] = pd.to_numeric(df_k_all[col], errors='coerce').fillna(0.0)

            # 载入预计算的财务数据主表
            if os.path.exists("finance_master.parquet"):
                df_fin = pd.read_parquet("finance_master.parquet")
                df_k_all = df_k_all.sort_values('date_dt')
                
                # 🌟 核心：Pandas 时间旅行级联，完美规避未来函数
                df_k_all = pd.merge_asof(
                    df_k_all, 
                    df_fin,
                    by='code',
                    left_on='date_dt',
                    right_on='publish_date',
                    direction='backward'
                )
            else:
                df_k_all['net_profit_ttm'] = 0.0
                df_k_all['net_assets'] = 0.0

            df_k_all['date'] = df_k_all['date_dt'].dt.strftime('%Y-%m-%d')
            df_k_all = df_k_all[(df_k_all['date'] >= start) & (df_k_all['date'] <= end)].copy()
            df_k_all = df_k_all.sort_values(['code', 'date'])
            
            # 基础衍生计算
            df_k_all['pctChg'] = df_k_all.groupby('code')['close'].pct_change() * 100
            df_k_all['pctChg'] = df_k_all['pctChg'].fillna(0.0)
            
            # 🌟 进阶衍生与估值计算
            # 换手率 = 成交量(股) / (流通股本(万股) * 10000) * 100
            df_k_all['turn'] = np.where(df_k_all['floatShares'] > 0, 
                                        (df_k_all['volume'] / (df_k_all['floatShares'] * 10000)) * 100, 0.0)
            
            # 总市值与流通市值 (单位：万元)。收盘价(元) * 股本(万股) = 市值(万元)
            df_k_all['total_mv'] = df_k_all['close'] * df_k_all['totalShares']
            df_k_all['float_mv'] = df_k_all['close'] * df_k_all['floatShares']
            
            # 滚动市盈率 与 最新市净率 (强行拦截负利润与资不抵债，防止回测失效)
            df_k_all['peTTM'] = np.where(df_k_all['net_profit_ttm'] > 0, 
                                         df_k_all['total_mv'] / df_k_all['net_profit_ttm'], 0.0)
            df_k_all['pbMRQ'] = np.where(df_k_all['net_assets'] > 0, 
                                         df_k_all['total_mv'] / df_k_all['net_assets'], 0.0)

            # 标记 ST
            st_map = {}
            if os.path.exists("stock_list_master.json"):
                with open("stock_list_master.json", "r", encoding="utf-8") as f:
                    m_list = json.load(f)
                    for x in m_list:
                        st_map[x['code']] = 1 if "ST" in x['code_name'] else 0
            df_k_all['isST'] = df_k_all['code'].map(st_map).fillna(0).astype(int)

    print("🚀 Fetching Sina Money Flow...")
    res_f = []
    with ProcessPoolExecutor(max_workers=min(8, os.cpu_count())) as executor:
        futures = [executor.submit(process_flow, c, start, end) for c in codes]
        for future in tqdm(as_completed(futures), total=len(codes)):
            c, f, err = future.result()
            if f is not None and not f.empty:
                res_f.append(f)

    os.makedirs("temp_parts", exist_ok=True)
    cleaner = DataCleaner()

    if not df_k_all.empty:
        df_k_all = cleaner.clean_stock_kline(df_k_all)
        df_k_all.to_parquet(f"temp_parts/kline_part_{args.index}.parquet", index=False)

    if res_f:
        df_f_all = pd.concat(res_f)
        df_f_all = cleaner.clean_money_flow(df_f_all)
        df_f_all.to_parquet(f"temp_parts/flow_part_{args.index}.parquet", index=False)
        
    print(f"✅ Job {args.index} Finished.")

if __name__ == "__main__":
    main()
