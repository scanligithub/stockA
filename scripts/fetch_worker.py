import sys
import os
import argparse
import json
import datetime
import requests
import time
import subprocess
import pandas as pd
from tqdm import tqdm
from concurrent.futures import ProcessPoolExecutor, as_completed

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from utils.cleaner import DataCleaner

HEADERS = {'User-Agent': 'Mozilla/5.0'}

# [保留] 稳健的新浪资金流接口
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
            df.rename(columns={
                'opendate': 'date', 'netamount': 'net_amount',
                'r0_net': 'main_net', 'r1_net': 'super_net',
                'r2_net': 'large_net', 'r3_net': 'medium_net', 'r4_net': 'small_net'
            }, inplace=True)
            df = df[(df['date'] >= start) & (df['date'] <= end)].copy()
            if not df.empty: df['code'] = code
            return df
        except:
            time.sleep(1)
    return pd.DataFrame()

def process_flow_tasks(code, start, end):
    df_f = fetch_sina_flow(code, start, end)
    return code, df_f

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

    # 1. 启动 Go 引擎执行本地计算
    csv_out = f"temp_kline_{args.index}.csv"
    subprocess.run(["./tdx_fetcher", f"-codes={','.join(codes)}", f"-out={csv_out}"], check=True)

    df_k_all = pd.DataFrame()
    if os.path.exists(csv_out):
        df_k_all = pd.read_csv(csv_out)
        if not df_k_all.empty:
            df_k_all['date_dt'] = pd.to_datetime(df_k_all['date'], errors='coerce')
            df_k_all = df_k_all.dropna(subset=['date_dt'])
            df_k_all['date'] = df_k_all['date_dt'].dt.strftime('%Y-%m-%d')
            df_k_all = df_k_all.drop(columns=['date_dt'])
            df_k_all = df_k_all[(df_k_all['date'] >= start) & (df_k_all['date'] <= end)].copy()
            df_k_all = df_k_all.sort_values(['code', 'date'])
            
            # Python端计算涨跌幅
            df_k_all['pctChg'] = df_k_all.groupby('code')['close'].pct_change() * 100
            df_k_all['pctChg'] = df_k_all['pctChg'].fillna(0.0)

            # ST 标记
            st_map = {}
            if os.path.exists("stock_list_master.json"):
                with open("stock_list_master.json", "r", encoding="utf-8") as f:
                    for x in json.load(f):
                        st_map[x['code']] = 1 if "ST" in x['code_name'] else 0
            df_k_all['isST'] = df_k_all['code'].map(st_map).fillna(0).astype(int)

            # 🛡️ 安全隔离：东财反爬严格，通达信不含历史财报。此处占位以保证 Schema 不崩
            df_k_all['peTTM'] = 0.0
            df_k_all['pbMRQ'] = 0.0

    # 2. 并发拉取新浪资金流
    print("🚀 Fetching Sina Flow...")
    res_f = []
    with ProcessPoolExecutor(max_workers=min(10, os.cpu_count())) as executor:
        futures = [executor.submit(process_flow_tasks, c, start, end) for c in codes]
        for future in tqdm(as_completed(futures), total=len(codes)):
            c, df_f = future.result()
            if not df_f.empty: res_f.append(df_f)

    # 3. 清洗并压缩落盘 Parquet
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
