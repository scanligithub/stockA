import sys
import os
import argparse
import json
import datetime
import requests
import time
import subprocess

# 向系统注册项目根目录，确保多层级目录下导入 utils 模块不报错
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import pandas as pd
from tqdm import tqdm
from concurrent.futures import ProcessPoolExecutor, as_completed
from utils.cleaner import DataCleaner

HEADERS = {'User-Agent': 'Mozilla/5.0'}

# ==============================
# 新浪资金流 (保持不变)
# ==============================
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

# ==============================
# 主函数
# ==============================
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

    # 1. 编译并调用 Go 引擎极速获取 K 线
    csv_out = f"temp_kline_{args.index}.csv"
    print("🚀 Invoking TDX Go Engine for K-lines...")
    go_cmd = ["./tdx_fetcher", f"-codes={','.join(codes)}", f"-out={csv_out}"]
    subprocess.run(go_cmd, check=True)

    # 2. Python 处理 K 线并补全 Schema
    df_k_all = pd.DataFrame()
    if os.path.exists(csv_out):
        df_k_all = pd.read_csv(csv_out)
        if not df_k_all.empty:
            
            # 🚀 核心修复：强制将从 CSV 读取的价格、数量和复权因子列转换为 float 数值类型，防止 pandas 误判为 str 导致计算崩溃
            num_cols = ['open', 'high', 'low', 'close', 'volume', 'amount', 'adjustFactor']
            for col in num_cols:
                if col in df_k_all.columns:
                    df_k_all[col] = pd.to_numeric(df_k_all[col], errors='coerce')

            # 过滤年份
            df_k_all = df_k_all[(df_k_all['date'] >= start) & (df_k_all['date'] <= end)].copy()
            
            # 计算衍生指标
            df_k_all = df_k_all.sort_values(['code', 'date'])
            df_k_all['pctChg'] = df_k_all.groupby('code')['close'].pct_change() * 100
            df_k_all['pctChg'] = df_k_all['pctChg'].fillna(0.0)
            
            # 填充缺失指标
            df_k_all['turn'] = 0.0
            df_k_all['peTTM'] = 0.0
            df_k_all['pbMRQ'] = 0.0
            
            # 标记 ST
            st_map = {}
            if os.path.exists("stock_list_master.json"):
                with open("stock_list_master.json", "r", encoding="utf-8") as f:
                    m_list = json.load(f)
                    for x in m_list:
                        st_map[x['code']] = 1 if "ST" in x['code_name'] else 0
            df_k_all['isST'] = df_k_all['code'].map(st_map).fillna(0).astype(int)

    # 3. 并发获取 Sina 资金流
    print("🚀 Fetching Sina Money Flow...")
    res_f = []
    with ProcessPoolExecutor(max_workers=min(8, os.cpu_count())) as executor:
        futures = [executor.submit(process_flow, c, start, end) for c in codes]
        for future in tqdm(as_completed(futures), total=len(codes)):
            c, f, err = future.result()
            if f is not None and not f.empty:
                res_f.append(f)

    # 4. 数据清洗与落盘
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
