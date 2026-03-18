import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import argparse
import json
import pandas as pd
import datetime
import requests
import time
import baostock as bs
from tqdm import tqdm
from concurrent.futures import ProcessPoolExecutor, as_completed
from utils.cleaner import DataCleaner

HEADERS = {
    'User-Agent': 'Mozilla/5.0'
}

# ==============================
# 通用重试装饰器
# ==============================
def retry(func, max_retry=3, delay=1):
    def wrapper(*args, **kwargs):
        for i in range(max_retry):
            try:
                return func(*args, **kwargs)
            except Exception as e:
                if i == max_retry - 1:
                    print(f"❌ Retry failed: {e}")
                    return None
                time.sleep(delay)
        return None
    return wrapper

# ==============================
# 新浪资金流
# ==============================
@retry
def fetch_sina_flow(code, start, end):
    symbol = code.replace(".", "")
    url = f"https://vip.stock.finance.sina.com.cn/quotes_service/api/json_v2.php/MoneyFlow.ssl_qsfx_lscjfb?page=1&num=10000&sort=opendate&asc=0&daima={symbol}"
    r = requests.get(url, headers=HEADERS, timeout=10)
    data = r.json()
    if not data:
        return pd.DataFrame()
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

# ==============================
# 核心：单股票处理（多进程执行）
# ==============================
def process_one(args):
    code, start, end = args
    try:
        lg = bs.login()
        if lg.error_code != '0':
            return None, None

        fields = "date,code,open,high,low,close,volume,amount,turn,pctChg,peTTM,pbMRQ,isST"

        # ---------- K 线 ----------
        rs = bs.query_history_k_data_plus(
            code, fields,
            start_date=start, end_date=end,
            frequency="d", adjustflag="3"
        )
        k_data = []
        if rs.error_code == '0':
            while rs.next():
                k_data.append(rs.get_row_data())
        df_k = pd.DataFrame(k_data, columns=fields.split(",")) if k_data else pd.DataFrame()

        # ---------- 复权因子 ----------
        if not df_k.empty:
            rs_fac = bs.query_adjust_factor(
                code, start_date="1990-01-01", end_date="2099-12-31"
            )
            fac_data = []
            if rs_fac.error_code == '0':
                while rs_fac.next():
                    fac_data.append(rs_fac.get_row_data())

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

        # ---------- 资金流 ----------
        df_f = fetch_sina_flow(code, start, end)

        bs.logout()
        return df_k, df_f

    except Exception as e:
        print(f"⚠️ Error {code}: {e}")
        try:
            bs.logout()
        except:
            pass
        return None, None

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

    cleaner = DataCleaner()
    res_k = []
    res_f = []

    # 🚀 多进程核心
    workers = min(6, os.cpu_count())
    with ProcessPoolExecutor(max_workers=workers) as executor:
        futures = [
            executor.submit(process_one, (code, start, end))
            for code in codes
        ]
        for future in tqdm(as_completed(futures), total=len(futures), desc="处理股票"):
            k, f = future.result()
            if k is not None and not k.empty:
                res_k.append(k)
            if f is not None and not f.empty:
                res_f.append(f)

    print(f"[+] K 线完成：{len(res_k)}")
    print(f"[+] 资金流完成：{len(res_f)}")

    os.makedirs("temp_parts", exist_ok=True)

    if res_k:
        df_k_all = pd.concat(res_k)
        df_k_all = cleaner.clean_stock_kline(df_k_all)
        df_k_all.to_parquet(f"temp_parts/kline_part_{args.index}.parquet", index=False)
        print(f"✅ Saved KLine {args.index}: {len(df_k_all)}")

    if res_f:
        df_f_all = pd.concat(res_f)
        df_f_all = cleaner.clean_money_flow(df_f_all)
        df_f_all.to_parquet(f"temp_parts/flow_part_{args.index}.parquet", index=False)
        print(f"✅ Saved Flow {args.index}: {len(df_f_all)}")

if __name__ == "__main__":
    main()
