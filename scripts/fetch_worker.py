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
# 获取交易日历
# ==============================
def get_trade_calendar(start_date, end_date):
    """获取交易日历"""
    lg = bs.login()
    rs = bs.query_trade_dates(start_date=start_date, end_date=end_date)
    dates = []
    while rs.next():
        row = rs.get_row_data()
        if row[1] == '1':
            dates.append(row[0])
    bs.logout()
    return set(dates)

# ==============================
# 检测缺失股票
# ==============================
def find_missing_stocks(df, trade_dates, tolerance=0):
    """
    检测数据缺失的股票
    tolerance: 允许缺失的比例（默认 10%，用于容忍停牌）
    """
    if df.empty:
        return list(df["code"].unique())

    grouped = df.groupby("code")["date"].nunique()
    expected = len(trade_dates)
    bad_codes = []

    for code, count in grouped.items():
        ratio = count / expected
        if ratio < (1 - tolerance):
            bad_codes.append(code)

    return bad_codes

# ==============================
# 核心：单股票处理（多进程执行，带重试）
# ==============================
def process_one(args):
    """
    下载单只股票的 K 线数据
    遇到"用户未登录"错误时会自动重试
    """
    code, start, end = args
    max_retry = 3

    for attempt in range(max_retry):
        try:
            lg = bs.login()
            if lg.error_code != '0':
                if attempt < max_retry - 1:
                    time.sleep(1)
                    continue
                print(f"❌ [{code}] baostock 登录失败：error_code={lg.error_code}, error_msg={lg.error_msg}")
                return None, None

            fields = "date,code,open,high,low,close,volume,amount,turn,pctChg,peTTM,pbMRQ,isST"

            # ---------- K 线 ----------
            rs = bs.query_history_k_data_plus(
                code, fields,
                start_date=start, end_date=end,
                frequency="d", adjustflag="3"
            )

            if rs.error_code != '0':
                bs.logout()
                # 如果是"用户未登录"错误，重试
                if "用户未登录" in rs.error_msg or rs.error_code == '10001001':
                    if attempt < max_retry - 1:
                        time.sleep(1)
                        continue
                print(f"❌ [{code}] K 线 API 失败：error_code={rs.error_code}, error_msg={rs.error_msg}")
                return None, None

            k_data = []
            while rs.next():
                k_data.append(rs.get_row_data())

            if not k_data:
                bs.logout()
                if attempt < max_retry - 1:
                    time.sleep(1)
                    continue
                print(f"⚠️ [{code}] 未获取到 K 线数据（可能无交易记录）")
                return None, None

            df_k = pd.DataFrame(k_data, columns=fields.split(","))

            # ---------- 复权因子 ----------
            rs_fac = bs.query_adjust_factor(
                code, start_date="1990-01-01", end_date="2099-12-31"
            )
            fac_data = []
            if rs_fac.error_code != '0':
                # 如果是"用户未登录"错误，重试
                if "用户未登录" in rs_fac.error_msg or rs_fac.error_code == '10001001':
                    bs.logout()
                    if attempt < max_retry - 1:
                        time.sleep(1)
                        continue
                print(f"⚠️ [{code}] 复权因子获取失败：error_code={rs_fac.error_code}")
            else:
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
            try:
                bs.logout()
            except:
                pass
            if attempt < max_retry - 1:
                time.sleep(2)
            else:
                print(f"⚠️ [{code}] 异常：{e}")
                return None, None

    return None, None

# ==============================
# 主函数
# ==============================
def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--index", type=int, required=True)
    parser.add_argument("--codes", type=str, required=True)
    parser.add_argument("--year", type=int, default=0)
    parser.add_argument("--refill", action="store_true", help="启用补抓模式：检测并补抓缺失股票")
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

    if args.refill:
        # 🔄 补抓模式：先检测缺失，再补抓
        print("[*] 读取已有数据，检测缺失...")
        existing_file = f"temp_parts/kline_part_{args.index}.parquet"
        if os.path.exists(existing_file):
            df_existing = pd.read_parquet(existing_file)
            trade_dates = get_trade_calendar(start, end)
            missing_codes = find_missing_stocks(df_existing, trade_dates, tolerance=0.1)
            print(f"[+] 检测到 {len(missing_codes)} 只股票需要补抓")

            if missing_codes:
                # 只补抓缺失的股票
                codes_to_fetch = missing_codes
                print(f"[*] 开始补抓 {len(codes_to_fetch)} 只股票...")
            else:
                print("✅ 数据完整，无需补抓")
                codes_to_fetch = []
        else:
            print("[*] 无现有数据，全量下载")
            codes_to_fetch = codes
            df_existing = None
    else:
        codes_to_fetch = codes
        df_existing = None

    # 🚀 多进程核心
    k_failed_codes = []  # K 线下载失败的股票
    flow_failed_codes = []  # 资金流下载失败的股票

    if codes_to_fetch:
        workers = min(4, os.cpu_count())
        with ProcessPoolExecutor(max_workers=workers) as executor:
            futures = {
                executor.submit(process_one, (code, start, end)): code
                for code in codes_to_fetch
            }
            for future in tqdm(as_completed(futures), total=len(futures), desc="处理股票"):
                code = futures[future]
                k, f = future.result()
                if k is not None and not k.empty:
                    res_k.append(k)
                else:
                    k_failed_codes.append(code)
                if f is not None and not f.empty:
                    res_f.append(f)
                else:
                    flow_failed_codes.append(code)

    # 🔄 自动补抓 K 线失败股票（最多 3 次）
    for retry_round in range(3):
        if not k_failed_codes:
            break
        print(f"\n[*] 第 {retry_round + 1} 次补抓 {len(k_failed_codes)} 只 K 线失败股票...")
        retry_codes = k_failed_codes.copy()
        k_failed_codes = []

        for code in tqdm(retry_codes, desc=f"补抓 K{retry_round + 1}"):
            k, f = process_one((code, start, end))
            if k is not None and not k.empty:
                res_k.append(k)
                # 如果资金流也成功了，从 flow_failed_codes 移除
                if f is not None and not f.empty and code in flow_failed_codes:
                    flow_failed_codes.remove(code)
            else:
                k_failed_codes.append(code)

    # 🔄 自动补抓资金流失败股票（最多 3 次）
    for retry_round in range(3):
        if not flow_failed_codes:
            break
        print(f"\n[*] 第 {retry_round + 1} 次补抓 {len(flow_failed_codes)} 只资金流失败股票...")
        retry_codes = flow_failed_codes.copy()
        flow_failed_codes = []

        for code in tqdm(retry_codes, desc=f"补抓 F{retry_round + 1}"):
            _, f = process_one((code, start, end))
            if f is not None and not f.empty:
                res_f.append(f)
            else:
                flow_failed_codes.append(code)

    # 合并已有数据和新数据（包含补抓成功的股票）
    if args.refill and df_existing is not None and res_k:
        print("[*] 合并已有数据和新数据...")
        # 删除已有数据中的缺失股票（本次下载的所有股票，包括补抓的）
        all_fetched_codes = codes_to_fetch  # 已经包含补抓的股票
        codes_to_remove = [c for c in all_fetched_codes if c in df_existing["code"].values]
        if codes_to_remove:
            df_existing = df_existing[~df_existing["code"].isin(codes_to_remove)]
        df_k_all = pd.concat([df_existing] + res_k)
    elif res_k:
        df_k_all = pd.concat(res_k)
    else:
        df_k_all = df_existing if df_existing is not None else pd.DataFrame()

    print(f"[+] K 线完成：{len(df_k_all)} 行，{df_k_all['code'].nunique()} 只")
    print(f"[+] 资金流完成：{len(res_f)} 只")

    # 输出失败列表
    if k_failed_codes:
        print(f"\n⚠️ K 线未下载成功股票 ({len(k_failed_codes)}只):")
        print(f"   {', '.join(k_failed_codes)}")

    if flow_failed_codes:
        print(f"\n⚠️ 资金流未下载股票 ({len(flow_failed_codes)}只):")
        print(f"   {', '.join(flow_failed_codes)}")

    os.makedirs("temp_parts", exist_ok=True)

    if not df_k_all.empty:
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
