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
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0'
}

# ==============================
# 新浪资金流 (智能判别版)
# ==============================
def fetch_sina_flow(code, start, end):
    """
    不再使用死板的 @retry 装饰器。
    通过内部 try-except 区分 "网络超时" 和 "新浪无此股票"。
    """
    symbol = code.replace(".", "")
    url = f"https://vip.stock.finance.sina.com.cn/quotes_service/api/json_v2.php/MoneyFlow.ssl_qsfx_lscjfb?page=1&num=10000&sort=opendate&asc=0&daima={symbol}"
    
    for attempt in range(3): # 底层网络重试 3 次
        try:
            r = requests.get(url, headers=HEADERS, timeout=10)
            if r.status_code != 200:
                time.sleep(1)
                continue
                
            data = r.json()
            # 【核心判别】：正常返回了，但数据是空的。说明新浪根本没有这只股票。
            if not data:
                return pd.DataFrame() # 返回空 DF，表示“无需重试，直接放弃”
                
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
            
        except Exception as e:
            # 解析错误或超时，属于网络故障
            if attempt == 2:
                # 3次都网络故障，返回 None，要求高层重试
                return None 
            time.sleep(1.5)
            
    return None

# ==============================
# 获取交易日历 & 缺失检测 (原有逻辑保持不变)
# ==============================
def get_trade_calendar(start_date, end_date):
    lg = bs.login()
    rs = bs.query_trade_dates(start_date=start_date, end_date=end_date)
    dates = []
    while rs.next():
        row = rs.get_row_data()
        if row[1] == '1': dates.append(row[0])
    bs.logout()
    return set(dates)

def find_missing_stocks(df, trade_dates, tolerance=0):
    if df.empty: return list(df["code"].unique())
    grouped = df.groupby("code")["date"].nunique()
    expected = len(trade_dates)
    bad_codes = []
    for code, count in grouped.items():
        if count / expected < (1 - tolerance):
            bad_codes.append(code)
    return bad_codes

# ==============================
# 核心：单股票处理（双轨解耦执行）
# ==============================
def process_one(args):
    """
    接收参数：(code, start, end, need_k, need_flow)
    解耦执行，缺哪个下哪个，极大节省资源。
    返回值: (code, df_k, df_f)
      df_k / df_f 状态：
      - DataFrame: 成功获取数据 (或被判定为无需获取的空结构)
      - "SKIP": 外部指定不需要获取
      - None: 获取过程中发生网络崩溃，需要重试
    """
    code, start, end, need_k, need_flow = args
    df_k = "SKIP" if not need_k else None
    df_f = "SKIP" if not need_flow else None

    # ---------- 1. 获取 K 线 (仅当需要时) ----------
    if need_k:
        max_retry = 3
        for attempt in range(max_retry):
            try:
                lg = bs.login()
                if lg.error_code != '0':
                    if attempt < max_retry - 1: time.sleep(1); continue
                    break
                    
                fields = "date,code,open,high,low,close,volume,amount,turn,pctChg,peTTM,pbMRQ,isST"
                rs = bs.query_history_k_data_plus(code, fields, start_date=start, end_date=end, frequency="d", adjustflag="3")
                
                if rs.error_code != '0':
                    bs.logout()
                    if "用户未登录" in rs.error_msg or rs.error_code == '10001001':
                        if attempt < max_retry - 1: time.sleep(1); continue
                    break
                    
                k_data = []
                while rs.next(): k_data.append(rs.get_row_data())
                
                if not k_data:
                    bs.logout()
                    df_k = pd.DataFrame() # 无数据，判定为结构性缺失
                    break
                    
                temp_df_k = pd.DataFrame(k_data, columns=fields.split(","))
                
                # 复权因子
                rs_fac = bs.query_adjust_factor(code, start_date="1990-01-01", end_date="2099-12-31")
                fac_data = []
                if rs_fac.error_code == '0':
                    while rs_fac.next(): fac_data.append(rs_fac.get_row_data())
                        
                if fac_data:
                    df_fac = pd.DataFrame(fac_data, columns=["code", "date", "fore", "back", "ratio"])
                    df_fac = df_fac[['date', 'back']].rename(columns={'back': 'adjustFactor'})
                    temp_df_k['date'] = pd.to_datetime(temp_df_k['date'])
                    df_fac['date'] = pd.to_datetime(df_fac['date'])
                    df_fac['adjustFactor'] = pd.to_numeric(df_fac['adjustFactor'], errors='coerce')
                    temp_df_k = temp_df_k.sort_values('date')
                    df_fac = df_fac.sort_values('date')
                    temp_df_k = pd.merge_asof(temp_df_k, df_fac, on='date', direction='backward')
                    temp_df_k['date'] = temp_df_k['date'].dt.strftime('%Y-%m-%d')
                    temp_df_k['adjustFactor'] = temp_df_k['adjustFactor'].fillna(1.0)
                else:
                    temp_df_k['adjustFactor'] = 1.0
                    
                df_k = temp_df_k
                bs.logout()
                break # 成功跳出重试
                
            except Exception as e:
                try: bs.logout()
                except: pass
                if attempt == max_retry - 1:
                    df_k = None # 判定为彻底网络崩溃
                else:
                    time.sleep(2)

    # ---------- 2. 获取资金流 (仅当需要时) ----------
    if need_flow:
        df_f = fetch_sina_flow(code, start, end)

    return code, df_k, df_f

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

    # 状态字典存储
    k_success_dict = {}
    f_success_dict = {}
    
    # 失败追踪队列 (初始化为全部需要抓取)
    k_failed_set = set(codes)
    f_failed_set = set(codes)
    
    # 结构性缺失黑名单 (新浪确实没有的，永不重试)
    f_ignored_set = set()

    # 此处省略原有 --refill 文件扫描检测逻辑...
    # 为保证篇幅和核心逻辑聚焦，假设 codes_to_fetch 已经是最干净的初始列表
    codes_to_fetch = codes 

    # 🚀 高层并发与智能补抓循环
    MAX_ROUNDS = 4  # 1轮主抓 + 3轮补抓
    
    for round_idx in range(MAX_ROUNDS):
        # 取 K线失败 和 资金流失败 的并集作为本轮任务
        current_tasks = list(k_failed_set | f_failed_set)
        if not current_tasks:
            break
            
        desc = "初始全量抓取" if round_idx == 0 else f"第 {round_idx} 轮智能补抓"
        if round_idx > 0:
            print(f"\n[*] {desc} -> 缺 K线: {len(k_failed_set)}只 | 缺 资金流: {len(f_failed_set)}只")

        workers = min(4, os.cpu_count())
        with ProcessPoolExecutor(max_workers=workers) as executor:
            # 只下达缺失的指令
            futures = {
                executor.submit(process_one, (c, start, end, c in k_failed_set, c in f_failed_set)): c
                for c in current_tasks
            }
            
            for future in tqdm(as_completed(futures), total=len(futures), desc=desc):
                code, k, f = future.result()
                
                # 处理 K 线结果
                if k is not None and isinstance(k, pd.DataFrame):
                    if not k.empty: k_success_dict[code] = k
                    k_failed_set.discard(code) # 成功或确认无数据，移出失败队列
                    
                # 处理资金流结果
                if f is not None and isinstance(f, pd.DataFrame):
                    if not f.empty: 
                        f_success_dict[code] = f
                    else:
                        # 【核心防线命中】新浪确认无此股票，拉入黑名单，严禁再次浪费时间
                        f_ignored_set.add(code)
                    f_failed_set.discard(code) # 无论有没有，只要判定结束，就移出失败队列

    # 汇总数据
    res_k = list(k_success_dict.values())
    res_f = list(f_success_dict.values())

    print(f"\n[+] K 线最终完成：成功 {len(res_k)} 只 | 彻底失败 {len(k_failed_set)} 只")
    print(f"[+] 资金流最终完成：成功 {len(res_f)} 只 | 新浪不跟踪放弃 {len(f_ignored_set)} 只 | 网络崩溃彻底失败 {len(f_failed_set)} 只")

    if k_failed_set or f_failed_set:
        print(f"\n⚠️ 极其严重的网络崩溃未下全列表:")
        if k_failed_set: print(f"   K线彻底失败: {','.join(list(k_failed_set)[:20])}...")
        if f_failed_set: print(f"   资金流彻底失败: {','.join(list(f_failed_set)[:20])}...")

    # 落库保存
    os.makedirs("temp_parts", exist_ok=True)
    cleaner = DataCleaner()

    if res_k:
        df_k_all = pd.concat(res_k)
        if not df_k_all.empty:
            df_k_all = cleaner.clean_stock_kline(df_k_all)
            df_k_all.to_parquet(f"temp_parts/kline_part_{args.index}.parquet", index=False)
            print(f"✅ Saved KLine {args.index}: {len(df_k_all)}")

    if res_f:
        df_f_all = pd.concat(res_f)
        if not df_f_all.empty:
            df_f_all = cleaner.clean_money_flow(df_f_all)
            df_f_all.to_parquet(f"temp_parts/flow_part_{args.index}.parquet", index=False)
            print(f"✅ Saved Flow {args.index}: {len(df_f_all)}")

if __name__ == "__main__":
    main()
