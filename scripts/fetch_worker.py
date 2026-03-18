import sys
import os
# 1. 解决导入路径问题
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import argparse
import concurrent.futures
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


def fetch_adjust_factor_batch(codes):
    """
    批量并发获取所有股票的复权因子，返回 {code: list_of_factor_rows}
    """
    all_factors = {}

    def fetch_one(code):
        try:
            rs = bs.query_adjust_factor(code, start_date="1990-01-01", end_date="2099-12-31")
            if rs.error_code == '0':
                factors = []
                while rs.next():
                    factors.append(rs.get_row_data())
                return code, factors
            return code, []
        except Exception as e:
            print(f"⚠️ Error fetching factor for {code}: {e}")
            return code, []

    # 并发度设为 10，平衡速度和 baostock 负载
    with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
        future_to_code = {executor.submit(fetch_one, code): code for code in codes}
        for future in concurrent.futures.as_completed(future_to_code):
            code, factors = future.result()
            all_factors[code] = factors

    return all_factors


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
    # 1. Baostock KLine (坚决使用 adjustflag="3" 获取不复权真实价格)
    fields = "date,code,open,high,low,close,volume,amount,turn,pctChg,peTTM,pbMRQ,isST" 
    try:
        rs = bs.query_history_k_data_plus(code, fields, start_date=start, end_date=end, frequency="d", adjustflag="3")
        k_data = []
        if rs.error_code == '0':
            while rs.next():
                k_data.append(rs.get_row_data())
        
        df_k = pd.DataFrame(k_data, columns=fields.split(",")) if k_data else pd.DataFrame()
        
        # ================= 修复后的复权因子获取逻辑 =================
        if not df_k.empty:
            # 获取从古至今所有的复权因子事件
            rs_fac = bs.query_adjust_factor(code, start_date="1990-01-01", end_date="2099-12-31")
            fac_data = []
            if rs_fac.error_code == '0':
                while rs_fac.next():
                    fac_data.append(rs_fac.get_row_data())
            
            if fac_data:
                # Baostock返回的5列: 股票代码, 除权除息日, 前复权因子, 后复权因子, 单次除权比例
                # 我们将最后一列命名为 ratio，避免与我们要用的目标字段混淆
                df_fac = pd.DataFrame(fac_data, columns=["code", "date", "fore", "back", "ratio"])
                
                # 【核心修复】：提取 back（后复权因子），并将其重命名为 adjustFactor 供系统使用
                df_fac = df_fac[['date', 'back']].rename(columns={'back': 'adjustFactor'})
                
                # 时间轴对齐准备
                df_k['date'] = pd.to_datetime(df_k['date'])
                df_fac['date'] = pd.to_datetime(df_fac['date'])
                df_fac['adjustFactor'] = pd.to_numeric(df_fac['adjustFactor'], errors='coerce')
                
                df_k = df_k.sort_values('date')
                df_fac = df_fac.sort_values('date')
                
                # 寻找小于等于当前 K 线日期的最新一条后复权因子记录
                df_k = pd.merge_asof(
                    df_k, 
                    df_fac, 
                    on='date', 
                    direction='backward'
                )
                
                df_k['date'] = df_k['date'].dt.strftime('%Y-%m-%d')
                
                # 完美自洽：如果某天早于历史上第一次分红（即上市初期），后复权因子本身就是 1.0
                df_k['adjustFactor'] = df_k['adjustFactor'].fillna(1.0)
            else:
                # 历史上从来没分红过，后复权因子全是 1.0
                df_k['adjustFactor'] = 1.0
        # ==============================================================
            
    except Exception as e:
        print(f"⚠️ Error fetching {code}: {e}")
        df_k = pd.DataFrame()

    # 2. Sina Flow
    try:
        df_f = fetch_sina_flow(code, start, end)
    except:
        df_f = pd.DataFrame()
    
    return df_k, df_f


def process_one_with_factors(code, start, end, adj_factors):
    """
    使用预获取的复权因子处理单只股票，避免重复 API 调用
    adj_factors: {code: list_of_factor_rows}
    """
    fields = "date,code,open,high,low,close,volume,amount,turn,pctChg,peTTM,pbMRQ,isST"
    df_k = pd.DataFrame()

    try:
        rs = bs.query_history_k_data_plus(code, fields, start_date=start, end_date=end, frequency="d", adjustflag="3")
        k_data = []
        if rs.error_code == '0':
            while rs.next():
                k_data.append(rs.get_row_data())

        df_k = pd.DataFrame(k_data, columns=fields.split(",")) if k_data else pd.DataFrame()

        # 使用预获取的复权因子
        if not df_k.empty:
            fac_data = adj_factors.get(code, [])
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
        print(f"⚠️ Error fetching {code}: {e}")
        df_k = pd.DataFrame()

    return df_k


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--index", type=int, required=True)
    parser.add_argument("--codes", type=str, required=True)
    parser.add_argument("--year", type=int, default=0, help="Year to fetch (0=YTD, 9999=Full History)")
    args = parser.parse_args()
    
    codes = json.loads(args.codes)
    
    # 极简逻辑：直接请求到最新时间 (2099年)
    future_date = "2099-12-31"
    
    if args.year == 9999:
        start, end = "2005-01-01", future_date
    elif args.year > 0:
        start, end = f"{args.year}-01-01", f"{args.year}-12-31"
    else:
        curr_year = datetime.datetime.now().year
        start, end = f"{curr_year}-01-01", future_date

    print(f"Job {args.index}: Fetching {len(codes)} stocks ({start}~{end})...")

    lg = bs.login()
    if lg.error_code != '0':
        print(f"Login failed: {lg.error_msg}")
        return

    cleaner = DataCleaner()

    # ================= 优化核心：批量并发获取复权因子 =================
    print(f"[*] 正在批量获取 {len(codes)} 只股票的复权因子...")
    start_factor_time = time.time()
    adj_factors = fetch_adjust_factor_batch(codes)
    print(f"[+] 复权因子获取完成：{len(codes)} 只 | 耗时 {time.time() - start_factor_time:.2f}秒")
    # ================================================================

    res_k = []
    res_f = []

    # 使用预获取的复权因子，只调用 K 线 API
    for code in tqdm(codes, desc="下载 K 线"):
        k = process_one_with_factors(code, start, end, adj_factors)
        if not k.empty:
            res_k.append(k)
        # Sina Flow 仍然单独获取（可选优化）
        try:
            f = fetch_sina_flow(code, start, end)
            if not f.empty:
                res_f.append(f)
        except:
            pass
        time.sleep(0.02)

    # 注释掉 logout，让进程自然退出时自动清理连接
    # bs.logout()
    
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
