import sys
import os
import argparse
import json
import pandas as pd
import datetime
import requests
import time
import baostock as bs
from tqdm import tqdm

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from utils.cleaner import DataCleaner

HEADERS = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/120.0.0.0 Safari/537.36'}

def fetch_sina_flow(code, start, end):
    symbol = code.replace(".", "")
    url = f"https://vip.stock.finance.sina.com.cn/quotes_service/api/json_v2.php/MoneyFlow.ssl_qsfx_lscjfb?page=1&num=10000&sort=opendate&asc=0&daima={symbol}"
    for attempt in range(3):
        try:
            r = requests.get(url, headers=HEADERS, timeout=10)
            if r.status_code != 200:
                time.sleep(1)
                continue
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
            if not df.empty: 
                df['code'] = code
            return df
        except: 
            time.sleep(1)
    return pd.DataFrame()

def process_one(code, start, end):
    """
    处理单只股票：抛弃在内部 login/logout，享受外部长连接的极速。
    遇到任何接口异常直接 raise，交由外层重试，绝不妥协静默赋 1.0。
    """
    # 1. 抓取 K 线 (坚决使用不复权 adjustflag="3")
    fields = "date,code,open,high,low,close,volume,amount,turn,pctChg,peTTM,pbMRQ,isST" 
    rs = bs.query_history_k_data_plus(code, fields, start_date=start, end_date=end, frequency="d", adjustflag="3")
    
    if rs.error_code != '0':
        raise Exception(f"K线接口报错: {rs.error_msg}")
        
    k_data = []
    while rs.next():
        k_data.append(rs.get_row_data())
    
    if not k_data:
        # 如果根本没有K线数据（停牌或未上市），直接返回空
        try: df_f = fetch_sina_flow(code, start, end)
        except: df_f = pd.DataFrame()
        return pd.DataFrame(), df_f
        
    df_k = pd.DataFrame(k_data, columns=fields.split(","))
    
    # 2. 抓取复权因子 (获取从古至今的因子)
    rs_fac = bs.query_adjust_factor(code, start_date="1990-01-01", end_date="2099-12-31")
    
    # 💥 核心防线：接口报错绝不允许往下走！直接抛出异常！
    if rs_fac.error_code != '0':
        raise Exception(f"复权因子接口报错: {rs_fac.error_msg}")
        
    fac_data = []
    while rs_fac.next():
        fac_data.append(rs_fac.get_row_data())
    
    if fac_data:
        # 提取正确的 "back" (后复权因子) 列，为兼容后续逻辑重命名为 adjustFactor
        df_fac = pd.DataFrame(fac_data, columns=["code", "date", "fore", "back", "ratio"])
        df_fac = df_fac[['date', 'back']].rename(columns={'back': 'adjustFactor'})
        
        # 严格进行时间排序与对齐
        df_k['date'] = pd.to_datetime(df_k['date'])
        df_fac['date'] = pd.to_datetime(df_fac['date'])
        df_fac['adjustFactor'] = pd.to_numeric(df_fac['adjustFactor'], errors='coerce')
        
        # 剔除无效因子并排序
        df_fac = df_fac.dropna(subset=['adjustFactor']).sort_values('date')
        
        # 物理规则断言：后复权因子不可能 <= 0
        if not df_fac.empty and (df_fac['adjustFactor'] <= 0).any():
            raise Exception("抓取到负数或零的异常因子，触发熔断！")

        df_k = df_k.sort_values('date')
        
        # 使用最近邻向后填充寻找因子
        df_k = pd.merge_asof(
            df_k, 
            df_fac[['date', 'adjustFactor']], 
            on='date', 
            direction='backward'
        )
        
        df_k['date'] = df_k['date'].dt.strftime('%Y-%m-%d')
        df_k['adjustFactor'] = df_k['adjustFactor'].fillna(1.0)
    else:
        # 只有在接口明确返回 success 且真的没有历史分红时，才安全赋 1.0
        df_k['adjustFactor'] = 1.0
        
    # 3. 抓取资金流
    try:
        df_f = fetch_sina_flow(code, start, end)
    except:
        df_f = pd.DataFrame()
        
    return df_k, df_f

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

    print(f"Job {args.index}: Fetching {len(codes)} stocks ({start}~{end})...")
    
    # === 极速核心：全局只登录一次，保持长连接 ===
    lg = bs.login()
    if lg.error_code != '0':
        print(f"Login failed: {lg.error_msg}")
        return

    res_k, res_f = [], []
    cleaner = DataCleaner()
    
    # 顺序极速遍历
    for code in tqdm(codes):
        k, f = None, None
        
        # 原地 3 次防抖重试，防止单次网络超时导致缺失
        for attempt in range(3):
            try:
                k, f = process_one(code, start, end)
                break  # 成功拿到了（即便没数据也是合法的返回），跳出重试
            except Exception as e:
                if attempt == 2:
                    print(f"⚠️ {code} 处理失败并放弃: {e}")
                time.sleep(1)
                
        if k is not None and not k.empty: res_k.append(k)
        if f is not None and not f.empty: res_f.append(f)
        
        # 极速的同时给服务器一丝丝喘息的机会，避免被封 IP
        time.sleep(0.02)
            
    # === 全局只登出一次 ===
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
