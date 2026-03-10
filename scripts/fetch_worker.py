import sys
import os
import datetime
import requests
import time
import baostock as bs
import pandas as pd
from concurrent.futures import ProcessPoolExecutor
from tqdm import tqdm

# 确保能正确导入 utils 下的模块
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if BASE_DIR not in sys.path:
    sys.path.append(BASE_DIR)

from utils.cleaner import DataCleaner

HEADERS = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/120.0.0.0 Safari/537.36'}

def fetch_sina_flow(code, start, end):
    """
    通过 Cloudflare Worker 代理拉取新浪资金流数据，
    避免单机高频请求暴露真实 IP 导致被新浪封禁。
    """
    symbol = code.replace(".", "")
    raw_url = os.getenv("CF_WORKER_URL", "").strip()
    
    # 如果未配置代理环境变量，直接放弃拉取以保护本机 IP
    if not raw_url:
        print(f"⚠️ 跳过 {code} 资金流: CF_WORKER_URL 未配置")
        return pd.DataFrame()
        
    worker_url = f"https://{raw_url}" if not raw_url.startswith("http") else raw_url
    
    # 组装透传给 JS 代理的参数，加上 target_func 路由
    params = {
        "target_func": "sina_flow",
        "page": 1,
        "num": 10000,
        "sort": "opendate",
        "asc": 0,
        "daima": symbol
    }
    
    for attempt in range(3):
        try:
            r = requests.get(worker_url, params=params, headers=HEADERS, timeout=15)
            if r.status_code == 200:
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
                
                # 按照请求的时间窗口过滤数据
                mask = (df['date'] >= start) & (df['date'] <= end)
                df = df.loc[mask].copy()
                if not df.empty: 
                    df['code'] = code
                return df
            else:
                time.sleep(1)
        except Exception as e:
            time.sleep(1)
            
    return pd.DataFrame()

def process_single_stock(args):
    """
    单进程执行函数。
    核心原则：必须在进程内部独立执行 bs.login() 和 logout()，
    否则底层 C 语言 socket 在多进程下会引发段错误(Segfault)或数据串位。
    """
    code, start, end = args
    bs.login()
    
    df_k = pd.DataFrame()
    df_f = pd.DataFrame()
    
    # 1. 获取 Baostock 日线数据与复权因子
    fields = "date,code,open,high,low,close,volume,amount,turn,pctChg,peTTM,pbMRQ,isST" 
    try:
        # 获取不复权真实价格 (adjustflag="3")
        rs = bs.query_history_k_data_plus(code, fields, start_date=start, end_date=end, frequency="d", adjustflag="3")
        k_data = []
        if rs.error_code == '0':
            while rs.next(): k_data.append(rs.get_row_data())
            
        df_k = pd.DataFrame(k_data, columns=fields.split(",")) if k_data else pd.DataFrame()
        
        # 匹配后复权因子
        if not df_k.empty:
            rs_fac = bs.query_adjust_factor(code, start_date="1990-01-01", end_date="2099-12-31")
            fac_data = []
            if rs_fac.error_code == '0':
                while rs_fac.next(): fac_data.append(rs_fac.get_row_data())
            
            if fac_data:
                df_fac = pd.DataFrame(fac_data, columns=["code", "date", "fore", "back", "ratio"])
                df_fac = df_fac[['date', 'back']].rename(columns={'back': 'adjustFactor'})
                
                df_k['date'] = pd.to_datetime(df_k['date'])
                df_fac['date'] = pd.to_datetime(df_fac['date'])
                df_fac['adjustFactor'] = pd.to_numeric(df_fac['adjustFactor'], errors='coerce')
                
                df_k = df_k.sort_values('date')
                df_fac = df_fac.sort_values('date')
                
                # asof 聚合复权因子
                df_k = pd.merge_asof(df_k, df_fac, on='date', direction='backward')
                df_k['date'] = df_k['date'].dt.strftime('%Y-%m-%d')
                df_k['adjustFactor'] = df_k['adjustFactor'].fillna(1.0)
            else:
                df_k['adjustFactor'] = 1.0
    except Exception as e:
        pass # 容错处理，由上层清理时过滤

    # 2. 获取新浪资金流
    try:
        df_f = fetch_sina_flow(code, start, end)
    except:
        pass

    bs.logout()
    time.sleep(0.1)  # 给目标服务器留点喘息时间，防止被判定为 DDoS
    return df_k, df_f

def run_stock_pipeline(year=0):
    future_date = "2099-12-31"
    
    if year == 9999:
        start, end = "2005-01-01", future_date
    elif year > 0:
        start, end = f"{year}-01-01", f"{year}-12-31"
    else:
        curr_year = datetime.datetime.now().year
        start, end = f"{curr_year}-01-01", future_date

    # ================= 主进程获取全量股票列表 =================
    bs.login()
    data = []
    
    # 【修复】：回溯前 10 天，寻找最近一个交易日的股票列表，防止周末或节假日返回空列表
    for i in range(10):
        d = (datetime.datetime.now() - datetime.timedelta(days=i)).strftime("%Y-%m-%d")
        rs = bs.query_all_stock(day=d)
        if rs.error_code == '0' and len(rs.data) > 0:
            while rs.next():
                data.append(rs.get_row_data())
            break
            
    bs.logout()

    # 过滤：仅保留沪深北 A 股，且股票名称不为空
    valid_codes = [x[0] for x in data if x[0].startswith(('sh.', 'sz.', 'bj.')) and x[2].strip()]
    print(f"Total {len(valid_codes)} valid A-shares to fetch (from {start} to {end}).")

    if not valid_codes:
        print("⚠️ 未获取到股票列表，请检查网络或 Baostock 服务状态！")
        return

    tasks = [(code, start, end) for code in valid_codes]
    res_k, res_f = [], []
    cleaner = DataCleaner()

    os.makedirs("temp_parts", exist_ok=True)
    
    # ================= 启动进程池高并发抓取 =================
    # 限制最大 5 个并发避免触发反爬机制断流
    with ProcessPoolExecutor(max_workers=5) as executor:
        for k, f in tqdm(executor.map(process_single_stock, tasks), total=len(tasks), desc="Fetching Stocks"):
            if not k.empty: res_k.append(k)
            if not f.empty: res_f.append(f)

    # ================= 清洗并保存至本地临时目录 =================
    if res_k: 
        print(f"Cleaning {len(res_k)} K-Line parts...")
        df_k_all = pd.concat(res_k)
        df_k_all = cleaner.clean_stock_kline(df_k_all)
        df_k_all.to_parquet("temp_parts/kline_part_all.parquet", index=False)
        print(f"✅ Saved K-Line total: {len(df_k_all)} rows.")
    
    if res_f: 
        print(f"Cleaning {len(res_f)} Money Flow parts...")
        df_f_all = pd.concat(res_f)
        df_f_all = cleaner.clean_money_flow(df_f_all)
        df_f_all.to_parquet("temp_parts/flow_part_all.parquet", index=False)
        print(f"✅ Saved Flow total: {len(df_f_all)} rows.")

if __name__ == "__main__":
    run_stock_pipeline()
