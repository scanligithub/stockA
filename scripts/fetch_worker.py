import sys
import os
import socket

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import argparse
import json
import pandas as pd
import datetime
import requests
import time
import baostock as bs
import signal  # 💥 引入系统级硬中断控制
from tqdm import tqdm
from concurrent.futures import ProcessPoolExecutor, as_completed
from utils.cleaner import DataCleaner

HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0'
}

# ==============================
# 跨平台超时熔断器 (防 C 扩展死锁)
# ==============================
def set_hard_timeout(seconds):
    """向操作系统注册定时炸弹，时间一到无条件砸断当前执行流"""
    if hasattr(signal, 'SIGALRM'):
        def timeout_handler(signum, frame):
            raise TimeoutError(f"底层 Socket 彻底卡死超过 {seconds} 秒，被系统硬中断强杀！")
        signal.signal(signal.SIGALRM, timeout_handler)
        signal.alarm(seconds)

def cancel_hard_timeout():
    """解除定时炸弹"""
    if hasattr(signal, 'SIGALRM'):
        signal.alarm(0)

# ==============================
# 新浪资金流
# ==============================
def fetch_sina_flow(code, start, end):
    symbol = code.replace(".", "")
    url = f"https://vip.stock.finance.sina.com.cn/quotes_service/api/json_v2.php/MoneyFlow.ssl_qsfx_lscjfb?page=1&num=10000&sort=opendate&asc=0&daima={symbol}"
    
    last_err = "未知网络错误"
    for attempt in range(4): 
        try:
            r = requests.get(url, headers=HEADERS, timeout=10)
            if r.status_code != 200:
                last_err = f"HTTP 状态码异常: {r.status_code}"
                time.sleep(1 + attempt)
                continue
                
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
            
        except Exception as e:
            last_err = str(e)
            time.sleep(1 + attempt)
            
    raise Exception(f"新浪资金流接口重试耗尽: {last_err}")

# ==============================
# 核心：单股票处理（系统级熔断版）
# ==============================
def process_one(args):
    """
    接收参数：(code, start, end, need_k, need_flow)
    返回值: (code, df_k, df_f, factor_error_triggered, err_k, err_f)
    """
    socket.setdefaulttimeout(15.0) # 将普通超时缩短，逼迫底层尽快抛错

    code, start, end, need_k, need_flow = args
    df_k = "SKIP" if not need_k else None
    df_f = "SKIP" if not need_flow else None
    
    factor_error_triggered = False  
    err_msg_k = ""  
    err_msg_f = ""  

    # ---------- 1. 获取 K 线及复权因子 ----------
    if need_k:
        max_retry = 4  # 避免过多的重试导致尾部耗时过长
        for attempt in range(max_retry):
            try:
                # 💥 启动硬中断定时炸弹，防止 Baostock 死锁挂起进程
                set_hard_timeout(40) 
                
                lg = bs.login()
                if lg.error_code != '0':
                    raise Exception(f"Baostock 登录失败: {lg.error_msg}")
                    
                fields = "date,code,open,high,low,close,volume,amount,turn,pctChg,peTTM,pbMRQ,isST"
                rs = bs.query_history_k_data_plus(code, fields, start_date=start, end_date=end, frequency="d", adjustflag="3")
                
                if rs.error_code != '0':
                    raise Exception(f"Baostock K线接口错误 [{rs.error_code}]: {rs.error_msg}")
                    
                k_data = []
                while rs.next(): k_data.append(rs.get_row_data())
                
                if not k_data:
                    bs.logout()
                    cancel_hard_timeout()
                    df_k = pd.DataFrame() 
                    break
                    
                temp_df_k = pd.DataFrame(k_data, columns=fields.split(","))
                
                # --- 获取复权因子 ---
                rs_fac = bs.query_adjust_factor(code, start_date="1990-01-01", end_date="2099-12-31")
                
                if rs_fac.error_code != '0':
                    raise ValueError(f"复权因子接口获取失败 [{rs_fac.error_code}]: {rs_fac.error_msg}")
                
                fac_data = []
                while rs_fac.next(): 
                    fac_data.append(rs_fac.get_row_data())
                        
                if fac_data:
                    df_fac = pd.DataFrame(fac_data, columns=["code", "date", "fore", "back", "ratio"])
                    df_fac = df_fac[['date', 'back']].rename(columns={'back': 'adjustFactor'})
                    df_fac['date'] = pd.to_datetime(df_fac['date'])
                    df_fac['adjustFactor'] = pd.to_numeric(df_fac['adjustFactor'], errors='coerce')
                    df_fac = df_fac.dropna(subset=['date', 'adjustFactor']).sort_values('date')

                    # 应对死因1：远古脏数据断层平滑
                    df_fac = df_fac[df_fac['adjustFactor'] > 0]
                    if not df_fac.empty:
                        df_fac['adjustFactor'] = df_fac['adjustFactor'].cummax()

                        temp_df_k['date'] = pd.to_datetime(temp_df_k['date'])
                        temp_df_k = temp_df_k.sort_values('date')
                        
                        temp_df_k = pd.merge_asof(temp_df_k, df_fac, on='date', direction='backward')
                        temp_df_k['date'] = temp_df_k['date'].dt.strftime('%Y-%m-%d')
                        temp_df_k['adjustFactor'] = temp_df_k['adjustFactor'].fillna(1.0)
                        
                        if temp_df_k['adjustFactor'].isna().any():
                            raise ValueError("拼接完成后仍存在无法解析的 NaN 因子！")
                    else:
                        temp_df_k['adjustFactor'] = 1.0
                else:
                    temp_df_k['adjustFactor'] = 1.0
                    
                df_k = temp_df_k
                bs.logout()
                cancel_hard_timeout() # 成功后解除炸弹
                break 
                
            except Exception as e:
                cancel_hard_timeout() # 报错也要解除炸弹
                err_msg = str(e)
                if "复权因子" in err_msg or "NaN 因子" in err_msg:
                    factor_error_triggered = True
                    
                try: bs.logout()
                except: pass
                
                if attempt == max_retry - 1:
                    df_k = None 
                    err_msg_k = err_msg  
                else:
                    # 应对死因2：更温柔的退避，1秒, 2秒, 3秒（避免尾部耗时过长）
                    time.sleep(1 + attempt) 

    # ---------- 2. 获取资金流 ----------
    if need_flow:
        try:
            df_f = fetch_sina_flow(code, start, end)
        except Exception as e:
            df_f = None
            err_msg_f = str(e)  

    return code, df_k, df_f, factor_error_triggered, err_msg_k, err_msg_f

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

    k_success_dict = {}
    f_success_dict = {}
    
    k_failed_set = set(codes)
    f_failed_set = set(codes)
    f_ignored_set = set()
    
    factor_retry_set = set() 
    k_error_dict = {}
    f_error_dict = {}

    MAX_ROUNDS = 4  
    
    for round_idx in range(MAX_ROUNDS):
        current_tasks = list(k_failed_set | f_failed_set)
        if not current_tasks:
            break
            
        desc = "初始全量抓取" if round_idx == 0 else f"第 {round_idx} 轮智能补抓"
        if round_idx > 0:
            print(f"\n[*] {desc} -> 缺 K线: {len(k_failed_set)}只 | 缺 资金流: {len(f_failed_set)}只")

        workers = min(4, os.cpu_count())
        
        executor = ProcessPoolExecutor(max_workers=workers)
        try:
            futures = {
                executor.submit(process_one, (c, start, end, c in k_failed_set, c in f_failed_set)): c
                for c in current_tasks
            }
            
            for future in tqdm(as_completed(futures), total=len(futures), desc=desc):
                try:
                    code, k, f, factor_err, err_k, err_f = future.result()
                    
                    if factor_err:
                        factor_retry_set.add(code)
                    
                    if k is not None and isinstance(k, pd.DataFrame):
                        if not k.empty: 
                            k_success_dict[code] = k
                        k_failed_set.discard(code)
                        k_error_dict.pop(code, None) 
                    elif k is None:
                        k_error_dict[code] = err_k
                        
                    if f is not None and isinstance(f, pd.DataFrame):
                        if not f.empty: 
                            f_success_dict[code] = f
                        else:
                            f_ignored_set.add(code)
                        f_failed_set.discard(code)
                        f_error_dict.pop(code, None)
                    elif f is None:
                        f_error_dict[code] = err_f
                        
                except Exception as e:
                    failed_code = futures[future]
                    if failed_code in k_failed_set: k_error_dict[failed_code] = f"进程崩溃异常: {str(e)}"
                    if failed_code in f_failed_set: f_error_dict[failed_code] = f"进程崩溃异常: {str(e)}"
                    
        finally:
            if sys.version_info >= (3, 9):
                executor.shutdown(wait=False, cancel_futures=True)
            else:
                executor.shutdown(wait=False)

    res_k = list(k_success_dict.values())
    res_f = list(f_success_dict.values())

    print(f"\n[+] Job {args.index} 结束: K线 成功 {len(res_k)}/失败 {len(k_failed_set)} | 资金流 成功 {len(res_f)}/放弃 {len(f_ignored_set)}/失败 {len(f_failed_set)}")

    # 阵亡名单与验尸报告打印
    if k_failed_set or f_failed_set:
        print("\n" + "="*60)
        print("🚨 FAILED STOCKS REPORT (阵亡名单与死因剖析)")
        print("="*60)
        
        if k_failed_set:
            print(f"\n❌ K线下载失败 ({len(k_failed_set)} 只):")
            for c in sorted(list(k_failed_set)):
                print(f"   [{c}] -> {k_error_dict.get(c, '未知内部异常')}")
                
        if f_failed_set:
            print(f"\n❌ 资金流下载失败 ({len(f_failed_set)} 只):")
            for c in sorted(list(f_failed_set)):
                print(f"   [{c}] -> {f_error_dict.get(c, '未知内部异常')}")
        print("="*60 + "\n")

    os.makedirs("temp_parts", exist_ok=True)
    cleaner = DataCleaner()

    if res_k:
        df_k_all = pd.concat(res_k)
        if not df_k_all.empty:
            df_k_all = cleaner.clean_stock_kline(df_k_all)
            df_k_all.to_parquet(f"temp_parts/kline_part_{args.index}.parquet", index=False)

    if res_f:
        df_f_all = pd.concat(res_f)
        if not df_f_all.empty:
            df_f_all = cleaner.clean_money_flow(df_f_all)
            df_f_all.to_parquet(f"temp_parts/flow_part_{args.index}.parquet", index=False)
            
    with open(f"temp_parts/factor_stats_{args.index}.json", "w") as f:
        json.dump(list(factor_retry_set), f)

if __name__ == "__main__":
    main()
