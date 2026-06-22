import os
import sys
import datetime
import requests
import pandas as pd
import numpy as np
import json
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm

# 全局标准浏览器请求头
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Referer": "https://quote.eastmoney.com/"
}

def get_all_a_shares():
    """采用安全的分页拉取算法 (每页 1000 条)，100% 绕过大包风控拦截"""
    print("📋 [Finance Master] 正在同步最新全量 A 股代码清单 (分页模式)...")
    url = "https://push2.eastmoney.com/api/qt/clist/get"
    
    stocks = []
    page = 1
    page_size = 1000
    
    while True:
        params = {
            "pn": page, 
            "pz": page_size, 
            "po": 1, 
            "np": 1, 
            "ut": "bd1d9ddb04089700cf9c27f6f7426281",
            "fltt": 2, 
            "invt": 2, 
            "fid": "f3",
            "fs": "m:0+t:6,m:0+t:80,m:1+t:2,m:1+t:23,m:0+t:81+s:2048",
            "fields": "f12,f13" # f12=code, f13=market
        }
        
        try:
            response = requests.get(url, params=params, headers=HEADERS, timeout=15)
            if response.status_code != 200:
                print(f"⚠️ 第 {page} 页拉取失败，HTTP 状态码: {response.status_code}")
                break
                
            res = response.json()
            if not res or 'data' not in res or 'diff' not in res['data']:
                # 没有更多数据，分页结束
                break
                
            diff = res['data']['diff']
            if not diff or len(diff) == 0:
                break
                
            for item in diff:
                code = str(item['f12']).zfill(6)
                # 完美的防错前缀算法：100% 解决北交所和创业板前缀归属问题
                if code.startswith('6'): prefix = "SH"
                elif code.startswith(('4', '8', '9', '2')): prefix = "BJ"
                else: prefix = "SZ"
                stocks.append(f"{prefix}{code}")
                
            # Actions 进度友好型输出
            print(f"    [#] 已拉取第 {page} 页 | 累计标的: {len(stocks)}")
            
            # 如果返回的数据量小于请求页长，说明已到最后一页
            if len(diff) < page_size:
                break
                
            page += 1
            
        except Exception as e:
            print(f"❌ 同步第 {page} 页异常: {e}")
            break
            
    if not stocks:
        print("❌ 严重错误: 无法获取任何个股种子，任务终止。")
        sys.exit(1)
        
    print(f"[+] 种子库同步成功！本期共捕获 A 股有效标的: {len(stocks)} 只。")
    return stocks

def fetch_single_f10(em_code):
    """拉取单只股票全历史财报 (type=0: 包含全部季度报告期)"""
    url = f"https://emweb.securities.eastmoney.com/PC_HSF10/NewFinanceAnalysis/ZYZBAjaxNew?type=0&code={em_code}"
    try:
        res = requests.get(url, headers=HEADERS, timeout=10)
        data = res.json()
        if data and data.get("data"):
            return em_code, data["data"], None
        return em_code, [], "NoData"
    except Exception as e:
        return em_code, [], str(e)

def main():
    start_time = datetime.datetime.now()
    stocks = get_all_a_shares()
    
    all_records = []
    success_count = 0
    fail_count = 0

    print(f"🚀 开始并发拉取全市场 F10 财务指标... (线程池: 30)")
    with ThreadPoolExecutor(max_workers=30) as executor:
        futures = {executor.submit(fetch_single_f10, c): c for c in stocks}
        for future in tqdm(as_completed(futures), total=len(stocks), desc="拉取F10财务"):
            em_code, data, err = future.result()
            if data:
                # 给数据打上股票代码标记
                for row in data:
                    row['code_raw'] = em_code
                all_records.extend(data)
                success_count += 1
            else:
                fail_count += 1

    print(f"\n[+] 财务拉取结束。成功: {success_count}只 | 失败: {fail_count}只")
    if not all_records:
        print("❌ 严重错误: 未能抓取到任何有效的财务数据！")
        sys.exit(1)

    # 转化为 DataFrame 整理
    df = pd.DataFrame(all_records)
    
    # 格式化代码 SH600519 -> sh.600519
    df['code'] = df['code_raw'].apply(lambda x: f"{x[:2].lower()}.{x[2:]}")
    
    # 格式化日期与数值
    df['report_date'] = pd.to_datetime(df['REPORT_DATE'], errors='coerce')
    df['publish_date'] = pd.to_datetime(df['UPDATE_DATE'], errors='coerce')
    df['jlr'] = pd.to_numeric(df['PARENTNETPROFIT'], errors='coerce').fillna(0.0) # 归母净利润 (元)
    df['bps'] = pd.to_numeric(df['BPS'], errors='coerce').fillna(0.0)             # 每股净资产 (元)

    # 剔除无效行
    df = df.dropna(subset=['report_date', 'publish_date'])
    df = df.sort_values(['code', 'report_date'])

    print("🧠 正在内存中执行高精度 TTM 跨期滚动推导计算...")
    df.set_index(['code', 'report_date'], inplace=True)
    
    ttm_list = []
    for (code, rdate), row in df.iterrows():
        current_ytd = row['jlr']
        # 年报 (12-31) 的 TTM 本身就是年报累计值
        if rdate.month == 12:
            ttm_list.append(current_ytd)
            continue
            
        last_year_q4_date = rdate.replace(year=rdate.year - 1, month=12, day=31)
        last_year_same_q_date = rdate.replace(year=rdate.year - 1)
        
        try:
            ly_q4 = df.loc[(code, last_year_q4_date), 'jlr']
            ly_sq = df.loc[(code, last_year_same_q_date), 'jlr']
            if isinstance(ly_q4, pd.Series): ly_q4 = ly_q4.iloc[0]
            if isinstance(ly_sq, pd.Series): ly_sq = ly_sq.iloc[0]
            
            # TTM = 本期累计 + (去年全年 - 去年同期累计)
            ttm = current_ytd + (ly_q4 - ly_sq)
            ttm_list.append(ttm)
        except KeyError:
            # 缺失时填充为 0
            ttm_list.append(0.0)
            
    df['net_profit_ttm'] = ttm_list
    df = df.reset_index()

    # 转换单位：东财财务数据的净利润是“元”，我们将其转换为“万元”以完美匹配市值的单位
    df['net_profit_ttm_wan'] = df['net_profit_ttm'] / 10000.0

    # 整理出最纯净的估值匹配表
    final_df = df[['code', 'publish_date', 'net_profit_ttm_wan', 'bps']].copy()
    final_df = final_df.sort_values(['code', 'publish_date']).dropna(subset=['publish_date'])

    final_df.to_parquet("finance_master.parquet", index=False)
    print(f"✅ [Finance Master] 财务主文件成功生成！大小: {len(final_df)}行 | 总耗时: {datetime.datetime.now() - start_time}")

if __name__ == "__main__":
    main()
