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

def load_stocks_from_local_json():
    """
    🌟 核心安全提升：0 网络请求。直接读取由 Go 引擎通过通达信极速拉取生成的本地 stock_list_master.json
    """
    print("📋 [Finance Master] 正在从本地 stock_list_master.json 读取 A 股种子清单...")
    master_path = "stock_list_master.json"
    
    if not os.path.exists(master_path):
        print(f"❌ 严重错误: 本地未检测到 {master_path}。请确保工作流先运行了 Go 引擎的 -mode=list。")
        sys.exit(1)
        
    try:
        with open(master_path, "r", encoding="utf-8") as f:
            master_list = json.load(f)
            
        stocks = []
        for item in master_list:
            code = item.get("code", "") # 格式如 "sh.600519"
            if not code or "." not in code:
                continue
                
            prefix, num_code = code.split(".")
            # 完美的防错前缀算法：100% 解决东财 F10 接口的北交所、科创板归属前缀
            if num_code.startswith('6'): 
                em_prefix = "SH"
            elif num_code.startswith(('4', '8', '9', '2')): 
                em_prefix = "BJ"
            else: 
                em_prefix = "SZ"
                
            stocks.append(f"{em_prefix}{num_code}")
            
        print(f"[✓] 成功载入本地 A 股种子 {len(stocks)} 只。")
        return stocks
    except Exception as e:
        print(f"❌ 严重错误: 解析本地股票列表 JSON 失败: {e}")
        sys.exit(1)

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
    
    # 0 网络请求，读取本地 Go 引擎下载好的股票列表
    stocks = load_stocks_from_local_json()
    
    all_records = []
    success_count = 0
    fail_count = 0

    print(f"🚀 开始并发拉取全市场 F10 财务指标... (线程池: 30)")
    with ThreadPoolExecutor(max_workers=30) as executor:
        futures = {executor.submit(fetch_single_f10, c): c for c in stocks}
        for future in tqdm(as_completed(futures), total=len(stocks), desc="拉取F10财务"):
            em_code, data, err = future.result()
            if data:
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

    df = pd.DataFrame(all_records)
    df['code'] = df['code_raw'].apply(lambda x: f"{x[:2].lower()}.{x[2:]}")
    
    df['report_date'] = pd.to_datetime(df['REPORT_DATE'], errors='coerce')
    df['publish_date'] = pd.to_datetime(df['UPDATE_DATE'], errors='coerce')
    df['jlr'] = pd.to_numeric(df['PARENTNETPROFIT'], errors='coerce').fillna(0.0) # 归母净利润 (元)
    df['bps'] = pd.to_numeric(df['BPS'], errors='coerce').fillna(0.0)             # 每股净资产 (元)

    df = df.dropna(subset=['report_date', 'publish_date'])
    df = df.sort_values(['code', 'report_date'])

    print("🧠 正在内存中执行高精度 TTM 跨期滚动推导计算...")
    df.set_index(['code', 'report_date'], inplace=True)
    
    ttm_list = []
    for (code, rdate), row in df.iterrows():
        current_ytd = row['jlr']
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
            
            ttm = current_ytd + (ly_q4 - ly_sq)
            ttm_list.append(ttm)
        except KeyError:
            ttm_list.append(0.0)
            
    df['net_profit_ttm'] = ttm_list
    df = df.reset_index()

    df['net_profit_ttm_wan'] = df['net_profit_ttm'] / 10000.0

    final_df = df[['code', 'publish_date', 'net_profit_ttm_wan', 'bps']].copy()
    final_df = final_df.sort_values(['code', 'publish_date']).dropna(subset=['publish_date'])

    final_df.to_parquet("finance_master.parquet", index=False)
    print(f"✅ [Finance Master] 财务主文件成功生成！大小: {len(final_df)}行 | 总耗时: {datetime.datetime.now() - start_time}")

if __name__ == "__main__":
    main()
