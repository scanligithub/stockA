import os
import sys
import pandas as pd
import numpy as np
from pytdx.hq import TdxHq_API  # 🎯 修正：将 TdxHqAPI 替换为正确的 TdxHq_API
import datetime

# 55 只量化指数
INDEX_LIST = {
    "sh.000001": "上证指数", "sz.399001": "深证成指", "sz.399006": "创业板指", "sh.000688": "科创50", "bj.899050": "北证50",
    "sh.000016": "上证50", "sh.000300": "沪深300", "sh.000905": "中证500", "sh.000852": "中证1000", "sh.000851": "中证2000",
    "sz.399303": "国证2000", "sh.000985": "中证全指", "sz.399330": "深证100", "sh.000090": "上证180",
    "sh.000922": "中证红利", "sz.399324": "深证红利", "sh.000015": "红利指数", "sh.000918": "沪深300成长", "sh.000919": "沪深300价值",
    "sh.000807": "中证超大盘", "sh.000827": "中证中盘", "sh.000925": "中证基本面50", "sh.000978": "中证大盘价值", "sz.399317": "国证1000",
    "sz.399807": "中证人工智能", "sz.399977": "中证机器人", "sh.932252": "中证低空经济主题", "sz.399812": "国证芯片", "sh.000973": "中证半导体",
    "sh.931160": "中证数据要素", "sz.399354": "国证人工智能", "sz.399673": "创业板50", "sz.399979": "中证空天安全", "sz.399285": "国证新能源",
    "sh.931151": "中证光伏产业", "sz.399008": "中小100", "sh.931494": "中证消费电子主题", "sh.931409": "中证电网设备", "sh.931152": "中证创新药产业",
    "sz.399993": "中证信息安全", "sz.399975": "证券公司", "sz.399986": "中证银行", "sz.399932": "中证消费", "sz.399933": "中证医药",
    "sz.399967": "中证军工", "sz.399989": "中证医疗", "sz.399971": "中证传媒", "sz.399997": "中证白酒", "sh.000934": "中证能源",
    "sh.000935": "中证原材料", "sz.399990": "煤炭等权", "sz.399998": "中证有色", "sz.399974": "国证国企", "sh.000157": "中证央企",
    "sh.930606": "中证黄金产业"
}

TDX_SERVERS = [
    {"ip": "119.147.212.81", "port": 7709},
    {"ip": "120.24.0.183", "port": 7709},
    {"ip": "119.29.25.16", "port": 7709}
]

def fetch_index_data(api, code_str, is_incremental=False):
    parts = code_str.split('.')
    prefix, pure_code = parts[0], parts[1]
    market = 1 if prefix == "sh" else 0 if prefix == "sz" else 2
    
    all_bars = []
    max_bars = 500 if is_incremental else 5600
    step = 800
    
    for start_idx in range(0, max_bars, step):
        count_to_fetch = min(step, max_bars - start_idx)
        bars = api.get_security_bars(9, market, pure_code, start_idx, count_to_fetch)
        if not bars:
            break
        all_bars.extend(bars)
        if len(bars) < count_to_fetch:
            break
    return all_bars

def main():
    is_incremental = "--incremental" in sys.argv or "-i" in sys.argv
    print(f"📥 Starting PyTDX Index Fetcher (Mode: {'Incremental' if is_incremental else 'Full History'})...")
    
    api = TdxHq_API()  # 🎯 修正：创建 TdxHq_API 实例
    connected = False
    for s in TDX_SERVERS:
        try:
            if api.connect(s['ip'], s['port']):
                connected = True
                print(f"✅ Connected to TDX Server: {s['ip']}")
                break
        except:
            continue
            
    if not connected:
        print("❌ Error: Failed to connect to any TDX server.")
        sys.exit(1)
        
    all_rows = []
    try:
        for code_str in INDEX_LIST.keys():
            bars = fetch_index_data(api, code_str, is_incremental=is_incremental)
            if not bars:
                continue
            
            for b in bars:
                dt_str = b['datetime'][:10]
                all_rows.append({
                    "date": dt_str,
                    "code": code_str,
                    "open": float(b['open']),
                    "high": float(b['high']),
                    "low": float(b['low']),
                    "close": float(b['close']),
                    "volume": float(b['vol']),
                    "amount": float(b['amount']),
                })
    finally:
        api.disconnect()
        
    if not all_rows:
        print("❌ Error: No index data fetched!")
        sys.exit(1)
        
    df = pd.DataFrame(all_rows)
    df['date_dt'] = pd.to_datetime(df['date'], errors='coerce')
    df = df.dropna(subset=['date_dt'])
    df['date'] = df['date_dt'].dt.strftime('%Y-%m-%d')
    df = df.drop(columns=['date_dt'])
    
    df = df.drop_duplicates(subset=['date', 'code'], keep='last')
    df = df.sort_values(['code', 'date'])
    
    # 物理计算每日涨跌幅百分比
    df['pctChg'] = df.groupby('code')['close'].pct_change() * 100
    df['pctChg'] = df['pctChg'].fillna(0.0)
    
    # 类型强制约束与降维
    df['open'] = df['open'].astype('float32')
    df['high'] = df['high'].astype('float32')
    df['low'] = df['low'].astype('float32')
    df['close'] = df['close'].astype('float32')
    df['volume'] = df['volume'].astype('float64')
    df['amount'] = df['amount'].astype('float64')
    df['pctChg'] = df['pctChg'].astype('float32')
    
    os.makedirs("temp_parts", exist_ok=True)
    out_path = "temp_parts/index_kline_all.parquet"
    df.to_parquet(out_path, index=False)
    print(f"🎉 Successfully saved {len(df):,} index rows to {out_path}!")

if __name__ == "__main__":
    main()
