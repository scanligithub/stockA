import os
import sys
import pandas as pd
import numpy as np
from pytdx.hq import TdxHq_API
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
    {"ip": "124.71.187.122", "port": 7709, "desc": "华为云高带宽节点"},
    {"ip": "119.29.25.16", "port": 7709, "desc": "腾讯云高带宽节点"},
    {"ip": "124.223.116.142", "port": 7709, "desc": "腾讯云备用节点"},
    {"ip": "119.147.171.115", "port": 7709, "desc": "招商证券深圳主站"},
    {"ip": "119.147.164.60", "port": 7709, "desc": "国信证券主站"},
    {"ip": "112.95.140.93", "port": 7709, "desc": "华泰证券主站"},
    {"ip": "115.238.90.165", "port": 7709, "desc": "浙江电信高带宽节点"},
    {"ip": "218.75.126.9", "port": 7709, "desc": "浙江联通高带宽节点"},
    {"ip": "120.24.0.183", "port": 7709, "desc": "深圳双线节点"},
    {"ip": "119.147.212.81", "port": 7709, "desc": "广东电信节点"}
]

def get_code_aliases(pure_code):
    """底层物理别名映射函数：自动生成通达信服务器内部使用的实际存储代码"""
    aliases = [pure_code]
    if pure_code.startswith("93"):
        aliases.append("H3" + pure_code[2:])
    elif pure_code.startswith("0009"):
        aliases.append("H309" + pure_code[4:])
        aliases.append("3999" + pure_code[4:])
    elif pure_code.startswith("3999"):
        aliases.append("H309" + pure_code[4:])
    return list(dict.fromkeys(aliases))

def fetch_index_data(api, code_str, is_incremental=False):
    parts = code_str.split('.')
    prefix, pure_code = parts[0], parts[1]
    
    primary_market = 1 if prefix == "sh" else 0 if prefix == "sz" else 2
    alternative_markets = [0, 1] if primary_market in [0, 1] else [2]
    markets_to_try = [primary_market] + [m for m in alternative_markets if m != primary_market]
    
    aliases = get_code_aliases(pure_code)
    
    for market in markets_to_try:
        for alias_code in aliases:
            all_bars = []
            max_bars = 500 if is_incremental else 5600
            step = 800
            
            for start_idx in range(0, max_bars, step):
                count_to_fetch = min(step, max_bars - start_idx)
                try:
                    # 双管齐下：优先专用包，备用标准包
                    bars = api.get_index_bars(9, market, alias_code, start_idx, count_to_fetch)
                    if not bars:
                        bars = api.get_security_bars(9, market, alias_code, start_idx, count_to_fetch)
                    
                    if not bars:
                        break
                    all_bars.extend(bars)
                    if len(bars) < count_to_fetch:
                        break
                except:
                    break
                    
            if all_bars:
                return all_bars, market, primary_market, alias_code
                
    return [], None, primary_market, pure_code

def main():
    is_incremental = "--incremental" in sys.argv or "-i" in sys.argv
    print(f"📥 Starting PyTDX Index Fetcher (Mode: {'Incremental' if is_incremental else 'Full History'})...")
    
    api = TdxHq_API()
    connected = False
    for s in TDX_SERVERS:
        print(f"🔌 Connection attempt to {s['ip']}:{s['port']} ({s['desc']})...")
        try:
            if api.connect(s['ip'], s['port']):
                connected = True
                print(f"✅ Connected to TDX Server: {s['ip']} ({s['desc']})")
                break
        except Exception as e:
            print(f"⚠️ Failed to connect to {s['ip']}: {e}")
            continue
            
    if not connected:
        print("❌ Error: Failed to connect to any TDX server.")
        sys.exit(1)
        
    all_rows = []
    
    # 🎯 统计容器，记录成功和失败指标
    success_records = []
    failed_records = []
    
    try:
        for code_str in INDEX_LIST.keys():
            print(f"📊 Pulling Index: {code_str}...")
            bars, actual_market, primary_market, alias_code = fetch_index_data(api, code_str, is_incremental=is_incremental)
            
            if not bars:
                # 记录失败
                print(f"❌ Failed: No bars fetched for {code_str} ({INDEX_LIST[code_str]})")
                failed_records.append({"code": code_str, "name": INDEX_LIST[code_str]})
                continue
            
            # 记录成功与是否重定向
            is_redirected = (actual_market != primary_market) or (alias_code != code_str.split('.')[1])
            success_records.append({
                "code": code_str,
                "name": INDEX_LIST[code_str],
                "count": len(bars),
                "redirected": is_redirected,
                "target_code": alias_code,
                "target_market": actual_market
            })
            
            if is_redirected:
                print(f"   🔄 [Self-Healed] Redirected {code_str} -> Market {actual_market}, Code '{alias_code}'")
                
            print(f"   Success. Fetched {len(bars)} records.")
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
        
    # 🎯 控制台输出精美、可读性极强的总结报告
    print("\n" + "="*80)
    print("📋 指数下载总结报告 (INDEX DOWNLOAD SUMMARY REPORT)")
    print("="*80)
    print(f"   - 目标抓取总数: {len(INDEX_LIST)} 只")
    print(f"   - 成功同步指数: {len(success_records)} / {len(INDEX_LIST)}")
    print(f"   - 失败异常指数: {len(failed_records)} / {len(INDEX_LIST)}")
    
    if failed_records:
        print("\n❌ 失败指数明细 (FAILED LIST):")
        for f in failed_records:
            print(f"   • {f['code']} ({f['name']}) -> [物理连接超时或该代码在服务器端已下线]")
            
    print("\n✅ 成功同步明细 (SUCCESSFUL LIST):")
    for s in success_records:
        heal_msg = f"  [自愈重定向: {s['target_code']}@M{s['target_market']}]" if s['redirected'] else ""
        print(f"   • {s['code']} ({s['name']}): 已拉取 {s['count']:,} 行 K 线{heal_msg}")
    print("="*80 + "\n")
        
    if not all_rows:
        print("❌ 严重错误: 未抓取到任何有效的指数行情数据。")
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
    print(f"🎉 物理落盘成功：已将 {len(df):,} 行高精度指数数据写入至 {out_path}!")

if __name__ == "__main__":
    main()
