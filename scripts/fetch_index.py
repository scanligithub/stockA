import os
import sys
import pandas as pd
import requests
from pytdx.hq import TdxHq_API

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
    {"ip": "112.95.140.93", "port": 7709, "desc": "华泰证券主站"}
]

def get_code_aliases(pure_code):
    aliases = [pure_code]
    if pure_code.startswith("93"):
        aliases.append("H3" + pure_code[2:])
    elif pure_code.startswith("0009"):
        aliases.append("H309" + pure_code[4:])
        aliases.append("3999" + pure_code[4:])
    elif pure_code.startswith("3999"):
        aliases.append("H309" + pure_code[4:])
    return list(dict.fromkeys(aliases))

# 🌟 引擎一：通达信抓取
def fetch_from_tdx(api, code_str, is_incremental=False):
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
                    bars = api.get_index_bars(9, market, alias_code, start_idx, count_to_fetch)
                    if not bars:
                        bars = api.get_security_bars(9, market, alias_code, start_idx, count_to_fetch)
                    
                    if not bars: break
                    all_bars.extend(bars)
                    if len(bars) < count_to_fetch: break
                except:
                    break
                    
            if all_bars:
                return all_bars, market, primary_market, alias_code
                
    return [], None, primary_market, pure_code

# 🌟 引擎二：东财原生 API 兜底抓取 (解决 TDX 缺失冷门/新定制中证指数的问题)
def fetch_from_eastmoney(code_str, is_incremental=False):
    prefix, pure_code = code_str.split('.')
    # 东财市场代码：上海 1，其他(含深圳、北交所)统一算 0
    secid = f"1.{pure_code}" if prefix == "sh" else f"0.{pure_code}"
    lmt = 500 if is_incremental else 10000
    
    url = "https://push2his.eastmoney.com/api/qt/stock/kline/get"
    params = {
        "secid": secid,
        "fields1": "f1,f2,f3,f4,f5,f6",
        "fields2": "f51,f52,f53,f54,f55,f56,f57,f58",
        "klt": "101",
        "fqt": "0",
        "end": "20500101",
        "lmt": str(lmt)
    }
    
    try:
        res = requests.get(url, params=params, timeout=10).json()
        if res and res.get("data") and res["data"].get("klines"):
            klines = res["data"]["klines"]
            bars = []
            for k in klines:
                parts = k.split(',')
                bars.append({
                    "datetime": parts[0],
                    "open": float(parts[1]),
                    "close": float(parts[2]),
                    "high": float(parts[3]),
                    "low": float(parts[4]),
                    "vol": float(parts[5]) * 100, # 东财是“手”，TDX是“股”，统一乘以 100 对齐
                    "amount": float(parts[6])
                })
            return bars
    except:
        pass
    return []

def main():
    is_incremental = "--incremental" in sys.argv or "-i" in sys.argv
    print(f"📥 Starting Dual-Engine Index Fetcher (Mode: {'Incremental' if is_incremental else 'Full History'})...")
    
    api = TdxHq_API()
    connected = False
    for s in TDX_SERVERS:
        print(f"🔌 TDX Connection attempt to {s['ip']}:{s['port']}...")
        try:
            if api.connect(s['ip'], s['port']):
                connected = True
                print(f"✅ TDX Engine Online: {s['ip']}")
                break
        except Exception:
            continue
            
    if not connected:
        print("❌ TDX Engine failed to connect. (Will rely fully on EastMoney if needed, but stopping for safety)")
        sys.exit(1)
        
    all_rows = []
    success_records = []
    failed_records = []
    
    try:
        for code_str in INDEX_LIST.keys():
            print(f"📊 Pulling Index: {code_str}...")
            
            # 第一优先级：通达信超高速拉取
            bars, actual_market, primary_market, alias_code = fetch_from_tdx(api, code_str, is_incremental=is_incremental)
            is_em_fallback = False
            
            # 第二优先级：东财 API 完美兜底
            if not bars:
                print(f"   ⚠️ TDX missed {code_str}. Triggering EastMoney Fallback Engine...")
                bars = fetch_from_eastmoney(code_str, is_incremental=is_incremental)
                if bars:
                    is_em_fallback = True
                    actual_market = "EastMoney_API"
                    alias_code = code_str

            if not bars:
                print(f"❌ Failed: Both engines failed for {code_str} ({INDEX_LIST[code_str]})")
                failed_records.append({"code": code_str, "name": INDEX_LIST[code_str]})
                continue
            
            is_redirected = is_em_fallback or (actual_market != primary_market) or (alias_code != code_str.split('.')[1])
            
            success_records.append({
                "code": code_str,
                "name": INDEX_LIST[code_str],
                "count": len(bars),
                "redirected": is_redirected,
                "target_code": alias_code,
                "target_market": actual_market
            })
            
            if is_redirected:
                engine = "🌐 EastMoney API" if is_em_fallback else f"TDX Market {actual_market}"
                print(f"   🔄 [Self-Healed] Redirected via: {engine} (Code: '{alias_code}')")
                
            print(f"   Success. Fetched {len(bars)} records.")
            for b in bars:
                all_rows.append({
                    "date": b['datetime'][:10],
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
        
    print("\n" + "="*80)
    print("📋 指数双擎下载总结报告 (INDEX DUAL-ENGINE DOWNLOAD SUMMARY)")
    print("="*80)
    print(f"   - 目标抓取总数: {len(INDEX_LIST)} 只")
    print(f"   - 成功同步指数: {len(success_records)} / {len(INDEX_LIST)}")
    print(f"   - 失败异常指数: {len(failed_records)} / {len(INDEX_LIST)}")
    
    if failed_records:
        print("\n❌ 失败指数明细 (FAILED LIST):")
        for f in failed_records:
            print(f"   • {f['code']} ({f['name']}) -> [双引擎均告失败]")
            
    print("\n✅ 成功同步明细 (SUCCESSFUL LIST):")
    for s in success_records:
        heal_msg = f"  [自愈重定向: {s['target_code']} @ {s['target_market']}]" if s['redirected'] else ""
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
    
    df['pctChg'] = df.groupby('code')['close'].pct_change() * 100
    df['pctChg'] = df['pctChg'].fillna(0.0)
    
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
