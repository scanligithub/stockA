import os
import sys
import pandas as pd
import requests
import json
import datetime
import time
from pytdx.hq import TdxHq_API

# 56 只量化指数
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
    "sh.931238": "SSH黄金股票", "sh.930606": "中证黄金产业"
}

TDX_SERVERS = [
    {"ip": "119.147.171.115", "port": 7709, "desc": "深圳招商"},
    {"ip": "124.71.187.122", "port": 7709, "desc": "华为云节点"},
    {"ip": "119.29.25.16", "port": 7709, "desc": "腾讯云节点"}
]

# 全局雪球 Session（增加 3 次容错握手）
XQ_SESSION = requests.Session()
XQ_SESSION.headers.update({"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"})
for _ in range(3):
    try:
        XQ_SESSION.get("https://xueqiu.com/", timeout=5)
        break
    except:
        time.sleep(1)

# [Engine 1] TDX 
def fetch_from_tdx(api, code_str, is_incremental=False):
    prefix, pure_code = code_str.split('.')
    market = 1 if prefix == "sh" else 0 if prefix == "sz" else 2
    
    aliases = [pure_code]
    if pure_code.startswith("93"): aliases.append("H3" + pure_code[2:])
    elif pure_code.startswith("3999"): aliases.append("H309" + pure_code[4:])
        
    for alias in aliases:
        all_bars = []
        max_bars = 500 if is_incremental else 5600
        step = 800
        for start_idx in range(0, max_bars, step):
            count = min(step, max_bars - start_idx)
            try:
                bars = api.get_index_bars(9, market, alias, start_idx, count)
                if not bars: break
                all_bars.extend(bars)
                if len(bars) < count: break
            except:
                break
        if all_bars:
            return all_bars, "TDX"
    return [], None

# [Engine 2] Xueqiu (安全边界版：硬性截断单次上限至 2000 行，绝不触发 400 报错)
def fetch_from_xueqiu(code_str, is_incremental=False):
    symbol = code_str.replace('.', '').upper()
    # 🎯 核心修正：单次请求行数超过 2000 会被雪球 WAF 直接拦截，这里强制截断
    limit = 500 if is_incremental else 2000
    ts = int(time.time() * 1000)
    url = f"https://stock.xueqiu.com/v5/stock/chart/kline.json?symbol={symbol}&begin={ts}&period=day&type=before&count=-{limit}&indicator=kline"
    
    try:
        res = XQ_SESSION.get(url, timeout=5).json()
        if res and res.get("data") and res["data"].get("item"):
            bars = []
            for item in res["data"]["item"]:
                dt = datetime.datetime.fromtimestamp(item[0]/1000).strftime('%Y-%m-%d')
                bars.append({
                    "datetime": dt, "open": float(item[2]), "high": float(item[3]),
                    "low": float(item[4]), "close": float(item[5]),
                    "vol": float(item[1]), "amount": float(item[9] or 0.0)
                })
            return bars, "❄️ Xueqiu"
    except:
        pass
    return [], None

# [Engine 3] Tencent (安全边界版：硬性截断单次上限至 1000 行)
def fetch_from_tencent(code_str, is_incremental=False):
    symbol = code_str.replace('.', '')
    # 🎯 核心修正：腾讯单次请求行数超过 1000 极易返回空数据或报错，强制截断
    limit = 500 if is_incremental else 1000
    url = f"https://proxy.finance.qq.com/ifzqgtimg/appstock/app/newiqkline/get?param={symbol},day,,,{limit},qfq"
    try:
        res = requests.get(url, headers={"Referer": "https://gu.qq.com/"}, timeout=5).json()
        if res and res.get("data") and res["data"].get(symbol):
            bars = []
            for k in res["data"][symbol].get("day", []):
                bars.append({"datetime": k[0], "open": float(k[1]), "close": float(k[2]),
                             "high": float(k[3]), "low": float(k[4]), "vol": float(k[5])*100 if len(k)>5 else 0.0, "amount": 0.0})
            return bars, "🐧 Tencent"
    except:
        pass
    return [], None

def main():
    is_incremental = "--incremental" in sys.argv or "-i" in sys.argv
    print(f"📥 Starting Tri-Core Sniper (三擎智能狙击) Index Fetcher (Mode: {'Incremental' if is_incremental else 'Full History'})...")
    
    api = TdxHq_API()
    connected = False
    # 🎯 核心修正：多服务器自动重试连接，直到连上最稳的节点
    for s in TDX_SERVERS:
        try:
            print(f"🔌 Trying to connect TDX Server: {s['desc']} ({s['ip']})...")
            if api.connect(s['ip'], s['port']):
                connected = True
                print(f"✅ TDX Engine Online: {s['desc']}")
                break
        except:
            continue

    if not connected:
        print("⚠️ TDX Engine offline. Using Web Engines fallback.")

    all_rows = []
    success_records = []
    failed_records = []
    
    for code_str, name in INDEX_LIST.items():
        print(f"📊 Pulling: {code_str} ({name}) ...", end="")
        
        bars, engine_used = [], None
        
        # 智能路由：如果是新定制指数，直接优先用雪球
        if code_str.startswith("sh.93"):
            engines_to_try = [
                lambda c: fetch_from_xueqiu(c, is_incremental),
                lambda c: fetch_from_tdx(api, c, is_incremental) if connected else ([], None)
            ]
        else:
            engines_to_try = [
                lambda c: fetch_from_tdx(api, c, is_incremental) if connected else ([], None),
                lambda c: fetch_from_tencent(c, is_incremental),
                lambda c: fetch_from_xueqiu(c, is_incremental)
            ]

        # 顺序探测
        for func in engines_to_try:
            bars, engine_used = func(code_str)
            if bars: 
                break

        # 无脑兜底
        if not bars:
            bars, engine_used = fetch_from_xueqiu(code_str, is_incremental)

        if not bars:
            print(" ❌ Failed.")
            failed_records.append(code_str)
            continue
            
        print(f" ✅ Success ({engine_used}, {len(bars)} rows)")
        success_records.append(code_str)
        
        for b in bars:
            all_rows.append({
                "date": b['datetime'][:10], "code": code_str, "open": float(b['open']),
                "high": float(b['high']), "low": float(b['low']), "close": float(b['close']),
                "volume": float(b['vol']), "amount": float(b['amount']),
            })
            
    if connected: api.disconnect()
        
    print(f"\n📋 INDEX SNIPER DOWNLOAD SUMMARY: {len(success_records)} / {len(INDEX_LIST)} SUCCESS")
    if failed_records:
        print(f"❌ FAILED: {failed_records}")
        
    if not all_rows: 
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
    
    for col in ['open', 'high', 'low', 'close', 'pctChg']: df[col] = df[col].astype('float32')
    for col in ['volume', 'amount']: df[col] = df[col].astype('float64')
    
    os.makedirs("temp_parts", exist_ok=True)
    out_path = "temp_parts/index_kline_all.parquet"
    df.to_parquet(out_path, index=False)
    print(f"🎉 物理落盘成功至 {out_path}!")

if __name__ == "__main__":
    main()
