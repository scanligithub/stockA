import os
import sys
import pandas as pd
import requests
import re
import json
import datetime
import time
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
    "sh.931238": "SSH黄金股票"
}

TDX_SERVERS = [
    {"ip": "124.71.187.122", "port": 7709, "desc": "华为云节点"},
    {"ip": "119.29.25.16", "port": 7709, "desc": "腾讯云节点"},
    {"ip": "119.147.171.115", "port": 7709, "desc": "招商证券主站"}
]

# 🎯 核心修复：各大引擎的 Referer 物理隔离，防止 WAF 跨站拦截
UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
EM_HEADERS = {"User-Agent": UA, "Referer": "https://quote.eastmoney.com/"}
SINA_HEADERS = {"User-Agent": UA, "Referer": "https://finance.sina.com.cn/"}
QQ_HEADERS = {"User-Agent": UA, "Referer": "https://gu.qq.com/"}
NETEASE_HEADERS = {"User-Agent": UA, "Referer": "http://quotes.money.163.com/"}

def get_code_aliases(pure_code):
    aliases = [pure_code]
    if pure_code.startswith("93"): aliases.append("H3" + pure_code[2:])
    elif pure_code.startswith("0009"): 
        aliases.extend(["H309" + pure_code[4:], "3999" + pure_code[4:]])
    elif pure_code.startswith("3999"): aliases.append("H309" + pure_code[4:])
    return list(dict.fromkeys(aliases))

# [Engine 1] TDX 
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

# [Engine 2] EastMoney (修复了 Referer)
def fetch_from_eastmoney(code_str, is_incremental=False):
    prefix, pure_code = code_str.split('.')
    lmt = 500 if is_incremental else 10000
    candidate_prefixes = ["1", "0", "119", "124", "90", "2", "47"]
    candidate_secids = [f"{p}.{pure_code}" for p in candidate_prefixes]
    
    EM_CODE_MAP = {
        "sh.932252": ["119.932252", "90.932252", "1.932252"],
        "sh.931160": ["119.931160", "90.931160", "1.931160"],
        "sh.931151": ["119.931151", "90.931151", "1.931151"],
        "sh.931494": ["119.931494", "90.931494", "1.931494"],
        "sh.931409": ["119.931409", "90.931409", "1.931409"],
        "sh.931152": ["119.931152", "90.931152", "1.931152"],
        "sh.930606": ["119.930606", "90.930606", "1.930606"],
        "sh.931238": ["119.931238", "90.931238", "1.931238"],
        "sz.399977": ["90.399977", "124.399977", "0.399977"],
        "sz.399979": ["90.399979", "124.399979", "0.399979"],
        "sh.000918": ["119.000918", "90.000918", "1.000918"]
    }
    if code_str in EM_CODE_MAP:
        candidate_secids = EM_CODE_MAP[code_str] + candidate_secids
        candidate_secids = list(dict.fromkeys(candidate_secids))

    url = "https://push2his.eastmoney.com/api/qt/stock/kline/get"
    for secid in candidate_secids:
        params = {"secid": secid, "fields1": "f1,f2,f3,f4,f5,f6", "fields2": "f51,f52,f53,f54,f55,f56,f57,f58", "klt": "101", "fqt": "0", "end": "20500101", "lmt": str(lmt)}
        try:
            # 🎯 强制使用东财专属 Header
            res = requests.get(url, params=params, headers=EM_HEADERS, timeout=5).json()
            if res and res.get("data") and res["data"].get("klines"):
                bars = []
                for k in res["data"]["klines"]:
                    parts = k.split(',')
                    bars.append({"datetime": parts[0], "open": float(parts[1]), "close": float(parts[2]),
                                 "high": float(parts[3]), "low": float(parts[4]), "vol": float(parts[5]) * 100, "amount": float(parts[6])})
                return bars, secid
        except:
            continue
    return [], None

# [Engine 3] Sina Official API
def fetch_from_sina(code_str, is_incremental=False):
    symbol = code_str.replace('.', '')
    limit = 500 if is_incremental else 6000
    url = f"https://money.finance.sina.com.cn/quotes_service/api/json_v2.php/CN_MarketData.getKLineData?symbol={symbol}&scale=240&ma=no&datalen={limit}"
    try:
        text = requests.get(url, headers=SINA_HEADERS, timeout=6).text
        match = re.search(r'\[.*\]', text)
        if match:
            array_str = re.sub(r'([{,])\s*([a-zA-Z_]+)\s*:', r'\1"\2":', match.group(0))
            bars = []
            for k in json.loads(array_str):
                bars.append({"datetime": k.get("day", ""), "open": float(k.get("open", 0)), "close": float(k.get("close", 0)),
                             "high": float(k.get("high", 0)), "low": float(k.get("low", 0)), "vol": float(k.get("volume", 0)), "amount": 0.0})
            return bars
    except:
        pass
    return []

# [Engine 4] Tencent API
def fetch_from_tencent(code_str, is_incremental=False):
    symbol = code_str.replace('.', '')
    limit = 500 if is_incremental else 6000
    url = f"https://proxy.finance.qq.com/ifzqgtimg/appstock/app/newiqkline/get?param={symbol},day,,,{limit},qfq"
    try:
        res = requests.get(url, headers=QQ_HEADERS, timeout=6).json()
        if res and res.get("data") and res["data"].get(symbol):
            bars = []
            for k in res["data"][symbol].get("day", []):
                bars.append({"datetime": k[0], "open": float(k[1]), "close": float(k[2]),
                             "high": float(k[3]), "low": float(k[4]), "vol": float(k[5]) * 100 if len(k) > 5 else 0.0, "amount": 0.0})
            return bars
    except:
        pass
    return []

# 🌟 [Engine 5] NetEase 163 (网易底层历史 CSV 归档接口，无差别兜底神器)
def fetch_from_netease(code_str, is_incremental=False):
    prefix, pure_code = code_str.split('.')
    # 网易代码体系：上海=0，深圳=1
    ne_prefix = "0" if prefix == "sh" else "1"
    symbol = ne_prefix + pure_code
    
    end_date = datetime.datetime.now().strftime("%Y%m%d")
    start_date = "19900101" if not is_incremental else (datetime.datetime.now() - datetime.timedelta(days=365)).strftime("%Y%m%d")
    url = f"http://quotes.money.163.com/service/chddata.html?code={symbol}&start={start_date}&end={end_date}&fields=TCLOSE;HIGH;LOW;TOPEN;VOTURNOVER;VATURNOVER"
    
    try:
        res = requests.get(url, headers=NETEASE_HEADERS, timeout=8)
        if res.status_code == 200 and "TCLOSE" in res.text:
            lines = res.text.strip().split('\n')
            bars = []
            for line in lines[1:]: # 忽略表头
                parts = line.split(',')
                # 网易返回: 日期,股票代码,名称,收盘价,最高价,最低价,开盘价,成交量,成交金额
                if len(parts) >= 9 and parts[3] != '0.0':
                    dt_str = f"{parts[0][:4]}-{parts[0][4:6]}-{parts[0][6:]}" # YYYY-MM-DD
                    bars.append({
                        "datetime": dt_str,
                        "close": float(parts[3] or 0), "high": float(parts[4] or 0),
                        "low": float(parts[5] or 0), "open": float(parts[6] or 0),
                        "vol": float(parts[7] or 0), "amount": float(parts[8] or 0)
                    })
            if bars:
                bars.reverse() # 网易是倒序，必须反转为正序
                return bars
    except Exception as e:
        pass
    return []

def main():
    is_incremental = "--incremental" in sys.argv or "-i" in sys.argv
    print(f"📥 Starting Penta-Engine Index Fetcher (Mode: {'Incremental' if is_incremental else 'Full History'})...")
    
    api = TdxHq_API()
    connected = False
    for s in TDX_SERVERS:
        try:
            if api.connect(s['ip'], s['port']):
                connected = True
                print(f"✅ TDX Engine Online: {s['ip']}")
                break
        except Exception:
            continue
            
    if not connected:
        print("⚠️ TDX Engine failed to connect. Relying entirely on HTTP Engines.")
        
    all_rows = []
    success_records = []
    failed_records = []
    
    MAX_RETRIES = 3 

    try:
        for code_str in INDEX_LIST.keys():
            print(f"📊 Pulling Index: {code_str}...")
            
            bars, actual_market, primary_market, alias_code = [], None, None, None
            used_engine = "TDX"
            
            for attempt in range(MAX_RETRIES):
                # [1] TDX
                if connected:
                    bars, actual_market, primary_market, alias_code = fetch_from_tdx(api, code_str, is_incremental)
                
                # [2] EastMoney (已修复 Referer，王者归来)
                if not bars:
                    bars, em_secid = fetch_from_eastmoney(code_str, is_incremental)
                    if bars: used_engine, actual_market, alias_code = "🌐 EastMoney", "Web", em_secid
                        
                # [3] Sina 
                if not bars:
                    bars = fetch_from_sina(code_str, is_incremental)
                    if bars: used_engine, actual_market, alias_code = "🧿 Sina", "Web", code_str
                    
                # [4] Tencent
                if not bars:
                    bars = fetch_from_tencent(code_str, is_incremental)
                    if bars: used_engine, actual_market, alias_code = "🐧 Tencent", "Web", code_str

                # [5] NetEase 163 CSV 暴力解析 (针对极冷门最新指数)
                if not bars:
                    print(f"   ⚠️ Escaping to 📦 NetEase Archive Engine for {code_str}...")
                    bars = fetch_from_netease(code_str, is_incremental)
                    if bars: used_engine, actual_market, alias_code = "📦 NetEase 163", "Web", code_str

                if bars:
                    break
                    
                if attempt < MAX_RETRIES - 1:
                    delay = 2 * (attempt + 1)
                    print(f"   ⏳ [Retry {attempt + 1}/{MAX_RETRIES - 1}] All engines missed {code_str}. Retrying in {delay}s...")
                    time.sleep(delay)

            if not bars:
                print(f"❌ Failed: ALL 5 Engines defeated by {code_str} after {MAX_RETRIES} attempts.")
                failed_records.append({"code": code_str, "name": INDEX_LIST[code_str]})
                continue
            
            is_redirected = (used_engine != "TDX") or (actual_market != primary_market) or (alias_code != code_str.split('.')[1])
            success_records.append({
                "code": code_str, "name": INDEX_LIST[code_str], "count": len(bars),
                "redirected": is_redirected, "target_code": alias_code, "engine": used_engine
            })
            
            if is_redirected:
                print(f"   🔄 [Healed] Handled by {used_engine} (Target: '{alias_code}')")
                
            print(f"   Success. Fetched {len(bars)} records.")
            for b in bars:
                all_rows.append({
                    "date": b['datetime'][:10], "code": code_str, "open": float(b['open']),
                    "high": float(b['high']), "low": float(b['low']), "close": float(b['close']),
                    "volume": float(b['vol']), "amount": float(b['amount']),
                })
    finally:
        if connected: api.disconnect()
        
    print("\n" + "="*85)
    print("📋 指数五擎大满贯架构总结报告 (INDEX PENTA-ENGINE DOWNLOAD SUMMARY)")
    print("="*85)
    print(f"   - 目标抓取总数: {len(INDEX_LIST)} 只")
    print(f"   - 成功同步指数: {len(success_records)} / {len(INDEX_LIST)}")
    print(f"   - 失败异常指数: {len(failed_records)} / {len(INDEX_LIST)}")
    
    if failed_records:
        print("\n❌ 失败明细 (这已是国内公开数据的极限边界):")
        for f in failed_records:
            print(f"   • {f['code']} ({f['name']}) -> [核心引擎 3 轮重试全部阵亡]")
            
    print("\n✅ 成功同步明细 (SUCCESSFUL LIST):")
    for s in success_records:
        heal_msg = f"  [{s['engine']} | Hit: {s['target_code']}]" if s['redirected'] else ""
        print(f"   • {s['code']} ({s['name']}): 已拉取 {s['count']:,} 行 K 线{heal_msg}")
    print("="*85 + "\n")
        
    if not all_rows: sys.exit(1)
        
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
