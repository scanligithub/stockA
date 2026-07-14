# FILE: scripts/fetch_index.py
import os
import sys
import pandas as pd
import requests
import json
import datetime
import time
import subprocess

# 已剔除 19 只不稳定指数，保留 37 只高冗余稳健核心指数
INDEX_LIST = {
    "sh.000001": "上证指数", "sz.399001": "深证成指", "sz.399006": "创业板指", "sh.000688": "科创50", "bj.899050": "北证50",
    "sh.000016": "上证50", "sh.000300": "沪深300", "sh.000905": "中证500", "sh.000852": "中证1000", "sh.000851": "中证2000",
    "sz.399303": "国证2000", "sz.399330": "深证100", "sh.000090": "上证180", "sz.399324": "深证红利", "sh.000015": "红利指数", 
    "sh.000827": "中证中盘", "sz.399317": "国证1000", "sz.399807": "中证人工智能", "sz.399812": "国证芯片", 
    "sz.399354": "国证人工智能", "sz.399673": "创业板50", "sz.399285": "国证新能源", "sz.399008": "中小100", 
    "sz.399993": "中证信息安全", "sz.399975": "证券公司", "sz.399986": "中证银行", "sz.399932": "中证消费", 
    "sz.399933": "中证医药", "sz.399967": "中证军工", "sz.399989": "中证医疗", "sz.399971": "中证传媒", 
    "sz.399997": "中证白酒", "sh.000934": "中证能源", "sh.000935": "中证原材料", "sz.399990": "煤炭等权", 
    "sz.399998": "中证有色", "sz.399974": "国证国企"
}

# 雪球 Session（保留作为可选 fallback）
XQ_SESSION = requests.Session()
XQ_SESSION.headers.update({
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
})
for _ in range(3):
    try:
        XQ_SESSION.get("https://xueqiu.com/", timeout=5)
        break
    except:
        time.sleep(1)


def fetch_from_go_engine(codes_str, is_incremental=False):
    """使用 Go 版 TDX 引擎获取指数K线"""
    csv_out = "temp_index_kline.csv"
    go_cmd = ["./tdx_fetcher", "-mode=index", f"-codes={codes_str}", f"-out={csv_out}"]
    
    try:
        result = subprocess.run(go_cmd, check=True, capture_output=True, text=True, timeout=120)
        if not os.path.exists(csv_out):
            return [], None
        
        df = pd.read_csv(csv_out)
        if df.empty:
            return [], None
        
        bars = []
        for _, row in df.iterrows():
            bars.append({
                "datetime": row["date"],
                "open": float(row["open"]),
                "high": float(row["high"]),
                "low": float(row["low"]),
                "close": float(row["close"]),
                "vol": float(row["volume"]),
                "amount": float(row["amount"])
            })
        return bars, "🚀 Go-TDX"
    except Exception as e:
        print(f"⚠️ Go engine failed: {e}")
        return [], None


def fetch_from_xueqiu(code_str, is_incremental=False):
    """雪球 API（可选 fallback）"""
    symbol = code_str.replace('.', '').upper()
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


def fetch_from_tencent(code_str, is_incremental=False):
    """腾讯财经 API（可选 fallback）"""
    symbol = code_str.replace('.', '')
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
    print(f"📥 Starting Index Fetcher (Mode: {'Incremental' if is_incremental else 'Full History'})...")
    
    # 构建指数代码字符串（逗号分隔，去除点号）
    all_codes = list(INDEX_LIST.keys())
    codes_str = ",".join([c.replace(".", "") for c in all_codes])
    
    all_rows = []
    success_records = []
    failed_records = []
    
    # 主引擎：Go TDX
    print("🔌 Primary Engine: Go-TDX")
    bars, engine_used = fetch_from_go_engine(codes_str, is_incremental)
    
    if bars:
        print(f"✅ Go-TDX Success: {len(bars)} rows")
        for b in bars:
            # 需要反查 code，Go 输出的 CSV 包含 code 列
            all_rows.append({
                "date": b['datetime'][:10], "code": b.get('code', ''), "open": float(b['open']),
                "high": float(b['high']), "low": float(b['low']), "close": float(b['close']),
                "volume": float(b['vol']), "amount": float(b['amount']),
            })
        success_records = list(INDEX_LIST.keys())
    else:
        print("⚠️ Go-TDX failed, falling back to HTTP engines...")
        
        # Fallback：逐个指数尝试 HTTP 引擎
        for code_str, name in INDEX_LIST.items():
            print(f"📊 Pulling: {code_str} ({name}) ...", end="")
            
            bars, engine_used = [], None
            for func in [fetch_from_xueqiu, fetch_from_tencent]:
                bars, engine_used = func(code_str, is_incremental)
                if bars:
                    break
            
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
    
    print(f"\n📋 INDEX DOWNLOAD SUMMARY: {len(success_records)} / {len(INDEX_LIST)} SUCCESS")
    if failed_records:
        print(f"❌ FAILED: {failed_records}")
        
    if not all_rows: 
        print("❌ No data fetched. Exiting.")
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
    print(f"🎉 Index data saved to {out_path}!")


if __name__ == "__main__":
    main()
