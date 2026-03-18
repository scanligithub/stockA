import requests
import polars as pl
import time
import random
import sys
import math
from datetime import datetime

# 轮换 User-Agent 列表
USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
]

def get_headers():
    return {
        "User-Agent": random.choice(USER_AGENTS),
        "Referer": "https://quote.eastmoney.com/center/gridlist.html",
        "Accept": "application/json, text/plain, */*",
        "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
        "Accept-Encoding": "gzip, deflate",
        "Connection": "keep-alive",
    }

def fetch_page(page_no, page_size, retries=3):
    """串行请求"""
    API_URL = "https://push2.eastmoney.com/api/qt/clist/get"
    params = {
        "pn": page_no,
        "pz": page_size,
        "po": "1",
        "np": "1",
        "ut": "bd1d9ddb04089700cf9c27f6f7426281",
        "fltt": "2",
        "invt": "2",
        "fid": "f3",
        "fs": "m:0 t:6,m:1 t:2,m:1 t:23,m:0 t:80,m:1 t:81,m:1 t:82",
        "fields": "f12,f14,f13,f2,f3",
        "_": int(time.time() * 1000),
    }

    headers = get_headers()

    for attempt in range(retries):
        try:
            # 随机休眠
            time.sleep(random.uniform(2.0, 5.0))

            resp = requests.get(API_URL, params=params, headers=headers, timeout=30)
            
            if resp.status_code == 200:
                data = resp.json()
                raw_list = data.get("data", {}).get("diff", [])

                clean_list = []
                for item in raw_list:
                    processed = {}
                    for k, v in item.items():
                        processed[k] = None if v == "-" else v
                    clean_list.append(processed)
                return clean_list

            elif resp.status_code == 403:
                print(f"[!] 403 被拒，休眠 30s...")
                time.sleep(30)
                continue

            wait_time = (attempt + 1) * 10
            print(f"[!] 状态码 {resp.status_code}，第 {attempt+1} 次重试，等待 {wait_time}s...")
            time.sleep(wait_time)

        except Exception as e:
            print(f"[-] 页面 {page_no} 错误：{type(e).__name__}: {e}")
            time.sleep(10)
    return []

def fetch_all_a_shares():
    API_URL = "https://push2.eastmoney.com/api/qt/clist/get"
    params = {
        "pn": "1",
        "pz": "1",
        "po": "1",
        "np": "1",
        "ut": "bd1d9ddb04089700cf9c27f6f7426281",
        "fltt": "2",
        "invt": "2",
        "fid": "f3",
        "fs": "m:0 t:6,m:1 t:2,m:1 t:23,m:0 t:80,m:1 t:81,m:1 t:82",
        "fields": "f12,f14,f13,f2,f3",
        "_": int(time.time() * 1000),
    }

    headers = get_headers()

    # 1. 获取总数
    print(f"[*] 正在查询总数... {datetime.now().strftime('%H:%M:%S')}")
    
    total_count = 0
    for retry in range(5):
        try:
            if retry > 0:
                print(f"[*] 第 {retry} 次重试，等待 20s...")
                time.sleep(20)

            resp = requests.get(API_URL, params=params, headers=headers, timeout=30)
            if resp.status_code == 200:
                res_json = resp.json()
                total_count = res_json.get("data", {}).get("total", 0)
                if total_count > 0:
                    break
        except Exception as e:
            print(f"[-] 获取总数失败：{type(e).__name__}: {e}")
            time.sleep(10)

    if total_count == 0:
        print("[-] 无法连接到行情网关，退出")
        sys.exit(1)

    print(f"[+] 目标总数：{total_count} 只")

    # 2. 分页获取
    page_size = 100
    total_pages = math.ceil(total_count / page_size)
    print(f"[*] 共 {total_pages} 页，开始获取...")

    pages_data = []
    for i in range(1, total_pages + 1):
        print(f"[*] 获取第 {i}/{total_pages} 页...", end=" ", flush=True)
        page_data = fetch_page(i, page_size)
        if page_data:
            pages_data.append(page_data)
            print(f"成功 ({len(page_data)}条)")
        else:
            print("失败")
        
        # 每页之间休眠
        if i < total_pages:
            time.sleep(random.uniform(5.0, 10.0))

    # 3. 汇总
    all_stocks = []
    for page in pages_data:
        if page:
            all_stocks.extend(page)

    print(f"[*] 成功获取 {len(all_stocks)}/{total_count} 条记录")
    if not all_stocks:
        print("[-] 没有获取到任何数据")
        sys.exit(1)

    # 4. Polars 处理
    df = pl.DataFrame(all_stocks)
    df = df.rename({
        "f12": "code", "f14": "name", "f13": "market_id",
        "f2": "price", "f3": "pct_chg"
    })

    df = df.with_columns([
        pl.when(pl.col("code").str.starts_with("6")).then(pl.col("code") + ".SH")
        .when(pl.col("code").str.starts_with("0")).then(pl.col("code") + ".SZ")
        .when(pl.col("code").str.starts_with("3")).then(pl.col("code") + ".SZ")
        .when(pl.col("code").str.starts_with("8")).then(pl.col("code") + ".BJ")
        .when(pl.col("code").str.starts_with("4")).then(pl.col("code") + ".BJ")
        .otherwise(pl.col("code"))
        .alias("symbol")
    ])

    df = df.with_columns([
        pl.col("price").cast(pl.Float64, strict=False),
        pl.col("pct_chg").cast(pl.Float64, strict=False)
    ])

    df = df.filter(pl.col("code").is_not_null()).unique(subset=["symbol"])
    df = df.sort("symbol")

    print(f"[+] 有效个股：{len(df)} 只")
    print(df.head(5))

    df.write_csv("a_share_list.csv")
    print("[*] a_share_list.csv 已就绪")

if __name__ == "__main__":
    fetch_all_a_shares()
