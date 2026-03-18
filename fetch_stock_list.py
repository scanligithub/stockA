import asyncio
import aiohttp
import polars as pl
import sys
import math
import time
import socket
import random
from datetime import datetime

# 极致稳健：限制并发为 1，串行请求避免被封锁
sem = asyncio.Semaphore(1)

async def fetch_page(session, page_no, pz, url, params, retries=5):
    """极致稳健的分页抓取：带随机休眠和深度重试"""
    async with sem:
        page_params = params.copy()
        page_params["pn"] = str(page_no)
        page_params["pz"] = str(pz)
        page_params["_"] = str(int(time.time() * 1000))
        
        for attempt in range(retries):
            try:
                # 每个任务前随机微休眠，打碎并发特征
                await asyncio.sleep(random.uniform(0.5, 1.5))
                
                async with session.get(url, params=page_params, timeout=60) as resp:
                    if resp.status == 200:
                        data = await resp.json()
                        raw_list = data.get("data", {}).get("diff", [])
                        
                        # --- 数据预清洗：解决 Polars 类型冲突的核心逻辑 ---
                        clean_list = []
                        for item in raw_list:
                            # 处理特殊字符 "-"，将其转为 None (Python null)
                            processed = {}
                            for k, v in item.items():
                                processed[k] = None if v == "-" else v
                            clean_list.append(processed)
                        return clean_list
                    
                    elif resp.status == 403:
                        print(f"[!] 403 被拒，请求过快，尝试深度休眠...")
                        await asyncio.sleep(10)
            except (aiohttp.ServerDisconnectedError, asyncio.TimeoutError, aiohttp.ClientError):
                wait_time = (attempt + 1) * 10  # 增加等待时间
                print(f"[!] 连接断开或超时，第 {attempt+1} 次重试，等待 {wait_time}s...")
                await asyncio.sleep(wait_time)
            except Exception as e:
                print(f"[-] 页面 {page_no} 未知错误: {type(e).__name__}")
                await asyncio.sleep(2)
        return []

async def fetch_all_a_shares():
    API_URL = "https://push2.eastmoney.com/api/qt/clist/get"
    base_params = {
        "po": "1", "np": "1", "ut": "bd1d9ddb04089700cf9c27f6f7426281",
        "fltt": "2", "invt": "2", "fid": "f3",
        "fs": "m:0 t:6,m:1 t:2,m:1 t:23,m:0 t:80,m:1 t:81,m:1 t:82",
        "fields": "f12,f14,f13,f2,f3" 
    }
    
    headers = {
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Referer": "https://quote.eastmoney.com/",
        "Accept": "application/json"
    }

    # 限制连接池，防止被识别为爬虫集群
    conn = aiohttp.TCPConnector(
        limit=5, 
        family=socket.AF_INET,
        keepalive_timeout=60
    )
    
    async with aiohttp.ClientSession(headers=headers, connector=conn) as session:
        # 1. 获取总数 (带重试)
        print(f"[*] 正在查询总数... {datetime.now().strftime('%H:%M:%S')}")
        total_count = 0
        for _ in range(5):
            try:
                test_p = {**base_params, "pn": "1", "pz": "1", "_": str(int(time.time() * 1000))}
                async with session.get(API_URL, params=test_p, timeout=20) as resp:
                    res_json = await resp.json()
                    total_count = res_json.get("data", {}).get("total", 0)
                    if total_count > 0: break
            except:
                await asyncio.sleep(3)
        
        if total_count == 0:
            print("[-] 无法连接到行情网关，退出")
            sys.exit(1)

        print(f"[+] 目标总数: {total_count} 只")
        
        # 2. 分页串行抓取（避免并发过高被封锁）
        page_size = 100
        total_pages = math.ceil(total_count / page_size)
        print(f"[*] 启动 {total_pages} 个串行抓取任务...")
        
        pages_data = []
        for i in range(1, total_pages + 1):
            print(f"[*] 正在获取第 {i}/{total_pages} 页...")
            page_data = await fetch_page(session, i, page_size, API_URL, base_params)
            if page_data:
                pages_data.append(page_data)
            else:
                print(f"[!] 第 {i} 页获取失败，跳过")
        
        # 3. 汇总数据
        all_stocks = []
        for page in pages_data:
            if page: all_stocks.extend(page)

        print(f"[*] 原始抓取完成，成功获取 {len(all_stocks)} 条记录")
        if not all_stocks:
            sys.exit(1)

        # 4. Polars 处理（此时数据已清洗过，不会报错）
        df = pl.DataFrame(all_stocks)
        df = df.rename({
            "f12": "code", "f14": "name", "f13": "market_id",
            "f2": "price", "f3": "pct_chg"
        })

        # 标准化后缀
        df = df.with_columns([
            pl.when(pl.col("code").str.starts_with("6")).then(pl.col("code") + ".SH")
              .when(pl.col("code").str.starts_with("0")).then(pl.col("code") + ".SZ")
              .when(pl.col("code").str.starts_with("3")).then(pl.col("code") + ".SZ")
              .when(pl.col("code").str.starts_with("8")).then(pl.col("code") + ".BJ")
              .when(pl.col("code").str.starts_with("4")).then(pl.col("code") + ".BJ")
              .otherwise(pl.col("code"))
              .alias("symbol")
        ])

        # 强制转换类型，此时 None 会自动变为 Polars 的 null
        df = df.with_columns([
            pl.col("price").cast(pl.Float64, strict=False),
            pl.col("pct_chg").cast(pl.Float64, strict=False)
        ])

        df = df.filter(pl.col("code").is_not_null()).unique(subset=["symbol"])
        df = df.sort("symbol")

        print(f"[+] 抓取成功！有效个股: {len(df)} 只")
        print(df.head(5))

        df.write_csv("a_share_list.csv")
        print("[*] 文件 a_share_list.csv 已就绪")

if __name__ == "__main__":
    asyncio.run(fetch_all_a_shares())
