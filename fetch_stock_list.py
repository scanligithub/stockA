import asyncio
import aiohttp
import polars as pl
import sys
import math
import time
import socket
from datetime import datetime

# 降低并发到更安全的 5，以确保在 GitHub Actions 的共享网络中绝对稳定
sem = asyncio.Semaphore(5)

async def fetch_page(session, page_no, pz, url, params, retries=3):
    """带重试机制和并发控制的分页抓取"""
    async with sem:
        page_params = params.copy()
        page_params["pn"] = str(page_no)
        page_params["pz"] = str(pz)
        # 核心伪装 1：加入时间戳，防止 CDN 缓存和防刷机制拦截
        page_params["_"] = str(int(time.time() * 1000))
        
        for attempt in range(retries):
            try:
                async with session.get(url, params=page_params, timeout=20) as resp:
                    if resp.status == 200:
                        data = await resp.json()
                        return data.get("data", {}).get("diff", [])
                    else:
                        print(f"[!] 页面 {page_no} HTTP异常: {resp.status}")
            except Exception as e:
                if attempt == retries - 1:
                    print(f"[-] 页面 {page_no} 最终失败: {type(e).__name__}")
                else:
                    await asyncio.sleep(2) # 失败后退避 2 秒再试
        return []

async def fetch_all_a_shares():
    API_URL = "https://push2.eastmoney.com/api/qt/clist/get"
    base_params = {
        "po": "1",
        "np": "1",
        "ut": "bd1d9ddb04089700cf9c27f6f7426281",
        "fltt": "2",
        "invt": "2",
        "fid": "f3",
        "fs": "m:0 t:6,m:1 t:2,m:1 t:23,m:0 t:80,m:1 t:81,m:1 t:82",
        "fields": "f12,f14,f13,f2,f3" 
    }
    
    # 核心伪装 2：更逼真的请求头
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
        "Referer": "https://quote.eastmoney.com/",
        "Accept": "application/json, text/javascript, */*; q=0.01",
        "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8"
    }

    # 核心杀手锏：强制绑定 IPv4！解决 GitHub Actions 到国内 CDN 路由超时的终极方案
    conn = aiohttp.TCPConnector(
        limit=10, 
        family=socket.AF_INET, # 禁用 IPv6
        ssl=False              # 绕过部分环境的 SSL 证书校验耗时
    )
    
    async with aiohttp.ClientSession(headers=headers, connector=conn) as session:
        # --- 步骤 1: 带有重试机制的总数查询 ---
        print(f"[*] 正在查询总数... {datetime.now().strftime('%H:%M:%S')}")
        total_count = 0
        
        for attempt in range(3):
            try:
                test_params = {**base_params, "pn": "1", "pz": "1", "_": str(int(time.time() * 1000))}
                async with session.get(API_URL, params=test_params, timeout=15) as resp:
                    data = await resp.json()
                    total_count = data.get("data", {}).get("total", 0)
                    break # 成功获取，跳出重试循环
            except Exception as e:
                print(f"[!] 尝试获取总数失败 ({attempt+1}/3): {type(e).__name__}")
                await asyncio.sleep(3)
                
        if total_count == 0:
            print("[-] 彻底无法获取数据总数，可能被拉黑或网络不可达，脚本退出。")
            sys.exit(1)

        print(f"[+] 成功检测到全量 A 股共计: {total_count} 只")
        
        # --- 步骤 2: 计算分页并开始抓取 ---
        page_size = 100
        total_pages = math.ceil(total_count / page_size)
        print(f"[*] 计划执行 {total_pages} 个分页抓取任务...")

        tasks = [fetch_page(session, i, page_size, API_URL, base_params) for i in range(1, total_pages + 1)]
        pages_data = await asyncio.gather(*tasks)
        
        all_stocks = []
        for page in pages_data:
            if page: all_stocks.extend(page)

        if not all_stocks:
            print("[-] 未能抓取到有效数据。")
            sys.exit(1)

        # --- 步骤 3: Polars 极速清洗 ---
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

        df = df.filter(pl.col("code").str.len_chars() == 6).unique(subset=["symbol"])
        df = df.sort("symbol")

        print(f"[+] 抓取完成！有效个股: {len(df)} 只")
        print(df.head(5))

        df.write_csv("a_share_list.csv")
        print("[*] 结果已成功存入 a_share_list.csv")

if __name__ == "__main__":
    asyncio.run(fetch_all_a_shares())
