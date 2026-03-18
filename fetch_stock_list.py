import asyncio
import aiohttp
import polars as pl
import sys
import math
from datetime import datetime

# 限制并发数：同时最多只允许 10 个请求在跑
sem = asyncio.Semaphore(10)

async def fetch_page(session, page_no, pz, url, params, retries=3):
    """带重试机制和并发控制的分页抓取"""
    async with sem:  # 使用信号量控制并发
        page_params = params.copy()
        page_params["pn"] = str(page_no)
        page_params["pz"] = str(pz)
        
        for attempt in range(retries):
            try:
                # 增加超时时间到 30s
                async with session.get(url, params=page_params, timeout=30) as resp:
                    if resp.status == 200:
                        data = await resp.json()
                        return data.get("data", {}).get("diff", [])
                    else:
                        print(f"[!] 页面 {page_no} 请求异常: HTTP {resp.status}")
            except Exception as e:
                if attempt == retries - 1:
                    print(f"[-] 页面 {page_no} 抓取最终失败: {e}")
                else:
                    await asyncio.sleep(1) # 失败重试前等待一秒
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
    
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
        "Referer": "https://quote.eastmoney.com/"
    }

    # TCPConnector 限制：防止底层连接数过多
    conn = aiohttp.TCPConnector(limit=20)
    async with aiohttp.ClientSession(headers=headers, connector=conn) as session:
        # 1. 获取总数
        print(f"[*] 正在查询总数... {datetime.now().strftime('%H:%M:%S')}")
        async with session.get(API_URL, params={**base_params, "pn": "1", "pz": "1"}, timeout=15) as resp:
            data = await resp.json()
            total_count = data.get("data", {}).get("total", 0)
            if total_count == 0:
                print("[-] 无法获取数据总数")
                return

        print(f"[+] 检测到全量 A 股共计: {total_count} 只")
        
        # 2. 计算并准备并发任务
        page_size = 100
        total_pages = math.ceil(total_count / page_size)
        print(f"[*] 计划执行 {total_pages} 个分页抓取任务 (限流并发)...")

        tasks = [fetch_page(session, i, page_size, API_URL, base_params) for i in range(1, total_pages + 1)]
        
        # 3. 执行任务
        pages_data = await asyncio.gather(*tasks)
        
        # 4. 数据汇总与清洗
        all_stocks = []
        for page in pages_data:
            if page: all_stocks.extend(page)

        if not all_stocks:
            print("[-] 未能抓取到任何有效个股数据。")
            sys.exit(1)

        df = pl.DataFrame(all_stocks)
        df = df.rename({
            "f12": "code", "f14": "name", "f13": "market_id",
            "f2": "price", "f3": "pct_chg"
        })

        # 转换 Symbol 后缀
        df = df.with_columns([
            pl.when(pl.col("code").str.starts_with("6")).then(pl.col("code") + ".SH")
              .when(pl.col("code").str.starts_with("0")).then(pl.col("code") + ".SZ")
              .when(pl.col("code").str.starts_with("3")).then(pl.col("code") + ".SZ")
              .when(pl.col("code").str.starts_with("8")).then(pl.col("code") + ".BJ")
              .when(pl.col("code").str.starts_with("4")).then(pl.col("code") + ".BJ")
              .otherwise(pl.col("code"))
              .alias("symbol")
        ])

        # 处理价格与涨幅
        df = df.with_columns([
            pl.col("price").cast(pl.Float64, strict=False),
            pl.col("pct_chg").cast(pl.Float64, strict=False)
        ])

        # 最终过滤与排序
        df = df.filter(pl.col("code").str.len_chars() == 6).unique(subset=["symbol"])
        df = df.sort("symbol")

        print(f"[+] 抓取完成！有效个股: {len(df)} 只")
        print(df.head(10))

        df.write_csv("a_share_list.csv")
        print("[*] 结果已成功存入 a_share_list.csv")

if __name__ == "__main__":
    asyncio.run(fetch_all_a_shares())
