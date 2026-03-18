import asyncio
import aiohttp
import polars as pl
import sys
import math
from datetime import datetime

async def fetch_page(session, page_no, pz, url, params):
    """抓取单个分页的函数"""
    page_params = params.copy()
    page_params["pn"] = str(page_no)
    page_params["pz"] = str(pz)
    
    async with session.get(url, params=page_params, timeout=10) as resp:
        if resp.status == 200:
            data = await resp.json()
            return data.get("data", {}).get("diff", [])
        return []

async def fetch_all_a_shares():
    API_URL = "https://push2.eastmoney.com/api/qt/clist/get"
    # 基础参数
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

    async with aiohttp.ClientSession(headers=headers) as session:
        # 第一步：获取总数
        print("[*] 正在查询股票总数...")
        async with session.get(API_URL, params={**base_params, "pn": "1", "pz": "1"}, timeout=10) as resp:
            data = await resp.json()
            total_count = data.get("data", {}).get("total", 0)
            if total_count == 0:
                print("[-] 无法获取总数，退出")
                return

        print(f"[+] 检测到全量 A 股共计: {total_count} 只")
        
        # 第二步：计算分页 (每页 100 条)
        page_size = 100
        total_pages = math.ceil(total_count / page_size)
        print(f"[*] 计划开启 {total_pages} 个并发任务进行全量抓取...")

        # 第三步：并发抓取所有页面
        tasks = [fetch_page(session, i, page_size, API_URL, base_params) for i in range(1, total_pages + 1)]
        pages_data = await asyncio.gather(*tasks)
        
        # 合并所有数据
        all_stocks = []
        for page in pages_data:
            all_stocks.extend(page)

        # 第四步：Polars 极速清洗
        df = pl.DataFrame(all_stocks)
        df = df.rename({
            "f12": "code", "f14": "name", "f13": "market_id",
            "f2": "price", "f3": "pct_chg"
        })

        # 标准化代码后缀
        df = df.with_columns([
            pl.when(pl.col("code").str.starts_with("6")).then(pl.col("code") + ".SH")
              .when(pl.col("code").str.starts_with("0")).then(pl.col("code") + ".SZ")
              .when(pl.col("code").str.starts_with("3")).then(pl.col("code") + ".SZ")
              .when(pl.col("code").str.starts_with("8")).then(pl.col("code") + ".BJ")
              .when(pl.col("code").str.starts_with("4")).then(pl.col("code") + ".BJ")
              .otherwise(pl.col("code"))
              .alias("symbol")
        ])

        # 修正价格精度 (直接使用 f2，不再除以 100)
        df = df.with_columns([
            pl.col("price").cast(pl.Float64),
            pl.col("pct_chg").cast(pl.Float64)
        ])

        # 过滤无效数据并去重
        df = df.filter(pl.col("code").str.len_chars() == 6).unique(subset=["symbol"])

        print(f"[+] 全量抓取完成！最终有效个股: {len(df)} 只")
        print(df.sort("pct_chg", descending=True).head(10)) # 查看涨幅榜前10

        df.write_csv("a_share_list.csv")
        print("[*] 数据已成功写入 a_share_list.csv")

if __name__ == "__main__":
    asyncio.run(fetch_all_a_shares())
