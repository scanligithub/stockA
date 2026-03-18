import asyncio
import aiohttp
import polars as pl
import sys
from datetime import datetime

async def fetch_all_a_shares():
    # 东方财富全量 A 股接口 (包含沪深京)
    # fs 参数定义：m:0 t:6 (深主板), m:1 t:2 (沪主板), m:1 t:23 (科创板), 
    #              m:0 t:80 (创业板), m:1 t:81 (北交所), m:1 t:82 (北交所)
    API_URL = "https://push2.eastmoney.com/api/qt/clist/get"
    params = {
        "pn": "1",
        "pz": "10000",  # 一次性请求 10000 条，足以覆盖全量 A 股
        "po": "1",
        "np": "1",
        "ut": "bd1d9ddb04089700cf9c27f6f7426281",
        "fltt": "2",
        "invt": "2",
        "fid": "f3",
        "fs": "m:0 t:6,m:1 t:2,m:1 t:23,m:0 t:80,m:1 t:81,m:1 t:82",
        "fields": "f12,f14,f13,f2,f3"  # 代码, 名称, 市场标识, 最新价, 涨跌幅
    }
    
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
        "Referer": "https://quote.eastmoney.com/center/gridlist.html"
    }

    print(f"[*] 开始获取全量 A 股目录... 时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

    async with aiohttp.ClientSession() as session:
        try:
            async with session.get(API_URL, params=params, headers=headers, timeout=20) as resp:
                if resp.status != 200:
                    print(f"[-] 接口响应异常，状态码: {resp.status}")
                    sys.exit(1)
                
                content = await resp.json()
                stocks = content.get("data", {}).get("diff", [])
                
                if not stocks:
                    print("[-] 未获取到有效数据。")
                    sys.exit(1)

                # --- 使用 Polars 进行量化预处理 ---
                # 1. 载入原始数据
                df = pl.DataFrame(stocks)
                
                # 2. 重命名
                df = df.rename({
                    "f12": "code",
                    "f14": "name",
                    "f13": "market_id",
                    "f2": "price",
                    "f3": "pct_chg"
                })

                # 3. 核心逻辑：标准化 symbol (后缀 .SH/.SZ/.BJ)
                # 规则：6开头.SH, 0或3开头.SZ, 4或8开头.BJ
                df = df.with_columns([
                    pl.when(pl.col("code").str.starts_with("6"))
                    .then(pl.col("code") + ".SH")
                    .when(pl.col("code").str.starts_with("0"))
                    .then(pl.col("code") + ".SZ")
                    .when(pl.col("code").str.starts_with("3"))
                    .then(pl.col("code") + ".SZ")
                    .when(pl.col("code").str.starts_with("4"))
                    .then(pl.col("code") + ".BJ")
                    .when(pl.col("code").str.starts_with("8"))
                    .then(pl.col("code") + ".BJ")
                    .otherwise(pl.col("code"))
                    .alias("symbol")
                ])

                # 4. 价格单位修正 (东财返回通常是分，需除以 100)
                df = df.with_columns([
                    pl.col("price").cast(pl.Float64) / 100,
                    pl.col("pct_chg").cast(pl.Float64)
                ])

                print(f"[+] 抓取成功！当前 A 股总计: {len(df)} 只")
                
                # 打印前 10 行预览
                print("\n[数据预览 - Top 10]")
                print(df.head(10))

                # 保存为 CSV (供 Action Artifacts 下载)
                df.write_csv("a_share_list.csv")
                print("\n[+] 数据已保存至 a_share_list.csv")

        except Exception as e:
            print(f"[-] 运行出错: {e}")
            sys.exit(1)

if __name__ == "__main__":
    asyncio.run(fetch_all_a_shares())
