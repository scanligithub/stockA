import asyncio
import aiohttp
import polars as pl
import sys
import math
import time
import socket
import random
from datetime import datetime

# 保守并发：限制为 1，串行执行
sem = asyncio.Semaphore(1)

# 轮换 User-Agent 列表
USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.2 Safari/605.1.15",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:121.0) Gecko/20100101 Firefox/121.0",
]

# 模拟浏览器请求的完整请求头
def get_headers():
    return {
        "User-Agent": random.choice(USER_AGENTS),
        "Referer": "https://quote.eastmoney.com/center/gridlist.html",
        "Accept": "application/json, text/plain, */*",
        "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
        "Accept-Encoding": "gzip, deflate, br",
        "Connection": "keep-alive",
        "Sec-Ch-Ua": '"Not_A Brand";v="8", "Chromium";v="120"',
        "Sec-Ch-Ua-Mobile": "?0",
        "Sec-Ch-Ua-Platform": '"Windows"',
        "Sec-Fetch-Dest": "empty",
        "Sec-Fetch-Mode": "cors",
        "Sec-Fetch-Site": "cross-site",
    }

async def fetch_page(page_no, pz, url, params, retries=5):
    """每次请求都创建新会话，使用随机 User-Agent"""
    async with sem:
        page_params = params.copy()
        page_params["pn"] = str(page_no)
        page_params["pz"] = str(pz)
        page_params["_"] = str(int(time.time() * 1000))

        headers = get_headers()

        for attempt in range(retries):
            try:
                # 每次请求前随机休眠（更长）
                await asyncio.sleep(random.uniform(3.0, 6.0))

                # 创建新连接池，避免复用
                conn = aiohttp.TCPConnector(limit=1, family=socket.AF_INET, ssl=False)
                async with aiohttp.ClientSession(headers=headers, connector=conn) as new_session:
                    async with new_session.get(url, params=page_params, timeout=60) as resp:
                        if resp.status == 200:
                            data = await resp.json()
                            raw_list = data.get("data", {}).get("diff", [])

                            # --- 数据预清洗 ---
                            clean_list = []
                            for item in raw_list:
                                processed = {}
                                for k, v in item.items():
                                    processed[k] = None if v == "-" else v
                                clean_list.append(processed)
                            return clean_list

                        elif resp.status == 403:
                            print(f"[!] 403 被拒，深度休眠 60s...")
                            await asyncio.sleep(60)
                            continue

                        elif resp.status == 503:
                            print(f"[!] 503 服务不可用，深度休眠 90s...")
                            await asyncio.sleep(90)
                            continue

                # 其他状态码
                wait_time = (attempt + 1) * 15
                print(f"[!] 状态码 {resp.status}，第 {attempt+1} 次重试，等待 {wait_time}s...")
                await asyncio.sleep(wait_time)

            except (aiohttp.ServerDisconnectedError, asyncio.TimeoutError, aiohttp.ClientError) as e:
                wait_time = (attempt + 1) * 20
                print(f"[!] 第 {attempt+1} 次重试，等待 {wait_time}s... ({type(e).__name__})")
                await asyncio.sleep(wait_time)
            except Exception as e:
                print(f"[-] 页面 {page_no} 未知错误：{type(e).__name__}: {e}")
                await asyncio.sleep(5)
                return []
        return []

async def fetch_all_a_shares():
    API_URL = "https://push2.eastmoney.com/api/qt/clist/get"
    base_params = {
        "po": "1", "np": "1", "ut": "bd1d9ddb04089700cf9c27f6f7426281",
        "fltt": "2", "invt": "2", "fid": "f3",
        "fs": "m:0 t:6,m:1 t:2,m:1 t:23,m:0 t:80,m:1 t:81,m:1 t:82",
        "fields": "f12,f14,f13,f2,f3"
    }

    # 1. 获取总数（带更长重试和休眠）
    print(f"[*] 正在查询总数... {datetime.now().strftime('%H:%M:%S')}")
    print("[*] 如果持续失败，可能是 IP 被临时封禁，请等待几分钟后重试")

    total_count = 0
    for retry in range(8):
        try:
            # 每次重试前休眠更久
            if retry > 0:
                wait = retry * 30
                print(f"[*] 第 {retry} 次重试，等待 {wait}s...")
                await asyncio.sleep(wait)

            test_p = {**base_params, "pn": "1", "pz": "1", "_": str(int(time.time() * 1000))}
            headers = get_headers()

            conn_test = aiohttp.TCPConnector(limit=1, family=socket.AF_INET, ssl=False)
            async with aiohttp.ClientSession(headers=headers, connector=conn_test) as test_session:
                async with test_session.get(API_URL, params=test_p, timeout=30) as resp:
                    if resp.status == 200:
                        res_json = await resp.json()
                        total_count = res_json.get("data", {}).get("total", 0)
                        if total_count > 0:
                            break
                    else:
                        print(f"[-] 状态码：{resp.status}")
            await conn_test.close()
        except Exception as e:
            print(f"[-] 获取总数失败：{type(e).__name__}: {e}")
            await asyncio.sleep(10)

    if total_count == 0:
        print("[-] 无法连接到行情网关，退出")
        print("[!] 可能是 IP 被临时封禁，请等待 10-30 分钟后重试")
        sys.exit(1)

    print(f"[+] 目标总数：{total_count} 只")

    # 2. 分页串行抓取
    page_size = 100
    total_pages = math.ceil(total_count / page_size)
    print(f"[*] 启动 {total_pages} 个串行抓取任务...")

    pages_data = []
    for i in range(1, total_pages + 1):
        print(f"[*] 正在获取第 {i}/{total_pages} 页...")
        page_data = await fetch_page(i, page_size, API_URL, base_params)
        if page_data:
            pages_data.append(page_data)
            print(f"[+] 第 {i} 页成功，获取 {len(page_data)} 条")
        else:
            print(f"[!] 第 {i} 页获取失败，跳过")

    # 3. 汇总数据
    all_stocks = []
    for page in pages_data:
        if page:
            all_stocks.extend(page)

    print(f"[*] 原始抓取完成，成功获取 {len(all_stocks)} 条记录")
    if not all_stocks:
        print("[-] 没有获取到任何数据，退出")
        sys.exit(1)

    # 4. Polars 处理
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

    # 强制转换类型
    df = df.with_columns([
        pl.col("price").cast(pl.Float64, strict=False),
        pl.col("pct_chg").cast(pl.Float64, strict=False)
    ])

    df = df.filter(pl.col("code").is_not_null()).unique(subset=["symbol"])
    df = df.sort("symbol")

    print(f"[+] 抓取成功！有效个股：{len(df)} 只")
    print(df.head(5))

    df.write_csv("a_share_list.csv")
    print("[*] 文件 a_share_list.csv 已就绪")

if __name__ == "__main__":
    asyncio.run(fetch_all_a_shares())
