import asyncio
import time
import random
import sys
from typing import List, Dict
import polars as pl
from curl_cffi.requests import AsyncSession

# ==============================================================================
# 潜行模式配置
# ==============================================================================
CONCURRENCY_LIMIT = 15  # 降低并发，确保在 GitHub Actions 的高频拦截下生存
TOKEN_UT = "bd1d9ddb04089700cf9c27f6f7426281"

# 补全更真实的浏览器 Headers
STEALTH_HEADERS = {
    "Referer": "https://quote.eastmoney.com/",
    "Accept": "*/*",
    "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
    "Accept-Encoding": "gzip, deflate, br",
    "Connection": "keep-alive",
}

# ==============================================================================
# 强化版 API 请求器
# ==============================================================================
async def fetch_api(session: AsyncSession, url: str, params: Dict, sem: asyncio.Semaphore, retries: int = 5) -> Dict:
    """带信号量控制和指数退避重试的请求包装器"""
    async with sem:
        for attempt in range(retries):
            try:
                # 随机微小延迟 (100ms - 300ms)，打破并发指纹
                await asyncio.sleep(random.uniform(0.1, 0.3))
                
                resp = await session.get(
                    url, 
                    params=params, 
                    headers=STEALTH_HEADERS, 
                    timeout=15,
                    # 关键：impersonate 会自动处理大部分特征，我们手动补齐头信息
                    impersonate="chrome120" 
                )
                
                if resp.status_code == 200:
                    return resp.json()
                elif resp.status_code == 403:
                    print(f"[!] 被暂时封禁 (403)，正在休眠后重试...")
                    await asyncio.sleep(5 * (attempt + 1))
            except Exception as e:
                # 捕捉 Connection closed abruptly (curl 56)
                wait_time = (attempt + 1) * 2
                if attempt < retries - 1:
                    print(f"[!] 连接波动 ({e}), {wait_time}s 后进行第 {attempt+2} 次尝试...")
                    await asyncio.sleep(wait_time)
                else:
                    print(f"[-] 彻底失败: {url}")
        return {}

# ==============================================================================
# 逻辑函数：任务 1/2/3
# ==============================================================================
async def fetch_all_boards(session: AsyncSession, sem: asyncio.Semaphore) -> pl.DataFrame:
    print("\n[*] 任务 1: 拉取板块列表...")
    url = "https://push2.eastmoney.com/api/qt/clist/get"
    board_types = {"行业": "m:90 t:2", "概念": "m:90 t:3", "地域": "m:90 t:1"}
    all_boards = []
    for name, fs in board_types.items():
        params = {"pn": "1", "pz": "2000", "ut": TOKEN_UT, "fs": fs, "fields": "f12,f14"}
        data = await fetch_api(session, url, params, sem)
        for item in data.get("data", {}).get("diff", []):
            all_boards.append({"board_code": item.get("f12"), "board_name": item.get("f14"), "board_type": name})
    return pl.DataFrame(all_boards)

async def fetch_constituents_task(session: AsyncSession, board_code: str, sem: asyncio.Semaphore) -> List:
    url = "https://push2.eastmoney.com/api/qt/clist/get"
    params = {"pn": "1", "pz": "2000", "ut": TOKEN_UT, "fs": f"b:{board_code}", "fields": "f12,f14"}
    data = await fetch_api(session, url, params, sem)
    return [{"board_code": board_code, "stock_code": x.get("f12"), "stock_name": x.get("f14")} for x in data.get("data", {}).get("diff", [])]

async def fetch_klines_task(session: AsyncSession, board_code: str, sem: asyncio.Semaphore) -> List:
    url = "https://push2his.eastmoney.com/api/qt/stock/kline/get"
    params = {
        "secid": f"90.{board_code}", "ut": TOKEN_UT, "klt": "101", "fqt": "1", "lmt": "100000",
        "fields2": "f51,f52,f53,f54,f55,f56,f57"
    }
    data = await fetch_api(session, url, params, sem)
    rows = []
    for k in data.get("data", {}).get("klines", []):
        rows.append([board_code, *k.split(",")])
    return rows

# ==============================================================================
# 主调度
# ==============================================================================
async def main():
    start_time = time.time()
    sem = asyncio.Semaphore(CONCURRENCY_LIMIT)
    
    async with AsyncSession() as session:
        # 1. 板块列表
        df_boards = await fetch_all_boards(session, sem)
        if df_boards.is_empty():
            print("[-] 未能获取板块列表，程序退出。")
            return
            
        codes = df_boards["board_code"].to_list()
        
        # 2. 成份股 (分批处理，防止 gather 任务数过多导致系统压力)
        print(f"[*] 任务 2: 正在拉取 {len(codes)} 个板块的成份股...")
        const_results = await asyncio.gather(*(fetch_constituents_task(session, c, sem) for c in codes))
        df_const = pl.DataFrame([item for sub in const_results for item in sub])
        
        # 3. K线数据
        print(f"[*] 任务 3: 正在拉取 {len(codes)} 个板块的历史 K 线...")
        kline_results = await asyncio.gather(*(fetch_klines_task(session, c, sem) for c in codes))
        all_k_rows = [row for sub in kline_results for row in sub]
        
        df_klines = pl.DataFrame(
            all_k_rows, 
            schema=[("board_code", pl.String), ("date", pl.String), ("open", pl.Float64), ("close", pl.Float64), ("high", pl.Float64), ("low", pl.Float64), ("volume", pl.Int64), ("amount", pl.Float64)],
            orient="row"
        ).with_columns(pl.col("date").str.to_date("%Y-%m-%d"))

    # 保存数据
    df_boards.write_parquet("quant_boards_list.parquet")
    df_const.write_parquet("quant_boards_constituents.parquet")
    df_klines.write_parquet("quant_boards_klines.parquet")
    
    print(f"\n[+] 任务全部成功！总耗时: {time.time() - start_time:.2f}s")
    print(f"[*] K线总行数: {len(df_klines)}")

if __name__ == "__main__":
    if sys.platform == 'win32':
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    asyncio.run(main())
