import asyncio
import time
import random
import sys
from typing import List, Dict, Any
import polars as pl
from curl_cffi.requests import AsyncSession

# ==============================================================================
# 潜行模式配置
# ==============================================================================
CONCURRENCY_LIMIT = 10  # 进一步降低并发，提升稳定性
TOKEN_UT = "bd1d9ddb04089700cf9c27f6f7426281"

STEALTH_HEADERS = {
    "Referer": "https://quote.eastmoney.com/",
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept": "application/json, text/javascript, */*; q=0.01",
    "Accept-Language": "zh-CN,zh;q=0.9",
}

# ==============================================================================
# 强化版安全请求器
# ==============================================================================
async def fetch_api(session: AsyncSession, url: str, params: Dict, sem: asyncio.Semaphore, retries: int = 5) -> Dict:
    async with sem:
        for attempt in range(retries):
            try:
                await asyncio.sleep(random.uniform(0.2, 0.5))
                resp = await session.get(url, params=params, headers=STEALTH_HEADERS, timeout=15, impersonate="chrome120")
                
                if resp.status_code == 200:
                    try:
                        res_json = resp.json()
                        # 核心防御：确保返回的是字典
                        return res_json if isinstance(res_json, dict) else {}
                    except:
                        return {}
                elif resp.status_code == 403:
                    await asyncio.sleep(10)
            except Exception:
                await asyncio.sleep(2 * (attempt + 1))
        return {}

# ==============================================================================
# 逻辑函数：带类型防御
# ==============================================================================
async def fetch_all_boards(session: AsyncSession, sem: asyncio.Semaphore) -> pl.DataFrame:
    print("\n[*] 任务 1: 拉取板块列表...")
    url = "https://push2.eastmoney.com/api/qt/clist/get"
    board_types = {"行业": "m:90 t:2", "概念": "m:90 t:3", "地域": "m:90 t:1"}
    all_boards = []
    
    for name, fs in board_types.items():
        params = {"pn": "1", "pz": "2000", "ut": TOKEN_UT, "fs": fs, "fields": "f12,f14"}
        data = await fetch_api(session, url, params, sem)
        
        # 深度防御：检查每一级数据结构
        diff = data.get("data", {}).get("diff", [])
        if isinstance(diff, list):
            for item in diff:
                # 关键修复：只有当 item 是字典时才进行解析
                if isinstance(item, dict):
                    all_boards.append({
                        "board_code": item.get("f12"),
                        "board_name": item.get("f14"),
                        "board_type": name
                    })
    return pl.DataFrame(all_boards)

async def fetch_constituents_task(session: AsyncSession, board_code: str, sem: asyncio.Semaphore) -> List:
    url = "https://push2.eastmoney.com/api/qt/clist/get"
    params = {"pn": "1", "pz": "2000", "ut": TOKEN_UT, "fs": f"b:{board_code}", "fields": "f12,f14"}
    data = await fetch_api(session, url, params, sem)
    
    results = []
    diff = data.get("data", {}).get("diff", [])
    if isinstance(diff, list):
        for x in diff:
            if isinstance(x, dict):
                results.append({
                    "board_code": board_code,
                    "stock_code": x.get("f12"),
                    "stock_name": x.get("f14")
                })
    return results

async def fetch_klines_task(session: AsyncSession, board_code: str, sem: asyncio.Semaphore) -> List:
    url = "https://push2his.eastmoney.com/api/qt/stock/kline/get"
    params = {
        "secid": f"90.{board_code}", "ut": TOKEN_UT, "klt": "101", "fqt": "1", "lmt": "100000",
        "fields2": "f51,f52,f53,f54,f55,f56,f57"
    }
    data = await fetch_api(session, url, params, sem)
    
    rows = []
    klines = data.get("data", {}).get("klines", [])
    if isinstance(klines, list):
        for k in klines:
            if isinstance(k, str): # K线数据通常返回字符串列表
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
            print("[-] 未能获取有效板块列表。")
            return
            
        codes = df_boards["board_code"].to_list()
        
        # 2. 成份股
        print(f"[*] 任务 2: 正在拉取 {len(codes)} 个板块的成份股...")
        const_results = await asyncio.gather(*(fetch_constituents_task(session, c, sem) for c in codes))
        
        flat_const = [item for sub in const_results for item in sub if item]
        df_const = pl.DataFrame(flat_const) if flat_const else pl.DataFrame()
        
        # 3. K线数据
        print(f"[*] 任务 3: 正在拉取 {len(codes)} 个板块的历史 K 线...")
        kline_results = await asyncio.gather(*(fetch_klines_task(session, c, sem) for c in codes))
        
        all_k_rows = [row for sub in kline_results for row in sub if len(row) == 8]
        
        if all_k_rows:
            df_klines = pl.DataFrame(
                all_k_rows, 
                schema=[
                    ("board_code", pl.String), ("date", pl.String), ("open", pl.Float64), 
                    ("close", pl.Float64), ("high", pl.Float64), ("low", pl.Float64), 
                    ("volume", pl.Int64), ("amount", pl.Float64)
                ],
                orient="row"
            ).with_columns(pl.col("date").str.to_date("%Y-%m-%d"))
        else:
            df_klines = pl.DataFrame()

    # 安全落地
    if not df_boards.is_empty(): df_boards.write_parquet("quant_boards_list.parquet")
    if not df_const.is_empty(): df_const.write_parquet("quant_boards_constituents.parquet")
    if not df_klines.is_empty(): df_klines.write_parquet("quant_boards_klines.parquet")
    
    print(f"\n[+] 任务完成！耗时: {time.time() - start_time:.2f}s")
    print(f"[*] K线总行数: {len(df_klines) if not df_klines.is_empty() else 0}")

if __name__ == "__main__":
    if sys.platform == 'win32':
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    asyncio.run(main())
