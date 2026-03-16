import asyncio
import time
import random
import sys
from typing import List, Dict, Any
import polars as pl
from curl_cffi.requests import AsyncSession

# ==============================================================================
# 潜行模式配置 - 针对 GitHub Actions 的超高稳健性设置
# ==============================================================================
CONCURRENCY_LIMIT = 1  # 极低并发，防止 GitHub IP 被东财防火墙直接 RST
TOKEN_UT = "bd1d9ddb04089700cf9c27f6f7426281"

STEALTH_HEADERS = {
    "Referer": "https://quote.eastmoney.com/",
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Accept": "application/json, text/javascript, */*; q=0.01",
    "Accept-Language": "zh-CN,zh;q=0.9",
    "Cache-Control": "no-cache",
}

# ==============================================================================
# 强化版安全请求器 (防 RST, 防脏数据)
# ==============================================================================
async def fetch_api(session: AsyncSession, url: str, params: Dict, sem: asyncio.Semaphore, retries: int = 5) -> Dict:
    async with sem:
        for attempt in range(retries):
            try:
                # 每次请求随机休眠 0.5-1.5 秒，模拟真人翻页速度
                await asyncio.sleep(random.uniform(0.5, 1.5))
                
                resp = await session.get(url, params=params, headers=STEALTH_HEADERS, timeout=20, impersonate="chrome120")
                
                if resp.status_code == 200:
                    try:
                        res_json = resp.json()
                        if isinstance(res_json, dict):
                            return res_json
                        else:
                            print(f"[!] 收到非字典格式响应: {str(res_json)[:100]}")
                    except Exception as je:
                        print(f"[!] JSON 解析失败: {je} | 响应内容前100字: {resp.text[:100]}")
                elif resp.status_code == 403:
                    print(f"[!] 403 Forbidden - IP 被限速，等待 20 秒...")
                    await asyncio.sleep(20)
            except Exception as e:
                wait_time = (attempt + 1) * 5
                print(f"[!] 连接异常 ({e})，将在 {wait_time}s 后重试...")
                await asyncio.sleep(wait_time)
        return {}

# ==============================================================================
# 任务函数 (加入绝对防御逻辑)
# ==============================================================================
async def fetch_all_boards(session: AsyncSession, sem: asyncio.Semaphore) -> pl.DataFrame:
    print("\n[*] 任务 1: 开始拉取板块列表...")
    url = "https://push2.eastmoney.com/api/qt/clist/get"
    board_types = {"行业": "m:90 t:2", "概念": "m:90 t:3", "地域": "m:90 t:1"}
    all_boards = []
    
    for name, fs in board_types.items():
        params = {"pn": "1", "pz": "2000", "ut": TOKEN_UT, "fs": fs, "fields": "f12,f14"}
        data = await fetch_api(session, url, params, sem)
        
        # 绝对防御：确保每一层都是预期的容器类型
        diff = data.get("data", {}).get("diff", []) if isinstance(data, dict) else []
        
        if isinstance(diff, list):
            for item in diff:
                try:
                    # 只有当 item 明确为 dict 时才调用 .get
                    if isinstance(item, dict):
                        code = item.get("f12")
                        board_name = item.get("f14")
                        if code and board_name:
                            all_boards.append({
                                "board_code": str(code),
                                "board_name": str(board_name),
                                "board_type": name
                            })
                except Exception:
                    continue # 遇到脏数据直接跳过，保证主流程不崩
        print(f"    - {name}板块拉取完成，当前累计: {len(all_boards)}")
        
    return pl.DataFrame(all_boards)

async def fetch_constituents_task(session: AsyncSession, board_code: str, sem: asyncio.Semaphore) -> List:
    url = "https://push2.eastmoney.com/api/qt/clist/get"
    params = {"pn": "1", "pz": "2000", "ut": TOKEN_UT, "fs": f"b:{board_code}", "fields": "f12,f14"}
    data = await fetch_api(session, url, params, sem)
    
    results = []
    diff = data.get("data", {}).get("diff", []) if isinstance(data, dict) else []
    
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
    klines = data.get("data", {}).get("klines", []) if isinstance(data, dict) else []
    
    if isinstance(klines, list):
        for k in klines:
            if isinstance(k, str):
                parts = k.split(",")
                if len(parts) == 7: # 校验 K 线字段数量
                    rows.append([board_code, *parts])
    return rows

# ==============================================================================
# 主调度系统
# ==============================================================================
async def main():
    start_time = time.time()
    sem = asyncio.Semaphore(CONCURRENCY_LIMIT)
    
    async with AsyncSession() as session:
        # 1. 获取板块列表
        df_boards = await fetch_all_boards(session, sem)
        if df_boards.is_empty():
            print("[-] 错误: 未能获取到任何板块数据，请检查网络或 Token。")
            return
            
        codes = df_boards["board_code"].to_list()
        
        # 2. 并发成份股
        print(f"\n[*] 任务 2: 并发获取 {len(codes)} 个板块的成份股...")
        const_results = await asyncio.gather(*(fetch_constituents_task(session, c, sem) for c in codes))
        flat_const = [item for sub in const_results for item in sub if isinstance(item, dict)]
        df_const = pl.DataFrame(flat_const) if flat_const else pl.DataFrame()
        
        # 3. 并发 K 线
        print(f"\n[*] 任务 3: 并发获取 {len(codes)} 个板块的历史 K 线...")
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

    # 安全存储
    if not df_boards.is_empty(): df_boards.write_parquet("quant_boards_list.parquet")
    if not df_const.is_empty(): df_const.write_parquet("quant_boards_constituents.parquet")
    if not df_klines.is_empty(): df_klines.write_parquet("quant_boards_klines.parquet")
    
    print(f"\n[+] 采集圆满完成！")
    print(f"[*] 统计: 板块 {len(df_boards)} | 成份股映射 {len(df_const)} | K线总行数 {len(df_klines)}")
    print(f"[*] 总耗时: {time.time() - start_time:.2f} 秒")

if __name__ == "__main__":
    if sys.platform == 'win32':
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    asyncio.run(main())
