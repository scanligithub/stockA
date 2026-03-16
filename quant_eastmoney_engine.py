import asyncio
import time
from typing import List, Dict, Any
import polars as pl
from curl_cffi.requests import AsyncSession

# ==============================================================================
# 核心配置项
# ==============================================================================
CONCURRENCY_LIMIT = 50  # 全局最大并发数（不要超过 100，避免由于本地端口耗尽或引发 WAF 频控）
COMMON_HEADERS = {"Referer": "https://quote.eastmoney.com/"}
# 公用 Token（常年有效）
TOKEN_UT = "bd1d9ddb04089700cf9c27f6f7426281"

# ==============================================================================
# 底层网络请求包装器 (带重试机制)
# ==============================================================================
async def fetch_api(session: AsyncSession, url: str, params: Dict, retries: int = 3) -> Dict:
    """高可用异步网络请求，失败自动重试"""
    for attempt in range(retries):
        try:
            resp = await session.get(url, params=params, headers=COMMON_HEADERS, timeout=10)
            if resp.status_code == 200:
                return resp.json()
        except Exception as e:
            if attempt == retries - 1:
                print(f"[-] 请求彻底失败: {url} | 错误: {e}")
                return {}
            await asyncio.sleep(0.5)  # 退避重试
    return {}

# ==============================================================================
# 需求 1：获取 行业、概念、地域 三大板块列表
# ==============================================================================
async def fetch_all_boards(session: AsyncSession) -> pl.DataFrame:
    print("\n[*] 任务 1: 正在拉取 [行业、概念、地域] 全量板块列表...")
    url = "https://push2.eastmoney.com/api/qt/clist/get"
    
    board_types = {
        "行业": "m:90 t:2",
        "概念": "m:90 t:3",
        "地域": "m:90 t:1"
    }
    
    all_boards = []
    
    for b_type_name, fs_val in board_types.items():
        params = {
            "pn": "1", "pz": "2000", "po": "1", "np": "1",
            "ut": TOKEN_UT, "fltt": "2", "invt": "2", "fid": "f3",
            "fs": fs_val, "fields": "f12,f14"  # f12:代码, f14:名称
        }
        
        data = await fetch_api(session, url, params)
        items = data.get("data", {}).get("diff", [])
        
        for item in items:
            all_boards.append({
                "board_code": item.get("f12"),
                "board_name": item.get("f14"),
                "board_type": b_type_name
            })
            
    df_boards = pl.DataFrame(all_boards)
    print(f"[+] 成功获取全量板块列表！共计 {len(df_boards)} 个板块。")
    return df_boards

# ==============================================================================
# 需求 2：获取全量板块的成份股列表
# ==============================================================================
async def fetch_constituents_for_board(
    session: AsyncSession, board_code: str, sem: asyncio.Semaphore
) -> List[Dict]:
    """获取单个板块的成份股（内部工作协程）"""
    async with sem:
        url = "https://push2.eastmoney.com/api/qt/clist/get"
        params = {
            "pn": "1", "pz": "2000", "po": "1", "np": "1",
            "ut": TOKEN_UT, "fltt": "2", "invt": "2", "fid": "f3",
            "fs": f"b:{board_code}", "fields": "f12,f14"
        }
        data = await fetch_api(session, url, params)
        items = data.get("data", {}).get("diff", [])
        
        if not items:
            return []
            
        return [{
            "board_code": board_code,
            "stock_code": item.get("f12"),
            "stock_name": item.get("f14")
        } for item in items]

async def fetch_all_constituents(session: AsyncSession, board_codes: List[str]) -> pl.DataFrame:
    print(f"\n[*] 任务 2: 正在并发拉取 {len(board_codes)} 个板块的成份股列表...")
    sem = asyncio.Semaphore(CONCURRENCY_LIMIT)
    
    # 构建全部并发任务
    tasks = [fetch_constituents_for_board(session, code, sem) for code in board_codes]
    
    # 进度提示
    results = await asyncio.gather(*tasks)
    
    # 扁平化二维数组
    all_constituents = [item for sublist in results for item in sublist]
    df_constituents = pl.DataFrame(all_constituents)
    
    print(f"[+] 成份股拉取完毕！共计获取 {len(df_constituents)} 条成份股映射关系。")
    return df_constituents

# ==============================================================================
# 需求 3：获取全量板块的历史日 K 线数据
# ==============================================================================
async def fetch_klines_for_board(
    session: AsyncSession, board_code: str, sem: asyncio.Semaphore
) -> List[list]:
    """获取单个板块的日K线（内部工作协程）"""
    async with sem:
        url = "https://push2his.eastmoney.com/api/qt/stock/kline/get"
        params = {
            "secid": f"90.{board_code}", # 板块内部代码以 90 开头
            "ut": TOKEN_UT,
            "fields1": "f1,f2,f3,f4,f5,f6",
            "fields2": "f51,f52,f53,f54,f55,f56,f57", # 日期,开,收,高,低,成交量,成交额
            "klt": "101",   # 101 代表日K线
            "fqt": "1",     # 复权(板块一般无复权，但带上稳妥)
            "end": "20500000",
            "lmt": "100000" # 极大数据量，确保拉取历史全量
        }
        
        data = await fetch_api(session, url, params)
        klines = data.get("data", {}).get("klines", [])
        
        if not klines:
            return []
            
        # 极速内存解析：将 "2024-01-01,10.1,10.2,9.8,10.0,1000,10000" 切片打散
        # 返回原生二维数组，这是给 Polars 喂数据的最高效方式
        parsed_rows = []
        for k in klines:
            # k.split(",") 得到 [日期, 开, 收, 高, 低, 成交量, 成交额]
            parsed_rows.append([board_code, *k.split(",")])
            
        return parsed_rows

async def fetch_all_klines(session: AsyncSession, board_codes: List[str]) -> pl.DataFrame:
    print(f"\n[*] 任务 3: 正在并发拉取 {len(board_codes)} 个板块的全量历史日 K 线...")
    sem = asyncio.Semaphore(CONCURRENCY_LIMIT)
    
    tasks = [fetch_klines_for_board(session, code, sem) for code in board_codes]
    results = await asyncio.gather(*tasks)
    
    # 扁平化数据
    all_kline_rows = [row for sublist in results for row in sublist]
    
    print("[*] 正在利用 Polars 引擎进行极速数据类型转换与构建...")
    
    # 直接基于原生二维数组构建 DataFrame，定义强类型的 Schema
    df_klines = pl.DataFrame(
        all_kline_rows, 
        schema=[
            ("board_code", pl.String),
            ("date", pl.String),
            ("open", pl.Float64),
            ("close", pl.Float64),
            ("high", pl.Float64),
            ("low", pl.Float64),
            ("volume", pl.Int64),
            ("amount", pl.Float64),
        ],
        orient="row"
    )
    
    # 将日期字符串转换为真正的 Date 类型 (可选，方便后续时序分析)
    df_klines = df_klines.with_columns(
        pl.col("date").str.to_date("%Y-%m-%d")
    )
    
    print(f"[+] K线拉取与构建完毕！共计清洗出 {len(df_klines)} 行 K 线数据。")
    return df_klines

# ==============================================================================
# 量化引擎主干调度
# ==============================================================================
async def main():
    start_time = time.time()
    
    # 使用 curl_cffi 维持全局会话，复用底层 TCP 与 TLS 状态，速度起飞
    async with AsyncSession(impersonate="chrome120") as session:
        
        # 1. 获取全量板块列表
        df_boards = await fetch_all_boards(session)
        board_codes = df_boards["board_code"].to_list()
        
        # 2. 并发获取所有板块的成份股
        df_constituents = await fetch_all_constituents(session, board_codes)
        
        # 3. 并发获取所有板块的历史全量日 K 线
        df_klines = await fetch_all_klines(session, board_codes)
        
    # ==========================================================================
    # 数据落地：利用 Polars 极速导出 Parquet 格式
    # ==========================================================================
    print("\n[*] 正在将数据持久化为量化标准的 Parquet 文件 (零拷贝落地)...")
    
    df_boards.write_parquet("quant_boards_list.parquet")
    df_constituents.write_parquet("quant_boards_constituents.parquet")
    df_klines.write_parquet("quant_boards_klines.parquet")
    
    print("[+] 全部任务完成！")
    print(f"[+] 总耗时: {time.time() - start_time:.2f} 秒")
    
    # 打印一下数据样例供检查
    print("\n--- 核心数据样例 (K线表头部) ---")
    print(df_klines.head(5))

if __name__ == "__main__":
    # 规避 Windows 下 asyncio 的 Proactor 报错问题
    import sys
    if sys.platform == 'win32':
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
        
    asyncio.run(main())
