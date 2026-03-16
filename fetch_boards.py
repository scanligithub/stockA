import asyncio
import json
import math
import sys
import polars as pl
from curl_cffi.requests import AsyncSession

# 信号量控制：同时最多 10 个并发请求，既保证速度又不会被识别为恶意攻击
SEMAPHORE = asyncio.Semaphore(10)

async def fetch_page(session, page_num, page_size=100):
    """
    抓取指定页码的数据
    """
    API_URL = "https://push2.eastmoney.com/api/qt/clist/get"
    params = {
        "pn": str(page_num),
        "pz": str(page_size),
        "po": "1",
        "np": "1",
        "ut": "bd1d9ddb04089700cf9c27f6f7426281",
        "fltt": "2",
        "invt": "2",
        "fid": "f3",
        "fs": "m:90 t:2", # 行业板块标识
        "fields": "f12,f14,f2,f3,f62,f184" # 增加 f62:主力净流入, f184:持仓人数等字段（可选）
    }
    
    headers = {"Referer": "https://quote.eastmoney.com/"}
    
    async with SEMAPHORE:
        try:
            response = await session.get(API_URL, params=params, headers=headers, timeout=15)
            if response.status_code == 200:
                return response.json().get("data", {}).get("diff", [])
            return []
        except Exception as e:
            print(f"[!] 第 {page_num} 页抓取失败: {e}")
            return []

async def fetch_all_industry_boards():
    """
    主控程序：全量抓取行业板块并用 Polars 处理
    """
    PAGE_SIZE = 100
    
    async with AsyncSession(impersonate="chrome120") as session:
        print("[*] 正在探测总板块数量...")
        
        # 1. 探测第一页，获取总数
        first_page_data = await fetch_page(session, 1, PAGE_SIZE)
        
        # 假设我们通过某种方式拿到了总数，或者从第一页返回的结构中提取
        # 重新请求一次小的，专门为了拿 total
        init_res = await session.get(
            "https://push2.eastmoney.com/api/qt/clist/get",
            params={"pn": "1", "pz": "1", "fields": "f12", "fs": "m:90 t:2"},
            headers={"Referer": "https://quote.eastmoney.com/"}
        )
        total_count = init_res.json().get("data", {}).get("total", 0)
        
        if total_count == 0:
            print("[-] 无法获取板块总数，退出。")
            return

        total_pages = math.ceil(total_count / PAGE_SIZE)
        print(f"[+] 探测到全量行业板块共 {total_count} 个，分为 {total_pages} 页抓取。")

        # 2. 构建并发任务清单 (从第 1 页到最后一页)
        tasks = [fetch_page(session, p, PAGE_SIZE) for p in range(1, total_pages + 1)]
        
        # 3. 执行并发抓取
        pages_content = await asyncio.gather(*tasks)
        
        # 4. 扁平化数据 (List of Lists -> List of Dicts)
        all_records = [item for sublist in pages_content for item in sublist]
        
        print(f"[*] 原始数据抓取完成，共 {len(all_records)} 条记录。开始进入 Polars 清洗...")

        # 5. 使用 Polars 进行数据处理
        # 映射字段名，让数据更具可读性
        df = pl.DataFrame(all_records).select([
            pl.col("f12").alias("board_code"),
            pl.col("f14").alias("board_name"),
            (pl.col("f2") / 100).alias("price"),        # 指数点位
            pl.col("f3").alias("pct_change"),           # 涨跌幅
            (pl.col("f62") / 100000000).alias("main_money_net_in_billion") # 主力净流入(亿元)
        ])

        # 过滤掉无效数据并按涨跌幅降序排列
        df = df.filter(pl.col("board_code") != "-").sort("pct_change", descending=True)

        # 6. 结果展示与导出
        print("\n[+] 行业板块全量数据 (前 20 名):")
        print(df.head(20))
        
        # 导出为极速的 Parquet 格式，供量化系统后续读取
        df.write_parquet("all_industry_boards.parquet")
        print(f"\n[*] 抓取完成！全量数据已保存至 all_industry_boards.parquet (共 {df.height} 行)")

if __name__ == "__main__":
    asyncio.run(fetch_all_industry_boards())
