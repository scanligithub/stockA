import asyncio
import aiohttp
import baostock as bs
import polars as pl
import sys
from datetime import datetime, timedelta

# --- 工业级配置 ---
MAX_CONCURRENT = 15  # GitHub Actions 并发控制
UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0"
STOCK_SECTOR_API = "https://push2.eastmoney.com/api/qt/slist/get?spt=3&ut=fa5fd1943c09a822273714f23b58f2d0&pi=0&pz=100&po=1&np=1&fields=f12,f14&secid={secid}"

def get_robust_date():
    bs.login()
    end_dt = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
    start_dt = (datetime.now() - timedelta(days=20)).strftime("%Y-%m-%d")
    rs = bs.query_trade_dates(start_date=start_dt, end_date=end_dt)
    dates = []
    while (rs.error_code == '0') & rs.next(): dates.append(rs.get_row_data())
    bs.logout()
    for d in reversed(dates):
        if d[1] == '1': return d[0]
    return end_dt

async def fetch_stock_task(session, semaphore, stock_info, counter):
    bs_code, stock_name = stock_info
    pure_code = bs_code.split(".")[1]
    prefix = "1." if bs_code.startswith("sh") else "0."
    secid = f"{prefix}{pure_code}"
    
    async with semaphore:
        headers = {"User-Agent": UA, "Referer": "https://quote.eastmoney.com/"}
        # 增加重试机制，应对 Server disconnected
        for attempt in range(3):
            try:
                async with session.get(STOCK_SECTOR_API.format(secid=secid), headers=headers, timeout=20) as resp:
                    if resp.status != 200: continue
                    data = await resp.json()
                    diff = data.get("data", {}).get("diff", [])
                    
                    # 实时进度
                    counter['done'] += 1
                    if counter['done'] % 500 == 0:
                        print(f"[*] 进度: {counter['done']} / {counter['total']} 支个股...")
                    
                    return [{"stock_code": pure_code, "stock_name": stock_name, "sector_code": x["f12"], "sector_name": x["f14"]} 
                            for x in diff if x.get("f12", "").startswith("BK")]
            except Exception:
                await asyncio.sleep(1) # 失败稍微等一下
        return []

async def main():
    start_time = datetime.now()
    
    # 1. 种子个股
    bs.login()
    target_date = get_robust_date()
    print(f"[*] 基准日期: {target_date}")
    rs = bs.query_all_stock(day=target_date)
    stocks = []
    while (rs.error_code == '0') & rs.next():
        row = rs.get_row_data()
        if row[0].startswith(("sh.6", "sz.0", "sz.3", "bj.4", "bj.8")):
            stocks.append((row[0], row[1]))
    bs.logout()
    
    if not stocks:
        print("[-] 错误: 无法获取个股名录")
        return

    # 2. 全量地毯式扫描
    total_count = len(stocks)
    print(f"[*] 绕道战略启动：通过全量 {total_count} 支个股反推板块库...")
    counter = {'done': 0, 'total': total_count}
    semaphore = asyncio.Semaphore(MAX_CONCURRENT)
    
    async with aiohttp.ClientSession() as session:
        tasks = [fetch_stock_task(session, semaphore, s, counter) for s in stocks]
        raw_results = await asyncio.gather(*tasks)
    
    # 3. 解析映射关系
    flat_data = [item for sublist in raw_results for item in sublist]
    if not flat_data:
        print("[-] 错误: 未能通过个股提取到任何板块信息。")
        sys.exit(1)

    df_mapping = pl.DataFrame(flat_data)

    # 4. 迂回分类逻辑：根据东财板块代码段打标签
    # 这是最稳的方式，不需要请求东财官方分类接口
    print("[*] 正在执行迂回特征分析，进行板块分类...")
    
    df_final = df_mapping.with_columns([
        pl.when(pl.col("sector_code").str.contains(r"BK014[5-9]|BK015|BK016|BK017|BK018|BK019"))
        .then(pl.lit("地域板块"))
        # 行业板块代码范围通常在 0400 到 1000 之间且固定
        .when(pl.col("sector_code").str.contains(r"BK042[7-9]|BK04[3-9]|BK0[5-8]|BK091[0-7]"))
        .then(pl.lit("行业板块"))
        # 剩下的大量 BK1xxx 和特定的都是概念
        .otherwise(pl.lit("概念板块"))
        .alias("sector_type")
    ])

    # 5. 生成结果
    sector_dictionary = (
        df_final.select(["sector_code", "sector_name", "sector_type"])
        .unique(subset=["sector_code"])
        .sort(["sector_type", "sector_code"])
    )
    
    stock_sector_map = (
        df_final.select(["stock_code", "stock_name", "sector_code", "sector_name", "sector_type"])
        .unique()
        .sort("stock_code")
    )

    # 6. 持久化
    sector_dictionary.write_csv("sector_dictionary.csv")
    stock_sector_map.write_csv("stock_sector_mapping.csv")

    duration = datetime.now() - start_time
    print("-" * 60)
    print(f"迂回扫描完成！总耗时: {duration}")
    print(f"1. 板块字典: sector_dictionary.csv (共 {len(sector_dictionary)} 个板块)")
    print(f"2. 映射关系: stock_sector_mapping.csv (共 {len(stock_sector_map)} 条)")
    print("-" * 60)
    
    # 分类统计预览
    print(sector_dictionary.group_by("sector_type").count())

if __name__ == "__main__":
    asyncio.run(main())
