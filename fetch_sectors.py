import asyncio
import aiohttp
import baostock as bs
import polars as pl
import sys
from datetime import datetime, timedelta

# --- 配置区 ---
MAX_CONCURRENT = 10 
SECTOR_API = "https://push2.eastmoney.com/api/qt/slist/get?spt=3&ut=fa5fd1943c09a822273714f23b58f2d0&pi=0&pz=100&po=1&np=1&fields=f12,f14&secid={secid}"

async def get_stock_sectors(session, semaphore, stock_info):
    bs_code, name = stock_info
    prefix = "1." if bs_code.startswith("sh") else "0."
    pure_code = bs_code.split(".")[1]
    secid = f"{prefix}{pure_code}"
    
    async with semaphore:
        url = SECTOR_API.format(secid=secid)
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Referer": "https://quote.eastmoney.com/"
        }
        try:
            async with session.get(url, headers=headers, timeout=15) as resp:
                if resp.status != 200: return []
                content = await resp.json()
                diff = content.get("data", {}).get("diff", [])
                if not diff: return []
                return [{"sector_code": x["f12"], "sector_name": x["f14"]} for x in diff if "f12" in x]
        except:
            return []

def get_safe_trade_date():
    """
    量化级健壮逻辑：获取昨天或前天的最后一个交易日
    """
    bs.login()
    # 强制回溯：从昨天开始往回找 15 天
    # 这样可以避开周末、节假日以及当天数据未生成的问题
    yesterday = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
    start_date = (datetime.now() - timedelta(days=15)).strftime("%Y-%m-%d")
    
    rs = bs.query_trade_dates(start_date=start_date, end_date=yesterday)
    dates_list = []
    while (rs.error_code == '0') & rs.next():
        dates_list.append(rs.get_row_data())
    
    # 从后往前找最近的一个交易日
    for date_info in reversed(dates_list):
        if date_info[1] == '1': # is_trading_day == '1'
            return date_info[0]
    return yesterday

async def main():
    print("[*] 正在初始化 Baostock 种子获取引擎...")
    bs.login()
    
    # 关键：获取已经存在历史快照的日期
    target_date = get_safe_trade_date()
    print(f"[*] 选定的稳健交易日: {target_date}")
    
    rs = bs.query_all_stock(day=target_date)
    stocks = []
    while (rs.error_code == '0') & rs.next():
        row = rs.get_row_data()
        # 筛选逻辑：沪深 A 股
        if row[0].startswith(("sh.6", "sz.0", "sz.3")):
            stocks.append((row[0], row[1]))
    
    bs.logout()
    
    if not stocks:
        print(f"[-] 严重错误: 无法获取 {target_date} 的个股名录。")
        sys.exit(1)

    # 选取 300 支进行测试。全量运行请改为 target_stocks = stocks
    test_limit = 300 
    target_stocks = stocks[:test_limit]
    print(f"[+] 成功获取种子列表: {len(stocks)} 支。准备扫描前 {len(target_stocks)} 支的板块分布...")

    # --- 异步扫描逻辑 ---
    semaphore = asyncio.Semaphore(MAX_CONCURRENT)
    async with aiohttp.ClientSession() as session:
        tasks = [get_stock_sectors(session, semaphore, s) for s in target_stocks]
        results = await asyncio.gather(*tasks)

    # --- Polars 高性能清洗 ---
    print("[*] 正在解析原始数据...")
    flat_data = [item for sublist in results for item in sublist]
    
    if not flat_data:
        print("[-] 错误: 所有个股请求均未返回有效板块，请检查网络环境。")
        sys.exit(1)

    df = pl.DataFrame(flat_data)
    
    # 清洗：只保留 BK 开头的板块代码，并去重
    cleaned_df = (
        df.filter(pl.col("sector_code").str.starts_with("BK"))
        .unique(subset=["sector_code"])
        .sort("sector_code")
    )

    print("-" * 60)
    print(f"[+] 扫描完成！共提取到唯一板块: {len(cleaned_df)} 个")
    print(f"[*] 样本数据展示 (Top 10):")
    print(cleaned_df.head(10))
    print("-" * 60)

    # 输出文件
    cleaned_df.write_csv("sector_list_cleaned.csv")
    print("[*] 板块库已持久化至: sector_list_cleaned.csv")

if __name__ == "__main__":
    asyncio.run(main())
