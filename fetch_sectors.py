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

def get_latest_trade_date():
    """
    动态获取最近一个有数据的交易日
    """
    bs.login()
    # 获取最近 10 天的日历，回溯查找最后一个交易日
    end_date = datetime.now().strftime("%Y-%m-%d")
    start_date = (datetime.now() - timedelta(days=10)).strftime("%Y-%m-%d")
    
    rs = bs.query_trade_dates(start_date=start_date, end_date=end_date)
    dates_list = []
    while (rs.error_code == '0') & rs.next():
        dates_list.append(rs.get_row_data())
    
    # 倒序查找 is_trading_day 为 '1' 的日期
    for date_info in reversed(dates_list):
        if date_info[1] == '1': # is_trading_day == '1'
            return date_info[0]
    return end_date # 兜底返回今天

async def main():
    print("[*] 正在初始化 Baostock...")
    bs.login()
    
    # 自动获取最近有效的交易日
    target_date = get_latest_trade_date()
    print(f"[*] 检测到最近有效交易日: {target_date}")
    
    # 再次查询该日期的个股列表
    rs = bs.query_all_stock(day=target_date)
    stocks = []
    while (rs.error_code == '0') & rs.next():
        row = rs.get_row_data()
        # 筛选沪深个股：sh.6(主板), sz.0(主板), sz.3(创业板), sz.0(中小板)
        if row[0].startswith(("sh.6", "sz.0", "sz.3")):
            stocks.append((row[0], row[1]))
    
    bs.logout()
    
    if not stocks:
        print(f"[-] 在 {target_date} 仍未获取到个股列表，请检查 Baostock 接口连通性。")
        sys.exit(1)

    # 为了 GitHub Actions 测试效率，先跑 300 支，全量请去掉切片
    test_limit = 300 
    target_stocks = stocks[:test_limit]
    print(f"[+] 成功获取种子列表，共 {len(stocks)} 支。开始处理前 {len(target_stocks)} 支...")

    # --- 异步抓取逻辑 ---
    semaphore = asyncio.Semaphore(MAX_CONCURRENT)
    async with aiohttp.ClientSession() as session:
        tasks = [get_stock_sectors(session, semaphore, s) for s in target_stocks]
        results = await asyncio.gather(*tasks)

    # --- Polars 清洗 ---
    print("[*] 正在使用 Polars 清洗板块数据...")
    flat_data = [item for sublist in results for item in sublist]
    
    if not flat_data:
        print("[-] 抓取结果为空，请检查网络或东财 API 参数。")
        sys.exit(1)

    df = pl.DataFrame(flat_data)
    cleaned_df = (
        df.filter(pl.col("sector_code").str.starts_with("BK"))
        .unique(subset=["sector_code"])
        .sort("sector_code")
    )

    print("-" * 50)
    print(f"[+] 清洗完成！唯一板块数量: {len(cleaned_df)}")
    print(cleaned_df.head(10))
    print("-" * 50)

    cleaned_df.write_csv("sector_list_cleaned.csv")
    print("[*] 结果已导出至 sector_list_cleaned.csv")

if __name__ == "__main__":
    asyncio.run(main())
