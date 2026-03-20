import asyncio
import aiohttp
import baostock as bs
import polars as pl
import sys
from datetime import datetime

# --- 配置区 ---
MAX_CONCURRENT = 10  # Actions 环境建议保守一点，设为 10
# 东财个股所属板块接口
SECTOR_API = "https://push2.eastmoney.com/api/qt/slist/get?spt=3&ut=fa5fd1943c09a822273714f23b58f2d0&pi=0&pz=100&po=1&np=1&fields=f12,f14&secid={secid}"

async def get_stock_sectors(session, semaphore, stock_info):
    """
    抓取单个个股所属的所有板块（行业+概念）
    """
    bs_code, name = stock_info
    # 转换代码格式: sh.601318 -> 1.601318, sz.000001 -> 0.000001
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
                if resp.status != 200:
                    return []
                content = await resp.json()
                diff = content.get("data", {}).get("diff", [])
                
                if not diff: return []
                
                # 提取板块 ID 和名称
                return [{"sector_code": x["f12"], "sector_name": x["f14"]} for x in diff if "f12" in x]
        except Exception:
            return []

async def main():
    # 1. Baostock 获取种子
    print("[*] 正在登录 Baostock...")
    bs.login()
    
    # 获取最新的交易日个股列表
    today = datetime.now().strftime("%Y-%m-%d")
    rs = bs.query_all_stock(day=today)
    
    stocks = []
    while (rs.error_code == '0') & rs.next():
        row = rs.get_row_data()
        # 只取沪深个股，排除指数
        if row[0].startswith(("sh.6", "sz.0", "sz.3")):
            stocks.append((row[0], row[1]))
    bs.logout()
    
    if not stocks:
        print("[-] 未获取到个股列表，请检查 Baostock 状态")
        return

    # 2. 并发抓取 (测试阶段先取 300 支，全量请去掉切片)
    test_limit = 300 
    target_stocks = stocks[:test_limit]
    print(f"[*] 准备从 {len(target_stocks)} 支个股中反推板块信息...")

    semaphore = asyncio.Semaphore(MAX_CONCURRENT)
    async with aiohttp.ClientSession() as session:
        tasks = [get_stock_sectors(session, semaphore, s) for s in target_stocks]
        results = await asyncio.gather(*tasks)

    # 3. Polars 清洗
    print("[*] 正在使用 Polars 进行数据去重和清洗...")
    
    # 展平列表
    flat_data = [item for sublist in results for item in sublist]
    
    if not flat_data:
        print("[-] 抓取结果为空，可能被 API 屏蔽或参数失效")
        sys.exit(1)

    # 转换为 DataFrame
    df = pl.DataFrame(flat_data)
    
    # 过滤非板块代码（东财板块通常以 BK 开头）并去重
    cleaned_df = (
        df.filter(pl.col("sector_code").str.starts_with("BK"))
        .unique(subset=["sector_code"])
        .sort("sector_code")
    )

    print("-" * 50)
    print(f"[+] 成功清洗出 {len(cleaned_df)} 个唯一板块")
    print(cleaned_df.head(10))
    print("-" * 50)

    # 保存结果
    cleaned_df.write_csv("sector_list_cleaned.csv")
    print("[*] 结果已导出至 sector_list_cleaned.csv")

if __name__ == "__main__":
    asyncio.run(main())
