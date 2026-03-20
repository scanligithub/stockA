import asyncio
import aiohttp
import baostock as bs
import polars as pl
import json
import sys

# --- 配置区 ---
MAX_CONCURRENT = 15  # 严格控制并发数，防止被封
SECTOR_API_TEMPLATE = "https://push2.eastmoney.com/api/qt/slist/get?spt=3&ut=fa5fd1943c09a822273714f23b58f2d0&pi=0&pz=100&po=1&np=1&fields=f12,f14&secid={secid}"

async def get_stock_sectors(session, semaphore, stock_info):
    """
    抓取单个个股所属的所有板块
    """
    bs_code, name = stock_info
    # Baostock 格式 sh.601318 -> 东财格式 1.601318 / sz.000001 -> 0.000001
    prefix = "1." if bs_code.startswith("sh") else "0."
    pure_code = bs_code.split(".")[1]
    secid = f"{prefix}{pure_code}"
    
    async with semaphore:
        url = SECTOR_API_TEMPLATE.format(secid=secid)
        try:
            async with session.get(url, timeout=10) as resp:
                if resp.status != 200:
                    return []
                content = await resp.json()
                data_list = content.get("data", {}).get("diff", [])
                
                # 提取板块代码和名称 (f12, f14)
                sectors = []
                for item in data_list:
                    # 过滤掉非板块数据（通常板块代码以 BK 开头）
                    if item.get("f12", "").startswith("BK"):
                        sectors.append({
                            "sector_code": item["f12"],
                            "sector_name": item["f14"]
                        })
                return sectors
        except Exception as e:
            # 静默处理异常，保证整体进度
            return []

async def main():
    # 1. 登录 Baostock 获取个股种子列表
    print("[*] 正在连接 Baostock 获取个股列表...")
    lg = bs.login()
    if lg.error_code != '0':
        print(f"[-] Baostock 登录失败: {lg.error_msg}")
        return

    # 获取全 A 股列表
    rs = bs.query_all_stock(day=pl.date_range(pl.date(2023,1,1), pl.date(2023,12,31), eager=True)[-1].strftime("%Y-%m-%d"))
    stocks = []
    while (rs.error_code == '0') & rs.next():
        row = rs.get_row_data()
        # 仅处理沪深主板/创业板/科创板，过滤指数
        if row[0].startswith(("sh.6", "sz.0", "sz.3")):
            stocks.append((row[0], row[1]))
    bs.logout()
    
    # 为了测试速度，这里可以切片前 300 支，若要全量则去掉 [:300]
    # 全量 5000 支大约需要 3-5 分钟
    test_stocks = stocks[:500] 
    print(f"[+] 种子获取成功，准备分析 {len(test_stocks)} 支个股的所属板块...")

    # 2. 异步抓取板块关系
    semaphore = asyncio.Semaphore(MAX_CONCURRENT)
    async with aiohttp.ClientSession(headers={"User-Agent": "Mozilla/5.0"}) as session:
        tasks = [get_stock_sectors(session, semaphore, s) for s in test_stocks]
        results = await asyncio.gather(*tasks)

    # 3. 使用 Polars 进行数据清洗和归纳
    print("[*] 正在使用 Polars 清洗板块数据...")
    
    # 展开嵌套列表
    flat_results = [item for sublist in results for item in sublist]
    
    if not flat_results:
        print("[-] 未获取到任何板块数据，请检查 API 连通性")
        sys.exit(1)

    df = pl.DataFrame(flat_results)
    
    # 核心步骤：去重得到唯一的板块列表
    unique_sectors = (
        df.unique(subset=["sector_code"])
        .sort("sector_code")
    )

    print("-" * 50)
    print(f"[+] 最终清洗完成！得到唯一板块数量: {len(unique_sectors)}")
    print(unique_sectors.head(10))
    print("-" * 50)

    # 保存结果，供 Action Artifacts 下载
    unique_sectors.write_csv("sector_list_cleaned.csv")
    print("[*] 结果已保存至 sector_list_cleaned.csv")

if __name__ == "__main__":
    asyncio.run(main())
