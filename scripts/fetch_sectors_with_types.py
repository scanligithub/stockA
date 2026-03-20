import asyncio
import aiohttp
import baostock as bs
import polars as pl
import sys
from datetime import datetime, timedelta

# --- 配置 ---
MAX_CONCURRENT = 15
UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0"

# 东财分类清单接口 (pz=1000 确保一次性拿完)
BASE_LIST_API = "https://push2.eastmoney.com/api/qt/clist/get?pn=1&pz=1000&po=1&np=1&ut=bd1d9ddb04089700cf9c27f6f7426281&fltt=2&invt=2&fid=f3&fs={fs}&fields=f12,f14"
# 个股所属板块接口
STOCK_SECTOR_API = "https://push2.eastmoney.com/api/qt/slist/get?spt=3&ut=fa5fd1943c09a822273714f23b58f2d0&pi=0&pz=100&po=1&np=1&fields=f12,f14&secid={secid}"

async def get_sector_type_map(session):
    """
    获取官方定义的三个分类基准表
    """
    # t:1 地域, t:2 行业, t:3 概念
    types = {"m:90+t:1": "地域", "m:90+t:2": "行业", "m:90+t:3": "概念"}
    all_types = []
    
    print("[*] 正在同步东财官方板块分类基准...")
    for fs_param, t_name in types.items():
        url = BASE_LIST_API.format(fs=fs_param)
        async with session.get(url) as resp:
            data = await resp.json()
            diff = data.get("data", {}).get("diff", [])
            for item in diff:
                all_types.append({"sector_code": item["f12"], "sector_type": t_name})
    
    return pl.DataFrame(all_types).unique(subset=["sector_code"])

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

async def fetch_stock_task(session, semaphore, stock_info):
    bs_code, stock_name = stock_info
    pure_code = bs_code.split(".")[1]
    prefix = "1." if bs_code.startswith("sh") else "0."
    secid = f"{prefix}{pure_code}"
    
    async with semaphore:
        headers = {"User-Agent": UA, "Referer": "https://quote.eastmoney.com/"}
        try:
            async with session.get(STOCK_SECTOR_API.format(secid=secid), headers=headers, timeout=15) as resp:
                data = await resp.json()
                diff = data.get("data", {}).get("diff", [])
                return [{"stock_code": pure_code, "stock_name": stock_name, "sector_code": x["f12"], "sector_name": x["f14"]} 
                        for x in diff if x.get("f12", "").startswith("BK")]
        except: return []

async def main():
    start_time = datetime.now()
    async with aiohttp.ClientSession() as session:
        # 1. 获取基准分类表
        type_lookup_df = await get_sector_type_map(session)
        print(f"[+] 基准表获取成功：地域/行业/概念共 {len(type_lookup_df)} 个记录")

        # 2. 获取个股种子
        print("[*] 登录 Baostock 获取种子...")
        bs.login()
        target_date = get_robust_date()
        rs = bs.query_all_stock(day=target_date)
        stocks = []
        while (rs.error_code == '0') & rs.next():
            row = rs.get_row_data()
            if row[0].startswith(("sh.6", "sz.0", "sz.3", "bj.4", "bj.8")):
                stocks.append((row[0], row[1]))
        bs.logout()
        
        # 3. 扫描个股板块映射 (全量运行)
        print(f"[*] 开始扫描全市场 {len(stocks)} 支个股，提取实时映射关系...")
        semaphore = asyncio.Semaphore(MAX_CONCURRENT)
        tasks = [fetch_stock_task(session, semaphore, s) for s in stocks]
        raw_results = await asyncio.gather(*tasks)
        
        # 4. 数据合并与打标签 (Polars 核心逻辑)
        print("[*] 正在利用 Polars 进行维度对齐与分类...")
        flat_data = [item for sublist in raw_results for item in sublist]
        df_mapping = pl.DataFrame(flat_data)
        
        # 将扫描结果与基准表做 Left Join
        df_final = df_mapping.join(type_lookup_df, on="sector_code", how="left")
        
        # 填充无法识别的板块（兜底设为“概念”）
        df_final = df_final.with_columns(pl.col("sector_type").fill_null("概念"))

        # 5. 产出最终结果
        # A: 唯一的板块字典 (包含分类)
        sector_dictionary = (
            df_final.select(["sector_code", "sector_name", "sector_type"])
            .unique(subset=["sector_code"])
            .sort(["sector_type", "sector_code"])
        )
        
        # B: 个股-板块映射
        stock_sector_map = (
            df_final.select(["stock_code", "stock_name", "sector_code", "sector_name", "sector_type"])
            .unique()
            .sort("stock_code")
        )

        # 6. 保存
        sector_dictionary.write_csv("sector_dictionary.csv")
        stock_sector_map.write_csv("stock_sector_mapping.csv")

    duration = datetime.now() - start_time
    print("-" * 60)
    print(f"任务完成！总耗时: {duration}")
    print(f"1. 板块字典: sector_dictionary.csv ({len(sector_dictionary)} 行)")
    print(f"2. 映射关系: stock_sector_mapping.csv ({len(stock_sector_map)} 行)")
    print("-" * 60)
    
    # 统计各分类数量
    stats = sector_dictionary.group_by("sector_type").count()
    print("[*] 板块分类统计：")
    print(stats)

if __name__ == "__main__":
    asyncio.run(main())
