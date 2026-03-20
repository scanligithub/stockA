import asyncio
import aiohttp
import baostock as bs
import polars as pl
import sys
from datetime import datetime, timedelta

# --- 工业级配置 ---
MAX_CONCURRENT = 15  # 在 GitHub Actions 海外 IP 环境下，15 是兼顾速度与稳定的平衡点
UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0"

# 东财基准分类接口
BASE_LIST_API = "https://push2.eastmoney.com/api/qt/clist/get?pn=1&pz=1500&po=1&np=1&ut=bd1d9ddb04089700cf9c27f6f7426281&fltt=2&invt=2&fid=f3&fs={fs}&fields=f12,f14"
# 个股所属板块接口
STOCK_SECTOR_API = "https://push2.eastmoney.com/api/qt/slist/get?spt=3&ut=fa5fd1943c09a822273714f23b58f2d0&pi=0&pz=100&po=1&np=1&fields=f12,f14&secid={secid}"

async def get_sector_type_map(session):
    """
    同步东财官方三大分类，建立标签库
    """
    types = {"m:90+t:1": "地域", "m:90+t:2": "行业", "m:90+t:3": "概念"}
    all_types = []
    print("[*] 正在拉取东财官方基准分类清单...")
    for fs_param, t_name in types.items():
        try:
            url = BASE_LIST_API.format(fs=fs_param)
            async with session.get(url, timeout=15) as resp:
                data = await resp.json()
                diff = data.get("data", {}).get("diff", [])
                for item in diff:
                    all_types.append({"sector_code": item["f12"], "sector_type": t_name})
        except Exception as e:
            print(f"[-] 警告: 获取分类 {t_name} 失败: {e}")
    return pl.DataFrame(all_types).unique(subset=["sector_code"])

def get_robust_date():
    bs.login()
    # 回溯逻辑：找昨天开始往前的最近一个交易日
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
    """
    单个个股抓取任务，带进度统计
    """
    bs_code, stock_name = stock_info
    pure_code = bs_code.split(".")[1]
    prefix = "1." if bs_code.startswith("sh") else "0."
    secid = f"{prefix}{pure_code}"
    
    async with semaphore:
        headers = {"User-Agent": UA, "Referer": "https://quote.eastmoney.com/"}
        try:
            async with session.get(STOCK_SECTOR_API.format(secid=secid), headers=headers, timeout=20) as resp:
                data = await resp.json()
                diff = data.get("data", {}).get("diff", [])
                
                # 任务计数与日志
                counter['done'] += 1
                if counter['done'] % 500 == 0:
                    print(f"[*] 已完成 {counter['done']} / {counter['total']} 支个股...")
                
                return [{"stock_code": pure_code, "stock_name": stock_name, "sector_code": x["f12"], "sector_name": x["f14"]} 
                        for x in diff if x.get("f12", "").startswith("BK")]
        except:
            return []

async def main():
    start_time = datetime.now()
    async with aiohttp.ClientSession() as session:
        # 1. 基准表 (用于精准给 1000+ 板块分类)
        type_lookup_df = await get_sector_type_map(session)

        # 2. 个股种子 (全量 5400+)
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
            print("[-] 错误: 无法获取个股列表")
            return

        # 3. 全量并发扫描
        total_count = len(stocks)
        print(f"[*] 启动全量扫描：共 {total_count} 支个股...")
        counter = {'done': 0, 'total': total_count}
        semaphore = asyncio.Semaphore(MAX_CONCURRENT)
        
        tasks = [fetch_stock_task(session, semaphore, s, counter) for s in stocks]
        raw_results = await asyncio.gather(*tasks)
        
        # 4. 数据聚合与分类对齐
        print("[*] 正在执行全量数据清洗与分类映射...")
        flat_data = [item for sublist in raw_results for item in sublist]
        df_mapping = pl.DataFrame(flat_data)
        
        # Left Join 官方分类表
        df_final = df_mapping.join(type_lookup_df, on="sector_code", how="left")
        
        # 兜底：不在官方大分类里的均归为“概念”
        df_final = df_final.with_columns(pl.col("sector_type").fill_null("概念"))

        # 5. 产出全量数据库
        # 文件 A: 全量板块字典 (包含 1000+ 个板块)
        sector_dictionary = (
            df_final.select(["sector_code", "sector_name", "sector_type"])
            .unique(subset=["sector_code"])
            .sort(["sector_type", "sector_code"])
        )
        
        # 文件 B: 全量个股-板块映射 (约 8-10 万行)
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
    print(f"全量任务完成！总耗时: {duration}")
    print(f"1. 板块字典: sector_dictionary.csv (共 {len(sector_dictionary)} 个板块)")
    print(f"2. 映射关系: stock_sector_mapping.csv (共 {len(stock_sector_map)} 条记录)")
    print("-" * 60)
    
    # 分类统计打印
    print(sector_dictionary.group_by("sector_type").count())

if __name__ == "__main__":
    asyncio.run(main())
