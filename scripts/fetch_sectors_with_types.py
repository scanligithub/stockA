import asyncio
import aiohttp
import baostock as bs
import polars as pl
import sys
from datetime import datetime, timedelta

# --- 工业级配置 ---
MAX_CONCURRENT = 15
UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0"
# 个股所属板块 API
STOCK_SECTOR_API = "https://push2.eastmoney.com/api/qt/slist/get?spt=3&ut=fa5fd1943c09a822273714f23b58f2d0&pi=0&pz=100&po=1&np=1&fields=f12,f14&secid={secid}"
# 东财轻量级种子接口 (仅用于 Baostock 失效时)
EM_SEED_API = "https://push2.eastmoney.com/api/qt/clist/get?pn=1&pz=6000&po=1&np=1&ut=bd1d9ddb04089700cf9c27f6f7426281&fltt=2&invt=2&fid=f3&fs=m:0+t:6,m:0+t:80,m:1+t:2,m:1+t:23,m:0+t:81+s:2048&fields=f12,f14"

def get_seeds_from_baostock():
    """从 Baostock 稳健获取种子"""
    try:
        bs.login()
        # 严格限制：只查找过去 15 天到昨天之间的交易日
        today_str = datetime.now().strftime("%Y-%m-%d")
        yesterday_obj = datetime.now() - timedelta(days=1)
        yesterday_str = yesterday_obj.strftime("%Y-%m-%d")
        start_str = (datetime.now() - timedelta(days=15)).strftime("%Y-%m-%d")
        
        rs_dates = bs.query_trade_dates(start_date=start_str, end_date=yesterday_str)
        trade_dates = []
        while (rs_dates.error_code == '0') & rs_dates.next():
            row = rs_dates.get_row_data()
            if row[1] == '1': trade_dates.append(row[0])
        
        if not trade_dates: return []
        
        # 从最近的交易日开始试，直到拿到股票列表
        for target_date in reversed(trade_dates):
            rs_stocks = bs.query_all_stock(day=target_date)
            stocks = []
            while (rs_stocks.error_code == '0') & rs_stocks.next():
                row = rs_stocks.get_row_data()
                if row[0].startswith(("sh.6", "sz.0", "sz.3", "bj.4", "bj.8")):
                    stocks.append((row[0], row[1]))
            if stocks:
                print(f"[*] 成功从 Baostock 获取种子 (日期: {target_date}, 数量: {len(stocks)})")
                return stocks
        return []
    except:
        return []
    finally:
        try: bs.logout()
        except: pass

async def get_seeds_from_em(session):
    """从东财轻量接口获取种子 (备份方案)"""
    try:
        async with session.get(EM_SEED_API, timeout=15) as resp:
            data = await resp.json()
            diff = data.get("data", {}).get("diff", [])
            stocks = []
            for item in diff:
                code = item['f12']
                prefix = "sh" if code.startswith("6") else "sz" # 简化处理
                stocks.append((f"{prefix}.{code}", item['f14']))
            if stocks:
                print(f"[*] 成功从东财 API 获取种子 (数量: {len(stocks)})")
            return stocks
    except:
        return []

async def fetch_stock_task(session, semaphore, stock_info, counter):
    bs_code, stock_name = stock_info
    pure_code = bs_code.split(".")[1]
    # 东财 secid 逻辑
    if bs_code.startswith("sh"): prefix = "1."
    elif bs_code.startswith("sz"): prefix = "0."
    else: prefix = "0." # 北交所兜底
    secid = f"{prefix}{pure_code}"
    
    async with semaphore:
        headers = {"User-Agent": UA, "Referer": "https://quote.eastmoney.com/"}
        try:
            async with session.get(STOCK_SECTOR_API.format(secid=secid), headers=headers, timeout=20) as resp:
                data = await resp.json()
                diff = data.get("data", {}).get("diff", [])
                counter['done'] += 1
                if counter['done'] % 1000 == 0:
                    print(f"[*] 进度: {counter['done']} / {counter['total']}")
                return [{"stock_code": pure_code, "stock_name": stock_name, "sector_code": x["f12"], "sector_name": x["f14"]} 
                        for x in diff if x.get("f12", "").startswith("BK")]
        except:
            return []

async def main():
    start_time = datetime.now()
    async with aiohttp.ClientSession() as session:
        # 1. 获取种子 (双保险)
        stocks = get_seeds_from_baostock()
        if not stocks:
            print("[!] Baostock 失败，切换东财种子接口...")
            stocks = await get_seeds_from_em(session)
        
        if not stocks:
            print("[-] 严重错误: 无法获取任何个股种子，任务终止。")
            sys.exit(1)

        # 2. 并发扫描
        total_count = len(stocks)
        counter = {'done': 0, 'total': total_count}
        semaphore = asyncio.Semaphore(MAX_CONCURRENT)
        
        print(f"[*] 开始扫描全量映射...")
        tasks = [fetch_stock_task(session, semaphore, s, counter) for s in stocks]
        raw_results = await asyncio.gather(*tasks)
        
        # 3. 数据处理
        flat_data = [item for sublist in raw_results for item in sublist]
        if not flat_data:
            print("[-] 错误: 未抓取到有效映射数据。")
            sys.exit(1)

        df_mapping = pl.DataFrame(flat_data)
        
        # 4. 智能分类
        df_final = df_mapping.with_columns([
            pl.when(pl.col("sector_code").str.contains(r"BK014[5-9]|BK015|BK016|BK017|BK018|BK019"))
            .then(pl.lit("地域板块"))
            .when(pl.col("sector_code").str.contains(r"BK042[7-9]|BK04[3-9]|BK0[5-8]|BK091[0-7]"))
            .then(pl.lit("行业板块"))
            .otherwise(pl.lit("概念板块"))
            .alias("sector_type")
        ])

        # 5. 输出
        sector_dict = df_final.select(["sector_code", "sector_name", "sector_type"]).unique(subset=["sector_code"]).sort(["sector_type", "sector_code"])
        mapping_table = df_final.select(["stock_code", "stock_name", "sector_code", "sector_name", "sector_type"]).unique().sort("stock_code")

        sector_dict.write_csv("sector_dictionary.csv")
        mapping_table.write_csv("stock_sector_mapping.csv")

    print("-" * 60)
    print(f"[*] 任务成功！耗时: {datetime.now() - start_time}")
    print(f"[*] 唯一板块: {len(sector_dict)} 个")
    print(sector_dict.group_by("sector_type").count())
    print("-" * 60)

if __name__ == "__main__":
    asyncio.run(main())
