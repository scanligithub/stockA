import asyncio
import aiohttp
import baostock as bs
import polars as pl
import sys
import os
from datetime import datetime, timedelta

# --- 工业级配置 ---
MAX_CONCURRENT = 15
UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0"

# 1. 20维探测接口 (用于拿真值)
PROBE_API = "https://push2.eastmoney.com/api/qt/clist/get?pn=1&pz=100&po={po}&np=1&ut=bd1d9ddb04089700cf9c27f6f7426281&fltt=2&invt=2&fid={fid}&fs={fs}&fields=f12,f14"
# 2. 个股所属板块接口 (用于全量反推)
STOCK_SECTOR_API = "https://push2.eastmoney.com/api/qt/slist/get?spt=3&ut=fa5fd1943c09a822273714f23b58f2d0&pi=0&pz=100&po=1&np=1&fields=f12,f14&secid={secid}"
# 3. 板块身份路径接口 (用于识别孤儿板块的亲爹)
BREADCRUMB_API = "https://push2.eastmoney.com/api/qt/slist/get?spt=1&ut=fa5fd1943c09a822273714f23b58f2d0&secid=90.{code}"

async def probe_official_categories(session):
    """【Phase 1】20维并发探测：获取官方行业/概念/地域真值表"""
    targets = {"行业板块": "m:90 t:2", "概念板块": "m:90 t:3", "地域板块": "m:90 t:1"}
    fids = ["f12", "f3", "f2", "f6", "f5", "f4", "f17", "f18", "f8", "f10", "f15", "f16"]
    
    print("[*] 正在执行 20 维探测，构建官方分类真值库...")
    results = []
    for label, fs in targets.items():
        tasks = []
        for fid in fids:
            for po in [0, 1]:
                tasks.append(session.get(PROBE_API.format(fs=fs, fid=fid, po=po), timeout=15))
        
        resps = await asyncio.gather(*tasks, return_exceptions=True)
        seen = set()
        for r in resps:
            if isinstance(r, Exception): continue
            data = await r.json()
            for item in data.get("data", {}).get("diff", []):
                code = item['f12']
                if code not in seen:
                    results.append({"sector_code": code, "sector_name": item['f14'], "sector_type": label})
                    seen.add(code)
        print(f"    [✓] {label} 探测完成: {len(seen)} 个")
    return pl.DataFrame(results).unique(subset=["sector_code"])

async def scan_all_stocks(session, semaphore, stocks):
    """【Phase 2】全量个股反推：拿到 100% 的板块映射关系"""
    print(f"[*] 正在扫描 {len(stocks)} 支个股映射关系...")
    counter = {'done': 0, 'total': len(stocks)}
    
    async def fetch_task(s_info):
        bs_code, s_name = s_info
        prefix = "1." if bs_code.startswith("sh") else "0."
        secid = f"{prefix}{bs_code.split('.')[1]}"
        async with semaphore:
            try:
                async with session.get(STOCK_SECTOR_API.format(secid=secid), timeout=20) as resp:
                    data = await resp.json()
                    counter['done'] += 1
                    if counter['done'] % 1000 == 0: print(f"    - 进度: {counter['done']}/{counter['total']}")
                    return [{"stock_code": bs_code.split('.')[1], "stock_name": s_name, 
                             "sector_code": x['f12'], "sector_name": x['f14']} 
                            for x in data.get("data", {}).get("diff", []) if x.get('f12','').startswith('BK')]
            except: return []

    tasks = [fetch_task(s) for s in stocks]
    raw = await asyncio.gather(*tasks)
    return pl.DataFrame([item for sublist in raw for item in sublist]).unique()

async def identify_orphan(session, code):
    """【Phase 4】孤儿穿透：通过面包屑路径判定分类"""
    try:
        async with session.get(BREADCRUMB_API.format(code=code), timeout=15) as resp:
            data = await resp.json()
            # 路径列表，通常包含 "行业板块", "概念板块" 等字样
            path_items = [x['f14'] for x in data.get("data", {}).get("diff", [])]
            path_str = "".join(path_items)
            if "行业" in path_str: return "行业板块"
            if "地域" in path_str: return "地域板块"
            return "概念板块" # 默认归为概念
    except:
        return "概念板块"

async def main():
    start_time = datetime.now()
    async with aiohttp.ClientSession(headers={"User-Agent": UA}) as session:
        # 1. 20维探测 (真值)
        df_truth = await probe_official_categories(session)

        # 2. 个股种子获取
        bs.login()
        # 自动找上一个交易日 (逻辑简化)
        target_date = (datetime.now() - timedelta(days=2)).strftime("%Y-%m-%d") 
        rs = bs.query_all_stock(day=target_date)
        stocks = []
        while (rs.error_code == '0') & rs.next():
            row = rs.get_row_data()
            if row[0].startswith(("sh.6", "sz.0", "sz.3")): stocks.append((row[0], row[1]))
        bs.logout()

        # 3. 个股全量扫描 (映射)
        semaphore = asyncio.Semaphore(MAX_CONCURRENT)
        df_mapping = await scan_all_stocks(session, semaphore, stocks)
        
        # 4. 找出孤儿板块
        all_sector_codes = df_mapping.select("sector_code").unique()
        truth_codes = df_truth.select("sector_code").unique()
        
        orphans = all_sector_codes.join(truth_codes, on="sector_code", how="anti")
        print(f"[*] 发现 {len(orphans)} 个孤儿板块，准备执行身份穿透...")

        # 5. 孤儿身份识别
        orphan_details = []
        if len(orphans) > 0:
            for code in orphans["sector_code"].to_list():
                label = await identify_orphan(session, code)
                # 补充名称
                name = df_mapping.filter(pl.col("sector_code") == code)["sector_name"][0]
                orphan_details.append({"sector_code": code, "sector_name": name, "sector_type": label})
        
        df_orphans = pl.DataFrame(orphan_details)

        # 6. 合并最终字典
        sector_dictionary = pl.concat([df_truth, df_orphans]).sort(["sector_type", "sector_code"])
        
        # 7. 合并最终映射 (打标)
        stock_sector_mapping = df_mapping.join(
            sector_dictionary.select(["sector_code", "sector_type"]), 
            on="sector_code", how="left"
        ).fill_null("概念板块")

        # 8. 持久化
        sector_dictionary.write_csv("sector_dictionary.csv")
        stock_sector_mapping.write_csv("stock_sector_mapping.csv")

    print("-" * 60)
    print(f"[✓] 任务圆满完成！耗时: {datetime.now() - start_time}")
    print(f"[*] 最终板块总数: {len(sector_dictionary)}")
    print(sector_dictionary.group_by("sector_type").count())
    print("-" * 60)

if __name__ == "__main__":
    asyncio.run(main())
