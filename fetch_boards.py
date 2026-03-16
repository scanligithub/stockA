import asyncio
import math
import random
import polars as pl
from curl_cffi.requests import AsyncSession, exceptions

# 信号量控制：降低瞬时并发压力
SEMAPHORE = asyncio.Semaphore(6)

def get_default_params():
    """统一参数模板，防止因参数缺失被指纹识别"""
    return {
        "pn": "1",
        "pz": "100",
        "po": "1",
        "np": "1",
        "ut": "bd1d9ddb04089700cf9c27f6f7426281",
        "fltt": "2",
        "invt": "2",
        "fid": "f3",
        "fs": "m:90 t:2",
        "fields": "f12,f14,f2,f3,f62"
    }

async def fetch_with_retry(session, page_num, max_retries=3):
    """带重试机制的抓取核心，应对 Connection closed abruptly"""
    url = "https://push2.eastmoney.com/api/qt/clist/get"
    params = get_default_params()
    params["pn"] = str(page_num)
    headers = {"Referer": "https://quote.eastmoney.com/"}

    for attempt in range(max_retries):
        async with SEMAPHORE:
            try:
                # 随机微小延迟，打散并发请求
                await asyncio.sleep(random.uniform(0.1, 0.5))
                
                resp = await session.get(url, params=params, headers=headers, timeout=15)
                if resp.status_code == 200:
                    json_data = resp.json()
                    return json_data.get("data", {})
                
                print(f"[-] 第 {page_num} 页请求返回状态码: {resp.status_code}")
            except (exceptions.ConnectionError, exceptions.TimeoutError) as e:
                wait_time = (attempt + 1) * 2
                print(f"[!] 第 {page_num} 页连接异常: {e}. 正在进行第 {attempt+1} 次重试，等待 {wait_time}s...")
                await asyncio.sleep(wait_time)
            except Exception as e:
                print(f"[!] 第 {page_num} 页发生未知错误: {e}")
                break
    return {}

async def main():
    async with AsyncSession(impersonate="chrome120") as session:
        print("[*] 正在获取第一页数据并探测总数...")
        
        # 1. 直接请求第一页
        first_page_res = await fetch_with_retry(session, 1)
        if not first_page_res:
            print("[-] 无法获取初始数据，请检查网络或防火墙策略。")
            return

        total_count = first_page_res.get("total", 0)
        first_page_data = first_page_res.get("diff", [])
        
        if total_count == 0:
            print("[-] 数据总数为 0，停止抓取。")
            return

        print(f"[+] 成功！总板块数: {total_count}。")
        
        # 2. 计算剩余页数
        page_size = 100
        total_pages = math.ceil(total_count / page_size)
        all_data = []
        all_data.extend(first_page_data)

        if total_pages > 1:
            print(f"[*] 正在并发抓取剩余 {total_pages - 1} 页数据...")
            # 从第 2 页开始抓取
            tasks = [fetch_with_retry(session, p) for p in range(2, total_pages + 1)]
            remaining_pages = await asyncio.gather(*tasks)
            
            for page_res in remaining_pages:
                if page_res:
                    all_data.extend(page_res.get("diff", []))

        # 3. Polars 数据清洗
        print(f"[*] 抓取完成，共获取 {len(all_data)} 条原始记录。进入 Polars 清洗...")
        
        df = pl.DataFrame(all_data).select([
            pl.col("f12").alias("code"),
            pl.col("f14").alias("name"),
            (pl.col("f2") / 100).alias("price"),
            pl.col("f3").alias("pct_change"),
            (pl.col("f62") / 1e8).alias("main_in_flow_bn") # 亿元
        ]).filter(pl.col("code") != "-")

        # 排序并展示结果
        df = df.sort("pct_change", descending=True)
        print("\n" + "="*50)
        print(df.head(15))
        print("="*50)
        
        # 保存结果
        df.write_parquet("industry_boards.parquet")
        print(f"[*] 全量数据已保存至 industry_boards.parquet")

if __name__ == "__main__":
    asyncio.run(main())
