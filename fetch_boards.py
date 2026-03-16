import asyncio
import math
import random
import polars as pl
from curl_cffi.requests import AsyncSession

# 控制瞬时压强
SEMAPHORE = asyncio.Semaphore(5)

# 核心武器：东方财富 CDN 边缘节点池
# 当某个节点对你的 IP 限流时，自动路由到其他节点
EASTMONEY_NODES = [
    "push2.eastmoney.com",
    "11.push2.eastmoney.com",
    "44.push2.eastmoney.com",
    "82.push2.eastmoney.com",
    "99.push2.eastmoney.com",
    "28.push2.eastmoney.com"
]

# 改进要求一：板块类型映射字典
BOARD_TYPES = {
    "行业": "m:90 t:2",
    "概念": "m:90 t:3",
    "地域": "m:90 t:1"
}

def get_default_params(fs_value):
    return {
        "pn": "1", "pz": "100", "po": "1", "np": "1",
        "ut": "bd1d9ddb04089700cf9c27f6f7426281",
        "fltt": "2", "invt": "2", "fid": "f3",
        "fs": fs_value,
        "fields": "f12,f14,f2,f3,f62"
    }

async def fetch_with_retry(session, page_num, board_name, fs_value, max_retries=5):
    """带节点轮询和指数退避的强健抓取函数"""
    params = get_default_params(fs_value)
    params["pn"] = str(page_num)
    headers = {"Referer": "https://quote.eastmoney.com/"}

    for attempt in range(max_retries):
        async with SEMAPHORE:
            # 随机挑选一个 CDN 节点，分散目标服务器的频控压力
            node = random.choice(EASTMONEY_NODES)
            url = f"https://{node}/api/qt/clist/get"
            
            try:
                # 随机微小延迟，打散并发并发峰值
                await asyncio.sleep(random.uniform(0.1, 0.6))
                
                resp = await session.get(url, params=params, headers=headers, timeout=10)
                
                if resp.status_code == 200:
                    json_data = resp.json()
                    data_dict = json_data.get("data", {})
                    # 如果成功，打上所属板块的标签返回
                    if data_dict:
                        return {"type": board_name, "data": data_dict}
                    return None
                
            except Exception as e:
                # 捕获所有异常（修复了之前的 AttributeError）
                wait_time = (attempt + 1) * 2
                print(f"[!] {board_name} 第 {page_num} 页 (节点 {node}) 抓取失败: {str(e)[:50]}... 等待 {wait_time}s 重试 (第 {attempt+1} 次)")
                await asyncio.sleep(wait_time)
                
    print(f"[-] {board_name} 第 {page_num} 页彻底失败，已达到最大重试次数。")
    return None

async def fetch_single_board_type(session, board_name, fs_value):
    """拉取单个类型板块的所有分页"""
    print(f"[*] 开始探测【{board_name}】板块总数...")
    first_page_res = await fetch_with_retry(session, 1, board_name, fs_value)
    
    if not first_page_res or not first_page_res.get("data"):
        print(f"[-] 无法获取【{board_name}】初始数据。")
        return []

    data_dict = first_page_res["data"]
    total_count = data_dict.get("total", 0)
    first_page_data = data_dict.get("diff", [])
    
    if total_count == 0:
        return []

    page_size = 100
    total_pages = math.ceil(total_count / page_size)
    print(f"[+] 【{board_name}】共计 {total_count} 个，分 {total_pages} 页拉取。")

    all_data = []
    # 为第一页数据注入板块类型字段
    for item in first_page_data:
        item["board_type"] = board_name
    all_data.extend(first_page_data)

    if total_pages > 1:
        tasks = [fetch_with_retry(session, p, board_name, fs_value) for p in range(2, total_pages + 1)]
        remaining_pages = await asyncio.gather(*tasks)
        
        for page_res in remaining_pages:
            if page_res and "data" in page_res and page_res["data"].get("diff"):
                page_diff = page_res["data"]["diff"]
                for item in page_diff:
                    item["board_type"] = board_name
                all_data.extend(page_diff)

    return all_data

async def main():
    # 模拟 Chrome 浏览器，解决 JA3 阻断
    async with AsyncSession(impersonate="chrome120") as session:
        all_boards_data = []
        
        # 依次并发拉取三大板块
        for b_name, fs_val in BOARD_TYPES.items():
            board_data = await fetch_single_board_type(session, b_name, fs_val)
            all_boards_data.extend(board_data)
            # 板块之间稍微歇息，保护 IP
            await asyncio.sleep(1)

        print(f"\n[*] 全量抓取完成，共获取 {len(all_boards_data)} 个板块！开始进行 Polars 聚合清洗...")

        if not all_boards_data:
            print("[-] 无数据可清洗。")
            return

        # Polars 数据清洗流水线
        df = pl.DataFrame(all_boards_data).select([
            pl.col("board_type").alias("板块大类"),
            pl.col("f12").alias("板块代码"),
            pl.col("f14").alias("板块名称"),
            (pl.col("f2") / 100).alias("最新点位"),
            pl.col("f3").alias("涨跌幅(%)"),
            (pl.col("f62") / 1e8).alias("主力净流入(亿元)")
        ]).filter(pl.col("板块代码") != "-")

        # 按照板块大类和涨跌幅排序
        df = df.sort(["板块大类", "涨跌幅(%)"], descending=[False, True])
        
        print("\n" + "="*70)
        print("各类型板块龙头 (节选预览):")
        # 每个板块大类打印前 3 名
        for b_name in BOARD_TYPES.keys():
            print(f"\n--- {b_name}板块 ---")
            print(df.filter(pl.col("板块大类") == b_name).head(3))
        print("="*70)
        
        # 导出汇总全量数据
        df.write_parquet("all_boards_merged.parquet")
        print(f"\n[*] 完美入库！三大板块全量数据已合并且保存至 all_boards_merged.parquet")

if __name__ == "__main__":
    asyncio.run(main())
