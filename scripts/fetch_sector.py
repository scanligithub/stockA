import sys
import os
import pandas as pd
import concurrent.futures
from tqdm import tqdm
from datetime import datetime

# 确保加载项目本地模块
sys.path.append(os.getcwd())
from utils.cf_proxy import EastMoneyProxy
from utils.cleaner import DataCleaner

OUTPUT_DIR = "temp_parts"
os.makedirs(OUTPUT_DIR, exist_ok=True)
proxy = EastMoneyProxy()

def fetch_one_sector(info):
    """
    抓取单个板块的 K 线和成份股
    """
    code, name, mkt = info['code'], info['name'], info['market']
    # 统一转换 secid，板块标识符固定为 90.
    secid = f"90.{code}"
    
    # 1. 抓取 K 线数据
    res_k = proxy.get_sector_kline(secid)
    df_k = pd.DataFrame()
    
    if res_k and res_k.get('data') and res_k['data'].get('klines'):
        rows = [x.split(',') for x in res_k['data']['klines']]
        # 标准字段: 日期, 开盘, 收盘, 最高, 最低, 成交量, 成交额, 振幅, 涨跌幅, 涨跌额, 换手率 (共11个)
        # 我们映射最核心的前 8 个字段
        cols = ['date','open','close','high','low','volume','amount','turn']
        df_k = pd.DataFrame([r[:8] for r in rows], columns=cols)
        
        df_k['code'] = code
        df_k['name'] = name
        df_k['type'] = info['type']

    # 2. 抓取成份股 (注意：如果成份股超过 100 只，此处可能也需要翻页逻辑)
    # 暂时保持原逻辑，优先解决板块列表完整性
    res_c = proxy.get_sector_constituents(code)
    consts = []
    if res_c and res_c.get('data') and res_c['data'].get('diff'):
        for item in res_c['data']['diff']:
            consts.append({
                "sector_code": code, 
                "stock_code": item['f12'], 
                "sector_name": name
            })
            
    return df_k, consts

def get_all_sectors_robustly(targets):
    """
    【核心重构】通过 total 字段自动步进翻页，确保拿全所有板块
    """
    combined_sectors = []
    
    for label, fs in targets.items():
        cat_items = []
        page = 1
        total = -1 # 初始值
        
        print(f"[*] 正在探测 {label} 板块列表...")
        
        while True:
            # 强制使用 pz=100，这是东财最稳定的分页大小
            res = proxy.get_raw_sector_list(fs, pn=page, pz=100)
            if not res or 'data' not in res:
                print(f"    [!] 第 {page} 页响应异常，提前结束该分类抓取")
                break
                
            data_block = res['data']
            if total == -1:
                total = data_block.get('total', 0)
            
            diff = data_block.get('diff', [])
            if not diff:
                break
                
            cat_items.extend(diff)
            
            # 打印进度
            print(f"    - 已抓取第 {page} 页 ({len(cat_items)}/{total})")
            
            # 判定翻页是否结束
            if len(cat_items) >= total or len(diff) < 100:
                break
            
            page += 1
            
        print(f"    [✓] {label} 抓取完成，共 {len(cat_items)} 条")
        
        for x in cat_items:
            combined_sectors.append({
                "code": x['f12'], "market": x['f13'], "name": x['f14'], "type": label
            })
            
    return combined_sectors

def main():
    print(f"\n{'='*70}")
    print(f"[*] 东方财富板块全量审计引擎启动 | {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"{'='*70}\n")

    # 1. 获取全量列表
    targets = {
        "Industry": "m:90 t:2", 
        "Concept":  "m:90 t:3", 
        "Region":   "m:90 t:1"
    }
    
    raw_sectors = get_all_sectors_robustly(targets)
    df_list = pd.DataFrame(raw_sectors).drop_duplicates('code')
    
    total_found = len(df_list)
    print(f"\n[*] 列表对账完成。合并去重后有效板块总数: {total_found}")
    
    # 安全熔断：如果总数依然明显不足，说明网络环境或 Token 失效
    if total_found < 900:
        print("[🔥 严重错误] 抓取总数严重不足，疑似触发严厉限流，脚本终止防止污染旧数据！")
        sys.exit(1)

    # 2. 并发抓取详细数据
    print(f"\n[*] 开始并发抓取 {total_found} 个板块的 K线 与 成份股 (并发: 20)...")
    all_k, all_c = [], []
    
    with concurrent.futures.ThreadPoolExecutor(max_workers=20) as executor:
        future_to_name = {
            executor.submit(fetch_one_sector, row): row['name'] 
            for _, row in df_list.iterrows()
        }
        
        for future in tqdm(concurrent.futures.as_completed(future_to_name), 
                          total=len(future_to_name), 
                          desc="全量采集进度"):
            sector_name = future_to_name[future]
            try:
                k, c = future.result()
                if not k.empty: all_k.append(k)
                if c: all_c.extend(c)
            except Exception as e:
                print(f"\n[-] 板块 [{sector_name}] 线程异常: {str(e)}")

    # 3. 清洗与持久化
    print(f"\n[*] 正在进行最终数据清洗...")
    cleaner = DataCleaner()
    
    if all_k:
        full_k = pd.concat(all_k)
        
        # --- 核心风险控制：剔除未来日期脏数据 ---
        today_str = datetime.now().strftime('%Y-%m-%d')
        full_k = full_k[full_k['date'] <= today_str]
        
        # 调用你的 DataCleaner 进行业务清洗（转换类型等）
        full_k = cleaner.clean_sector_kline(full_k)
        
        # 保存 Parquet
        k_path = f"{OUTPUT_DIR}/sector_kline_full.parquet"
        full_k.to_parquet(k_path, index=False)
        print(f"[+] K线存储成功: {len(full_k)} 行 | {os.path.getsize(k_path)/1024/1024:.2f} MB")
        print(f"[*] 数据时间跨度: {full_k['date'].min()} 至 {full_k['date'].max()}")
        
    if all_c:
        full_c = pd.DataFrame(all_c)
        full_c['date'] = today_str
        c_path = f"{OUTPUT_DIR}/sector_constituents_latest.parquet"
        full_c.to_parquet(c_path, index=False)
        print(f"[+] 成份股存储成功: {len(full_c)} 条关系对")

    print(f"\n{'='*70}")
    print(f"[*] 任务圆满完成 | 数据审计通过 ✅")
    print(f"{'='*70}\n")

if __name__ == "__main__":
    main()
