import sys
import os
import pandas as pd
import concurrent.futures
from tqdm import tqdm
from datetime import datetime

# 确保能加载 utils
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
    # 统一转换 secid，板块在东财内部通常以 90. 开头
    secid = f"90.{code}" if str(mkt) == '90' else f"{mkt}.{code}"
    
    # 1. 抓取 K 线数据
    res_k = proxy.get_sector_kline(secid)
    df_k = pd.DataFrame()
    
    if res_k and res_k.get('data') and res_k['data'].get('klines'):
        # 东财返回的是逗号分隔字符串列表
        rows = [x.split(',') for x in res_k['data']['klines']]
        # 字段说明: 日期, 开盘, 收盘, 最高, 最低, 成交量, 成交额, 振幅, 涨跌幅, 涨跌额, 换手率
        # 根据你之前的测试返回，共有 11 个字段。这里先按你的 8 列取前 8 个，建议后续对齐。
        cols = ['date','open','close','high','low','volume','amount','turn']
        df_k = pd.DataFrame([r[:8] for r in rows], columns=cols)
        
        df_k['code'] = code
        df_k['name'] = name
        df_k['type'] = info['type']

    # 2. 抓取成份股
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

def main():
    print(f"\n{'='*70}")
    print(f"[*] 东方财富板块数据审计引擎启动 | {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"{'='*70}\n")

    # 1. 获取列表并审计完整性
    targets = {
        "Industry": "m:90 t:2", # 行业板块
        "Concept":  "m:90 t:3", # 概念板块
        "Region":   "m:90 t:1"  # 地域板块
    }
    
    all_sectors_list = []
    audit_log = []

    print("[1/4] 正在拉取板块分类列表...")
    for label, fs in targets.items():
        # 获取各分类原始列表
        # 注意：此处假设你的 proxy.get_sector_list 返回的是 'diff' 里的列表
        lst = proxy.get_sector_list(fs)
        count = len(lst)
        all_sectors_list.extend([
            {"code": x['f12'], "market": x['f13'], "name": x['f14'], "type": label} 
            for x in lst
        ])
        
        # 审计逻辑：如果长度正好是 100，极大概率是被服务器截断了
        status = "⚠️  疑似截断(100条)" if count == 100 else "✅ 正常"
        audit_log.append({"分类": label, "抓取数量": count, "状态": status})

    # 数据去重
    df_list = pd.DataFrame(all_sectors_list).drop_duplicates('code')
    
    # 打印审计报告
    print(f"\n【板块列表审计报告】")
    print("-" * 50)
    for entry in audit_log:
        print(f"  - {entry['分类']}: {entry['抓取数量']} 个 {entry['状态']}")
    print("-" * 50)
    print(f"[*] 合并去重后有效板块总数: {len(df_list)}")
    print(f"{'='*70}\n")

    # 2. 成份股截断抽样检查
    if not df_list.empty:
        sample = df_list.iloc[0]
        print(f"[2/4] 正在进行成份股截断抽检 (样本: {sample['name']})...")
        _, sample_consts = fetch_one_sector(sample)
        s_count = len(sample_consts)
        if s_count == 100:
            print(f"    [🔥 警告] 抽检发现成份股数量正好为 100，API 极大概率存在分页截断！")
        else:
            print(f"    [OK] 抽检成份股数量为 {s_count}，状态良好。")

    # 3. 并发抓取 K 线与成份股数据
    print(f"\n[3/4] 开始并发抓取 1005 个板块的详细数据 (线程池: 20)...")
    all_k, all_c = [], []
    
    with concurrent.futures.ThreadPoolExecutor(max_workers=20) as executor:
        # 建立任务映射
        future_to_name = {
            executor.submit(fetch_one_sector, row): row['name'] 
            for _, row in df_list.iterrows()
        }
        
        # 使用 tqdm 显示进度
        for future in tqdm(concurrent.futures.as_completed(future_to_name), 
                          total=len(future_to_name), 
                          desc="数据采集进度"):
            sector_name = future_to_name[future]
            try:
                k, c = future.result()
                if not k.empty:
                    all_k.append(k)
                if c:
                    all_c.extend(c)
            except Exception as e:
                print(f"\n[-] 板块 [{sector_name}] 处理失败: {str(e)}")

    # 4. 数据清洗与落库检查
    print(f"\n[4/4] 正在进行数据聚合与清洗...")
    cleaner = DataCleaner()
    
    if all_k:
        full_k = pd.concat(all_k)
        # 检查时间线是否有未来数据或缺失
        max_date = full_k['date'].max()
        min_date = full_k['date'].min()
        print(f"[*] K线数据审计: 覆盖板块 {len(all_k)} 个 | 行数 {len(full_k)}")
        print(f"[*] 时间跨度: {min_date} >> {max_date}")
        
        full_k = cleaner.clean_sector_kline(full_k)
        full_k.to_parquet(f"{OUTPUT_DIR}/sector_kline_full.parquet", index=False)
        print(f"[+] K线全量数据已存至: {OUTPUT_DIR}/sector_kline_full.parquet")
        
    if all_c:
        full_c = pd.DataFrame(all_c)
        full_c['date'] = pd.Timestamp.now().strftime('%Y-%m-%d')
        print(f"[*] 成份股审计: 累计关系对 {len(full_c)} 条")
        full_c.to_parquet(f"{OUTPUT_DIR}/sector_constituents_latest.parquet", index=False)
        print(f"[+] 成份股数据已存至: {OUTPUT_DIR}/sector_constituents_latest.parquet")

    print(f"\n{'='*70}")
    print(f"[*] 抓取任务圆满完成 | 生成文件总量: {os.path.getsize(f'{OUTPUT_DIR}/sector_kline_full.parquet') / 1024 / 1024:.2f} MB")
    print(f"{'='*70}\n")

if __name__ == "__main__":
    main()
