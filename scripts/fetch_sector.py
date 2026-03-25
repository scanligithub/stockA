import sys
import os
import time
import pandas as pd
import concurrent.futures
from tqdm import tqdm
from datetime import datetime

# 确保能正确加载项目本地模块
sys.path.append(os.getcwd())
from utils.cf_proxy import EastMoneyProxy
from utils.cleaner import DataCleaner
from utils.sector_catalog_builder import build_sector_catalog

OUTPUT_DIR = "temp_parts"
os.makedirs(OUTPUT_DIR, exist_ok=True)
proxy = EastMoneyProxy()

def fetch_one_sector(info):
    """
    状态解耦版：分离 K 线与成份股的成功标志，确保指数时序数据不因成份股缺失而陪葬。
    返回值: (k_success: bool, c_success: bool, k_data: list, consts: list, error_msg: str)
    """
    code, name = info['code'], info['name']
    secid = f"90.{code}"
    
    k_success, c_success = False, False
    k_data, consts = [], []
    err_msgs = []
    
    try:
        # --- 1. 获取 K 线 ---
        res_k = proxy.get_sector_kline(secid)
        if res_k is None:
            err_msgs.append("K线超时")
        elif res_k.get('data') and res_k['data'].get('klines'):
            k_success = True
            for row_str in res_k['data']['klines']:
                r = row_str.split(',')
                k_data.append({
                    'date': r[0], 'open': r[1], 'close': r[2], 'high': r[3], 'low': r[4], 
                    'volume': r[5], 'amount': r[6], 'amplitude': r[7],
                    'code': code, 'name': name, 'type': info['type']
                })
        else:
            err_msgs.append(f"K线无数据({res_k.get('rc', 'unk')})")

        # --- 2. 获取成份股 ---
        res_c = proxy.get_sector_constituents(code)
        if res_c is None:
            err_msgs.append("成份股超时")
        elif res_c.get('data') and res_c['data'].get('diff'):
            c_success = True
            diff_data = res_c['data']['diff']
            items_list = diff_data.values() if isinstance(diff_data, dict) else diff_data
            for item in items_list:
                consts.append({"sector_code": code, "stock_code": item['f12'], "sector_name": name})
        else:
            err_msgs.append(f"成份股无数据({res_c.get('rc', 'unk')})")
            
        return k_success, c_success, k_data, consts, " | ".join(err_msgs)
        
    except Exception as e:
        return False, False, [], [], f"异常: {str(e)}"

def main():
    start_time = datetime.now()
    print(f"\n{'='*70}\n[*] 东方财富极速全量引擎 (全维解耦抢救版) | {start_time}\n{'='*70}\n")
    
    # 1. 获取完整板块列表
    all_sectors = build_sector_catalog(proxy)

    df_list = pd.DataFrame(all_sectors).drop_duplicates('code')
    total_found = len(df_list)
    print(f"\n[*] 审计报告：去重后唯一板块总数: {total_found}")

    if total_found < 950:
        print(f"[🔥 严重警告] 抓取总数 {total_found} 偏低，停止后续采集防污染。")
        sys.exit(1)

    sectors_to_fetch = df_list.to_dict('records')
    all_k_flat, all_c_flat = [], []
    MAX_RETRIES = 3
    
    # 【阶段 2.1】：高并发轮询抓取 (前 3 轮追求双全)
    for attempt in range(MAX_RETRIES):
        if not sectors_to_fetch:
            break
            
        print(f"\n[*] 开始第 {attempt + 1} 轮采集，等待任务: {len(sectors_to_fetch)} 个 (并发: 40)...")
        failed_sectors = []
        
        with concurrent.futures.ThreadPoolExecutor(max_workers=40) as executor:
            future_map = {executor.submit(fetch_one_sector, row): row for row in sectors_to_fetch}
            
            for future in tqdm(concurrent.futures.as_completed(future_map), total=len(future_map), desc=f"第 {attempt + 1} 轮进度"):
                row = future_map[future]
                try:
                    k_succ, c_succ, k_list, c_list, err_msg = future.result()
                    # 前3轮逻辑：只要缺一个(通常是成份股)，就全部重试
                    if k_succ and c_succ:
                        all_k_flat.extend(k_list)
                        all_c_flat.extend(c_list)
                    else:
                        row['err_msg'] = err_msg
                        failed_sectors.append(row)
                except Exception:
                    failed_sectors.append(row)
                    
        sectors_to_fetch = failed_sectors
        if sectors_to_fetch and attempt < MAX_RETRIES - 1:
            time.sleep(3)

    # 【阶段 2.2】：终极降级单线顺序抓取 (局部抢救模式)
    if sectors_to_fetch:
        print(f"\n[!] 触发防御与抢救策略：剩余 {len(sectors_to_fetch)} 个顽固板块，启动单线解耦补抓...")
        final_failed_sectors = []
        
        for row in tqdm(sectors_to_fetch, desc="终极抢救中"):
            time.sleep(1) # 强行间隔，防频控
            try:
                k_succ, c_succ, k_list, c_list, err_msg = fetch_one_sector(row)
                
                # 抢救逻辑核心：只要 K线 成功，就直接入库！放弃成份股不强求。
                if k_succ:
                    all_k_flat.extend(k_list)
                    if c_succ:
                        all_c_flat.extend(c_list)
                    else:
                        print(f"\n    [抢救成功] {row['name']} ({row['code']}): K线已入库，仅成份股遗失 ({err_msg})")
                else:
                    # 如果连 K 线都没拿到，记录彻底死亡
                    row['err_msg'] = err_msg
                    final_failed_sectors.append(row)
                    
            except Exception as e:
                row['err_msg'] = str(e)
                final_failed_sectors.append(row)
                
        sectors_to_fetch = final_failed_sectors

    # 打印最终死透的日志
    if sectors_to_fetch:
        print(f"\n[!] 极其严重警告: 仍有 {len(sectors_to_fetch)} 个板块的 K线 彻底获取失败 (历史数据缺失):")
        for s in sectors_to_fetch:
            print(f"    - {s['name']} ({s['code']}) | 失败原因: {s.get('err_msg', '未知')}")
    else:
        print("\n[+] 完美！所有板块的 K线 数据均已成功采集，无数据遗漏。")

    # 3. 清洗、时间线封锁与落库
    print(f"\n[*] 正在清洗合并数百万行数据并生成 Parquet 压缩文件...")
    cleaner = DataCleaner()
    today_dt = pd.Timestamp.now().normalize()
    
    if all_k_flat:
        full_k = pd.DataFrame(all_k_flat)
        full_k['date_dt'] = pd.to_datetime(full_k['date'].astype(str).str.strip(), errors='coerce')
        full_k = full_k[(full_k['date_dt'] <= today_dt) & (full_k['date_dt'].notnull())]
        full_k = full_k.drop(columns=['date_dt'])
        
        full_k = cleaner.clean_sector_kline(full_k)
        full_k.to_parquet(f"{OUTPUT_DIR}/sector_kline_full.parquet", index=False)
        print(f"[+] K线数据存储成功: {len(full_k)} 行 | 真实覆盖日期至 {full_k['date'].max()}")

    if all_c_flat:
        full_c = pd.DataFrame(all_c_flat)
        full_c['date'] = today_dt.strftime('%Y-%m-%d')
        full_c.to_parquet(f"{OUTPUT_DIR}/sector_constituents_latest.parquet", index=False)
        print(f"[+] 成份股关系存储成功: {len(full_c)} 条映射对")

    print(f"\n{'='*70}\n[*] 任务圆满完成 | 成功抓取 K线 板块数: {total_found - len(sectors_to_fetch)} | 耗时: {datetime.now() - start_time}\n{'='*70}\n")

if __name__ == "__main__":
    main()
