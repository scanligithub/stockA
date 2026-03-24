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

# ==========================================
# 第一阶段：三步式获取完整板块列表和分类 (交由独立模块完成)
# ==========================================


# ==========================================
# 第二阶段：并发抓取详细 K 线与成份股 (支持失败多轮补抓 + 终极单线降级)
# ==========================================

def fetch_one_sector(info):
    """
    内存优化：直接返回原生字典列表，避免在多线程中频繁实例化 DataFrame
    返回值: (success: bool, k_data: list, consts: list, error_msg: str)
    """
    code, name = info['code'], info['name']
    secid = f"90.{code}"
    
    try:
        # --- 1. 获取 K 线 ---
        res_k = proxy.get_sector_kline(secid)
        if res_k is None:
            return False, [], [], "K线 API 网络层超时或无响应"
            
        k_data = []
        if res_k.get('data') and res_k['data'].get('klines'):
            for row_str in res_k['data']['klines']:
                r = row_str.split(',')
                # 扁平化字典，极大降低内存碎片
                k_data.append({
                    'date': r[0], 'open': r[1], 'close': r[2], 'high': r[3], 'low': r[4], 
                    'volume': r[5], 'amount': r[6], 'amplitude': r[7],
                    'code': code, 'name': name, 'type': info['type']
                })
        elif res_k.get('data') is None:
            # 如果完全没有 data 节点，说明是被拦截或接口变更，标记为失败
            return False, [], [], f"K线 API 被拦截或无数据: rc={res_k.get('rc', 'unknown')}"

        # --- 2. 获取成份股 ---
        res_c = proxy.get_sector_constituents(code)
        if res_c is None:
            return False, [], [], "成份股 API 网络层超时或无响应"
            
        consts = []
        if res_c.get('data') and res_c['data'].get('diff'):
            diff_data = res_c['data']['diff']
            items_list = diff_data.values() if isinstance(diff_data, dict) else diff_data
            for item in items_list:
                consts.append({"sector_code": code, "stock_code": item['f12'], "sector_name": name})
        elif res_c.get('data') is None:
             return False, [], [], f"成份股 API 被拦截或无数据: rc={res_c.get('rc', 'unknown')}"
             
        # 全部成功获取，返回数据
        return True, k_data, consts, ""
        
    except Exception as e:
        # 捕获未知异常（如 JSON 解析错误、字段缺失等）
        return False, [], [], f"发生本地异常: {str(e)}"

def main():
    start_time = datetime.now()
    print(f"\n{'='*70}\n[*] 东方财富极速全量引擎 (高可靠降级版) | {start_time}\n{'='*70}\n")
    
    # 1. 三步式获取完整板块列表和分类
    all_sectors = build_sector_catalog(proxy)

    df_list = pd.DataFrame(all_sectors).drop_duplicates('code')
    total_found = len(df_list)
    print(f"\n[*] 审计报告：去重后唯一板块总数: {total_found}")

    if total_found < 950:
        print(f"[🔥 严重警告] 抓取总数 {total_found} 偏低，停止后续采集防污染。")
        sys.exit(1)

    # 2. 具备重试机制的并发采集池
    sectors_to_fetch = df_list.to_dict('records')
    all_k_flat, all_c_flat = [], []
    MAX_RETRIES = 3
    
    # 【阶段 2.1】：高并发轮询抓取
    for attempt in range(MAX_RETRIES):
        if not sectors_to_fetch:
            break
            
        print(f"\n[*] 开始第 {attempt + 1} 轮采集，等待任务: {len(sectors_to_fetch)} 个 (并发: 20)...")
        failed_sectors = []
        
        with concurrent.futures.ThreadPoolExecutor(max_workers=20) as executor:
            future_map = {executor.submit(fetch_one_sector, row): row for row in sectors_to_fetch}
            
            for future in tqdm(concurrent.futures.as_completed(future_map), total=len(future_map), desc=f"第 {attempt + 1} 轮进度"):
                row = future_map[future]
                try:
                    success, k_list, c_list, err_msg = future.result()
                    if success:
                        if k_list: all_k_flat.extend(k_list)
                        if c_list: all_c_flat.extend(c_list)
                    else:
                        failed_sectors.append(row)
                except Exception:
                    failed_sectors.append(row)
                    
        sectors_to_fetch = failed_sectors
        if sectors_to_fetch and attempt < MAX_RETRIES - 1:
            print(f"[-] 警告: 本轮有 {len(sectors_to_fetch)} 个板块抓取失败，暂停 3 秒后发起补抓...")
            time.sleep(3)

    # 【阶段 2.2】：终极降级单线顺序抓取 (规避频控)
    if sectors_to_fetch:
        print(f"\n[!] 触发防御降级策略：剩余 {len(sectors_to_fetch)} 个顽固板块，启动单线低速补抓...")
        final_failed_sectors = []
        
        for row in tqdm(sectors_to_fetch, desc="终极单线补抓"):
            # 强制休眠 1 秒，彻底打消目标服务器的并发防卫机制
            time.sleep(1)
            try:
                success, k_list, c_list, err_msg = fetch_one_sector(row)
                if success:
                    if k_list: all_k_flat.extend(k_list)
                    if c_list: all_c_flat.extend(c_list)
                else:
                    row['err_msg'] = err_msg # 记录死因
                    final_failed_sectors.append(row)
            except Exception as e:
                row['err_msg'] = str(e)
                final_failed_sectors.append(row)
                
        sectors_to_fetch = final_failed_sectors

    # 打印最终失败日志
    if sectors_to_fetch:
        print(f"\n[!] 极其严重警告: 经过终极降级补抓，仍有 {len(sectors_to_fetch)} 个板块彻底失败 (数据已遗失):")
        for s in sectors_to_fetch:
            # 此时打印出 error_msg，方便分析到底是不是真的没有数据
            print(f"    - {s['name']} ({s['code']}) | 失败原因: {s.get('err_msg', '未知')}")
    else:
        print("\n[+] 完美！所有板块数据均已成功采集入列。")

    # 3. 清洗、时间线封锁与落库
    print(f"\n[*] 正在清洗合并数百万行数据并生成 Parquet 压缩文件...")
    cleaner = DataCleaner()
    today_dt = pd.Timestamp.now().normalize()
    
    if all_k_flat:
        # 一次性建表，速度极快
        full_k = pd.DataFrame(all_k_flat)
        
        # [核心防线]：物理切断所有未来日期数据 (例如 2026 年)
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

    print(f"\n{'='*70}\n[*] 任务圆满完成 | 最终入库板块数: {total_found - len(sectors_to_fetch)} | 耗时: {datetime.now() - start_time}\n{'='*70}\n")

if __name__ == "__main__":
    main()
