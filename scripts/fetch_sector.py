import sys
import os
import pandas as pd
import concurrent.futures
from tqdm import tqdm
from datetime import datetime
import time

# 加载项目本地模块
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
    code, name = info['code'], info['name']
    secid = f"90.{code}"
    
    res_k = proxy.get_sector_kline(secid)
    df_k = pd.DataFrame()
    
    if res_k and res_k.get('data') and res_k['data'].get('klines'):
        rows = [x.split(',') for x in res_k['data']['klines']]
        # 对应 Proxy 中的 f51~f58 8个字段
        cols = ['date','open','close','high','low','volume','amount','amplitude']
        df_k = pd.DataFrame([r[:8] for r in rows], columns=cols)
        df_k['code'], df_k['name'], df_k['type'] = code, name, info['type']

    res_c = proxy.get_sector_constituents(code)
    consts = []
    if res_c and res_c.get('data') and res_c['data'].get('diff'):
        diff_data = res_c['data']['diff']
        items = diff_data.values() if isinstance(diff_data, dict) else diff_data
        for item in items:
            consts.append({"sector_code": code, "stock_code": item['f12'], "sector_name": name})
            
    return df_k, consts

def get_category_full_data(fs_code, label):
    """
    【对冲抓取】正向2页 + 反向2页，确保覆盖 400+ 记录
    """
    print(f"[*] 正在深度扫描 {label} 分类...")
    results = []
    seen_codes = set()

    # 定义抓取维度：(排序po, 页码pn)
    # 通过正反双向抓取，即使 pn=2 偶尔失败，也能靠另一头的 pn=2 补齐
    dimensions = [
        (1, 1), (1, 2), (1, 3), # 正向排名的前 300
        (0, 1), (0, 2), (0, 3)  # 反向排名的前 300
    ]

    for po, pn in dimensions:
        try:
            items = proxy.get_sector_list(fs_code, po=po, pn=pn, pz=100)
            if not items:
                continue
            
            new_count = 0
            for x in items:
                code = x['f12']
                if code not in seen_codes:
                    results.append({
                        "code": code, "market": x['f13'], "name": x['f14'], "type": label
                    })
                    seen_codes.add(code)
                    new_count += 1
            
            if new_count > 0:
                print(f"    - [维度 po={po}, pn={pn}] 新获: {new_count} 条 | 累计: {len(results)}")
            
            # 短暂休眠，规避 IP 频控
            time.sleep(0.3)
        except Exception as e:
            print(f"    [!] 维度 po={po}, pn={pn} 请求失败: {e}")

    return results

def main():
    start_time = datetime.now()
    print(f"\n{'='*70}")
    print(f"[*] 工业级板块数据全量扫描引擎 | {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"{'='*70}\n")

    targets = {"Industry": "m:90 t:2", "Concept": "m:90 t:3", "Region": "m:90 t:1"}
    all_sectors = []

    # 1. 多维扫描
    for label, fs in targets.items():
        cat_data = get_category_full_data(fs, label)
        all_sectors.extend(cat_data)

    df_list = pd.DataFrame(all_sectors).drop_duplicates('code')
    total_found = len(df_list)
    
    print(f"\n[*] 审计报告：去重后唯一板块总数: {total_found}")
    
    # 调整熔断阈值：根据经验，三类板块总和至少应在 950 以上
    if total_found < 900:
        print(f"[🔥 严重错误] 最终总数 {total_found} 远低于预期，数据残缺，强制中断。")
        sys.exit(1)

    # 2. 并发抓取 K 线与成份股
    print(f"\n[*] 开始并发采集 {total_found} 个板块详情 (并发: 20)...")
    all_k, all_c = [], []
    with concurrent.futures.ThreadPoolExecutor(max_workers=20) as executor:
        future_map = {executor.submit(fetch_one_sector, row): row['name'] for _, row in df_list.iterrows()}
        for future in tqdm(concurrent.futures.as_completed(future_map), total=len(future_map), desc="总进度"):
            try:
                k, c = future.result()
                if not k.empty: all_k.append(k)
                if c: all_c.extend(c)
            except: pass

    # 3. 数据清洗与落库
    cleaner = DataCleaner()
    today = datetime.now().strftime('%Y-%m-%d')
    
    if all_k:
        full_k = pd.concat(all_k)
        # 剔除未来脏数据
        full_k = full_k[full_k['date'] <= today]
        full_k = cleaner.clean_sector_kline(full_k)
        full_k.to_parquet(f"{OUTPUT_DIR}/sector_kline_full.parquet", index=False)
        print(f"\n[+] K线数据已更新: {len(full_k)} 行")

    if all_c:
        full_c = pd.DataFrame(all_c)
        full_c['date'] = today
        full_c.to_parquet(f"{OUTPUT_DIR}/sector_constituents_latest.parquet", index=False)
        print(f"[+] 成份股关系已更新: {len(full_c)} 条")

    end_time = datetime.now()
    print(f"\n{'='*70}")
    print(f"[*] 任务圆满完成 | 总耗时: {end_time - start_time}")
    print(f"{'='*70}\n")

if __name__ == "__main__":
    main()
