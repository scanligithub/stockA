import sys
import os
import pandas as pd
import concurrent.futures
from tqdm import tqdm
from datetime import datetime
import time

# 确保能正确加载项目本地模块
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
    
    # 1. 抓取板块日K线数据
    res_k = proxy.get_sector_kline(secid)
    df_k = pd.DataFrame()
    
    if res_k and res_k.get('data') and res_k['data'].get('klines'):
        rows = [x.split(',') for x in res_k['data']['klines']]
        # 对应 Proxy.get_sector_kline 中的 fields2: f51~f58
        cols = ['date','open','close','high','low','volume','amount','amplitude']
        df_k = pd.DataFrame([r[:8] for r in rows], columns=cols)
        
        df_k['code'] = code
        df_k['name'] = name
        df_k['type'] = info['type']

    # 2. 抓取板块成份股列表
    res_c = proxy.get_sector_constituents(code)
    consts = []
    if res_c and res_c.get('data') and res_c['data'].get('diff'):
        diff_data = res_c['data']['diff']
        # 兼容东财有时返回 dict (以索引为 key) 或 list 的情况
        items_list = diff_data.values() if isinstance(diff_data, dict) else diff_data
        for item in items_list:
            consts.append({
                "sector_code": code, 
                "stock_code": item['f12'], 
                "sector_name": name
            })
            
    return df_k, consts

def get_category_full_data_brute_force(fs_code, label):
    """
    【终极暴力包抄 V5】20 维度正反扫描，强行榨干最后一滴数据
    """
    print(f"[*] 正在对 {label} 分类执行 20 维度终极包抄...")
    seen_codes = {}
    
    # 扩展到 20 个不同视角的物理属性，彻底打碎中间地带的“死水板块”
    # f12:代码, f3:涨跌幅, f2:最新价, f6:成交额, f5:最高价, f4:最低价, f17:开盘价, f18:昨收价, f8:换手率, f10:量比
    # f15:最高价(备用), f16:最低价(备用), f11:涨跌额, f9:市盈率, f23:市净率, f20:总市值, f21:流通市值, f22:涨速, f24:60日涨跌幅, f25:年初至今涨跌幅
    fids = [
        "f12", "f3", "f2", "f6", "f5", "f4", "f17", "f18", "f8", "f10", 
        "f15", "f16", "f11", "f9", "f23", "f20", "f21", "f22", "f24", "f25"
    ]
    
    for fid in fids:
        for po in [1, 0]: # 每个维度分别请求正序和倒序的首屏
            try:
                # 依然死守 pz=100 的安全底线，绝不触碰网关截断红线
                items = proxy.get_sector_list(fs_code, fid=fid, po=po, pn=1, pz=100)
                if not items: 
                    continue
                
                new_in_round = 0
                for x in items:
                    c = x['f12']
                    if c not in seen_codes:
                        seen_codes[c] = {
                            "code": c, "market": x['f13'], 
                            "name": x['f14'], "type": label
                        }
                        new_in_round += 1
                
                if new_in_round > 0:
                    print(f"    - [维度 {fid:<3} po={po}] 吐出: {len(items):<3} | 新增: {new_in_round:<3} | 累计: {len(seen_codes)}")
                
                # 饱和阈值：行业极限约 496，概念约 483。当累计数量达到该值时，提前结束无意义的扫描
                if len(seen_codes) >= 505: 
                    print(f"    [+] {label} 分类已达到饱和阈值，提前结束该分类扫描。")
                    break
                
                # 极短休眠，既保护 API 又保证执行速度
                time.sleep(0.05) 
            except Exception as e:
                # 记录但不中断，继续下一个维度的扫描
                pass
        
        # 外层循环也要判断是否跳出
        if len(seen_codes) >= 505: 
            break
            
    return list(seen_codes.values())

def main():
    start_time = datetime.now()
    print(f"\n{'='*70}")
    print(f"[*] 东方财富板块终极全量采集引擎 (V5版) | {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"{'='*70}\n")
    
    # 1. 维度包抄获取全量板块列表
    targets = {
        "Industry": "m:90 t:2", 
        "Concept":  "m:90 t:3", 
        "Region":   "m:90 t:1"
    }
    
    all_sectors = []
    for label, fs in targets.items():
        all_sectors.extend(get_category_full_data_brute_force(fs, label))

    # 去重
    df_list = pd.DataFrame(all_sectors).drop_duplicates('code')
    total_found = len(df_list)
    print(f"\n[*] 审计报告：去重后唯一板块总数: {total_found}")

    # 现在的目标是逼近 1010，所以我们将熔断阈值提高到 980
    if total_found < 980:
        print(f"[🔥 严重警告] 抓取总数 {total_found} 仍低于预期，疑似接口出现重大变更或严厉封禁，停止后续采集防污染。")
        sys.exit(1)

    # 2. 并发采集板块 K 线与成份股详情
    print(f"\n[*] 开始并发采集 {total_found} 个板块的历史数据 (并发线程: 20)...")
    all_k, all_c = [], []
    
    # 使用 ThreadPoolExecutor 进行 IO 密集型并发
    with concurrent.futures.ThreadPoolExecutor(max_workers=20) as executor:
        future_to_name = {
            executor.submit(fetch_one_sector, row): row['name'] 
            for _, row in df_list.iterrows()
        }
        
        # 实时显示采集进度
        for future in tqdm(concurrent.futures.as_completed(future_to_name), 
                          total=len(future_to_name), desc="全量详情采集进度"):
            try:
                k, c = future.result()
                if not k.empty: all_k.append(k)
                if c: all_c.extend(c)
            except Exception as e:
                # 个别板块采集失败不影响整体流水线
                pass

    # 3. 数据清洗、脏数据过滤与持久化落库
    print(f"\n[*] 正在清洗数据并生成 Parquet 压缩文件...")
    cleaner = DataCleaner()
    today_str = datetime.now().strftime('%Y-%m-%d')
    
    if all_k:
        full_k = pd.concat(all_k)
        
        # [核心防线]：物理剔除因接口测试或异常返回的未来日期数据 (例如 2026 年的数据)
        full_k = full_k[full_k['date'] <= today_str]
        
        # 调用自定义的 Cleaner 进行类型转换
        full_k = cleaner.clean_sector_kline(full_k)
        
        k_path = f"{OUTPUT_DIR}/sector_kline_full.parquet"
        full_k.to_parquet(k_path, index=False)
        print(f"[+] K线数据存储成功: {len(full_k)} 行 | 覆盖日期至 {full_k['date'].max()}")

    if all_c:
        full_c = pd.DataFrame(all_c)
        full_c['date'] = today_str
        
        c_path = f"{OUTPUT_DIR}/sector_constituents_latest.parquet"
        full_c.to_parquet(c_path, index=False)
        print(f"[+] 成份股关系存储成功: {len(full_c)} 条映射对")

    end_time = datetime.now()
    print(f"\n{'='*70}")
    print(f"[*] 任务圆满完成 | 最终入库板块数: {total_found} | 总耗时: {end_time - start_time}")
    print(f"{'='*70}\n")

if __name__ == "__main__":
    main()
