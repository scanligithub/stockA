# FILE: scripts/fetch_events_forecast.py
import baostock as bs
import pandas as pd
import polars as pl
import os
import json
import time

def safe_float(val):
    try: return float(val)
    except: return 0.0

def main():
    start_time = time.time()
    print("\n" + "="*70)
    print("[*] 生产级 A 股公告事件库: 历史【业绩预告】全量深度同步引擎 | 启动")
    print("="*70)
    
    # 1. 载入本地 Master 股票列表
    master_json_path = "stock_list_master.json"
    if not os.path.exists(master_json_path):
        print(f"❌ 严重错误：未在根目录下找到股票种子文件 {master_json_path}")
        return

    with open(master_json_path, 'r', encoding='utf-8') as f:
        master_list = json.load(f)

    # 2. 清洗并标准化为 Baostock 格式 (如 "600519" -> "sh.600519")
    valid_stocks = []
    for s in master_list:
        code = s['code'].replace('.', '').lower()
        prefix = code[:2]
        pure_num = code[2:]
        if prefix in ['sh', 'sz', 'bj']:
            valid_stocks.append(f"{prefix}.{pure_num}")

    total_stocks = len(valid_stocks)
    print(f"[+] 成功解析 Master 种子。待同步全市场标的共计: {total_stocks} 只")

    # 3. 登录直连系统
    lg = bs.login()
    if lg.error_code != '0':
        print(f"❌ Baostock 登录失败: {lg.error_msg}")
        return
    print("✅ Baostock 底层 Socket 链路握手成功！")

    all_events = []
    success_count = 0
    
    # 设定历史拉取起点 (追溯 16 年全历史业绩预告，构建深厚特征湖)
    start_date = "2010-01-01"
    end_date = time.strftime('%Y-%m-%d')
    
    print(f"🚀 开始全量串行扫描... (预计耗时 7-8 分钟)")
    
    for idx, code in enumerate(valid_stocks):
        pure_code = code.split('.')[1]
        
        # 极速探测
        rs = bs.query_forecast_report(code, start_date=start_date, end_date=end_date)
        
        if rs.error_code != '0':
            continue
            
        records = []
        while (rs.error_code == '0') & rs.next():
            row = rs.get_row_data()
            notice_date = row[1]
            if not notice_date: continue
            
            min_val = safe_float(row[5])
            max_val = safe_float(row[6])
            yoy_mid = (min_val + max_val) / 2.0
            
            records.append({
                "code": pure_code,
                "notice_date": notice_date,
                "report_date": row[2],
                "forecast_type": row[3],
                "forecast_yoy_mid": yoy_mid,
                "summary": row[4]
            })
            
        if records:
            success_count += 1
            all_events.extend(records)
            
        # 进度打印监控
        if (idx + 1) % 500 == 0:
            print(f"   进度: [{idx + 1}/{total_stocks}] 标的 | 已捕获事件 {len(all_events):,} 条")
            
    # 登出
    bs.logout()

    print("\n" + "="*70)
    print(f"✅ 全量扫描同步完毕！")
    print(f"    - 扫描标的总数: {total_stocks} 只")
    print(f"    - 存在历史预告标的: {success_count} 只")
    print(f"    - 累计捕获预告公告: {len(all_events):,} 条")
    print("="*70 + "\n")

    if all_events:
        print("💾 正在调用 Polars 强类型引擎极致落盘...")
        schema = {
            "code": pl.Utf8, "notice_date": pl.Utf8, "report_date": pl.Utf8,
            "forecast_type": pl.Utf8, "forecast_yoy_mid": pl.Float64, "summary": pl.Utf8
        }
        df = pl.DataFrame(all_events, schema=schema).sort(["code", "notice_date"])
        
        os.makedirs("output", exist_ok=True)
        out_path = "output/event_earnings_forecast.parquet"
        df.write_parquet(out_path, compression="zstd")
        print(f"🎉 [生产事件库构建成功] 物理文件已落盘: {out_path} (大小: {os.path.getsize(out_path)/(1024*1024):.2f} MB)")
        print(f"⏱️ 任务总耗时: {time.time() - start_time:.1f} 秒")

if __name__ == "__main__":
    main()
