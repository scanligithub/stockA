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
    print("[*] 自愈级 A 股公告事件库: 历史【业绩预告】全量同步引擎 | 启动")
    print("="*70)
    
    # 1. 载入本地 Master 股票列表
    master_json_path = "stock_list_master.json"
    if not os.path.exists(master_json_path):
        print(f"❌ 严重错误：未在根目录下找到股票种子文件 {master_json_path}")
        return

    with open(master_json_path, 'r', encoding='utf-8') as f:
        master_list = json.load(f)

    # 2. 格式清洗
    valid_stocks = []
    for s in master_list:
        code = s['code'].replace('.', '').lower()
        prefix = code[:2]
        pure_num = code[2:]
        if prefix in ['sh', 'sz', 'bj']:
            valid_stocks.append(f"{prefix}.{pure_num}")

    total_stocks = len(valid_stocks)
    print(f"[+] 成功解析 Master 种子。全市场标的共计: {total_stocks} 只")

    # 3. 首次登录
    lg = bs.login()
    if lg.error_code != '0':
        print(f"❌ Baostock 登录失败: {lg.error_msg}")
        return
    print("✅ Baostock 底层 Socket 链路首次握手成功！")

    all_events = []
    success_count = 0
    
    start_date = "2010-01-01"
    end_date = time.strftime('%Y-%m-%d')
    
    print(f"🚀 开始全量扫描... (带有主动重连与自愈保护，预计耗时 8-9 分钟)")
    
    for idx, code in enumerate(valid_stocks):
        pure_code = code.split('.')[1]
        
        # 🛡️ 屏障 1：主动防封重连 (每 300 只股票主动切断并重建 TCP 会话)
        if idx > 0 and idx % 300 == 0:
            print(f"🔄 [主动重连] 已累计请求 {idx} 次，正在重建链路以防止服务器端阻断...")
            bs.logout()
            time.sleep(2.0)
            bs.login()
            
        # 🛡️ 屏障 2：微秒级物理冷却，极大降低服务器防火墙触发概率
        time.sleep(0.03) 
        
        # 🛡️ 屏障 3：被动异常自愈重试机制
        rs = None
        for attempt in range(3):
            try:
                rs = bs.query_forecast_report(code, start_date=start_date, end_date=end_date)
                if rs.error_code == '0':
                    break # 成功拿回，跳出重试循环
                else:
                    # 如果服务器返回错码（可能连接已断开），执行被动重连
                    time.sleep(2.0)
                    bs.login()
            except Exception as e:
                # 捕获 Broken pipe / Connection reset 等网络层异常，断线自愈
                print(f"\n📡 [网络自愈] 检测到连接中断: {e}。正在执行第 {attempt + 1} 次重置会话...")
                try: bs.logout()
                except: pass
                time.sleep(2.0)
                bs.login()

        if rs is None or rs.error_code != '0':
            # 三次重试均失败（极其罕见，可能停牌或代码失效），跳过该股
            continue
            
        # 数据解析
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
            
        if (idx + 1) % 500 == 0:
            print(f"   进度监控: [{idx + 1}/{total_stocks}] | 累计捕获预告公告: {len(all_events):,} 条")
            
    # 安全退出
    try: bs.logout()
    except: pass

    print("\n" + "="*70)
    print(f"✅ 全量扫描同步完毕！")
    print(f"    - 扫描标的总数: {total_stocks} 只")
    print(f"    - 存在历史预告标的: {success_count} 只")
    print(f"    - 累计捕获预告公告: {len(all_events):,} 条")
    print("="*70 + "\n")

    if all_events:
        print("💾 正在调用 Polars 强类型缩落盘...")
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
