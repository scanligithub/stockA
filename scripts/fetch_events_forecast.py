# FILE: scripts/fetch_events_forecast.py
import socket
# 强制设置全局 TCP 连接与接收超时为 15 秒，彻底封杀 Baostock 底层死锁卡死
socket.setdefaulttimeout(15.0)

import baostock as bs
import polars as pl
import os
import json
import time
import argparse

def safe_float(val):
    try: return float(val)
    except: return 0.0

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--index", type=int, required=True)
    parser.add_argument("--codes", type=str, required=True)
    args = parser.parse_args()

    # 1. 解析当前节点负责的股票切片
    codes = json.loads(args.codes)
    total_stocks = len(codes)
    
    print(f"\n" + "="*70, flush=True)
    print(f"🚀 [Job {args.index}] 分布式事件抓取节点启动 | 负责标的: {total_stocks} 只", flush=True)
    print("="*70, flush=True)

    try: bs.login()
    except: pass

    all_events = []
    start_date = "2005-01-01"
    end_date = time.strftime('%Y-%m-%d')
    
    for idx, code in enumerate(codes):
        pure_code = code.split('.')[1]
        
        # 🛡️ 防卡死连接池自愈：每 100 只股强制重置连接
        if idx > 0 and idx % 100 == 0:
            try: bs.logout()
            except: pass
            time.sleep(1.5)
            try: bs.login()
            except: pass

        stock_success = False
        stock_events = []

        # 🔄 单股整体级 3 次重试闭环（涵盖：接口呼叫 + 数据流逐行拉取）
        for attempt in range(3):
            try:
                rs = bs.query_forecast_report(code, start_date=start_date, end_date=end_date)
                
                if rs is None or rs.error_code != '0':
                    # 若接口响应不正常，强制重建 Socket 链路
                    time.sleep(1.0)
                    try: bs.logout()
                    except: pass
                    bs.login()
                    continue  # 进入下一次尝试

                # 逐行读取数据流（若在此发生异常，同样会被 Exception 捕获并重试）
                temp_records = []
                while (rs.error_code == '0') & rs.next():
                    row = rs.get_row_data()
                    notice_date = row[1]
                    if not notice_date: 
                        continue
                    yoy_mid = (safe_float(row[5]) + safe_float(row[6])) / 2.0
                    temp_records.append({
                        "code": pure_code,
                        "notice_date": notice_date,
                        "report_date": row[2],
                        "forecast_type": row[3],
                        "forecast_yoy_mid": yoy_mid,
                        "summary": row[4]
                    })
                
                # 数据拉取与解析均无异常，标记成功并跳出重试
                stock_events = temp_records
                stock_success = True
                break

            except Exception as e:
                # 捕获包括超时、网络瞬断在内的所有异常，重建链路后等待下一次重试
                print(f"\n📡 [Job {args.index} - Socket重试] {code} 在第 {attempt + 1} 次尝试时触发异常: {e}. 正在重建链路...", flush=True)
                try: bs.logout()
                except: pass
                time.sleep(2.0)
                try: bs.login()
                except: pass

        # 判定最终合并状态
        if stock_success:
            all_events.extend(stock_events)
        else:
            print(f"\n⚠️ [Job {args.index}] 警告: {code} 连续 3 次下载解析均告失败，已自动越过该股以保护整体流水线。", flush=True)
            
        # 📈 实时进度日志：每处理 50 只股票或到达末尾时，强制打印进度
        if (idx + 1) % 50 == 0 or (idx + 1) == total_stocks:
            print(f"   [Job {args.index}] 进度监控: [{idx + 1}/{total_stocks}] | 本节点累计捕获预告: {len(all_events):,} 条", flush=True)
            
    try: bs.logout()
    except: pass

    print(f"\n✅ [Job {args.index}] 扫描完毕。共计捕获预告公告: {len(all_events)} 条", flush=True)

    # 3. 强类型 Parquet 分片落盘
    if all_events:
        schema = {
            "code": pl.Utf8, "notice_date": pl.Utf8, "report_date": pl.Utf8,
            "forecast_type": pl.Utf8, "forecast_yoy_mid": pl.Float64, "summary": pl.Utf8
        }
        df = pl.DataFrame(all_events, schema=schema).sort(["code", "notice_date"])
        
        os.makedirs("temp_parts", exist_ok=True)
        out_path = f"temp_parts/event_forecast_part_{args.index}.parquet"
        df.write_parquet(out_path, compression="zstd")
        print(f"💾 分片保存成功: {out_path}", flush=True)
    else:
        print(f"⚠️ [Job {args.index}] 未捕获到任何有效事件，跳过落盘。", flush=True)

if __name__ == "__main__":
    main()
