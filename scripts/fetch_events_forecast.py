# FILE: scripts/fetch_events_forecast.py
import socket
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
    
    print(f"\n" + "="*50, flush=True)
    print(f"🚀 [Job {args.index}] 分布式节点启动 | 负责标的: {total_stocks} 只", flush=True)
    print("="*50, flush=True)

    try: bs.login()
    except: pass

    all_events = []
    start_date = "2005-01-01"
    end_date = time.strftime('%Y-%m-%d')
    
    for idx, code in enumerate(codes):
        pure_code = code.split('.')[1]
        
        # 防卡死：每 100 只股强制重置连接
        if idx > 0 and idx % 100 == 0:
            try: bs.logout()
            except: pass
            time.sleep(1.5)
            try: bs.login()
            except: pass

        rs = None
        for attempt in range(3):
            try:
                rs = bs.query_forecast_report(code, start_date=start_date, end_date=end_date)
                if rs is not None and rs.error_code == '0': break
                time.sleep(1.0)
                try: bs.logout()
                except: pass
                bs.login()
            except:
                try: bs.logout()
                except: pass
                time.sleep(2.0)
                try: bs.login()
                except: pass

        if rs is None or rs.error_code != '0':
            continue
            
        try:
            while (rs.error_code == '0') & rs.next():
                row = rs.get_row_data()
                notice_date = row[1]
                if not notice_date: continue
                
                yoy_mid = (safe_float(row[5]) + safe_float(row[6])) / 2.0
                all_events.append({
                    "code": pure_code,
                    "notice_date": notice_date,
                    "report_date": row[2],
                    "forecast_type": row[3],
                    "forecast_yoy_mid": yoy_mid,
                    "summary": row[4]
                })
        except:
            continue
            
    try: bs.logout()
    except: pass

    print(f"\n✅ [Job {args.index}] 扫描完毕。捕获预告公告: {len(all_events)} 条", flush=True)

    # 落盘为该节点的专属小分片
    if all_events:
        schema = {
            "code": pl.Utf8, "notice_date": pl.Utf8, "report_date": pl.Utf8,
            "forecast_type": pl.Utf8, "forecast_yoy_mid": pl.Float64, "summary": pl.Utf8
        }
        df = pl.DataFrame(all_events, schema=schema).sort(["code", "notice_date"])
        
        os.makedirs("temp_parts", exist_ok=True)
        out_path = f"temp_parts/event_forecast_part_{args.index}.parquet"
        df.write_parquet(out_path, compression="zstd")
        print(f"💾 分片已保存: {out_path}")

if __name__ == "__main__":
    main()
