# FILE: scripts/fetch_events_forecast.py

# 🚨 极其关键：必须在文件最顶部、导入任何库之前执行！
# 强制设置全局 TCP 连接与接收超时为 15 秒。
# 彻底封杀 Baostock 底层因网络丢包导致的无限期死锁卡死。
import socket
socket.setdefaulttimeout(15.0)

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
    # 🎯 强制设置 flush=True，确保日志实时打印，绝不留存在缓冲区中
    print("\n" + "="*70, flush=True)
    print("[*] 自愈级 A 股公告事件库: 历史【业绩预告】全量同步引擎 | 启动", flush=True)
    print("="*70, flush=True)
    
    # 1. 载入本地 Master 股票列表
    master_json_path = "stock_list_master.json"
    if not os.path.exists(master_json_path):
        print(f"❌ 严重错误：未在根目录下找到股票种子文件 {master_json_path}", flush=True)
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
    print(f"[+] 成功解析 Master 种子。全市场标的共计: {total_stocks} 只", flush=True)

    # 3. 首次登录（带超时保护与异常捕获）
    print("[*] 正在尝试建立与 Baostock 远程服务器的 Socket 首次握手...", flush=True)
    try:
        lg = bs.login()
        if lg.error_code != '0':
            print(f"❌ Baostock 首次握手失败: {lg.error_msg}", flush=True)
            return
        print("✅ Baostock 底层 Socket 链路首次握手成功！", flush=True)
    except Exception as e:
        print(f"❌ 首次握手发生 Socket 异常/超时: {e}。程序将尝试在循环中自愈重试...", flush=True)

    all_events = []
    success_count = 0
    
    start_date = "2010-01-01"
    end_date = time.strftime('%Y-%m-%d')
    
    print(f"🚀 开始全量扫描... (带主动断开重连与 15s 强制超时锁，预计耗时 8-9 分钟)", flush=True)
    
    for idx, code in enumerate(valid_stocks):
        pure_code = code.split('.')[1]
        
        # 4. 主动防卡死：每 400 只股票强制完全销毁并重建 Socket，释放服务器端连接池
        if idx > 0 and idx % 400 == 0:
            print(f"\n🔄 [连接池维护] 已扫描 {idx} 只，主动重建 Socket 连接...", flush=True)
            try: bs.logout()
            except: pass
            time.sleep(2.0)
            for reconn in range(3):
                try:
                    bs.login()
                    break
                except Exception as ex:
                    print(f"⚠️ 连接池重建失败，尝试第 {reconn+1} 次重连: {ex}", flush=True)
                    time.sleep(3.0)

        # 5. 单股事件抓取（多重超时与重试自愈）
        rs = None
        for attempt in range(3):
            try:
                rs = bs.query_forecast_report(code, start_date=start_date, end_date=end_date)
                if rs is not None and rs.error_code == '0':
                    break # 成功拿到结果，跳出重试
                else:
                    # 遭遇服务器端返回异常码，重置连接
                    time.sleep(1.5)
                    try: bs.logout()
                    except: pass
                    bs.login()
            except Exception as e:
                # 🎯 核心：任何超时 (Timeout) 或连接中断在此被捕获，强制 logout 后重新 login 抢救
                print(f"\n📡 [Socket自愈] {code} 触发异常/15秒超时 [{e}]。正在执行第 {attempt + 1} 次自愈重连...", flush=True)
                try: bs.logout()
                except: pass
                time.sleep(2.5)
                try: bs.login()
                except: pass

        if rs is None or rs.error_code != '0':
            continue
            
        # 数据解析
        records = []
        try:
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
        except Exception as parse_err:
            # 防止解析单只股票数据时发生不可预知的 socket 截断崩溃
            print(f"\n⚠️ 解析 {code} 时发生异常: {parse_err}，已自动跳过该股", flush=True)
            continue
            
        if records:
            success_count += 1
            all_events.extend(records)
            
        if (idx + 1) % 500 == 0:
            print(f"   进度监控: [{idx + 1}/{total_stocks}] | 累计捕获预告公告: {len(all_events):,} 条", flush=True)
            
    # 安全退出
    try: bs.logout()
    except: pass

    print("\n" + "="*70, flush=True)
    print(f"✅ 全量扫描同步完毕！", flush=True)
    print(f"    - 扫描标的总数: {total_stocks} 只", flush=True)
    print(f"    - 存在历史预告标的: {success_count} / {total_stocks}", flush=True)
    print(f"    - 累计捕获预告公告: {len(all_events):,} 条", flush=True)
    print("="*70 + "\n", flush=True)

    if all_events:
        print("💾 正在调用 Polars 强类型压缩落盘...", flush=True)
        schema = {
            "code": pl.Utf8, "notice_date": pl.Utf8, "report_date": pl.Utf8,
            "forecast_type": pl.Utf8, "forecast_yoy_mid": pl.Float64, "summary": pl.Utf8
        }
        df = pl.DataFrame(all_events, schema=schema).sort(["code", "notice_date"])
        
        os.makedirs("output", exist_ok=True)
        out_path = "output/event_earnings_forecast.parquet"
        df.write_parquet(out_path, compression="zstd")
        print(f"🎉 [生产事件库构建成功] 物理文件已落盘: {out_path} (大小: {os.path.getsize(out_path)/(1024*1024):.2f} MB)", flush=True)
        print(f"⏱️ 任务总耗时: {time.time() - start_time:.1f} 秒", flush=True)

if __name__ == "__main__":
    main()
