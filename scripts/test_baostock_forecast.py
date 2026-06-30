# FILE: scripts/test_baostock_forecast.py
import baostock as bs
import pandas as pd
import polars as pl
import os
import time

TEST_STOCKS = [
    "sh.600519", "sz.300750", "sz.002594", "sz.002460", "sh.688041",
    "sz.002371", "sz.300438", "sh.601127", "sz.000938", "sh.600111",
    "sz.300274", "sz.000002", "sh.600418", "sz.002156", "sh.603501"
]

def safe_float(val):
    try: return float(val)
    except: return 0.0

def main():
    print("\n" + "="*70)
    print("🧪 Baostock [业绩预告事件] 直连测试 (Socket 底层协议，免疫 WAF) | 启动")
    print("="*70)
    
    # 1. 登录 Baostock 系统 (TCP 直连)
    lg = bs.login()
    if lg.error_code != '0':
        print(f"❌ Baostock 登录失败: {lg.error_msg}")
        return
    print("✅ Baostock 系统登录成功！")

    all_events = []
    success_count = 0
    
    # 设定拉取的历史时间跨度 (最近 15 年)
    start_date = "2010-01-01"
    end_date = time.strftime('%Y-%m-%d')
    
    print(f"🚀 开始拉取 {len(TEST_STOCKS)} 只股票的业绩预告 ({start_date} ~ 至今)...")
    
    # Baostock 不建议多线程并发，采用单线程极速循环即可
    for code in TEST_STOCKS:
        pure_code = code.split('.')[1]
        
        # 核心 API 调用
        rs = bs.query_forecast_report(code, start_date=start_date, end_date=end_date)
        
        if rs.error_code != '0':
            print(f"⚠️ {code} 拉取异常: {rs.error_msg}")
            continue
            
        records = []
        while (rs.error_code == '0') & rs.next():
            row = rs.get_row_data()
            # Baostock 返回字段: 
            # 0:code, 1:profitForcastExpPubDate(公告日), 2:profitForcastExpStatDate(报告期), 
            # 3:profitForcastType(类型), 4:profitForcastAbstract(摘要), 
            # 5:profitChangeMin(下限), 6:profitChangeMax(上限)
            
            notice_date = row[1]
            if not notice_date: continue
            
            # 计算增速中枢
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
            print(f"  - {code} 成功拉取 {len(records)} 条预告")
        else:
            print(f"  - {code} 历史上未发预告")
            
    # 登出系统
    bs.logout()

    print("\n" + "="*70)
    print(f"✅ 测试完成！成功获取 {success_count}/{len(TEST_STOCKS)} 只股票的数据。")
    print(f"📊 累计捕获历史业绩预告事件: {len(all_events)} 条")
    print("="*70 + "\n")

    if all_events:
        schema = {
            "code": pl.Utf8, "notice_date": pl.Utf8, "report_date": pl.Utf8,
            "forecast_type": pl.Utf8, "forecast_yoy_mid": pl.Float64, "summary": pl.Utf8
        }
        df = pl.DataFrame(all_events, schema=schema).sort(["code", "notice_date"])
        
        print("👀 数据抽样预览 (截取最后 5 条):")
        print(df.tail(5))
        
        os.makedirs("output_test", exist_ok=True)
        out_path = "output_test/baostock_forecast_test.parquet"
        df.write_parquet(out_path, compression="zstd")
        print(f"\n💾 物理落盘测试成功: {out_path}")

if __name__ == "__main__":
    main()
