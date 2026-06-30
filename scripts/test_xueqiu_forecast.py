# FILE: scripts/test_xueqiu_forecast.py
import os
import time
import requests
import polars as pl
from concurrent.futures import ThreadPoolExecutor, as_completed

# 模拟标准浏览器头部
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept": "application/json, text/plain, */*",
    "Referer": "https://xueqiu.com/",
    "Connection": "keep-alive"
}

# 选取 15 只具有代表性、且历史上频繁发布业绩预告的 A 股标的进行测试
TEST_STOCKS = [
    "SH600519", # 贵州茅台 (主板白马)
    "SZ300750", # 宁德时代 (创业板龙头)
    "SZ002594", # 比亚迪 
    "SZ002460", # 赣锋锂业 (强周期，业绩波动大，预告多)
    "SH688041", # 海光信息 (科创板)
    "SZ002371", # 北方华创 (半导体)
    "SZ300438", # 鹏辉能源 (储能)
    "SH601127", # 赛力斯 (汽车反转扭亏代表)
    "SZ000938", # 紫光股份 (算力)
    "SH600111", # 北方稀土 
    "SZ300274", # 阳光电源 
    "SZ000002", # 万科A (地产，近年常发预减/首亏)
    "SH600418", # 江淮汽车
    "SZ002156", # 通富微电
    "SH603501"  # 韦尔股份
]

def ms_to_date_str(ms):
    """将雪球的时间戳转换为标准 YYYY-MM-DD"""
    if not ms: return None
    try:
        return time.strftime('%Y-%m-%d', time.localtime(int(ms) / 1000))
    except:
        return None

def fetch_single_forecast(session, raw_code):
    """拉取单只股票雪球业绩预告明细"""
    pure_code = raw_code[2:]
    
    # 雪球 V5 业绩预告 API
    url = f"https://stock.xueqiu.com/v5/stock/finance/cn/forecast.json?symbol={raw_code}&count=150"
    
    for attempt in range(3):
        try:
            res = session.get(url, timeout=10)
            if res.status_code == 200:
                data = res.json()
                if data.get("error_code") == 0 and data.get("data"):
                    return pure_code, data["data"].get("list", []), None
                else:
                    return pure_code, [], data.get("error_description", "API Unknown Error")
            time.sleep(1.0)
        except Exception as e:
            time.sleep(1.0)
    return pure_code, [], "Max retries exceeded"

def main():
    print("\n" + "="*70)
    print("🧪 雪球 V5 [业绩预告事件] 独立隔离测试 | 启动")
    print("="*70)
    
    session = requests.Session()
    session.headers.update(HEADERS)
    
    xq_token = os.getenv("XQ_A_TOKEN", "").strip()
    if xq_token:
        print("🔑 检测到 XQ_A_TOKEN 安全凭证，正在注入 Session...")
        session.cookies.set("xq_a_token", xq_token, domain=".xueqiu.com")
    else:
        print("❌ 严重错误: 未检测到环境变量中的 XQ_A_TOKEN 密钥，测试终止。")
        return
        
    all_events = []
    success_count = 0
    
    print(f"🚀 开始并发拉取 {len(TEST_STOCKS)} 只样本股票的业绩预告...")
    
    with ThreadPoolExecutor(max_workers=10) as executor:
        futures = {executor.submit(fetch_single_forecast, session, s): s for s in TEST_STOCKS}
        for future in as_completed(futures):
            pure_code, records, err = future.result()
            
            if records:
                success_count += 1
                for rec in records:
                    notice_date = ms_to_date_str(rec.get("notice_date"))
                    report_date = ms_to_date_str(rec.get("report_date"))
                    
                    if not notice_date: continue
                    
                    # 计算预告增速的中枢值
                    yoy_min = rec.get("yoy_min")
                    yoy_max = rec.get("yoy_max")
                    try:
                        yoy_min_val = float(yoy_min) if yoy_min is not None else 0.0
                        yoy_max_val = float(yoy_max) if yoy_max is not None else 0.0
                        yoy_mid = (yoy_min_val + yoy_max_val) / 2.0
                    except:
                        yoy_mid = 0.0

                    all_events.append({
                        "code": pure_code,
                        "notice_date": notice_date,
                        "report_date": report_date,
                        "forecast_type": str(rec.get("type_name", "")),
                        "forecast_yoy_mid": yoy_mid,
                        "summary": str(rec.get("summary", ""))
                    })
            elif err:
                print(f"⚠️ 股票 {pure_code} 抓取失败: {err}")

    print("\n" + "="*70)
    print(f"✅ 测试完成！成功抓取 {success_count}/{len(TEST_STOCKS)} 只股票。")
    print(f"📊 累计捕获历史业绩预告: {len(all_events)} 条")
    print("="*70 + "\n")

    if all_events:
        schema = {
            "code": pl.Utf8, "notice_date": pl.Utf8, "report_date": pl.Utf8, 
            "forecast_type": pl.Utf8, "forecast_yoy_mid": pl.Float64, "summary": pl.Utf8
        }
        df = pl.DataFrame(all_events, schema=schema).sort(["code", "notice_date"])
        
        # 打印部分数据预览
        print("👀 数据预览 (截取最后 10 条):")
        print(df.tail(10))
        
        os.makedirs("output_test", exist_ok=True)
        out_path = "output_test/xueqiu_forecast_test_sample.parquet"
        df.write_parquet(out_path, compression="zstd")
        print(f"\n💾 物理落盘测试成功: {out_path}")
    else:
        print("⚠️ 未抓取到任何数据，请检查 API Token 或网络连通性。")

if __name__ == "__main__":
    main()
