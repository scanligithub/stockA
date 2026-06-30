# FILE: scripts/test_xueqiu_forecast.py
import os
import time
import requests
import polars as pl
from concurrent.futures import ThreadPoolExecutor, as_completed

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Accept": "application/json, text/plain, */*",
    "Referer": "https://xueqiu.com/",
    "Connection": "keep-alive"
}

TEST_STOCKS = [
    "SH600519", "SZ300750", "SZ002594", "SZ002460", "SH688041",
    "SZ002371", "SZ300438", "SH601127", "SZ000938", "SH600111",
    "SZ300274", "SZ000002", "SH600418", "SZ002156", "SH603501"
]

def ms_to_date_str(ms):
    if not ms: return None
    try: return time.strftime('%Y-%m-%d', time.localtime(int(ms) / 1000))
    except: return None

def init_xueqiu_session():
    """🌟 核心升级：动态匿名握手，永久告别过期 Token 烦恼"""
    session = requests.Session()
    session.headers.update(HEADERS)
    
    print("🤝 正在与雪球主站进行匿名安全握手...")
    try:
        # 访问主站，迫使服务器下发最新的安全 Cookie (含 xq_a_token)
        res = session.get("https://xueqiu.com/", timeout=10)
        if "xq_a_token" in session.cookies:
            token = session.cookies["xq_a_token"]
            print(f"✅ 动态握手成功！获取到全新 Token: {token[:8]}...{token[-4:]}")
        else:
            print("⚠️ 握手成功但未捕获到 Token，尝试注入环境变量作为兜底。")
    except Exception as e:
        print(f"⚠️ 动态握手失败，网络异常: {e}")

    # 兜底逻辑：如果握手失败，仍然尝试使用环境变量中的密钥
    env_token = os.getenv("XQ_A_TOKEN", "").strip()
    if env_token and "xq_a_token" not in session.cookies:
        print("🔑 注入环境变量 XQ_A_TOKEN 作为兜底...")
        session.cookies.set("xq_a_token", env_token, domain=".xueqiu.com")
        
    return session

def fetch_single_forecast(session, raw_code):
    pure_code = raw_code[2:]
    url = f"https://stock.xueqiu.com/v5/stock/finance/cn/forecast.json?symbol={raw_code}&count=150"
    
    for attempt in range(3):
        try:
            res = session.get(url, timeout=10)
            if res.status_code == 200:
                data = res.json()
                if data.get("error_code") == 0 and data.get("data"):
                    return pure_code, data["data"].get("list", []), None
                else:
                    err_msg = data.get("error_description", "API Data Empty")
                    return pure_code, [], f"API Error: {err_msg}"
            else:
                # 🛠️ 深度 Debug：打印出非 200 状态码的真实阻断原因
                print(f"[Debug] {raw_code} HTTP {res.status_code}: {res.text[:100]}")
                time.sleep(1.0 * (attempt + 1))
        except Exception as e:
            print(f"[Debug] {raw_code} Request Error: {e}")
            time.sleep(1.0 * (attempt + 1))
            
    return pure_code, [], "Max retries exceeded"

def main():
    print("\n" + "="*70)
    print("🧪 雪球 V5 [业绩预告事件] 独立隔离测试 (自愈版) | 启动")
    print("="*70)
    
    # 获取带自愈属性的会话
    session = init_xueqiu_session()
    
    all_events = []
    success_count = 0
    
    print(f"\n🚀 开始并发拉取 {len(TEST_STOCKS)} 只样本股票的业绩预告...")
    
    with ThreadPoolExecutor(max_workers=5) as executor: # 稍微降低测试并发度防封
        futures = {executor.submit(fetch_single_forecast, session, s): s for s in TEST_STOCKS}
        for future in as_completed(futures):
            pure_code, records, err = future.result()
            
            if records:
                success_count += 1
                for rec in records:
                    notice_date = ms_to_date_str(rec.get("notice_date"))
                    report_date = ms_to_date_str(rec.get("report_date"))
                    if not notice_date: continue
                    
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
        
        print("👀 数据预览 (截取最后 5 条):")
        print(df.tail(5))
        
        os.makedirs("output_test", exist_ok=True)
        out_path = "output_test/xueqiu_forecast_test_sample.parquet"
        df.write_parquet(out_path, compression="zstd")
        print(f"\n💾 物理落盘测试成功: {out_path}")

if __name__ == "__main__":
    main()
