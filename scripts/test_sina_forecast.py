# FILE: scripts/test_sina_forecast.py
import os
import time
import requests
import pandas as pd
import polars as pl
import io
import re
from concurrent.futures import ThreadPoolExecutor, as_completed
from bs4 import BeautifulSoup

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8",
    "Accept-Language": "zh-CN,zh;q=0.9"
}

TEST_STOCKS = [
    "SH600519", "SZ300750", "SZ002594", "SZ002460", "SH688041",
    "SZ002371", "SZ300438", "SH601127", "SZ000938", "SH600111",
    "SZ300274", "SZ000002", "SH600418", "SZ002156", "SH603501"
]

def extract_yoy_from_summary(summary):
    if not isinstance(summary, str): return 0.0
    nums = re.findall(r'(-?\d+(?:\.\d+)?)%', summary)
    if not nums: return 0.0
    nums = [float(n) for n in nums]
    if any(keyword in summary for keyword in ["下降", "亏损", "减少", "降幅"]):
        nums = [-abs(n) for n in nums]
    return sum(nums) / len(nums)

def fetch_sina_forecast(raw_code):
    pure_code = raw_code[2:]
    # 💥 核心修改：降级为 http 协议，避免新浪垃圾 CDN 的 HTTPS 重定向
    url = f"http://vip.stock.finance.sina.com.cn/corp/go.php/vCB_Bulletin/wx/PerformanceForecast/displaytype/4/stockid/{pure_code}.phtml"
    
    try:
        res = requests.get(url, headers=HEADERS, timeout=10)
        res.encoding = 'gbk' 
        
        if res.status_code != 200:
            return pure_code, [], f"HTTP Status: {res.status_code}"
            
        html_text = res.text
        
        # 🕵️ X光透视：用 BeautifulSoup 提取网页标题，看看到底是个啥页面
        soup = BeautifulSoup(html_text, 'lxml')
        title = soup.title.string.strip() if soup.title else "无标题"
        
        # 宽泛匹配：只要表头里有“公告日期”，我们就抓出来
        if "公告日期" not in html_text:
            return pure_code, [], f"页面正常，但未找到'公告日期'表格。页面标题: [{title}]"
            
        try:
            dfs = pd.read_html(io.StringIO(html_text), match="公告日期", header=0)
            if not dfs:
                return pure_code, [], "找到了关键字，但 pandas 解析表格失败"
                
            df = dfs[0]
            records = []
            
            for _, row in df.iterrows():
                # 兼容新浪可能存在的换行符和脏数据
                row_dict = {str(k).strip(): str(v).strip() for k, v in row.items()}
                
                notice_date = row_dict.get('公告日期', '')
                if not notice_date or notice_date == 'nan': continue
                
                f_type = row_dict.get('业绩预告类型', '')
                summary = row_dict.get('业绩预告摘要', '')
                
                yoy_mid = extract_yoy_from_summary(summary)
                
                records.append({
                    "code": pure_code,
                    "notice_date": notice_date[:10], # 取前10位防脏数据
                    "forecast_type": f_type,
                    "forecast_yoy_mid": yoy_mid,
                    "summary": summary[:200]
                })
            return pure_code, records, None
            
        except ValueError as ve:
            return pure_code, [], f"Pandas 解析异常: {ve}"
            
    except Exception as e:
        return pure_code, [], f"网络异常: {e}"

def main():
    print("\n" + "="*70)
    print("🧪 新浪财经 [业绩预告] X光透视 Debug 版 | 启动")
    print("="*70)
    
    all_events = []
    success_count = 0
    
    print(f"\n🚀 开始拉取 {len(TEST_STOCKS)} 只股票...\n")
    
    with ThreadPoolExecutor(max_workers=5) as executor:
        futures = {executor.submit(fetch_sina_forecast, s): s for s in TEST_STOCKS}
        for future in as_completed(futures):
            pure_code, records, err = future.result()
            
            if records:
                success_count += 1
                all_events.extend(records)
                print(f"✅ {pure_code}: 成功抓取 {len(records)} 条预告")
            elif err:
                print(f"⚠️ {pure_code} 失败 -> {err}")

    print("\n" + "="*70)
    print(f"✅ 测试结束！成功 {success_count}/{len(TEST_STOCKS)}。")
    print(f"📊 累计捕获预告: {len(all_events)} 条")
    print("="*70 + "\n")

    if all_events:
        schema = {
            "code": pl.Utf8, "notice_date": pl.Utf8, 
            "forecast_type": pl.Utf8, "forecast_yoy_mid": pl.Float64, "summary": pl.Utf8
        }
        df = pl.DataFrame(all_events, schema=schema).sort(["code", "notice_date"])
        print(df.tail(5))
        os.makedirs("output_test", exist_ok=True)
        df.write_parquet("output_test/sina_forecast_test_sample.parquet", compression="zstd")

if __name__ == "__main__":
    main()
