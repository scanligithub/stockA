# FILE: scripts/test_sina_forecast.py
import os
import time
import requests
import pandas as pd
import polars as pl
import io
import re
from concurrent.futures import ThreadPoolExecutor, as_completed

# 极简头部，无需任何 Token
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
}

TEST_STOCKS = [
    "SH600519", "SZ300750", "SZ002594", "SZ002460", "SH688041",
    "SZ002371", "SZ300438", "SH601127", "SZ000938", "SH600111",
    "SZ300274", "SZ000002", "SH600418", "SZ002156", "SH603501"
]

def extract_yoy_from_summary(summary):
    """🧠 智能正则引擎：从新浪长文本摘要中提取同比增速中枢"""
    if not isinstance(summary, str): return 0.0
    
    # 提取所有百分比数字，例如 "增长 50%~100%" -> [50.0, 100.0]
    nums = re.findall(r'(-?\d+(?:\.\d+)?)%', summary)
    if not nums: return 0.0
    
    nums = [float(n) for n in nums]
    
    # 根据语境判断正负
    if any(keyword in summary for keyword in ["下降", "亏损", "减少", "降幅"]):
        nums = [-abs(n) for n in nums]
        
    # 返回区间的中枢值
    return sum(nums) / len(nums)

def fetch_sina_forecast(raw_code):
    """直连新浪财经远古 PHP 页面，解析 HTML 表格"""
    pure_code = raw_code[2:]
    # 新浪业绩预告专属远古路由
    url = f"https://vip.stock.finance.sina.com.cn/corp/go.php/vCB_Bulletin/wx/PerformanceForecast/displaytype/4/stockid/{pure_code}.phtml"
    
    for attempt in range(3):
        try:
            res = requests.get(url, headers=HEADERS, timeout=10)
            res.encoding = 'gbk' # 💥 核心：新浪页面采用 GBK 编码
            
            if res.status_code != 200:
                time.sleep(1)
                continue
                
            # 检查页面中是否包含有效表格
            if "业绩预告类型" not in res.text:
                return pure_code, [], None # 该股历史上从未发过业绩预告
                
            # 利用 pandas 强大的 read_html 瞬间将 HTML 转换为 DataFrame
            dfs = pd.read_html(io.StringIO(res.text), match="业绩预告类型", header=0)
            if not dfs:
                return pure_code, [], None
                
            df = dfs[0]
            records = []
            
            for _, row in df.iterrows():
                notice_date = str(row.get('公告日期', '')).strip()
                if not notice_date or notice_date == 'nan': continue
                
                f_type = str(row.get('业绩预告类型', '')).strip()
                summary = str(row.get('业绩预告摘要', '')).strip()
                
                # 调用正则提取增速中枢
                yoy_mid = extract_yoy_from_summary(summary)
                
                records.append({
                    "code": pure_code,
                    "notice_date": notice_date,
                    "forecast_type": f_type,
                    "forecast_yoy_mid": yoy_mid,
                    "summary": summary
                })
            return pure_code, records, None
            
        except ValueError:
            # 页面存在但 pd 无法找到匹配表格，静默跳过
            return pure_code, [], None
        except Exception as e:
            print(f"[Debug] {pure_code} HTML Parse Error: {e}")
            time.sleep(1)
            
    return pure_code, [], "Max retries exceeded"

def main():
    print("\n" + "="*70)
    print("🧪 新浪财经 [业绩预告事件] 直连解析测试 (零风控版) | 启动")
    print("="*70)
    
    all_events = []
    success_count = 0
    
    print(f"\n🚀 开始并发拉取 {len(TEST_STOCKS)} 只样本股票的业绩预告...")
    
    with ThreadPoolExecutor(max_workers=10) as executor:
        futures = {executor.submit(fetch_sina_forecast, s): s for s in TEST_STOCKS}
        for future in as_completed(futures):
            pure_code, records, err = future.result()
            
            if records:
                success_count += 1
                all_events.extend(records)
            elif err:
                print(f"⚠️ 股票 {pure_code} 抓取失败: {err}")

    print("\n" + "="*70)
    print(f"✅ 测试完成！成功获取 {success_count}/{len(TEST_STOCKS)} 只股票的数据。")
    print(f"📊 累计捕获历史业绩预告事件: {len(all_events)} 条")
    print("="*70 + "\n")

    if all_events:
        schema = {
            "code": pl.Utf8, "notice_date": pl.Utf8, 
            "forecast_type": pl.Utf8, "forecast_yoy_mid": pl.Float64, "summary": pl.Utf8
        }
        df = pl.DataFrame(all_events, schema=schema).sort(["code", "notice_date"])
        
        print("👀 数据抽样预览 (截取最后 5 条):")
        print(df.tail(5))
        
        os.makedirs("output_test", exist_ok=True)
        out_path = "output_test/sina_forecast_test_sample.parquet"
        df.write_parquet(out_path, compression="zstd")
        print(f"\n💾 物理落盘测试成功: {out_path}")

if __name__ == "__main__":
    main()
