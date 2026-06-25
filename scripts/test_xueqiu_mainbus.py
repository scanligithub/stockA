import json
import os
import time
import requests
import polars as pl
from datetime import datetime

# 标准浏览器请求头
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Referer": "https://xueqiu.com/"
}

# 10 只具有行业代表性的测试股票（覆盖各板块与新老股）
TEST_STOCKS = [
    {"code": "SH600519", "name": "贵州茅台"}, # 沪市主板
    {"code": "SZ300750", "name": "宁德时代"}, # 创业板核心
    {"code": "SH688111", "name": "金micro"}, # 科创板
    {"code": "BJ835181", "name": "德源药业"}, # 北交所
    {"code": "SZ000002", "name": "万科A"},    # 房地产开发
    {"code": "SZ002460", "name": "赣锋锂业"}, # 锂电池上游
    {"code": "SH600104", "name": "上汽集团"}, # 汽车下游
    {"code": "SH600036", "name": "招商银行"}, # 金融类（测试主营结构差异）
    {"code": "SZ300059", "name": "东方财富"}, # 互联网金融
    {"code": "SZ000100", "name": "TCL科技"}   # 面板半导体
]

def ms_to_date_str(ms):
    if not ms: return ""
    try:
        return datetime.fromtimestamp(int(ms) / 1000).strftime('%Y-%m-%d')
    except:
        return ""

def main():
    print("="*70)
    print("📡 Actions 环境雪球 V5 财务数据空间连通性与吞吐量测试")
    print("="*70)
    
    session = requests.Session()
    session.headers.update(HEADERS)
    
    # 1. 模拟首页握手获取安全 Cookie
    print("[*] 正在向雪球主站执行安全握手...")
    try:
        r = session.get("https://xueqiu.com/", timeout=10)
        if r.status_code == 200:
            print("✅ 握手成功，Cookie 挂载完成。")
        else:
            print(f"❌ 握手失败，HTTP 状态码: {r.status_code}")
            return
    except Exception as e:
        print(f"❌ 握手发生异常: {e}")
        return

    all_rows = []
    success_count = 0
    
    # 2. 遍历拉取测试股全部历史
    print(f"[*] 开始拉取 {len(TEST_STOCKS)} 只代表性个股的全部历史主营业务构成...")
    for s in TEST_STOCKS:
        code, name = s["code"], s["name"]
        print(f" 📥 正在请求: {name} ({code}) ... ", end="")
        
        url = f"https://stock.xueqiu.com/v5/stock/f10/cn/business.json?symbol={code}"
        try:
            res = session.get(url, timeout=10)
            if res.status_code != 200:
                print(f"❌ 失败 (HTTP {res.status_code})")
                continue
                
            data = res.json()
            if data.get("error_code") != 0 or not data.get("data"):
                print(f"❌ 失败 (API 错误: {data.get('error_description')})")
                continue
                
            records = data["data"].get("business_anal", [])
            if not records:
                print("⚠️ 无数据 (可能为极新上市股票)")
                continue
                
            print(f"✅ 成功，捕获到 {len(records)} 个历史财务报告期。")
            success_count += 1
            
            for rec in records:
                report_date = ms_to_date_str(rec.get("report_date"))
                report_name = rec.get("report_name", "")
                class_list = rec.get("class_list", [])
                
                for clazz in class_list:
                    class_standard = clazz.get("class_standard") # 1=行业, 2=产品, 3=地区
                    if class_standard not in [1, 2]:
                        continue
                        
                    bus_list = clazz.get("business_list", [])
                    for bus in bus_list:
                        all_rows.append({
                            "code": code,
                            "name": name,
                            "report_date": report_date,
                            "report_name": report_name,
                            "item_type": int(class_standard),
                            "item_name": str(bus.get("project_announced_name", "")).strip(),
                            "income": float(bus.get("prime_operating_income") or 0.0),
                            "income_ratio": float(bus.get("income_ratio") or 0.0) * 100.0,
                            "gross_margin": float(bus.get("gross_profit_rate") or 0.0) * 100.0
                        })
            # 略作休眠，模拟温和请求
            time.sleep(0.5)
        except Exception as e:
            print(f"❌ 异常: {e}")

    # 3. 数据总结与落盘
    print("\n" + "="*70)
    print(f"📊 测试结束统计：")
    print(f"    - 计划测试股票数: {len(TEST_STOCKS)}")
    print(f"    - 成功获取历史数: {success_count}")
    print(f"    - 累计生成物理行: {len(all_rows):,}")
    print("="*70 + "\n")

    if all_rows:
        schema = {
            "code": pl.Utf8, "name": pl.Utf8, "report_date": pl.Utf8, "report_name": pl.Utf8,
            "item_type": pl.Int32, "item_name": pl.Utf8, "income": pl.Float64,
            "income_ratio": pl.Float64, "gross_margin": pl.Float64
        }
        df = pl.DataFrame(all_rows, schema=schema)
        os.makedirs("output_test", exist_ok=True)
        out_path = "output_test/xueqiu_mainbus_test_sample.parquet"
        df.write_parquet(out_path, compression="zstd")
        print(f"💾 测试 Parquet 文件已输出至: {out_path} ({os.path.getsize(out_path)/(1024):.2f} KB)")
    else:
        print("❌ 未捕获到任何有效数据，测试未通过。")
        exit(1)

if __name__ == "__main__":
    main()
