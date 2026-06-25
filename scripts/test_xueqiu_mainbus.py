import json
import os
import time
import requests
import polars as pl
from datetime import datetime

# 完善标准浏览器请求头，全面模拟真实浏览器行为
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8",
    "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
    "Referer": "https://xueqiu.com/",
    "Connection": "keep-alive"
}

TEST_STOCKS = [
    {"code": "SH600519", "name": "贵州茅台"}, 
    {"code": "SZ300750", "name": "宁德时代"}, 
    {"code": "SH688111", "name": "金半导体"}, 
    {"code": "BJ835181", "name": "德源药业"}, 
    {"code": "SZ000002", "name": "万科A"},    
    {"code": "SZ002460", "name": "赣锋锂业"}, 
    {"code": "SH600104", "name": "上汽集团"}, 
    {"code": "SH600036", "name": "招商银行"}, 
    {"code": "SZ300059", "name": "东方财富"}, 
    {"code": "SZ000100", "name": "TCL科技"}   
]

def ms_to_date_str(ms):
    if not ms: return ""
    try:
        return datetime.fromtimestamp(int(ms) / 1000).strftime('%Y-%m-%d')
    except:
        return ""

def main():
    print("="*70)
    print("📡 Actions 环境雪球 V5 财务数据空间连通性与吞吐量测试 (Cookie 自愈版)")
    print("="*70)
    
    session = requests.Session()
    session.headers.update(HEADERS)
    
    # 1. 第一步：尝试首页握手
    print("[*] 正在执行第一阶段首页握手...")
    try:
        session.get("https://xueqiu.com/", timeout=10)
    except Exception as e:
        print(f"⚠️ 首页握手异常 (跳过): {e}")

    # 2. 第二步核心：模拟访问真实的个股 HTML 详情页，强迫雪球下发安全 Token
    print("[*] 正在执行第二阶段：模拟访问个股 HTML 落地页以激活完整安全 Cookie...")
    try:
        # 访问个股 HTML 页面（非 API）
        r_html = session.get("https://xueqiu.com/S/SZ300750", timeout=10)
        print(f"    - 页面请求状态: {r_html.status_code}")
    except Exception as e:
        print(f"❌ 激活安全 Cookie 异常: {e}")
        return

    # 3. 诊断当前获取到的 Cookie
    cookies_dict = session.cookies.get_dict()
    print("\n" + "-"*50)
    print(f"📡 诊断信息：当前 Session 已挂载的 Cookie: \n{json.dumps(cookies_dict, indent=4)}")
    print("-"*50 + "\n")
    
    if "xq_a_token" not in cookies_dict:
        print("❌ 严重警告: 未能在 Cookie 中检测到 'xq_a_token'，雪球可能会拒绝后续的 API 请求 (HTTP 400)。")
    else:
        print("✅ 成功捕获安全 Token (xq_a_token)，进入高能采集状态。")

    all_rows = []
    success_count = 0
    
    # 4. 遍历拉取测试股全部历史
    print(f"\n[*] 开始拉取 {len(TEST_STOCKS)} 只代表性个股的全部历史主营业务构成...")
    for s in TEST_STOCKS:
        code, name = s["code"], s["name"]
        print(f" 📥 正在请求: {name} ({code}) ... ", end="")
        
        url = f"https://stock.xueqiu.com/v5/stock/finance/cn/business.json?symbol={code}"
        try:
            # 必须在请求头中明确告知服务器我们接受 JSON，防止防盗链拦截
            api_headers = HEADERS.copy()
            api_headers["Accept"] = "application/json, text/plain, */*"
            
            res = session.get(url, headers=api_headers, timeout=10)
            if res.status_code != 200:
                print(f"❌ 失败 (HTTP {res.status_code})")
                continue
                
            data = res.json()
            if data.get("error_code") != 0 or not data.get("data"):
                print(f"❌ 失败 (API 错误: {data.get('error_description')})")
                continue
                
            records = data["data"].get("list", [])
            if not records:
                print("⚠️ 无数据")
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
                        ratio = float(bus.get("income_ratio") or 0.0)
                        if ratio < 1.0: 
                            ratio = ratio * 100.0
                            
                        margin = float(bus.get("gross_profit_rate") or 0.0)
                        if margin < 1.0 and margin > 0:
                            margin = margin * 100.0

                        all_rows.append({
                            "code": code,
                            "name": name,
                            "report_date": report_date,
                            "report_name": report_name,
                            "item_type": int(class_standard),
                            "item_name": str(bus.get("project_announced_name", "")).strip(),
                            "income": float(bus.get("prime_operating_income") or 0.0),
                            "income_ratio": ratio,
                            "gross_margin": margin
                        })
            time.sleep(0.5)
        except Exception as e:
            print(f"❌ 异常: {e}")

    # 5. 数据总结与落盘
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
