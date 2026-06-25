import requests
import json

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Referer": "https://quote.eastmoney.com/"
}

TEST_STOCKS = [
    {"code": "300750", "name": "宁德时代"},
    {"code": "002594", "name": "比亚迪"},
    {"code": "600519", "name": "贵州茅台"}
]

def test_single_stock(pure_code, name):
    print(f"📡 正在测试直连拉取: {name} ({pure_code}) 主营构成...")
    url = "https://datacenter.eastmoney.com/api/data/v1/get"
    
    # 构建针对 RPT_F10_FN_MAINORBCOMP 的过滤条件
    params = {
        "sortColumns": "REPORT_DATE,ITEM_TYPE",
        "sortTypes": "-1,1",
        "pageSize": "30",  # 测试拉取最近的 30 条明细
        "pageNumber": "1",
        "reportName": "RPT_F10_FN_MAINORBCOMP",
        "columns": "SECURITY_CODE,REPORT_DATE,NOTICE_DATE,ITEM_NAME,ITEM_TYPE,MBI_RATIO,GROSS_RPOFIT_RATIO",
        "filter": f'(SECURITY_CODE="{pure_code}")',
        "client": "WEB"
    }
    
    try:
        res = requests.get(url, params=params, headers=HEADERS, timeout=10)
        if res.status_code != 200:
            print(f"❌ 请求失败，状态码: {res.status_code}")
            return False
            
        data = res.json()
        if data.get("code") == 0 and data.get("result"):
            records = data["result"]["data"]
            print(f"✅ 成功捕获 {len(records)} 条产品明细。样例前 3 条:")
            for item in records[:3]:
                print(f"   - 报告期: {item.get('REPORT_DATE')[:10]} | 公告日: {item.get('NOTICE_DATE')[:10]} | 产品: {item.get('ITEM_NAME')} | 占比: {item.get('MBI_RATIO')}% | 毛利率: {item.get('GROSS_RPOFIT_RATIO')}%")
            return True
        else:
            print(f"❌ 接口返回异常: {data.get('message', '无返回消息')}")
            return False
    except Exception as e:
        print(f"❌ 请求异常: {str(e)}")
        return False

def main():
    print("="*60)
    print("🔬 开始测试: F10 个股主营构成 (微观)")
    print("="*60)
    success = 0
    for stock in TEST_STOCKS:
        if test_single_stock(stock["code"], stock["name"]):
            success += 1
    print(f"\n📊 测试完成: 成功 {success}/{len(TEST_STOCKS)}\n")

if __name__ == "__main__":
    main()
