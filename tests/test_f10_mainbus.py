import requests

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Referer": "https://emweb.securities.eastmoney.com/"
}

TEST_STOCKS = [
    {"code": "SZ300750", "name": "宁德时代"},
    {"code": "SZ002594", "name": "比亚迪"},
    {"code": "SH600519", "name": "贵州茅台"}
]

def test_real_f10(secucode, name):
    print(f"📡 正在请求 PC_HSF10 专用主营分析接口: {name} ({secucode})...")
    url = f"https://emweb.securities.eastmoney.com/PC_HSF10/OperationsAnalysis/OperationsAnalysisAjax?code={secucode}"
    
    try:
        res = requests.get(url, headers=HEADERS, timeout=8)
        if res.status_code != 200:
            print(f"   ❌ 状态码异常: {res.status_code}")
            return False
            
        data = res.json()
        
        # 该专用接口返回三大主营口径：zygc (主营构成), zydq (主营地区), zycp (主营产品)
        zycp_list = data.get("zycp", [])
        if zycp_list:
            # 拿到最近一个报告期的主营产品明细
            latest_date = zycp_list[0].get("REPORT_DATE", "")
            records = [x for x in zycp_list if x.get("REPORT_DATE") == latest_date]
            
            print(f"   ✅ 成功！报告期: {latest_date[:10]} | 捕获产品明细 {len(records)} 条:")
            for item in records[:3]:
                print(f"      - 产品: {item.get('MAINOP_BUSINESS_ITEM')} | 营收: {float(item.get('MAIN_BUSINESS_INCOME', 0))/1e8:.2f}亿 | 占比: {item.get('MBI_RATIO')}%")
            return True
        else:
            print("   ❌ 未能在返回的 JSON 中找到 'zycp' 节点")
            return False
    except Exception as e:
        print(f"   ❌ 请求异常: {str(e)}")
        return False

def main():
    print("="*70)
    print("🔬 验证真实 HSF10 经营分析接口")
    print("="*70)
    for s in TEST_STOCKS:
        test_real_f10(s["code"], s["name"])
    print("="*70 + "\n")

if __name__ == "__main__":
    main()
