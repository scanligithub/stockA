import requests

def test_hsf10_with_proper_headers():
    print("="*75)
    print("🔬 [诊断 1] HSF10 专用接口对齐校验 (强对齐 Referer 与 Code)")
    print("="*75)
    
    # 强制让 Referer 与个股代码逻辑闭环，防止后端判定为盗链而返回“无 F10 资料”
    codes = ["SZ300750", "sz300750", "300750"]
    
    for c in codes:
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Referer": f"https://emweb.securities.eastmoney.com/PC_HSF10/OperationsAnalysis/Index?code={c}"
        }
        url = f"https://emweb.securities.eastmoney.com/PC_HSF10/OperationsAnalysis/OperationsAnalysisAjax?code={c}"
        try:
            res = requests.get(url, headers=headers, timeout=5)
            text_snippet = res.text.strip()[:180]
            print(f"📡 输入 code={c} | HTTP状态码: {res.status_code}")
            print(f"   📄 返回前 150 字符: {text_snippet}")
            if "无 F10 资料" in text_snippet or "æ— F10èµ„æ–™" in text_snippet:
                print("   ❌ 仍被判定为: 无 F10 资料 (需继续对齐入参或 Cookie)")
            else:
                print("   🎉 【突破成功！】成功获取到了真实经营数据 JSON")
        except Exception as e:
            print(f"   ❌ 请求发生异常: {e}")

def test_gxb_subdomain():
    print("\n" + "="*75)
    print("🔬 [诊断 2] 探测独立子域名 gxb.eastmoney.com 连通性")
    print("="*75)
    
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Referer": "https://gxb.eastmoney.com/"
    }
    
    # 测试独立的 gxb 子域名以及其下可能的接口
    urls = [
        "https://gxb.eastmoney.com/",
        "https://gxb.eastmoney.com/gxb/",
        "https://gxb.eastmoney.com/api/getChainList"
    ]
    
    for url in urls:
        try:
            res = requests.get(url, headers=headers, timeout=5)
            print(f"📡 探测: {url} $\rightarrow$ 状态码: {res.status_code}")
            if res.status_code == 200:
                print(f"   🎉 【连通成功！】前 150 字符: {res.text.strip()[:150]}")
        except Exception as e:
            print(f"   ❌ 失败: {e}")

if __name__ == "__main__":
    test_hsf10_with_proper_headers()
    test_gxb_subdomain()
