import requests

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Referer": "https://quote.eastmoney.com/"
}

# 1. 诊断 F10 接口返回的真实内容
def debug_f10():
    print("="*70)
    print("🔬 [诊断 1] 打印 F10 经营分析接口的原始返回内容")
    print("="*70)
    
    # 测试三种常见的代码入参格式
    formats = ["SZ300750", "sz300750", "300750"]
    
    for fmt in formats:
        url = f"https://emweb.securities.eastmoney.com/PC_HSF10/OperationsAnalysis/OperationsAnalysisAjax?code={fmt}"
        try:
            res = requests.get(url, headers=HEADERS, timeout=8)
            print(f"\n📡 测试格式: code={fmt} | HTTP状态码: {res.status_code}")
            
            raw_text = res.text.strip()
            if not raw_text:
                print("   ⚠️ 返回内容为空！")
                continue
                
            print("   📄 原始返回内容前 300 个字符:")
            print("-" * 50)
            print(raw_text[:300])
            print("-" * 50)
            
            # 尝试简单解析
            if raw_text.startswith("{"):
                print("   💡 格式提示: 看起来是标准的 JSON")
            elif raw_text.startswith("jQuery") or "(" in raw_text:
                print("   💡 格式提示: 这是一个 JSONP 回调 (被 JavaScript 包裹)")
            elif "<html" in raw_text.lower() or "<!doctype" in raw_text.lower():
                print("   💡 格式提示: 这是一个 HTML 网页 (极可能是 WAF 拦截或验证码页面)")
                
        except Exception as e:
            print(f"   ❌ 请求发生异常: {e}")

# 2. 包抄扫频东财行业图谱/产业链门户 URL
def debug_macro_portal():
    print("\n" + "="*70)
    print("🔬 [诊断 2] 行业图谱 / 产业链门户 URL 扫频")
    print("="*70)
    
    # 候选拼音子路径
    candidates = [
        "hytp",    # 行业图谱 (Hang Ye Tu Pu)
        "cyl",     # 产业链 (Chan Ye Lian)
        "cyy",     # 产业链 (Chan Ye Ye)
        "gxb"      # 股侠宝 (验证 404)
    ]
    
    for cat in candidates:
        url = f"https://data.eastmoney.com/{cat}/"
        try:
            res = requests.get(url, headers=HEADERS, timeout=5)
            print(f"📡 探测门户: {url} $\rightarrow$ 状态码: {res.status_code}")
            if res.status_code == 200:
                print(f"   🎉 【成功发现真实门户！】")
                # 打印网页标题看看是不是图谱
                title = ""
                if "<title>" in res.text:
                    title = res.text.split("<title>")[1].split("</title>")[0]
                print(f"   网页标题: {title}")
        except Exception as e:
            print(f"   ❌ 探测失败: {e}")

def main():
    debug_f10()
    debug_macro_portal()

if __name__ == "__main__":
    main()
