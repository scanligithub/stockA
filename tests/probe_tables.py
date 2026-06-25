import requests
import re
from urllib.parse import urljoin

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Referer": "https://gxb.eastmoney.com/"
}

# 1. 修复字符编码后的 F10 真实探针
def test_hsf10_strict():
    print("="*80)
    print("🔬 [诊断 1] 严格编码对齐 HSF10 (测试真正经营分析接口)")
    print("="*80)
    
    # 穷举测试可能的路径与参数组合
    paths = [
        "PC_HSF10/OperationsAnalysis/OperationsAnalysisAjax",
        "PC_HSF10/BusinessAnalysis/BusinessAnalysisAjax",
        "pc_hsf10/operationsanalysis/operationsanalysisajax",
        "pc_hsf10/businessanalysis/businessanalysisajax"
    ]
    
    for path in paths:
        url = f"https://emweb.securities.eastmoney.com/{path}?code=SZ300750"
        try:
            res = requests.get(url, headers={"User-Agent": HEADERS["User-Agent"], "Referer": "https://emweb.securities.eastmoney.com/"}, timeout=5)
            # 🛡️ 强制修复：使用 apparent_encoding 彻底解决 Latin-1 乱码问题，确保字符对齐
            res.encoding = res.apparent_encoding
            
            snippet = res.text.strip()[:200]
            if "无 F10" in snippet or "无F10" in snippet:
                print(f"❌ 路径: {path} $\rightarrow$ 返回: [无 F10 资料]")
            elif "{" in snippet:
                print(f"🎯 【重大突破】成功找到真实 API 路径: {path}！")
                print(f"   样例数据: {snippet[:200]}")
                break
            else:
                print(f"⚠️ 路径: {path} $\rightarrow$ 返回未知 HTML 模板 (前 50 字): {snippet[:50]}")
        except Exception as e:
            print(f"❌ 路径: {path} 异常: {e}")

# 2. SPA 前端静态文件解包与 API 地毯式搜索
def sniff_spa_js():
    print("\n" + "="*80)
    print("🔬 [诊断 2] 股侠宝 SPA 前端静态 JS 源码嗅探 (寻找隐藏 API)")
    print("="*80)
    
    base_url = "https://gxb.eastmoney.com/"
    try:
        res = requests.get(base_url, headers=HEADERS, timeout=5)
        res.encoding = 'utf-8'
        html = res.text
        
        # 提取所有携带打包特征的 JS 文件 (如 index-xxx.js, app-xxx.js, main-xxx.js)
        js_relative_paths = re.findall(r'src="([^"]+\.js)"', html)
        # 兼容 chunk-vendors 或者是 chunk-xxx.js 链路
        js_href_paths = re.findall(r'href="([^"]+\.js)"', html)
        
        js_paths = list(set(js_relative_paths + js_href_paths))
        print(f"📦 发现 {len(js_paths)} 个打包好的前端 JS 静态文件:")
        
        # 拼接为绝对路径
        js_urls = [urljoin(base_url, p) for p in js_paths]
        for u in js_urls:
            print(f"  - {u}")
            
        # 开始下载并对源码执行高精度检索
        print("\n⚡ 正在对前端源码执行 API 特征地毯式匹配...")
        for u in js_urls:
            try:
                js_content = requests.get(u, headers=HEADERS, timeout=10).text
                
                # 检索特征 A：找到所有的 API 根路径或微服务网关 (形如 /api/xxx 或者是 http开头的网关)
                api_endpoints = set(re.findall(r'"(/api/[^"]+)"', js_content))
                # 检索特征 B：找到图谱相关关键词
                graph_keywords = set(re.findall(r'"([^"]*(?:chain|node|relation|getChain|gxb)[^"]*)"', js_content, re.IGNORECASE))
                
                if api_endpoints or graph_keywords:
                    print(f"\n🎯 在 JS 文件 [{u.split('/')[-1]}] 中捞出关键线索:")
                    if api_endpoints:
                        print("   🔹 捞出可能的前端 API 路由 (过滤前 10 个):")
                        for ep in list(api_endpoints)[:10]:
                            print(f"      - {ep}")
                    if graph_keywords:
                        print("   🔹 捞出带有图特征的关键词 (过滤前 10 个):")
                        for kw in list(graph_keywords)[:10]:
                            print(f"      - {kw}")
            except Exception as e:
                print(f"❌ 抓取静态文件 {u} 失败: {e}")
                
    except Exception as e:
        print(f"❌ 抓取 SPA 首页失败: {e}")

if __name__ == "__main__":
    test_hsf10_strict()
    sniff_spa_js()
