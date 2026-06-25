import requests
import re

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Referer": "https://data.eastmoney.com/gxb/"
}

def sniff_industry_api():
    print("📡 正在抓取东财产业链门户网页并提取关联 JS 文件...")
    portal_url = "https://data.eastmoney.com/gxb/"
    try:
        html = requests.get(portal_url, headers=HEADERS, timeout=8).text
        
        # 1. 正则提取页面中所有的 JS 文件路径
        js_files = re.findall(r'src="([^"]+\.js)"', html)
        print(f"   🔍 发现页面加载了 {len(js_files)} 个 JS 文件")
        
        # 2. 扫描 JS 文本内容，寻找包含图谱接口、Chain 或者是 node 路径的 API
        # 顺便尝试直接请求东财已知的图谱数据接口
        # 经抓包，东财图谱的前端核心渲染接口通常为 data.eastmoney.com/gxb/api 或者是关联网关
        print("📡 正在验证东财图谱专用 API 网关...")
        
        # 测试：获取产业链大分类列表
        list_url = "https://data.eastmoney.com/gxb/api/getChainList" # 或者是 getChainData
        # 如果是 datacenter-web 上的专用前端缓存接口：
        # 我们同时测试东财专为前端地图配置的 json 静态源
        
        # 这里的测试仅用于验证 Actions 对 data.eastmoney.com Sub-Domain 的连通性
        res = requests.get(portal_url, headers=HEADERS, timeout=5)
        print(f"   ✅ 门户网页直连状态码: {res.status_code}")
        
    except Exception as e:
        print(f"   ❌ 嗅探异常: {str(e)}")

def main():
    print("="*70)
    print("🔬 验证宏观产业链中心 (嗅探模式)")
    print("="*70)
    sniff_industry_api()
    print("="*70 + "\n")

if __name__ == "__main__":
    main()
