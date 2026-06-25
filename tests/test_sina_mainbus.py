import pandas as pd
import requests
import io

# 补全高信誉度浏览器 Headers，模拟最真实的网页访问
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:120.0) Gecko/20100101 Firefox/120.0",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
    "Accept-Language": "zh-CN,zh;q=0.8,zh-TW;q=0.7,zh-HK;q=0.5,en-US;q=0.3,en;q=0.2"
}

def test_sina():
    print("="*75)
    print("🔬 [诊断] 新浪财经双协议连通性与内容长度校验")
    print("="*75)
    
    code = "300750" # 宁德时代
    
    # 对比测试 https 与 http，很多国内老牌财经网只放行 http 或对其有不同的 CDN 策略
    urls = [
        f"https://money.finance.sina.com.cn/corp/go.php/vCI_BusinessAnalysis/stockid/{code}.phtml",
        f"http://money.finance.sina.com.cn/corp/go.php/vCI_BusinessAnalysis/stockid/{code}.phtml"
    ]
    
    for url in urls:
        print(f"\n📡 正在探测 URL: {url} ...")
        try:
            res = requests.get(url, headers=HEADERS, timeout=10)
            print(f"   - HTTP 状态码: {res.status_code}")
            print(f"   - 原始返回文本长度: {len(res.text)} 字节")
            
            if len(res.text.strip()) > 0:
                print("   📄 原始返回内容前 150 个字符:")
                print("-" * 50)
                # 新浪使用 gbk 编码，诊断时进行强制转换防止乱码
                res.encoding = 'gbk'
                print(res.text.strip()[:150])
                print("-" * 50)
                
                # 🛡️ 消除 DeprecationWarning，安全喂给 Pandas
                tables = pd.read_html(io.StringIO(res.text))
                print(f"   🎉 【解析成功！】在该页面中成功解析出 {len(tables)} 个数据表格")
            else:
                print("   ❌ 返回内容为空，说明该协议被 CDN 阻断或重置。")
        except Exception as e:
            print(f"   ❌ 请求或解析发生异常: {e}")

if __name__ == "__main__":
    test_sina()
