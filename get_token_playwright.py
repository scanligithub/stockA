from playwright.sync_api import sync_playwright
import requests

def get_tokens_via_browser():
    print("正在启动无头浏览器获取 Token...")
    with sync_playwright() as p:
        # 启动 Chromium
        browser = p.chromium.launch(headless=True)
        # 创建上下文并设置 User-Agent
        context = browser.new_context(
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
        )
        page = context.new_page()
        
        try:
            # 访问雪球首页，等待网络处于空闲状态
            page.goto("https://xueqiu.com", wait_until="networkidle", timeout=15000)
            
            # 获取当前页面的所有 Cookies
            cookies = context.cookies()
            
            # 提取目标 token
            xq_a_token = next((c['value'] for c in cookies if c['name'] == 'xq_a_token'), None)
            xq_r_token = next((c['value'] for c in cookies if c['name'] == 'xq_r_token'), None)
            
            return xq_a_token, xq_r_token
            
        except Exception as e:
            print(f"浏览器抓取发生异常: {e}")
            return None, None
        finally:
            browser.close()

if __name__ == "__main__":
    a_token, r_token = get_tokens_via_browser()
    
    if r_token:
        print("✅ 成功通过浏览器环境获取到 Token:")
        print(f"xq_a_token = {a_token}")
        print(f"xq_r_token = {r_token}")
        
        # -- 下面演示如何使用获取到的 Token 请求 API --
        api_url = "https://stock.xueqiu.com/v5/stock/quote.json?symbol=SH510300"
        headers = {
            'User-Agent': "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            'Cookie': f'xq_a_token={a_token}; xq_r_token={r_token};'
        }
        res = requests.get(api_url, headers=headers)
        print("\n✅ API 请求测试结果:")
        print(res.status_code)
        # print(res.json()) # 查看返回的行情数据
    else:
        print("❌ 获取 Token 失败")
