import asyncio
import json
import re
import sys
from playwright.async_api import async_playwright, TimeoutError as PlaywrightTimeoutError

async def fetch_eastmoney_boards():
    TARGET_API_PATTERN = re.compile(r"api/qt/clist/get")
    TARGET_URL = "https://quote.eastmoney.com/center/boardlist.html#boards-90.BK0000"

    async with async_playwright() as p:
        browser = await p.chromium.launch(
            headless=True, 
            args=[
                "--no-sandbox", 
                "--disable-setuid-sandbox",
                "--disable-blink-features=AutomationControlled" # 基础反爬特征抹除
            ]
        )
        
        context = await browser.new_context(
            viewport={'width': 1920, 'height': 1080},
            user_agent="Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36"
        )
        
        # 注入 JS 进一步抹除 WebDriver 特征
        await context.add_init_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
        
        page = await context.new_page()

        # [新增] 网络请求监控，打印前20个请求，看看网页到底加载了什么
        req_count = 0
        def log_request(req):
            nonlocal req_count
            if req_count < 20:
                print(f"[Network] 请求 -> {req.url[:80]}...")
                req_count += 1
                
        page.on("request", log_request)

        print(f"[*] 正在导航至东方财富板块列表页...")

        try:
            # 延长超时时间到 20 秒，应对海外 IP 延迟
            async with page.expect_response(TARGET_API_PATTERN, timeout=20000) as response_info:
                # 换用 networkidle 等待策略，确保页面充分加载
                await page.goto(TARGET_URL, wait_until="networkidle")

            response = await response_info.value
            print(f"\n[+] 成功拦截到底层 API: {response.url[:100]}...\n")
            
            raw_text = await response.text()
            
            match = re.search(r'^[^\(]*\((.*)\)[^\)]*$', raw_text, re.DOTALL)
            json_str = match.group(1) if match else raw_text
                
            data_dict = json.loads(json_str)
            board_list = data_dict.get("data", {}).get("diff", [])
            
            if not board_list:
                print("[-] 警告: 数据解析为空。")
                sys.exit(1)

            print(f"[+] 获取板块数量: {len(board_list)}")
            print("-" * 50)
            for board in board_list[:5]:
                print(f"{board.get('f12', 'N/A'):<10} | {board.get('f14', 'N/A'):<15} | {board.get('f3', 'N/A'):<10}")
            print("-" * 50)

        except PlaywrightTimeoutError:
            print("\n[-] 错误: 等待底层 API 响应超时。正在进行现场取证...")
            
            # [核心排错逻辑] 打印当前页面标题和 DOM 片段
            title = await page.title()
            print(f"[*] 当前页面标题: {title}")
            
            html = await page.content()
            print(f"[*] 页面 HTML 前 500 个字符:\n{html[:500]}\n...")
            
            if "403" in html or "Forbidden" in html or "访问拒绝" in html:
                print("\n[!] 诊断结论: GitHub Actions 的海外 IP 被东方财富 WAF 防火墙拦截 (403)。")
                print("[!] 解决方案: 需要在 Action 中配置国内代理 IP，或直接使用 Python 请求该 API 接口。")
            else:
                print("\n[!] 诊断结论: 页面加载正常，但 API 接口未能按预期触发，可能是 JS 加载极度缓慢。")
            
            # 保存截图供下载查看
            await page.screenshot(path="error_trace.png")
            print("[*] 截图已保存至 error_trace.png，可在 Action Artifact 中下载查看。")
            sys.exit(1)
            
        except Exception as e:
            print(f"[-] 发生未知异常: {e}")
            sys.exit(1)
        finally:
            await browser.close()

if __name__ == "__main__":
    asyncio.run(fetch_eastmoney_boards())
