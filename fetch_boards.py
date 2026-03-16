import asyncio
import json
import re
import sys
from playwright.async_api import async_playwright, TimeoutError as PlaywrightTimeoutError

async def fetch_eastmoney_boards():
    """
    使用 Playwright 拦截东方财富行业板块列表 API 数据
    """
    # 东方财富的底层行情接口特征
    TARGET_API_PATTERN = re.compile(r"api/qt/clist/get")
    
    # 行业板块行情页 URL
    TARGET_URL = "https://quote.eastmoney.com/center/boardlist.html#boards-90.BK0000"

    async with async_playwright() as p:
        # 在 GitHub Actions 中必须使用 headless=True
        browser = await p.chromium.launch(headless=True, args=["--no-sandbox", "--disable-setuid-sandbox"])
        
        # 创建独立的上下文，可在此处挂载代理或伪装 User-Agent
        context = await browser.new_context(
            viewport={'width': 1920, 'height': 1080},
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
        )
        page = await context.new_page()

        print(f"[*] 正在导航至东方财富板块列表页...")

        try:
            # 核心杀手锏：不解析网页DOM，而是等待底层 API 响应
            # 这里设置超时时间为 15 秒
            async with page.expect_response(TARGET_API_PATTERN, timeout=15000) as response_info:
                # 触发页面加载
                await page.goto(TARGET_URL, wait_until="domcontentloaded")

            response = await response_info.value
            print(f"[*] 成功拦截到底层 API: {response.url[:80]}...")
            
            # 读取响应内容
            raw_text = await response.text()
            
            # 东方财富通常返回 JSONP 格式，例如：jQuery11240212_17000({"data": ...});
            # 我们需要用正则提取括号内的纯 JSON 数据
            match = re.search(r'^[^\(]*\((.*)\)[^\)]*$', raw_text, re.DOTALL)
            if match:
                json_str = match.group(1)
            else:
                json_str = raw_text # Fallback：如果已经是纯JSON
                
            data_dict = json.loads(json_str)
            
            # 提取板块列表数据 (东方财富的字段通常是 f12:代码, f14:名称, f3:涨跌幅)
            board_list = data_dict.get("data", {}).get("diff", [])
            
            if not board_list:
                print("[-] 警告: 数据解析为空，东方财富可能更改了数据结构。")
                sys.exit(1)

            print(f"[+] 成功获取当前页板块数量: {len(board_list)}")
            print("-" * 50)
            print(f"{'板块代码':<10} | {'板块名称':<15} | {'涨跌幅(%)':<10}")
            print("-" * 50)
            
            for board in board_list[:10]:  # 仅打印前 10 条作为示例
                code = board.get('f12', 'N/A')
                name = board.get('f14', 'N/A')
                change = board.get('f3', 'N/A')
                print(f"{code:<10} | {name:<15} | {change:<10}")
                
            print("-" * 50)
            print("[*] 提示: 此方案仅获取了第一页数据。在实际量化系统中，您可以直接提取截获的 API URL，用 aiohttp 修改 pn (页码) 参数进行极速全量并发抓取。")

        except PlaywrightTimeoutError:
            print("[-] 错误: 等待底层 API 响应超时。网页可能加载失败或接口特征已变更。")
            sys.exit(1)
        except Exception as e:
            print(f"[-] 发生未知异常: {e}")
            sys.exit(1)
        finally:
            await browser.close()

if __name__ == "__main__":
    asyncio.run(fetch_eastmoney_boards())
