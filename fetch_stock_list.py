import asyncio
import polars as pl
import sys
from datetime import datetime

async def fetch_all_a_shares():
    """使用 Playwright 异步获取股票列表"""
    print(f"[*] 正在启动浏览器... {datetime.now().strftime('%H:%M:%S')}")
    
    try:
        from playwright.async_api import async_playwright
    except ImportError:
        print("[-] 请先安装 playwright: pip install playwright")
        print("[-] 然后运行：playwright install")
        sys.exit(1)

    async with async_playwright() as p:
        browser = await p.chromium.launch(
            headless=True,
            args=["--no-sandbox", "--disable-setuid-sandbox", "--disable-dev-shm-usage", "--disable-gpu"]
        )
        page = await browser.new_page(
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            viewport={"width": 1920, "height": 1080}
        )
        
        try:
            print("[*] 打开页面...")
            
            # 先设置监听器，再跳转
            response_future = page.wait_for_event("response",
                lambda r: "push2.eastmoney.com/api/qt/clist/get" in r.url,
                timeout=60000
            )
            
            await page.goto("https://quote.eastmoney.com/center/gridlist.html", timeout=60000)
            
            print("[*] 等待接口返回...")
            response = await response_future
            
            data = await response.json()
            stocks = data.get("data", {}).get("diff", [])
            print(f"[+] 获取成功：{len(stocks)} 条")
            
            await browser.close()
            
        except Exception as e:
            await browser.close()
            print(f"[-] 获取失败：{type(e).__name__}: {e}")
            sys.exit(1)

    if not stocks:
        print("[-] 没有获取到任何数据")
        sys.exit(1)

    # 处理数据
    clean_data = []
    for item in stocks:
        code = item.get("f12")
        name = item.get("f14")
        price = None if item.get("f2") == "-" else item.get("f2")
        pct_chg = None if item.get("f3") == "-" else item.get("f3")
        
        # 添加市场后缀
        if code.startswith("6"):
            symbol = code + ".SH"
        elif code.startswith(("0", "3")):
            symbol = code + ".SZ"
        elif code.startswith(("8", "4")):
            symbol = code + ".BJ"
        else:
            symbol = code
        
        clean_data.append({
            "code": code,
            "name": name,
            "symbol": symbol,
            "price": float(price) if price else None,
            "pct_chg": float(pct_chg) if pct_chg else None
        })

    df = pl.DataFrame(clean_data)
    df = df.sort("symbol")
    
    print(f"[+] 有效个股：{len(df)} 只")
    print(df.head(5))
    
    df.write_csv("a_share_list.csv")
    print("[*] a_share_list.csv 已就绪")

if __name__ == "__main__":
    asyncio.run(fetch_all_a_shares())
