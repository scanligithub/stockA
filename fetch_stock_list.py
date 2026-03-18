import polars as pl
import sys
from datetime import datetime

def fetch_all_a_shares():
    """使用 Playwright 模拟真浏览器获取股票列表"""
    print(f"[*] 正在启动浏览器... {datetime.now().strftime('%H:%M:%S')}")
    
    try:
        from playwright.sync_api import sync_playwright
    except ImportError:
        print("[-] 请先安装 playwright: pip install playwright")
        print("[-] 然后运行：playwright install")
        sys.exit(1)

    with sync_playwright() as p:
        # 启动浏览器（headless 模式）
        browser = p.chromium.launch(headless=True)
        context = browser.new_context(
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            viewport={"width": 1920, "height": 1080}
        )
        page = context.new_page()
        
        try:
            print("[*] 正在访问东方财富股票列表页面...")
            page.goto("https://quote.eastmoney.com/center/gridlist.html#stock_a_all", timeout=60000)
            
            # 等待表格加载
            print("[*] 等待数据加载...")
            page.wait_for_selector(".table-content table", timeout=60000)
            
            # 提取表格数据
            print("[*] 提取数据...")
            rows = page.query_selector_all(".table-content table tbody tr")
            
            data = []
            for row in rows:
                cells = row.query_selector_all("td")
                if len(cells) >= 5:
                    code = cells[0].inner_text()
                    name = cells[1].inner_text()
                    price = cells[2].inner_text()
                    pct_chg = cells[3].inner_text()
                    
                    # 处理 "-" 值
                    price = None if price == "-" else float(price)
                    pct_chg = None if pct_chg == "-" else float(pct_chg)
                    
                    # 添加市场后缀
                    if code.startswith("6"):
                        symbol = code + ".SH"
                    elif code.startswith(("0", "3")):
                        symbol = code + ".SZ"
                    elif code.startswith(("8", "4")):
                        symbol = code + ".BJ"
                    else:
                        symbol = code
                    
                    data.append({
                        "code": code,
                        "name": name,
                        "symbol": symbol,
                        "price": price,
                        "pct_chg": pct_chg
                    })
            
            print(f"[+] 成功获取 {len(data)} 条数据")
            
            browser.close()
            
        except Exception as e:
            browser.close()
            print(f"[-] 获取失败：{type(e).__name__}: {e}")
            sys.exit(1)

    if not data:
        print("[-] 没有获取到任何数据")
        sys.exit(1)

    # 转换为 Polars DataFrame
    df = pl.DataFrame(data)
    df = df.sort("symbol")
    
    print(f"[+] 有效个股：{len(df)} 只")
    print(df.head(5))
    
    df.write_csv("a_share_list.csv")
    print("[*] a_share_list.csv 已就绪")

if __name__ == "__main__":
    fetch_all_a_shares()
