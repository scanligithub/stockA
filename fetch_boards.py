import asyncio
import sys
from curl_cffi.requests import AsyncSession

async def fetch_boards_direct_api():
    """
    量化级极速抓取：使用 curl_cffi 模拟真实浏览器 TLS 指纹，裸连底层 API
    """
    API_URL = "https://push2.eastmoney.com/api/qt/clist/get"
    
    params = {
        "pn": "1",
        "pz": "50",
        "po": "1",
        "np": "1",
        "ut": "bd1d9ddb04089700cf9c27f6f7426281",
        "fltt": "2",
        "invt": "2",
        "fid": "f3",
        "fs": "m:90 t:2",
        "fields": "f12,f14,f2,f3"
    }
    
    # 只需要保留 Referer，User-Agent 会由 impersonate 参数自动处理得天衣无缝
    headers = {
        "Referer": "https://quote.eastmoney.com/"
    }

    print("[*] 启动底层网络穿透，伪装 Chrome 120 TLS 指纹请求 API...")
    
    try:
        # 核心魔法：impersonate="chrome120" 让底层的 C 库完全模拟 Chrome 120 的网络协议栈
        async with AsyncSession(impersonate="chrome120") as session:
            # 发起异步 GET 请求
            response = await session.get(
                API_URL, 
                params=params, 
                headers=headers, 
                timeout=15
            )
            
            # curl_cffi 使用 status_code 属性
            if response.status_code != 200:
                print(f"[-] HTTP 请求失败，状态码: {response.status_code}")
                sys.exit(1)
            
            # curl_cffi 的 json() 是同步方法，直接调用即可
            data = response.json()
            
            board_list = data.get("data", {}).get("diff", [])
            total_boards = data.get("data", {}).get("total", 0)
            
            if not board_list:
                print("[-] 警告: 数据解析为空，东方财富可能更改了接口参数。")
                print(f"[*] 原始响应: {response.text[:200]}")
                sys.exit(1)

            print(f"[+] 极速穿透成功！总共有 {total_boards} 个行业板块。")
            print("-" * 55)
            print(f"{'板块代码':<10} | {'板块名称':<15} | {'最新指数':<10} | {'涨跌幅(%)':<10}")
            print("-" * 55)
            
            for board in board_list[:15]:
                code = board.get('f12', 'N/A')
                name = board.get('f14', 'N/A')
                price = board.get('f2', 'N/A')
                
                # f2如果是纯数字，东财通常放大了100倍
                if isinstance(price, (int, float)):
                    price = round(price / 100, 2)
                    
                change = board.get('f3', 'N/A')
                print(f"{code:<10} | {name:<15} | {price:<10} | {change:<10}")
                
            print("-" * 55)
            print("[*] 提示：由于彻底解决了 TLS 阻断，你可以基于此会话(Session)用 asyncio.gather 并发拉取所有页码。")

    except Exception as e:
        print(f"[-] 发生未知异常: {e}")
        sys.exit(1)

if __name__ == "__main__":
    asyncio.run(fetch_boards_direct_api())
