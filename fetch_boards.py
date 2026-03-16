import asyncio
import aiohttp
import sys

async def fetch_boards_direct_api():
    """
    量化级极速抓取：直接裸连东方财富底层 Push2 API
    不再依赖脆弱的浏览器 DOM 和 JS 渲染
    """
    
    # 东方财富获取“行业板块”的底层真实接口
    # 参数解析：
    # fs=m:90+t:2  (m:90代表东财指数，t:2代表行业板块。如果是概念板块，通常是 t:3)
    # pn=1 (页码) pz=50 (每页数量)
    # ut=bd1d9ddb04089700cf9c27f6f7426281 (固定的公钥 Token，常年不变)
    # fields=f12,f14,f2,f3 (只请求我们需要的数据：f12代码, f14名称, f2最新价, f3涨跌幅)
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
    
    # 伪装基本的浏览器请求头，防止被极简的反爬网关拦截
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Referer": "https://quote.eastmoney.com/"
    }

    print("[*] 正在绕过前端渲染，直接请求底层行情 API...")
    
    # 使用 aiohttp 进行高并发异步请求
    async with aiohttp.ClientSession() as session:
        try:
            async with session.get(API_URL, params=params, headers=headers, timeout=10) as response:
                if response.status != 200:
                    print(f"[-] HTTP 请求失败，状态码: {response.status}")
                    sys.exit(1)
                
                # 直接获取纯净 JSON，告别乱七八糟的 JSONP 和正则匹配！
                data = await response.json()
                
                board_list = data.get("data", {}).get("diff", [])
                total_boards = data.get("data", {}).get("total", 0)
                
                if not board_list:
                    print("[-] 警告: 数据解析为空，东方财富可能更改了接口参数。")
                    sys.exit(1)

                print(f"[+] 极速抓取成功！总共有 {total_boards} 个行业板块。")
                print("-" * 55)
                print(f"{'板块代码':<10} | {'板块名称':<15} | {'最新指数':<10} | {'涨跌幅(%)':<10}")
                print("-" * 55)
                
                for board in board_list[:15]: # 打印前15条
                    code = board.get('f12', 'N/A')
                    name = board.get('f14', 'N/A')
                    price = board.get('f2', 'N/A')
                    
                    # f2如果是纯数字，东财通常放大了100倍，如果是字符串"-"代表停牌或无数据
                    if isinstance(price, (int, float)):
                        price = round(price / 100, 2)
                        
                    change = board.get('f3', 'N/A')
                    print(f"{code:<10} | {name:<15} | {price:<10} | {change:<10}")
                    
                print("-" * 55)
                print("[*] 提示: 这种直连方式，在 Action 上运行通常只需要 0.2 秒。")

        except asyncio.TimeoutError:
            print("[-] 错误: API 请求超时。")
            sys.exit(1)
        except Exception as e:
            print(f"[-] 发生未知异常: {e}")
            sys.exit(1)

if __name__ == "__main__":
    # 请确保在 Action 的 requirements.txt 或步骤中安装了 aiohttp
    # pip install aiohttp
    asyncio.run(fetch_boards_direct_api())
