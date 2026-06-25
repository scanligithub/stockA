import requests

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Referer": "https://quote.eastmoney.com/"
}

def test_macro_chain():
    print("📡 正在测试直连拉取东财产业链中心地图...")
    url = "https://datacenter-web.eastmoney.com/api/data/v1/get"
    
    # 尝试拉取全量产业链拓扑树 (RPT_CH_MAP_NODE)
    params = {
        "sortColumns": "CHAIN_CODE,NODE_LEVEL",
        "sortTypes": "1,1",
        "pageSize": "30",  # 测试拉取前 30 条关系即可
        "pageNumber": "1",
        "reportName": "RPT_CH_MAP_NODE",
        "columns": "CHAIN_CODE,CHAIN_NAME,NODE_CODE,NODE_NAME,NODE_LEVEL,PARENT_NODE_CODE",
        "client": "WEB"
    }
    
    try:
        res = requests.get(url, params=params, headers=HEADERS, timeout=10)
        if res.status_code != 200:
            print(f"❌ 请求失败，状态码: {res.status_code}")
            return False
            
        data = res.json()
        if data.get("code") == 0 and data.get("result"):
            records = data["result"]["data"]
            print(f"✅ 成功捕获官方产业链拓扑节点。样例前 5 条:")
            for item in records[:5]:
                level_map = {"1": "上游", "2": "中游", "3": "下游"}
                level = level_map.get(str(item.get("NODE_LEVEL")), "其他")
                print(f"   - 产业链: {item.get('CHAIN_NAME')} | 节点: {item.get('NODE_NAME')} ({level}) | 父节点ID: {item.get('PARENT_NODE_CODE')}")
            return True
        else:
            print(f"❌ 接口返回异常: {data.get('message', '无返回消息')}")
            return False
    except Exception as e:
        print(f"❌ 请求异常: {str(e)}")
        return False

def main():
    print("="*60)
    print("🔬 开始测试: 东财官方产业链中心数据 (宏观)")
    print("="*60)
    test_macro_chain()
    print("="*60 + "\n")

if __name__ == "__main__":
    main()
