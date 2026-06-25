import requests

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Referer": "https://quote.eastmoney.com/"
}

# 两个并存的东财数据中心网关
DOMAINS = [
    "https://datacenter.eastmoney.com",
    "https://datacenter-web.eastmoney.com"
]

# 1. 主营构成候选表名 (重点加入 RPT_LICO_ 空间)
CANDIDATES_MAINBUS = [
    "RPT_LICO_FN_MAINOPBCOMP",    # 候选 1 (最符合 LICO 规范: Listed Co Main Operating Business Composition)
    "RPT_LICO_FN_MAINOPBDETAIL", # 候选 2 (Listed Co Main Operating Business Detail)
    "RPT_LICO_MAINOPBCOMP",       # 候选 3 (无 FN 缩写)
    "RPT_LICO_FN_MAINOPB",        # 候选 4
    "RPT_F10_OPBCOMP",            # 候选 5
    "RPT_F10_MAINOPBCOMP",        # 候选 6
]

# 2. 产业链图谱候选表名 (行业图谱、板块地图相关)
CANDIDATES_CHAIN = [
    "RPT_MAP_CH_NODE",           # 候选 1 (Map Chain Node)
    "RPT_MAP_CH_RELATION",       # 候选 2 (Map Chain Relation)
    "RPT_MAP_CH_STOCK",          # 候选 3
    "RPT_INDUSTRY_CHAIN_NODE",   # 候选 4
    "RPT_INDUSTRY_CHAIN_MAP",    # 候选 5
    "RPT_CH_MAP_NODE"            # 候选 6
]

def probe_table(domain, report_name, is_stock=True):
    url = f"{domain}/api/data/v1/get"
    params = {
        "pageSize": "2",
        "pageNumber": "1",
        "reportName": report_name,
        "columns": "ALL",
        "client": "WEB"
    }
    if is_stock:
        params["filter"] = '(SECURITY_CODE="300750")'
    else:
        params["sortColumns"] = "UPDATE_DATE"
        params["sortTypes"] = "-1"

    try:
        res = requests.get(url, params=params, headers=HEADERS, timeout=5).json()
        if res.get("code") == 0 and res.get("result"):
            return True, None
        return False, res.get("message", "Unknown error")
    except Exception as e:
        return False, str(e)

def main():
    print("="*75)
    print("🔍 启动双域名 + LICO 空间深度联合探测")
    print("="*75)

    # 1. 探测主营构成
    print("\n[1] 开始探测主营产品构成数据源...")
    found_mainbus = False
    for domain in DOMAINS:
        domain_name = domain.split("//")[1]
        for name in CANDIDATES_MAINBUS:
            success, err = probe_table(domain, name, is_stock=True)
            if success:
                print(f"    🎯 【成功】在 [{domain_name}] 下探测到主营业务表: {name}")
                found_mainbus = True
                break
            else:
                # 仅打印异常原因，忽略配置不存在的常规报错
                if "配置不存在" not in str(err):
                    print(f"    ⚠️ [{domain_name}] {name} -> {err}")
        if found_mainbus:
            break

    if not found_mainbus:
        print("    ❌ 未能匹配到主营业务表，我们将继续排查。")

    # 2. 探测产业链
    print("\n[2] 开始探测宏观产业链中心数据源...")
    found_chain = False
    for domain in DOMAINS:
        domain_name = domain.split("//")[1]
        for name in CANDIDATES_CHAIN:
            success, err = probe_table(domain, name, is_stock=False)
            if success:
                print(f"    🎯 【成功】在 [{domain_name}] 下探测到产业链表: {name}")
                found_chain = True
                break
            else:
                if "配置不存在" not in str(err):
                    print(f"    ⚠️ [{domain_name}] {name} -> {err}")
        if found_chain:
            break

    if not found_chain:
        print("    ❌ 未能匹配到产业链地图表，我们将继续排查。")

    print("\n" + "="*75)
    if found_mainbus or found_chain:
        print("🎉 【联合探测取得突破】请根据控制台输出的成功表名进行后续对接。")
    else:
        print("⚠️ 未能探测到目标表名。")
    print("="*75 + "\n")

if __name__ == "__main__":
    main()
