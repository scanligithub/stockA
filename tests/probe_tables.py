import requests
from concurrent.futures import ThreadPoolExecutor, as_completed

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Referer": "https://quote.eastmoney.com/"
}

DOMAINS = [
    "https://datacenter.eastmoney.com",
    "https://datacenter-web.eastmoney.com"
]

# 生成主营构成（Micro）全排列候选池
CANDIDATES_MAINBUS = [
    # RPT_LICO 系列
    "RPT_LICO_FN_MAINOPBCOMP", "RPT_LICO_FN_MAINOPBDETAIL", "RPT_LICO_FN_MAINOPB",
    "RPT_LICO_MAINOPBCOMP", "RPT_LICO_MAINOPBDETAIL", "RPT_LICO_MAINOPB",
    "RPT_LICO_FN_MAINBUSINESS", "RPT_LICO_MAINBUSINESS", "RPT_LICO_OPBCOMP",
    # RPT_F10 系列
    "RPT_F10_FN_MAINOPBCOMP", "RPT_F10_FN_MAINOPBDETAIL", "RPT_F10_FN_MAINOPB",
    "RPT_F10_MAINOPBCOMP", "RPT_F10_MAINOPBDETAIL", "RPT_F10_MAINOPB",
    "RPT_F10_FN_MAINBUSINESS", "RPT_F10_MAINBUSINESS", "RPT_F10_OPBCOMP",
    # RPT_FREE / RPT_DMS / RPT_ORG 系列
    "RPT_FREE_FN_MAINOPBCOMP", "RPT_DMS_F10_MAINOPB", "RPT_ORG_FN_MAINOPB",
    # 极简系列
    "RPT_MAINOPBCOMP", "RPT_MAINOPBDETAIL", "RPT_MAIN_OPBCOMP", "RPT_MAIN_OPBDETAIL"
]

# 生成产业链图谱（Macro）全排列候选池
CANDIDATES_CHAIN = [
    "RPT_MAP_CH_NODE", "RPT_MAP_CH_RELATION", "RPT_MAP_CH_STOCK",
    "RPT_CH_MAP_NODE", "RPT_CH_MAP_RELATION", "RPT_CH_MAP_STOCK",
    "RPT_INDUSTRY_MAP_NODE", "RPT_INDUSTRY_MAP_RELATION",
    "RPT_MAP_INDUSTRY_NODE", "RPT_MAP_INDUSTRY_RELATION",
    "RPT_A_SHARE_CHAIN_NODE", "RPT_A_SHARE_CHAIN_RELATION",
    "RPT_INDUSTRY_CHAIN_NODE", "RPT_INDUSTRY_CHAIN_RELATION",
    "RPT_CH_NODE", "RPT_CH_RELATION", "RPT_MAP_NODE", "RPT_MAP_RELATION"
]

def check_single_table(domain, name):
    url = f"{domain}/api/data/v1/get"
    # 无任何 Filter 干净探路，绝不引发因列名错误的报错
    params = {
        "pageSize": "1",
        "pageNumber": "1",
        "reportName": name,
        "columns": "ALL",
        "client": "WEB"
    }
    try:
        res = requests.get(url, params=params, headers=HEADERS, timeout=4).json()
        if res.get("code") == 0:
            return name, domain, True, "成功"
        err_msg = res.get("message", "Unknown")
        return name, domain, False, err_msg
    except Exception as e:
        return name, domain, False, str(e)

def main():
    print("="*80)
    print("🚀 启动 40+ 候选表名高并发扫除雷达")
    print("="*80)

    # 1. 扫描主营业务表
    print("\n[1] 开始并行扫描主营业务表名...")
    futures = []
    with ThreadPoolExecutor(max_workers=30) as executor:
        for domain in DOMAINS:
            for name in CANDIDATES_MAINBUS:
                futures.append(executor.submit(check_single_table, domain, name))
                
        success_bus = []
        for f in as_completed(futures):
            name, domain, is_ok, msg = f.result()
            if is_ok:
                domain_clean = domain.split("//")[1]
                success_bus.append(f"{name} ({domain_clean})")
                
    if success_bus:
        print("    🎯 【发现可用主营业务表】:")
        for s in success_bus:
            print(f"      - {s}")
    else:
        print("    ❌ 扫描完毕，未发现可用主营业务表。")

    # 2. 扫描产业链图谱表
    print("\n[2] 开始并行扫描产业链图谱表名...")
    futures = []
    with ThreadPoolExecutor(max_workers=30) as executor:
        for domain in DOMAINS:
            for name in CANDIDATES_CHAIN:
                futures.append(executor.submit(check_single_table, domain, name))
                
        success_chain = []
        for f in as_completed(futures):
            name, domain, is_ok, msg = f.result()
            if is_ok:
                domain_clean = domain.split("//")[1]
                success_chain.append(f"{name} ({domain_clean})")
                
    if success_chain:
        print("    🎯 【发现可用产业链表】:")
        for s in success_chain:
            print(f"      - {s}")
    else:
        print("    ❌ 扫描完毕，未发现可用产业链表。")

    print("\n" + "="*80)
    if success_bus or success_chain:
        print("🎉 雷达扫描取得突破！")
    else:
        print("⚠️ 未匹配到任何表名。")
    print("="*80 + "\n")

if __name__ == "__main__":
    main()
