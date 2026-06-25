import requests

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Referer": "https://quote.eastmoney.com/"
}

# 1. 主营构成候选表名
CANDIDATES_MAINBUS = [
    "RPT_F10_FN_MAINOPBCOMP",    # 候选 1 (最可能，OPB 代表 Operating Business)
    "RPT_F10_FN_MAINOPBDETAIL", # 候选 2 (明细表)
    "RPT_F10_FN_MAINBUSSINESS", # 候选 3
    "RPT_F10_FN_MAINORBCOMP"    # 候选 4 (之前失败的)
]

# 2. 产业链图谱候选表名
CANDIDATES_CHAIN = [
    "RPT_MAP_CH_NODE",           # 候选 1 (最可能，图谱链节点)
    "RPT_MAP_CH_RELATION",       # 候选 2 (图谱链关系)
    "RPT_INDUSTRY_MAP_NODE",     # 候选 3 (行业图谱节点)
    "RPT_INDUSTRY_MAP_RELATION", # 候选 4 (行业图谱关系)
    "RPT_CH_MAP_NODE"            # 候选 5 (之前失败的)
]

def probe_table(report_name, is_stock=True):
    url = "https://datacenter.eastmoney.com/api/data/v1/get"
    params = {
        "pageSize": "3",
        "pageNumber": "1",
        "reportName": report_name,
        "columns": "ALL",
        "client": "WEB"
    }
    if is_stock:
        # 个股查询需要带上过滤条件防止全表扫描报错
        params["filter"] = '(SECURITY_CODE="300750")'
    else:
        # 产业链宏观表不需要个股过滤，只需限制数量
        params["sortColumns"] = "CHAIN_CODE" if "CHAIN" in report_name or "CH" in report_name else "UPDATE_DATE"
        params["sortTypes"] = "1"

    try:
        res = requests.get(url, params=params, headers=HEADERS, timeout=8).json()
        if res.get("code") == 0 and res.get("result"):
            return True, None
        return False, res.get("message", "Unknown error")
    except Exception as e:
        return False, str(e)

def main():
    print("="*60)
    print("🔍 启动东财数据网关物理表名深度探测")
    print("="*60)

    # 探测主营构成
    print("\n[1] 正在探测主营产品构成数据源...")
    found_mainbus = False
    for name in CANDIDATES_MAINBUS:
        success, err = probe_table(name, is_stock=True)
        if success:
            print(f"    🎯 【成功探测】主营业务真实表名为: {name}")
            found_mainbus = True
            break
        else:
            print(f"    ❌ {name} -> 失败原因: {err}")

    # 探测产业链
    print("\n[2] 正在探测宏观产业链中心数据源...")
    found_chain = False
    for name in CANDIDATES_CHAIN:
        success, err = probe_table(name, is_stock=False)
        if success:
            print(f"    🎯 【成功探测】产业链中心真实表名为: {name}")
            found_chain = True
            break
        else:
            print(f"    ❌ {name} -> 失败原因: {err}")

    print("\n" + "="*60)
    if found_mainbus and found_chain:
        print("🎉 【探测圆满成功】所有真实表名均已对齐，可直接进行下一步开发！")
    else:
        print("⚠️ 探测部分完成，请查看上述输出日志。")
    print("="*60 + "\n")

if __name__ == "__main__":
    main()
