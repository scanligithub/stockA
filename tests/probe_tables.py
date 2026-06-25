import requests

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Referer": "https://gxb.eastmoney.com/"
}

# 1. 验证 client 参数越权越狱（绕过“配置不存在”拦截）
def test_gateway_bypass():
    print("="*80)
    print("🔬 [诊断 1] 验证 Datacenter 网关 client 客户端越权阻断绕过")
    print("="*80)
    
    # 待验证的真实表名
    target_tables = ["RPT_F10_FN_MAINOPBCOMP", "RPT_MAP_CH_NODE"]
    # 穷举测试不同的客户端特征参数，或不传 client
    client_variants = ["APP", "PC", "PHONE", "IPAD", "None"]
    
    url = "https://datacenter.eastmoney.com/api/data/v1/get"
    
    for table in target_tables:
        print(f"\n📡 开始探测表: {table} ...")
        for clt in client_variants:
            params = {
                "pageSize": "1",
                "pageNumber": "1",
                "reportName": table,
                "columns": "ALL",
            }
            if clt != "None":
                params["client"] = clt
                
            # 只有个股表需要带上过滤防全表扫描拦截
            if "MAINOPB" in table:
                params["filter"] = '(SECURITY_CODE="300750")'
            else:
                params["sortColumns"] = "UPDATE_DATE"
                params["sortTypes"] = "-1"
                
            try:
                res = requests.get(url, params=params, headers=HEADERS, timeout=5).json()
                code = res.get("code")
                msg = res.get("message", "Success")
                
                if code == 0:
                    print(f"   🎯 【越狱成功！】当 client='{clt}' 时，成功绕过白名单！返回数据样例:")
                    print(f"      - {str(res.get('result', {}).get('data', []))[:180]}")
                    break
                else:
                    print(f"   ❌ client='{clt}' $\rightarrow$ 失败原因: {msg}")
            except Exception as e:
                print(f"   ❌ client='{clt}' 异常: {e}")

# 2. 彻底打印股侠宝首页 HTML 源码进行人工审计
def print_gxb_html():
    print("\n" + "="*80)
    print("🔬 [诊断 2] 彻底解包股侠宝 (gxb.eastmoney.com) 首页 HTML 源码")
    print("="*80)
    
    url = "https://gxb.eastmoney.com/"
    try:
        res = requests.get(url, headers=HEADERS, timeout=5)
        res.encoding = 'utf-8'
        print(f"✅ 成功下载首页，总字数: {len(res.text)} 字。源码展现如下:")
        print("-" * 80)
        print(res.text)
        print("-" * 80)
    except Exception as e:
        print(f"❌ 下载失败: {e}")

if __name__ == "__main__":
    test_gateway_bypass()
    print_gxb_html()
