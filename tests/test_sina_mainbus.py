import pandas as pd
import requests

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
}

TEST_STOCKS = ["300750", "002594", "600519"]

def test_sina_mainbus():
    print("="*75)
    print("🔬 [测试] 通过新浪财经 (Sina Finance) 静态页面获取主营业务及占比")
    print("="*75)
    
    for code in TEST_STOCKS:
        url = f"https://money.finance.sina.com.cn/corp/go.php/vCI_BusinessAnalysis/stockid/{code}.phtml"
        print(f"\n📡 正在从新浪财经拉取并解析: {code} ...")
        try:
            res = requests.get(url, headers=HEADERS, timeout=10)
            # 🛡️ 核心对齐：新浪财经页面强制采用 GBK/GB2312 编码，必须指定防止中文乱码
            res.encoding = 'gbk'
            
            # 使用 pandas 极速硬解析网页内的所有标准 <table> 标签
            tables = pd.read_html(res.text)
            
            # 在解析出的多个表格中，寻找包含主营业务构成特征的表
            found = False
            for i, tbl in enumerate(tables):
                tbl_str = tbl.to_string()
                # 寻找特征词：“主营项目” 或 “按产品分类”
                if "主营项目" in tbl_str and ("比例" in tbl_str or "占比" in tbl_str):
                    print(f"🎯 成功！在新浪页面第 {i} 个表格中检索到主营构成数据。")
                    print("📊 主营产品占比数据样例:")
                    print("-" * 75)
                    # 打印前 6 行数据（通常前两行为表头，后几行为核心产品及占比）
                    print(tbl.head(6).to_string(header=False, index=False))
                    print("-" * 75)
                    found = True
                    break
            if not found:
                print("❌ 未能在该页面的 HTML 中检索到满足主营构成的 Table 结构。")
        except Exception as e:
            print(f"❌ 请求或解析异常: {e}")

if __name__ == "__main__":
    test_sina_mainbus()
