import akshare as ak
import pandas as pd

TEST_STOCKS = ["300750", "002594", "600519"]

def test_akshare_mainbus():
    print("="*75)
    print("🔬 [测试] 通过 AkShare 调用东财移动 APP 端主营业务构成接口")
    print("="*75)
    
    for symbol in TEST_STOCKS:
        print(f"\n📡 正在通过 AkShare 请求: {symbol} ...")
        try:
            # 调用东财移动端主营构成接口
            df = ak.stock_zygc_em(symbol=symbol)
            
            if df is not None and not df.empty:
                print(f"✅ 成功！捕获到 {len(df)} 行主营数据明细。")
                
                # 东方财富移动端返回的标准列名映射
                # 核心列：'报告期', '分类方向', '主营构成', '占主营业务收入比例'
                cols_to_print = [c for c in ['报告期', '分类方向', '主营构成', '占主营业务收入比例', '毛利率'] if c in df.columns]
                
                # 展示前 5 条产品明细
                print("-" * 75)
                print(df[cols_to_print].head(5).to_string(index=False))
                print("-" * 75)
            else:
                print("❌ 接口返回空数据 DataFrame")
        except Exception as e:
            print(f"❌ 发生异常: {e}")

if __name__ == "__main__":
    test_akshare_mainbus()
