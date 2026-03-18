import akshare as ak
import polars as pl
import sys
from datetime import datetime

def fetch_all_a_shares():
    """使用 akshare 获取全市场 A 股列表"""
    print(f"[*] 正在获取股票列表... {datetime.now().strftime('%H:%M:%S')}")
    
    try:
        # 使用静态股票列表接口（不走高频行情接口，几乎不会被封）
        df_ak = ak.stock_info_a_code_name()
        
        if df_ak is None or df_ak.empty:
            print("[-] 获取数据为空")
            sys.exit(1)
        
        print(f"[+] 原始数据：{len(df_ak)} 条")
        
        # 转换为 polars DataFrame
        df = pl.from_pandas(df_ak)
        
        # 重命名列以匹配原有格式
        # akshare 返回的列名：代码、名称、最新价、涨跌幅...
        df = df.rename({
            "代码": "code",
            "名称": "name",
            "最新价": "price",
            "涨跌幅": "pct_chg",
            "涨跌额": "change",
            "成交量": "volume",
            "成交额": "amount",
            "总市值": "market_value",
            "市盈率": "pe",
            "市净率": "pb",
        })
        
        # 添加市场后缀
        df = df.with_columns([
            pl.when(pl.col("code").str.starts_with("6")).then(pl.col("code") + ".SH")
            .when(pl.col("code").str.starts_with("0")).then(pl.col("code") + ".SZ")
            .when(pl.col("code").str.starts_with("3")).then(pl.col("code") + ".SZ")
            .when(pl.col("code").str.starts_with("8")).then(pl.col("code") + ".BJ")
            .when(pl.col("code").str.starts_with("4")).then(pl.col("code") + ".BJ")
            .otherwise(pl.col("code"))
            .alias("symbol")
        ])
        
        # 类型转换
        df = df.with_columns([
            pl.col("price").cast(pl.Float64, strict=False),
            pl.col("pct_chg").cast(pl.Float64, strict=False),
        ])
        
        # 去重并排序
        df = df.filter(pl.col("code").is_not_null()).unique(subset=["symbol"])
        df = df.sort("symbol")
        
        print(f"[+] 有效个股：{len(df)} 只")
        print(df.head(5))
        
        df.write_csv("a_share_list.csv")
        print("[*] a_share_list.csv 已就绪")
        
    except Exception as e:
        print(f"[-] 获取失败：{type(e).__name__}: {e}")
        sys.exit(1)

if __name__ == "__main__":
    fetch_all_a_shares()
