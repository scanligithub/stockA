import akshare as ak
import pandas as pd
import time


def safe_fetch(func, name, retry=3):
    for i in range(retry):
        try:
            print(f"获取{name}... 第{i+1}次")
            df = func()
            if df is not None and len(df) > 0:
                return df
        except Exception as e:
            print(f"{name}失败: {e}")
        
        time.sleep(3)

    raise Exception(f"{name}获取失败")


def main():
    concept_df = safe_fetch(ak.stock_board_concept_name_em, "概念板块")
    industry_df = safe_fetch(ak.stock_board_industry_name_em, "行业板块")

    concept_df.to_csv("concept_boards.csv", index=False, encoding="utf-8-sig")
    industry_df.to_csv("industry_boards.csv", index=False, encoding="utf-8-sig")

    print("完成 ✅")


if __name__ == "__main__":
    main()
