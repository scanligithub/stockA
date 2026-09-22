import os
import pandas as pd


CSV_PATH = "temp_index_kline.csv"


def main():
    print("=" * 70)
    print("stockA fetch_index.py integration test")
    print("=" * 70)

    if not os.path.exists(CSV_PATH):
        raise RuntimeError(f"CSV not found: {CSV_PATH}")

    df = pd.read_csv(CSV_PATH)

    print(f"Raw rows: {len(df):,}")
    print(f"Columns: {list(df.columns)}")
    print()

    # ----------------------------------------------------------
    # 1. 验证 Go CSV 中 code 没有丢失
    # ----------------------------------------------------------

    if "code" not in df.columns:
        raise RuntimeError("❌ code column missing")

    missing_code = df["code"].isna() | (df["code"] == "")

    if missing_code.any():
        bad = df.loc[missing_code].head(10)
        print("❌ Found rows with empty code:")
        print(bad)
        raise RuntimeError("code was lost")

    print("✅ All rows have code")

    # ----------------------------------------------------------
    # 2. 日期解析
    # ----------------------------------------------------------

    df["date_dt"] = pd.to_datetime(
        df["date"],
        errors="coerce",
    )

    bad_date = df["date_dt"].isna()

    if bad_date.any():
        print("❌ Invalid dates:")
        print(df.loc[bad_date].head(10))
        raise RuntimeError("invalid date")

    df["date"] = df["date_dt"].dt.strftime("%Y-%m-%d")
    df = df.drop(columns=["date_dt"])

    # ----------------------------------------------------------
    # 3. raw coverage
    # ----------------------------------------------------------

    print()
    print("🔍 Go-TDX raw coverage:")
    print(
        df.groupby("code")["date"]
        .agg(["count", "min", "max"])
        .sort_index()
        .to_string()
    )

    # ----------------------------------------------------------
    # 4. 检查是否发生指数之间的数据碰撞
    # ----------------------------------------------------------

    duplicate_count = df.duplicated(
        subset=["date", "code"]
    ).sum()

    print()
    print(f"Duplicate (date, code): {duplicate_count}")

    if duplicate_count:
        raise RuntimeError(
            f"❌ Found {duplicate_count} duplicate rows"
        )

    # ----------------------------------------------------------
    # 5. 模拟 fetch_index.py 的 dedup
    # ----------------------------------------------------------

    df = df.drop_duplicates(
        subset=["date", "code"],
        keep="last",
    )

    df = df.sort_values(
        ["code", "date"]
    )

    # ----------------------------------------------------------
    # 6. pctChg
    # ----------------------------------------------------------

    df["pctChg"] = (
        df.groupby("code")["close"]
        .pct_change()
        * 100
    )

    df["pctChg"] = df["pctChg"].fillna(0.0)

    # ----------------------------------------------------------
    # 7. 最终 coverage
    # ----------------------------------------------------------

    print()
    print("🔍 Final coverage:")
    print(
        df.groupby("code")["date"]
        .agg(["count", "min", "max"])
        .sort_index()
        .to_string()
    )

    # ----------------------------------------------------------
    # 8. 特别检查中证2000
    # ----------------------------------------------------------

    csi2000 = df[df["code"] == "sh.000851"]

    print()
    print("==============================================================")
    print("中证2000 verification")
    print("==============================================================")

    if csi2000.empty:
        raise RuntimeError(
            "❌ sh.000851 not found"
        )

    print(f"Rows: {len(csi2000):,}")
    print(f"First: {csi2000['date'].min()}")
    print(f"Last:  {csi2000['date'].max()}")

    if csi2000["date"].max() < "2026-09-22":
        raise RuntimeError(
            "❌ 中证2000没有到 2026-09-22"
        )

    print("✅ 中证2000 reaches 2026-09-22")

    # ----------------------------------------------------------
    # 9. 最终检查指数数量
    # ----------------------------------------------------------

    codes = sorted(df["code"].unique())

    print()
    print("==============================================================")
    print("Final result")
    print("==============================================================")
    print(f"Unique index codes: {len(codes)}")

    for code in codes:
        rows = df[df["code"] == code]

        print(
            f"{code:12s} "
            f"{len(rows):5d} "
            f"{rows['date'].min()} -> "
            f"{rows['date'].max()}"
        )

    print()
    print("🎉 Integration test PASSED")


if __name__ == "__main__":
    main()
