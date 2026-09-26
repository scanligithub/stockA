"""Normalize TDX GBBQ category=1 stock corporate actions for backtesting.

GBBQ contains multiple security types (A-share stocks, funds, bonds, etc.).
BlinkQuant corporate_actions is intentionally limited to A-share stock symbols.
TDX GBBQ category=1 values are stored on a per-10-share basis.
"""

from __future__ import annotations

import argparse

import pandas as pd


ALIASES = {
    "market": ["market", "exchange"],
    "code": ["code"],
    "date": ["date", "datetime", "time"],
    "category": ["category", "type"],
    "fenhong": ["fenhong", "fen_hong", "hongli", "hongli_panqianliutong"],
    "peigujia": ["peigujia", "peigu_jia", "peigujia_qianzongguben"],
    "songzhuangu": [
        "songzhuangu",
        "songgu",
        "zhuangu",
        "songgu_qianzongguben",
    ],
    "peigu": ["peigu", "peigu_houzongguben"],
}


def pick(df: pd.DataFrame, name: str, required: bool = True) -> str | None:
    for col in ALIASES[name]:
        if col in df.columns:
            return col
    if required:
        raise ValueError(f"Missing required GBBQ column {name}; got {list(df.columns)}")
    return None


def normalize_code(code: object) -> str:
    text = str(code).strip().lower()
    if "." in text:
        text = text.split(".", 1)[0]
    return text.zfill(6)


def is_a_share_code(code: str) -> bool:
    return (
        code.startswith(("600", "601", "603", "605", "688", "689"))
        or code.startswith(("000", "001", "002", "003", "300", "301"))
        or code.startswith(("4", "8", "92"))
    )


def market_code(code: object, market: object = None) -> str | None:
    """Normalize an A-share stock code; return None for non-stock instruments."""
    pure = normalize_code(code)
    if not is_a_share_code(pure):
        return None

    if pure.startswith(("4", "8", "92")):
        return f"bj.{pure}"
    if pure.startswith(("600", "601", "603", "605", "688", "689")):
        return f"sh.{pure}"
    if pure.startswith(("000", "001", "002", "003", "300", "301")):
        return f"sz.{pure}"

    # Defensive fallback for future/manual code additions.
    try:
        market_int = int(float(market)) if market is not None else None
    except (TypeError, ValueError):
        market_int = None
    if market_int == 1:
        return f"sh.{pure}"
    if market_int == 0:
        return f"sz.{pure}"
    return None


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("input")
    ap.add_argument("output")
    args = ap.parse_args()

    # dtype=str preserves leading zeros such as 000001.
    df = pd.read_csv(args.input, dtype={"code": str})
    market_col = pick(df, "market", False)
    code_col = pick(df, "code")
    date_col = pick(df, "date")
    category_col = pick(df, "category")
    fenhong_col = pick(df, "fenhong", False)
    peigujia_col = pick(df, "peigujia", False)
    songzhuangu_col = pick(df, "songzhuangu", False)
    peigu_col = pick(df, "peigu", False)

    category_df = df[df[category_col].astype(str).str.strip().eq("1")].copy()
    category_df[date_col] = pd.to_datetime(
        category_df[date_col], errors="coerce"
    ).dt.date
    if category_df[date_col].isna().any():
        raise ValueError("GBBQ contains invalid dates")

    records = []
    skipped_non_stock = 0

    for _, row in category_df.iterrows():
        market = row[market_col] if market_col else None
        code = market_code(row[code_col], market)
        if code is None:
            skipped_non_stock += 1
            continue

        date = row[date_col]
        cash10 = float(row[fenhong_col]) if fenhong_col else 0.0
        rights_price = float(row[peigujia_col]) if peigujia_col else 0.0
        bonus10 = float(row[songzhuangu_col]) if songzhuangu_col else 0.0
        rights10 = float(row[peigu_col]) if peigu_col else 0.0

        if cash10:
            records.append({
                "date": date.isoformat(),
                "code": code,
                "action_type": "cash_dividend",
                "cash_dividend_per_share": cash10 / 10.0,
                "split_ratio": 1.0,
                "rights_price": 0.0,
                "rights_ratio": 0.0,
            })
        if bonus10:
            records.append({
                "date": date.isoformat(),
                "code": code,
                "action_type": "bonus_shares",
                "cash_dividend_per_share": 0.0,
                "split_ratio": 1.0 + bonus10 / 10.0,
                "rights_price": 0.0,
                "rights_ratio": 0.0,
            })
        if rights10:
            records.append({
                "date": date.isoformat(),
                "code": code,
                "action_type": "rights_issue",
                "cash_dividend_per_share": 0.0,
                "split_ratio": 1.0,
                "rights_price": rights_price,
                "rights_ratio": rights10 / 10.0,
            })

    columns = [
        "date",
        "code",
        "action_type",
        "cash_dividend_per_share",
        "split_ratio",
        "rights_price",
        "rights_ratio",
    ]
    out = pd.DataFrame(records, columns=columns)

    if not out.empty:
        action_order = {
            "cash_dividend": 0,
            "bonus_shares": 1,
            "rights_issue": 2,
        }
        out["_action_order"] = out["action_type"].map(action_order).fillna(99)
        out = (
            out.sort_values(["code", "date", "_action_order"])
            .drop(columns="_action_order")
            .drop_duplicates()
            .reset_index(drop=True)
        )

    out.to_parquet(args.output, index=False)
    print(f"GBBQ category=1 rows: {len(category_df):,}")
    print(f"Skipped non-A-share instruments: {skipped_non_stock:,}")
    print(f"Normalized rows: {len(out):,}")
    print(f"Output: {args.output}")


if __name__ == "__main__":
    main()
