"""Normalize TDX GBBQ category=1 records for backtesting.

The TDX GBBQ source stores XRXD values per 10 shares. This script emits
one normalized row per applicable component and converts cash/stock ratios
to per-share semantics.
"""
from __future__ import annotations

import argparse
import pandas as pd


ALIASES = {
    "code": ["code"],
    "date": ["date", "datetime", "time"],
    "category": ["category", "type"],
    "fenhong": ["fenhong", "fen_hong"],
    "peigujia": ["peigujia", "peigu_jia"],
    "songzhuangu": ["songzhuangu", "songgu", "zhuangu"],
    "peigu": ["peigu"],
}


def pick(df: pd.DataFrame, name: str, required: bool = True) -> str | None:
    for col in ALIASES[name]:
        if col in df.columns:
            return col
    if required:
        raise ValueError(f"Missing required GBBQ column {name}; got {list(df.columns)}")
    return None


def market_code(code: str) -> str:
    code = str(code).strip().lower()
    if "." in code:
        return code
    if code.startswith("6"):
        return f"sh.{code}"
    if code.startswith(("0", "3")):
        return f"sz.{code}"
    if code.startswith(("4", "8")):
        return f"bj.{code}"
    raise ValueError(f"Cannot infer exchange for stock code: {code}")


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("input")
    ap.add_argument("output")
    args = ap.parse_args()

    df = pd.read_csv(args.input)
    code_col = pick(df, "code")
    date_col = pick(df, "date")
    category_col = pick(df, "category")
    fenhong_col = pick(df, "fenhong", False)
    peigujia_col = pick(df, "peigujia", False)
    songzhuangu_col = pick(df, "songzhuangu", False)
    peigu_col = pick(df, "peigu", False)

    df = df[df[category_col].astype(str).str.strip().eq("1")].copy()
    df[date_col] = pd.to_datetime(df[date_col], errors="coerce").dt.date
    if df[date_col].isna().any():
        raise ValueError("GBBQ contains invalid dates")

    records = []
    for _, row in df.iterrows():
        code = market_code(row[code_col])
        date = row[date_col]
        cash10 = float(row[fenhong_col]) if fenhong_col else 0.0
        rights_price = float(row[peigujia_col]) if peigujia_col else 0.0
        bonus10 = float(row[songzhuangu_col]) if songzhuangu_col else 0.0
        rights10 = float(row[peigu_col]) if peigu_col else 0.0

        if cash10:
            records.append({
                "date": date.isoformat(), "code": code,
                "action_type": "cash_dividend",
                "cash_dividend_per_share": cash10 / 10.0,
                "split_ratio": 1.0, "rights_price": 0.0, "rights_ratio": 0.0,
            })
        if bonus10:
            records.append({
                "date": date.isoformat(), "code": code,
                "action_type": "bonus_shares",
                "cash_dividend_per_share": 0.0,
                "split_ratio": 1.0 + bonus10 / 10.0,
                "rights_price": 0.0, "rights_ratio": 0.0,
            })
        if rights10:
            records.append({
                "date": date.isoformat(), "code": code,
                "action_type": "rights_issue",
                "cash_dividend_per_share": 0.0, "split_ratio": 1.0,
                "rights_price": rights_price, "rights_ratio": rights10 / 10.0,
            })

    out = pd.DataFrame(records, columns=[
        "date", "code", "action_type", "cash_dividend_per_share",
        "split_ratio", "rights_price", "rights_ratio"
    ])
    if not out.empty:
        out = out.sort_values(["code", "date", "action_type"]).drop_duplicates().reset_index(drop=True)
    out.to_parquet(args.output, index=False)
    print(f"GBBQ category=1 rows: {len(df):,}; normalized rows: {len(out):,}")
    print(f"Output: {args.output}")


if __name__ == "__main__":
    main()
