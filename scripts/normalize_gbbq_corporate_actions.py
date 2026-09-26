"""Normalize TDX GBBQ category=1 records for backtesting.

The TDX GBBQ source stores XRXD values per 10 shares. This script emits
one normalized row per applicable component and converts cash/stock ratios
to per-share semantics.
"""
from __future__ import annotations

import argparse

import pandas as pd

ALIASES = {
    "market": ["market", "exchange"],
    "code": ["code"],
    "date": ["date", "datetime", "time"],
    "category": ["category", "type"],
    "fenhong": ["fenhong", "fen_hong", "hongli"],
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


def market_code(code: str, market: object = None) -> str:
    """Normalize TDX code to sh./sz./bj. using the decoded market when present."""
    code = str(code).strip().lower()
    if "." in code:
        return code

    # parse_gbbq.py writes numeric codes such as 1 for 000001. Restore
    # the six-digit security code before determining the exchange.
    code = code.zfill(6)

    # TDX market convention: 0=Shenzhen, 1=Shanghai. For market=0,
    # distinguish Beijing Stock Exchange codes by their code prefix.
    try:
        market_int = int(float(market)) if market is not None else None
    except (TypeError, ValueError):
        market_int = None

    if market_int == 1:
        return f"sh.{code}"
    if market_int == 0:
        if code.startswith(("4", "8", "92")):
            return f"bj.{code}"
        return f"sz.{code}"

    # Fallback for older/manual inputs that do not contain market.
    if code.startswith("6"):
        return f"sh.{code}"
    if code.startswith(("4", "8", "92")):
        return f"bj.{code}"
    if code.startswith(("0", "2", "3")):
        return f"sz.{code}"
    raise ValueError(f"Cannot infer exchange for stock code: {code}")


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("input")
    ap.add_argument("output")
    args = ap.parse_args()

    # dtype=str is intentional: otherwise pandas turns 000001 into 1.
    df = pd.read_csv(args.input, dtype={"code": str})
    market_col = pick(df, "market", False)
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
        market = row[market_col] if market_col else None
        code = market_code(row[code_col], market)
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

    columns = [
        "date", "code", "action_type", "cash_dividend_per_share",
        "split_ratio", "rights_price", "rights_ratio",
    ]
    out = pd.DataFrame(records, columns=columns)

    if not out.empty:
        # Same ex-date accounting order: cash dividend, bonus shares, rights issue.
        action_order = {"cash_dividend": 0, "bonus_shares": 1, "rights_issue": 2}
        out["_action_order"] = out["action_type"].map(action_order).fillna(99)
        out = (
            out.sort_values(["code", "date", "_action_order"])
            .drop(columns="_action_order")
            .drop_duplicates()
            .reset_index(drop=True)
        )

    out.to_parquet(args.output, index=False)
    print(f"GBBQ category=1 rows: {len(df):,}; normalized rows: {len(out):,}")
    print(f"Output: {args.output}")


if __name__ == "__main__":
    main()
