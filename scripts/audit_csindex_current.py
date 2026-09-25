#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Audit current CSI index constituents against production PIT membership.

Production history remains sourced exclusively from Sina vCI_CorpXiangGuan.
This script is an independent current-membership audit using CSI's official
downloadable constituent XLS files.
"""

from __future__ import annotations

import json
import re
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
import requests

ROOT = Path(__file__).resolve().parents[1]
OUT = ROOT / "data" / "index_membership" / "csindex_current_audit"
OUT.mkdir(parents=True, exist_ok=True)

UA = (
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/130.0 Safari/537.36"
)

# These are the CSI indexes for which the official current constituent XLS
# endpoint has been verified. More indexes can be added after endpoint audit.
CSI_INDEXES = {
    "000300": "沪深300",
    "000905": "中证500",
    "000852": "中证1000",
    "000904": "中证中盘200",
}


def normalize_code(value: object) -> str:
    if value is None or pd.isna(value):
        return ""
    s = str(value).strip()
    s = re.sub(r"^(sh|sz|bj)\.?\s*", "", s, flags=re.I)
    digits = re.sub(r"\D", "", s)
    if not digits:
        return ""
    return digits.zfill(6) if len(digits) <= 6 else ""


def read_official_xls(session: requests.Session, index_id: str) -> tuple[pd.DataFrame, str]:
    url = (
        "https://oss-ch.csindex.com.cn/static/html/csindex/public/uploads/file/"
        f"autofile/cons/{index_id}cons.xls"
    )
    response = session.get(url, timeout=30)
    response.raise_for_status()
    if len(response.content) < 1000:
        raise RuntimeError(f"{index_id}: official XLS response is suspiciously small")
    path = OUT / f"{index_id}cons.xls"
    path.write_bytes(response.content)

    # CSI's files are old-style XLS; pandas/openpyxl dependencies are already
    # installed by the production workflow. xlrd is installed explicitly below.
    raw = pd.read_excel(path, sheet_name=0, header=None)
    return raw, url


def extract_codes(raw: pd.DataFrame, index_id: str) -> tuple[set[str], str]:
    # Locate the header row by looking for common CSI code-column labels.
    header_row = None
    code_col = None
    for i in range(min(len(raw), 20)):
        for j, value in enumerate(raw.iloc[i].tolist()):
            text = str(value).strip().lower()
            if text in {"成分券代码", "证券代码", "股票代码", "样本代码", "代码"}:
                header_row, code_col = i, j
                break
        if header_row is not None:
            break

    if header_row is None:
        # Fallback: find the column with the largest number of six-digit values.
        best_col = None
        best_count = 0
        for j in range(raw.shape[1]):
            count = sum(bool(re.fullmatch(r"\d{6}", normalize_code(v))) for v in raw.iloc[:, j])
            if count > best_count:
                best_col, best_count = j, count
        if best_col is None or best_count < 50:
            raise RuntimeError(f"{index_id}: cannot identify constituent-code column")
        code_col = best_col
        header_row = 0

    codes = {
        normalize_code(v)
        for v in raw.iloc[header_row + 1:, code_col]
        if normalize_code(v)
    }
    if len(codes) < 10:
        raise RuntimeError(f"{index_id}: parsed only {len(codes)} official constituent codes")
    return codes, str(raw.iloc[header_row, code_col])


def load_production_current(index_id: str) -> set[str]:
    path = ROOT / "data" / "index_membership" / "index_membership_history.parquet"
    if not path.exists():
        raise FileNotFoundError(path)
    df = pd.read_parquet(path)
    required = {"index_id", "stock_id", "start_date", "end_date"}
    if not required.issubset(df.columns):
        raise RuntimeError(f"production parquet missing columns: {required - set(df.columns)}")
    current = df[
        (df["index_id"].astype(str) == index_id)
        & (df["end_date"].fillna("").astype(str).str.strip() == "")
    ]
    return {normalize_code(v) for v in current["stock_id"] if normalize_code(v)}


def main() -> None:
    session = requests.Session()
    session.headers.update({"User-Agent": UA, "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8"})

    results = []
    all_ok = True

    for index_id, index_name in CSI_INDEXES.items():
        try:
            raw, url = read_official_xls(session, index_id)
            official, code_header = extract_codes(raw, index_id)
            production = load_production_current(index_id)

            official_only = sorted(official - production)
            production_only = sorted(production - official)
            intersection = official & production

            status = "PASS" if not official_only and not production_only else "MISMATCH"
            if status != "PASS":
                all_ok = False

            detail = pd.DataFrame(
                [{"index_id": index_id, "stock_id": code, "side": "official_only"} for code in official_only]
                + [{"index_id": index_id, "stock_id": code, "side": "production_only"} for code in production_only]
            )
            detail.to_csv(OUT / f"{index_id}_diff.csv", index=False, encoding="utf-8-sig")

            result = {
                "index_id": index_id,
                "index_name": index_name,
                "status": status,
                "official_count": len(official),
                "production_current_count": len(production),
                "intersection_count": len(intersection),
                "official_only_count": len(official_only),
                "production_only_count": len(production_only),
                "official_only": official_only,
                "production_only": production_only,
                "official_xls_url": url,
                "official_code_header": code_header,
                "checked_at_utc": datetime.now(timezone.utc).isoformat(),
            }
            results.append(result)

            print(
                f"{index_id} {index_name}: {status} "
                f"official={len(official)} production={len(production)} "
                f"official_only={len(official_only)} production_only={len(production_only)}"
            )
            if official_only:
                print("  official_only:", ", ".join(official_only))
            if production_only:
                print("  production_only:", ", ".join(production_only))

        except Exception as exc:
            all_ok = False
            result = {
                "index_id": index_id,
                "index_name": index_name,
                "status": "ERROR",
                "error": repr(exc),
                "checked_at_utc": datetime.now(timezone.utc).isoformat(),
            }
            results.append(result)
            print(f"{index_id} {index_name}: ERROR {exc}")

    summary = {
        "status": "PASS" if all_ok else "MISMATCH_OR_ERROR",
        "source": "CSI official current constituent XLS",
        "production_source": "Sina vCI_CorpXiangGuan historical PIT dataset",
        "indexes_checked": len(CSI_INDEXES),
        "results": results,
    }
    (OUT / "summary.json").write_text(
        json.dumps(summary, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )

    print(json.dumps(summary, ensure_ascii=False, indent=2))
    # This is deliberately REPORT_ONLY: current official data can be published
    # at a different time than Sina. The production build must not fail merely
    # because the two sources are temporarily out of sync.
    print("CSI CURRENT CONSTITUENT AUDIT: REPORT_ONLY")
    if not all_ok:
        print("CSI CURRENT CONSTITUENT AUDIT RESULT: MISMATCH_OR_ERROR")
    else:
        print("CSI CURRENT CONSTITUENT AUDIT RESULT: PASS")


if __name__ == "__main__":
    main()
