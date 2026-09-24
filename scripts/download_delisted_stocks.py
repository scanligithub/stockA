#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Download all delisted A-share stocks from SSE and SZSE."""

from __future__ import annotations

import io
import random
import time
from pathlib import Path

import pandas as pd
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

OUTPUT_DIR = Path(__file__).resolve().parents[1] / "data" / "universe"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
SSE_URL = "https://query.sse.com.cn/commonQuery.do"
SZSE_URL = "https://www.szse.cn/api/report/ShowReport"

SSE_HEADERS = {
    "Accept": "*/*",
    "Accept-Encoding": "gzip, deflate",
    "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
    "Cache-Control": "no-cache",
    "Connection": "keep-alive",
    "Host": "query.sse.com.cn",
    "Pragma": "no-cache",
    "Referer": "https://www.sse.com.cn/",
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/150.0.0.0 Safari/537.36",
}
SZSE_HEADERS = {
    "Accept": "*/*",
    "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
    "Referer": "https://www.szse.cn/market/stock/suspend/index.html",
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/150.0.0.0 Safari/537.36",
}
REQUEST_TIMEOUT = (15, 60)
HTTP_RETRIES = 2
SOURCE_ATTEMPTS = 4


def build_session():
    session = requests.Session()
    retry = Retry(
        total=HTTP_RETRIES,
        connect=HTTP_RETRIES,
        read=HTTP_RETRIES,
        status=HTTP_RETRIES,
        backoff_factor=2.0,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=frozenset({"GET"}),
        raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retry, pool_connections=2, pool_maxsize=2)
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    return session


def get_with_retry(session, url, *, params, headers, label):
    last_error = None
    for attempt in range(1, HTTP_RETRIES + 1):
        try:
            response = session.get(url, params=params, headers=headers, timeout=REQUEST_TIMEOUT)
            response.raise_for_status()
            if not response.content:
                raise RuntimeError(f"{label}: empty HTTP response")
            print(f"[{label}] HTTP {response.status_code}, {len(response.content):,} bytes, attempt={attempt}")
            return response
        except Exception as exc:
            last_error = exc
            if attempt == HTTP_RETRIES:
                break
            delay = min(30, 2 ** (attempt - 1) + random.random())
            print(f"[{label}] attempt {attempt}/{HTTP_RETRIES} failed: {type(exc).__name__}: {exc}; retry in {delay:.1f}s")
            time.sleep(delay)
    raise RuntimeError(f"{label}: all HTTP retries failed") from last_error


def normalize_codes(df, exchange):
    """Normalize exchange codes to six digits.

    SZSE's XLSX may let Excel/pandas interpret codes such as 000003 as
    integers, yielding values like "3". Any non-empty 1-6 digit value is
    therefore a valid code candidate and is left-padded to six digits.
    Empty rows are rejected as non-stock rows; other non-numeric values fail.
    """
    raw = df["code"].astype("string").str.strip()

    blank = raw.isna() | raw.eq("")
    numeric_code = raw.str.fullmatch(r"\d{1,6}", na=False)
    malformed = ~blank & ~numeric_code
    if malformed.any():
        values = raw.loc[malformed].tolist()
        raise RuntimeError(
            f"{exchange}: malformed non-empty stock codes: {values[:20]}"
        )

    rejected = df.loc[blank].copy()
    if not rejected.empty:
        rejected.insert(0, "exchange", exchange)
        rejected.insert(1, "reject_reason", "blank stock code")
        print(f"{exchange}: ignored {len(rejected)} row(s) with blank stock code")

    valid = df.loc[~blank].copy()
    valid["code"] = raw.loc[~blank].str.zfill(6)
    return valid, rejected


def fetch_sse(session):
    params = {
        "sqlId": "COMMON_SSE_CP_GPJCTPZ_GPLB_GP_L",
        "isPagination": "true",
        "STOCK_CODE": "",
        "CSRC_CODE": "",
        "REG_PROVINCE": "",
        "STOCK_TYPE": "1,2,8",
        "COMPANY_STATUS": "3",
        "type": "inParams",
        "pageHelp.cacheSize": "1",
        "pageHelp.beginPage": "1",
        "pageHelp.pageSize": "500",
        "pageHelp.pageNo": "1",
        "pageHelp.endPage": "1",
        "_ts": str(int(time.time() * 1000)),
    }
    response = get_with_retry(session, SSE_URL, params=params, headers=SSE_HEADERS, label="SSE")
    payload = response.json()
    result = payload.get("result")
    if not isinstance(result, list):
        raise RuntimeError(f"SSE: unexpected JSON structure, keys={list(payload)[:10]}")
    df = pd.DataFrame(result)
    required = {"COMPANY_CODE", "COMPANY_ABBR", "LIST_DATE", "DELIST_DATE"}
    missing = required - set(df.columns)
    if missing:
        raise RuntimeError(f"SSE: missing columns: {sorted(missing)}")
    df = df.rename(columns={
        "COMPANY_CODE": "code", "COMPANY_ABBR": "name",
        "LIST_DATE": "list_date", "DELIST_DATE": "delist_date",
    })[["code", "name", "list_date", "delist_date"]]
    df, rejected = normalize_codes(df, "SSE")
    df["list_date"] = pd.to_datetime(df["list_date"], errors="coerce").dt.date
    df["delist_date"] = pd.to_datetime(df["delist_date"], errors="coerce").dt.date
    df["exchange"] = "SSE"

    duplicate_mask = df.duplicated("code", keep=False)
    duplicate_count = int(df.loc[duplicate_mask, "code"].nunique())
    if duplicate_count:
        print(
            f"[SSE] source contains {duplicate_count} duplicated code(s); "
            "deduplicating by code and keeping the first row"
        )
        df = df.drop_duplicates("code", keep="first").reset_index(drop=True)

    return df, rejected


def fetch_szse(session):
    params = {
        "SHOWTYPE": "xlsx",
        "CATALOGID": "1793_ssgs",
        "TABKEY": "tab2",
        "random": f"{random.random():.16f}",
    }
    response = get_with_retry(session, SZSE_URL, params=params, headers=SZSE_HEADERS, label="SZSE")
    try:
        df = pd.read_excel(io.BytesIO(response.content))
    except Exception as exc:
        raise RuntimeError("SZSE: response is not a readable XLSX file") from exc
    required = {"证券代码", "证券简称", "上市日期", "终止上市日期"}
    missing = required - set(df.columns)
    if missing:
        raise RuntimeError(f"SZSE: missing columns: {sorted(missing)}; actual columns={list(df.columns)}")
    df = df.rename(columns={
        "证券代码": "code", "证券简称": "name",
        "上市日期": "list_date", "终止上市日期": "delist_date",
    })[["code", "name", "list_date", "delist_date"]]
    df, rejected = normalize_codes(df, "SZSE")
    df["list_date"] = pd.to_datetime(df["list_date"], errors="coerce").dt.date
    df["delist_date"] = pd.to_datetime(df["delist_date"], errors="coerce").dt.date
    df["exchange"] = "SZSE"

    duplicate_mask = df.duplicated("code", keep=False)
    duplicate_count = int(df.loc[duplicate_mask, "code"].nunique())
    if duplicate_count:
        print(
            f"[SZSE] source contains {duplicate_count} duplicated code(s); "
            "deduplicating by code and keeping the first row"
        )
        df = df.drop_duplicates("code", keep="first").reset_index(drop=True)

    return df, rejected


def validate(df):
    if df.empty:
        raise RuntimeError("merged delisted stock list is empty")
    invalid_code = ~df["code"].astype("string").str.fullmatch(r"\d{6}", na=False)
    if invalid_code.any():
        raise RuntimeError(f"invalid stock codes: {df.loc[invalid_code, 'code'].tolist()[:10]}")
    duplicated = df[df.duplicated("code", keep=False)]
    if not duplicated.empty:
        raise RuntimeError(f"duplicate stock codes remain: {duplicated['code'].tolist()[:20]}")
    if len(df) < 300:
        raise RuntimeError(f"delisted stock count unexpectedly low: {len(df)} < 300")
    if df["delist_date"].notna().sum() < 300:
        raise RuntimeError("too many missing delist dates")
    print(f"VALIDATION PASS: {len(df)} unique delisted stocks")


def fetch_source_with_retry(session, fetcher, label):
    """Retry the complete source fetch, including parsing and validation."""
    last_error = None
    for attempt in range(1, SOURCE_ATTEMPTS + 1):
        try:
            df, rejected = fetcher(session)
            if df.empty:
                raise RuntimeError("parsed dataframe is empty")
            if df["code"].nunique() != len(df):
                raise RuntimeError("duplicate stock codes remain after source normalization")
            if df["delist_date"].notna().sum() == 0:
                raise RuntimeError("no valid delist dates in source response")
            print(f"[{label}] source validation PASS, rows={len(df)}, source_attempt={attempt}")
            return df, rejected
        except Exception as exc:
            last_error = exc
            if attempt == SOURCE_ATTEMPTS:
                break
            delay = min(60, 5 * attempt + random.uniform(0, 3))
            print(f"[{label}] source attempt {attempt}/{SOURCE_ATTEMPTS} failed: {type(exc).__name__}: {exc}; retry in {delay:.1f}s")
            time.sleep(delay)
    raise RuntimeError(f"{label}: all {SOURCE_ATTEMPTS} source attempts failed") from last_error


def main():
    session = build_session()

    print("Downloading SSE delisted stocks...")
    sse, sse_rejected = fetch_source_with_retry(session, fetch_sse, "SSE")
    print(f"SSE rows: {len(sse)}")

    print("Downloading SZSE delisted stocks...")
    szse, szse_rejected = fetch_source_with_retry(session, fetch_szse, "SZSE")
    print(f"SZSE rows: {len(szse)}")

    sse.to_csv(OUTPUT_DIR / "sse_delisted.csv", index=False, encoding="utf-8-sig")
    szse.to_csv(OUTPUT_DIR / "szse_delisted.csv", index=False, encoding="utf-8-sig")

    rejected = pd.concat([sse_rejected, szse_rejected], ignore_index=True)
    rejected.to_csv(
        OUTPUT_DIR / "invalid_delisted_rows.csv",
        index=False,
        encoding="utf-8-sig",
    )

    merged_raw = pd.concat([sse, szse], ignore_index=True)
    duplicate_codes = merged_raw.loc[
        merged_raw.duplicated("code", keep=False), "code"
    ].nunique()

    merged = merged_raw.drop_duplicates(subset=["code"], keep="first").sort_values(
        ["delist_date", "code"], ascending=[False, True], na_position="last"
    ).reset_index(drop=True)
    validate(merged)

    output = OUTPUT_DIR / "a_share_delisted_all.csv"
    merged.to_csv(output, index=False, encoding="utf-8-sig")

    print("========== RESULT ==========")
    print(f"SSE valid rows:       {len(sse)}")
    print(f"SZSE valid rows:      {len(szse)}")
    print(f"Blank-code rows:      {len(rejected)}")
    print(f"Duplicate codes:      {duplicate_codes}")
    print(f"UNIQUE delisted:      {len(merged)}")
    print(f"OUTPUT:               {output}")
    print("=============================")


if __name__ == "__main__":
    main()
