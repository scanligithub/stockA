#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Build production historical membership for the configured Sina indexes.

The implementation is intentionally based on the validated
test/tdx_index_api/test_sina_xiangguan_full_universe.py flow:

    TDX current + SSE/SZSE/BSE terminated + BSE old aliases
        -> one Sina XiangGuan request per query code
        -> target 37 indexes
        -> interval normalization
        -> A/B completeness audit
        -> PIT/boundary validation
        -> index_membership_history.parquet

Query code and canonical stock identity are kept separate so the 248 BSE
pre-switch codes can be queried without creating duplicate securities in the
final membership table.
"""

from __future__ import annotations

import json
import os
import re
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
import requests
from bs4 import BeautifulSoup

BASE = "https://vip.stock.finance.sina.com.cn"
ROOT = Path(__file__).resolve().parents[1]
OUT = ROOT / "data" / "index_membership"
CACHE = ROOT / "data" / "cache" / "sina_xiangguan"
PARSED_CACHE = ROOT / "data" / "cache" / "sina_xiangguan_parsed"
OUT.mkdir(parents=True, exist_ok=True)
CACHE.mkdir(parents=True, exist_ok=True)
PARSED_CACHE.mkdir(parents=True, exist_ok=True)

WORKERS = int(os.getenv("SINA_WORKERS", "16"))
DELAY = float(os.getenv("SINA_DELAY", "0.15"))
TIMEOUT = 20
RETRIES = 4
CACHE_VERSION = 6
CACHE_TTL_SEC = int(os.getenv("SINA_CACHE_TTL_SEC", "86400"))
REFRESH_CURRENT = os.getenv("SINA_REFRESH_CURRENT", "1") != "0"

# Only genuine aliases/equivalent identifiers are normalized here. Wrong
# historical code/name pairs are deliberately NOT aliased into another index.
INDEX_ALIASES = {
    "000001": "000001", "399001": "399001", "399006": "399006",
    "000688": "000688", "899050": "899050", "000016": "000016",
    "000300": "000300", "399300": "000300",
    "000905": "000905", "399905": "000905",
    "000852": "000852", "399852": "000852",
    "000851": "000851", "932000": "000851",
    "399303": "399303", "399330": "399330",
    "000010": "000010", "399324": "399324", "000015": "000015",
    "000904": "000904", "399311": "399311",
    "930713": "930713", "980017": "980017",
    "399354": "399354",
    "399673": "399673", "399412": "399412", "399005": "399005",
    "399994": "399994", "399975": "399975", "399986": "399986",
    "399932": "399932", "399933": "399933", "399967": "399967",
    "399989": "399989", "399971": "399971", "399997": "399997",
    "000928": "000928", "000929": "000929", "399990": "399990",
    "930708": "930708", "399974": "399974",
}

TARGET_INDEXES = {
    "000001": "上证指数", "399001": "深证成指", "399006": "创业板指",
    "000688": "科创50", "899050": "北证50", "000016": "上证50",
    "000300": "沪深300", "000905": "中证500", "000852": "中证1000",
    "000851": "中证2000", "399303": "国证2000", "399330": "深证100",
    "000010": "上证180", "399324": "深证红利", "000015": "红利指数",
    "000904": "中证中盘200", "399311": "国证1000",
    "930713": "中证人工智能主题", "980017": "国证芯片",
    "399354": "分析师指数", "399673": "创业板50",
    "399412": "国证新能源", "399005": "中小100",
    "399994": "中证信息安全", "399975": "证券公司",
    "399986": "中证银行", "399932": "中证消费", "399933": "中证医药",
    "399967": "中证军工", "399989": "中证医疗", "399971": "中证传媒",
    "399997": "中证白酒", "000928": "中证能源", "000929": "中证原材料",
    "399990": "煤炭等权", "930708": "中证有色", "399974": "国证国企",
}


UA = (
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/130.0 Safari/537.36"
)


def normalize_code(value: object) -> str:
    if value is None or pd.isna(value):
        return ""
    raw = str(value).strip()
    raw = re.sub(r"^(sh|sz|bj)\.?", "", raw, flags=re.I)
    return raw.zfill(6) if re.fullmatch(r"\d{1,6}", raw) else ""



def text_value(value: object) -> str:
    if value is None or pd.isna(value):
        return ""
    return str(value).strip()


def read_csv_required(path: Path, *, code_columns: tuple[str, ...]) -> pd.DataFrame:
    if not path.exists():
        raise FileNotFoundError(f"required input not found: {path}")
    df = pd.read_csv(path, dtype="string")
    for col in code_columns:
        if col not in df.columns:
            raise RuntimeError(f"{path}: missing required column {col!r}")
    return df


def load_tdx_current() -> pd.DataFrame:
    path = OUT / "stock_list_master.json"
    if not path.exists():
        raise FileNotFoundError(
            f"Missing {path}; workflow must build TDX current stock list first"
        )
    data = json.loads(path.read_text(encoding="utf-8"))
    rows = []
    for item in data:
        code = normalize_code(item.get("code"))
        if not re.fullmatch(r"\d{6}", code):
            continue
        rows.append(
            {
                "query_code": code,
                "stock_id": code,
                "name": text_value(item.get("code_name")),
                "source": "tdx_current",
                "current_code": code,
            }
        )
    result = pd.DataFrame(rows).drop_duplicates("query_code")
    if len(result) < 5000:
        raise RuntimeError(
            f"TDX current universe suspiciously small: {len(result)}"
        )
    return result


def load_delisted() -> pd.DataFrame:
    path = ROOT / "data" / "universe" / "a_share_delisted_all.csv"
    df = read_csv_required(path, code_columns=("code",))
    rows = []
    for _, item in df.iterrows():
        code = normalize_code(item["code"])
        if not code:
            continue
        current_code = normalize_code(item.get("current_code", "")) or code
        rows.append(
            {
                "query_code": code,
                "stock_id": current_code,
                "name": text_value(item.get("name")),
                "source": "delisted",
                "current_code": current_code,
            }
        )
    result = pd.DataFrame(rows).drop_duplicates("query_code")
    if result.empty:
        raise RuntimeError("delisted universe is empty")
    return result


def load_bse_aliases() -> pd.DataFrame:
    path = ROOT / "data" / "universe" / "bse_old_code_aliases.csv"
    df = read_csv_required(path, code_columns=("code", "current_code"))
    rows = []
    for _, item in df.iterrows():
        query_code = normalize_code(item["code"])
        current_code = normalize_code(item["current_code"])
        if not query_code or not current_code:
            continue
        rows.append(
            {
                "query_code": query_code,
                "stock_id": current_code,
                "name": str(item.get("name") or "").strip(),
                "source": "bse_old_code_alias",
                "current_code": current_code,
            }
        )
    result = pd.DataFrame(rows).drop_duplicates("query_code")
    if len(result) != 248:
        raise RuntimeError(
            f"BSE old-code alias universe must contain 248 rows, got {len(result)}"
        )
    return result


def build_universe() -> pd.DataFrame:
    current = load_tdx_current()
    delisted = load_delisted()
    aliases = load_bse_aliases()

    universe = pd.concat(
        [current, delisted, aliases],
        ignore_index=True,
    )
    priority = {
        "tdx_current": 0,
        "delisted": 1,
        "bse_old_code_alias": 2,
    }
    universe["priority"] = universe["source"].map(priority).fillna(9)
    universe = (
        universe.sort_values(["query_code", "priority"])
        .drop_duplicates("query_code", keep="first")
        .drop(columns="priority")
        .sort_values("query_code")
        .reset_index(drop=True)
    )

    if universe["query_code"].duplicated().any():
        raise RuntimeError("duplicate query codes remain")
    if not universe["query_code"].str.fullmatch(r"\d{6}").all():
        raise RuntimeError("universe contains malformed query codes")

    print(
        f"Universe: {len(universe)} query codes "
        f"(TDX={len(current)}, delisted={len(delisted)}, BSE aliases={len(aliases)})",
        flush=True,
    )
    return universe


def save_universe(universe: pd.DataFrame) -> None:
    universe.to_csv(
        OUT / "query_universe.csv",
        index=False,
        encoding="utf-8-sig",
    )


def canonical_map(universe: pd.DataFrame) -> dict[str, str]:
    return universe.set_index("query_code")["stock_id"].to_dict()


def parsed_cache_path(code: str) -> Path:
    return PARSED_CACHE / f"{code}.json"


def html_cache_path(code: str) -> Path:
    return CACHE / f"{code}.html"


def cache_is_fresh(path: Path) -> bool:
    if not path.exists() or path.stat().st_size == 0:
        return False
    if CACHE_TTL_SEC <= 0:
        return True
    age = time.time() - path.stat().st_mtime
    return age <= CACHE_TTL_SEC


def load_parsed_cache(code: str):
    path = parsed_cache_path(code)
    if not cache_is_fresh(path):
        return None
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
        if (
            payload.get("version") != CACHE_VERSION
            or payload.get("query_code") != code
        ):
            return None
        rows = payload.get("rows")
        status = payload.get("status")
        if not isinstance(rows, list) or status not in {"ok", "parse_error"}:
            return None
        return rows, status
    except Exception:
        return None


def save_parsed_cache(code: str, rows, status: str) -> None:
    parsed_cache_path(code).write_text(
        json.dumps(
            {
                "version": CACHE_VERSION,
                "query_code": code,
                "status": status,
                "rows": rows,
            },
            ensure_ascii=False,
            separators=(",", ":"),
        ),
        encoding="utf-8",
    )


def fetch_html(
    session: requests.Session,
    code: str,
    *,
    force_network: bool = False,
) -> tuple[str, str]:
    path = html_cache_path(code)
    if (
        not force_network
        and path.exists()
        and path.stat().st_size > 1000
        and cache_is_fresh(path)
    ):
        return path.read_text(encoding="gb2312", errors="ignore"), "cache"

    url = f"{BASE}/corp/go.php/vCI_CorpXiangGuan/stockid/{code}.phtml"
    last_error = ""
    for attempt in range(RETRIES):
        try:
            if attempt:
                time.sleep(min(8.0, 0.8 * (2 ** (attempt - 1))))
            response = session.get(url, timeout=TIMEOUT)
            if response.status_code in (403, 429):
                last_error = f"HTTP {response.status_code}"
                continue
            response.raise_for_status()
            response.encoding = "gb2312"
            text = response.text
            if len(text) < 1000:
                last_error = f"short response {len(text)}"
                continue
            path.write_text(text, encoding="gb2312", errors="ignore")
            if DELAY:
                time.sleep(DELAY)
            return text, "network"
        except Exception as exc:
            last_error = repr(exc)
    raise RuntimeError(last_error or "unknown error")


def find_xiangguan_table(soup: BeautifulSoup):
    required = {"指数名称", "指数代码", "进入日期", "退出日期"}
    for table in soup.find_all("table"):
        rows = table.find_all("tr")
        for row in rows[:4]:
            headers = {
                x.get_text(" ", strip=True)
                for x in row.find_all(["th", "td"])
            }
            if required.issubset(headers):
                return table
    return None


def parse_xiangguan(html: str, query_code: str) -> tuple[list[dict], str]:
    soup = BeautifulSoup(html, "html.parser")
    table = find_xiangguan_table(soup)
    if table is None:
        return [], "parse_error"

    rows = table.find_all("tr")
    header_idx = None
    headers: list[str] = []
    for idx, row in enumerate(rows[:6]):
        vals = [x.get_text(" ", strip=True) for x in row.find_all(["th", "td"])]
        if {"指数名称", "指数代码", "进入日期", "退出日期"}.issubset(set(vals)):
            header_idx = idx
            headers = vals
            break
    if header_idx is None:
        return [], "parse_error"

    pos = {h: i for i, h in enumerate(headers)}
    result: list[dict] = []
    for row in rows[header_idx + 1:]:
        vals = [x.get_text(" ", strip=True) for x in row.find_all(["th", "td"])]
        if len(vals) < len(headers):
            continue

        index_name = vals[pos["指数名称"]].strip()
        index_code = vals[pos["指数代码"]].strip()
        start = vals[pos["进入日期"]].strip()
        end = vals[pos["退出日期"]].strip()

        canonical = INDEX_ALIASES.get(index_code, index_code)
        if canonical not in TARGET_INDEXES:
            continue
        if not re.fullmatch(r"\d{4}-\d{2}-\d{2}", start):
            continue
        if end and not re.fullmatch(r"\d{4}-\d{2}-\d{2}", end):
            continue

        result.append(
            {
                "query_code": query_code,
                "index_id": canonical,
                "index_name": TARGET_INDEXES[canonical],
                "start_date": start,
                "end_date": end or "",
                "raw_index_code": index_code,
                "raw_index_name": index_name,
            }
        )
    return result, "ok"


def repair_placeholder_starts(
    df: pd.DataFrame,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Audit XiangGuan 1900-01-01 admission-date placeholders.

    Production history is sourced exclusively from stock-level
    vCI_CorpXiangGuan. No HistoryComponent/NewestComponent data is used.

    The index-level earliest XiangGuan date is only a conservative fallback.
    It is accepted only when it is strictly earlier than the affected record's
    exit date. If the fallback would create a zero/negative-length interval,
    the source row is excluded from production and recorded as UNRESOLVED.
    We must not manufacture a membership interval from an unknown admission
    date.
    """
    audit_columns = [
        "index_id", "stock_id", "old_start_date",
        "end_date", "new_start_date", "source", "status",
    ]
    if df.empty:
        return df, pd.DataFrame(columns=audit_columns)

    affected = df[df["start_date"] == "1900-01-01"].copy()
    if affected.empty:
        return df, pd.DataFrame(columns=audit_columns)

    repaired = df.copy()
    drop_indexes: list[int] = []
    audit_rows: list[dict] = []

    for index_id, group in df.groupby("index_id"):
        valid_dates = sorted(set(
            group.loc[group["start_date"] != "1900-01-01", "start_date"].tolist()
        ))
        index_earliest = valid_dates[0] if valid_dates else ""

        for row in affected[affected["index_id"] == index_id].itertuples():
            end_date = str(row.end_date or "").strip()
            if index_earliest and (not end_date or index_earliest < end_date):
                repaired.loc[row.Index, "start_date"] = index_earliest
                audit_rows.append({
                    "index_id": index_id,
                    "stock_id": row.stock_id,
                    "old_start_date": "1900-01-01",
                    "end_date": end_date,
                    "new_start_date": index_earliest,
                    "source": "xiangguan_index_earliest",
                    "status": "REPAIRED",
                })
            else:
                # The admission date is unknown. Never turn it into a
                # zero-length interval or invent a date at/after the known exit.
                drop_indexes.append(row.Index)
                audit_rows.append({
                    "index_id": index_id,
                    "stock_id": row.stock_id,
                    "old_start_date": "1900-01-01",
                    "end_date": end_date,
                    "new_start_date": "",
                    "source": (
                        "xiangguan_index_earliest_not_before_exit"
                        if index_earliest
                        else "xiangguan_unavailable"
                    ),
                    "status": "UNRESOLVED",
                })

    if drop_indexes:
        repaired = repaired.drop(index=drop_indexes).reset_index(drop=True)

    return repaired, pd.DataFrame(audit_rows, columns=audit_columns)

def make_session() -> requests.Session:
    session = requests.Session()
    session.headers.update(
        {
            "User-Agent": UA,
            "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
        }
    )
    return session


def worker(code: str, *, force_network: bool = False):
    if not force_network:
        cached = load_parsed_cache(code)
        if cached is not None:
            rows, status = cached
            # Parsed caches can outlive TARGET_INDEXES changes. Re-apply the
            # current target filter on every cache read so stale entries can
            # never re-enter production output.
            if rows:
                rows = [
                    row for row in rows
                    if INDEX_ALIASES.get(
                        str(row.get("index_id", "")).strip(),
                        str(row.get("index_id", "")).strip(),
                    ) in TARGET_INDEXES
                ]
            return code, rows, status, "parsed_cache", ""

    session = make_session()
    try:
        html, source = fetch_html(
            session,
            code,
            force_network=force_network,
        )
        rows, status = parse_xiangguan(html, code)
        save_parsed_cache(code, rows, status)
        return code, rows, status, source, ""
    except Exception as exc:
        return code, [], "http_error", "network", repr(exc)


def normalize_intervals(df: pd.DataFrame) -> pd.DataFrame:
    columns = ["index_id", "stock_id", "start_date", "end_date"]
    if df.empty:
        return pd.DataFrame(columns=columns)

    work = df[columns].drop_duplicates().copy()
    work["start_date_dt"] = pd.to_datetime(work["start_date"], errors="coerce")
    work["end_date_dt"] = pd.to_datetime(
        work["end_date"].replace("", pd.NA), errors="coerce"
    )
    work = work[work["start_date_dt"].notna()].copy()

    normalized = []
    for (index_id, stock_id), group in work.groupby(
        ["index_id", "stock_id"], sort=False
    ):
        group = group.sort_values(
            ["start_date_dt", "end_date_dt"],
            na_position="last",
        )
        starts = group["start_date_dt"].tolist()
        for row in group.to_dict("records"):
            if pd.isna(row["end_date_dt"]):
                later = [
                    start
                    for start in starts
                    if start > row["start_date_dt"]
                ]
                if later:
                    row["end_date_dt"] = min(later)
                    row["end_date"] = row["end_date_dt"].strftime("%Y-%m-%d")
            normalized.append(row)

    result = pd.DataFrame(normalized)
    result["start_date"] = result["start_date_dt"].dt.strftime("%Y-%m-%d")
    result["end_date"] = result["end_date_dt"].apply(
        lambda x: "" if pd.isna(x) else x.strftime("%Y-%m-%d")
    )
    return (
        result[columns]
        .drop_duplicates()
        .sort_values(columns)
        .reset_index(drop=True)
    )


def validate_intervals(df: pd.DataFrame) -> list[str]:
    errors: list[str] = []
    key = ["index_id", "stock_id", "start_date", "end_date"]
    if df.empty:
        return ["No normalized membership rows"]
    if df.duplicated(key).any():
        errors.append("duplicate normalized intervals remain")

    for (idx, stock), group in df.groupby(["index_id", "stock_id"]):
        group = group.sort_values("start_date")
        previous_end = None
        for row in group.itertuples(index=False):
            start = row.start_date
            end = row.end_date or None
            if end is not None and start >= end:
                errors.append(f"invalid interval {idx}/{stock}: {start}->{end}")
            if previous_end is not None and start < previous_end:
                errors.append(
                    f"overlap {idx}/{stock}: previous_end={previous_end}, start={start}"
                )
            previous_end = end
    return errors


def is_member(df: pd.DataFrame, index_id: str, stock_id: str, as_of: str) -> bool:
    group = df[(df.index_id == index_id) & (df.stock_id == stock_id)]
    return any(
        row.start_date <= as_of
        and (not row.end_date or as_of < row.end_date)
        for row in group.itertuples()
    )


def boundary_audit(df: pd.DataFrame, raw_df: pd.DataFrame) -> dict:
    rows: list[dict] = []
    failures: list[dict] = []

    synthetic = set()
    raw = raw_df[
        ["index_id", "stock_id", "start_date", "end_date"]
    ].drop_duplicates()
    for (idx, stock), group in raw.groupby(["index_id", "stock_id"]):
        starts = sorted(pd.to_datetime(group["start_date"]).tolist())
        for row in group.itertuples(index=False):
            if not row.end_date:
                later = [x for x in starts if x > pd.Timestamp(row.start_date)]
                if later:
                    synthetic.add((idx, stock, min(later)))

    interval_map: dict[tuple[str, str], list[tuple[pd.Timestamp, pd.Timestamp | None]]] = {}
    for (idx, stock), group in df.groupby(["index_id", "stock_id"]):
        interval_map[(idx, stock)] = [
            (
                pd.Timestamp(row.start_date),
                pd.Timestamp(row.end_date) if row.end_date else None,
            )
            for row in group.itertuples(index=False)
        ]

    def member_on(idx: str, stock: str, date: pd.Timestamp) -> bool:
        return any(
            start <= date and (end is None or date < end)
            for start, end in interval_map.get((idx, stock), [])
        )

    for row in df.itertuples(index=False):
        start = pd.Timestamp(row.start_date)
        before = start - pd.Timedelta(days=1)
        rollover = (row.index_id, row.stock_id, start) in synthetic
        before_ok = member_on(row.index_id, row.stock_id, before)
        on_ok = member_on(row.index_id, row.stock_id, start)
        expected_before = rollover
        ok = (before_ok == expected_before) and on_ok
        item = {
            "index_id": row.index_id,
            "stock_id": row.stock_id,
            "change_type": "rollover" if rollover else "entry",
            "change_date": row.start_date,
            "before_member": before_ok,
            "on_member": on_ok,
            "status": "PASS" if ok else "FAIL",
        }
        rows.append(item)
        if not ok:
            failures.append(item)

    exits = df[df.end_date != ""]
    for row in exits.itertuples(index=False):
        end = pd.Timestamp(row.end_date)
        if (row.index_id, row.stock_id, end) in synthetic:
            continue
        before = end - pd.Timedelta(days=1)
        before_ok = member_on(row.index_id, row.stock_id, before)
        next_starts = [
            start
            for start, _ in interval_map.get((row.index_id, row.stock_id), [])
            if start > pd.Timestamp(row.start_date)
        ]
        contiguous = any(start == end for start in next_starts)
        on_ok = member_on(row.index_id, row.stock_id, end)
        ok = before_ok and (on_ok == contiguous)
        item = {
            "index_id": row.index_id,
            "stock_id": row.stock_id,
            "change_type": "exit",
            "change_date": row.end_date,
            "before_member": before_ok,
            "on_member": on_ok,
            "status": "PASS" if ok else "FAIL",
        }
        rows.append(item)
        if not ok:
            failures.append(item)

    report = pd.DataFrame(rows)
    report.to_csv(
        OUT / "boundary_audit.csv",
        index=False,
        encoding="utf-8-sig",
    )
    failure_df = pd.DataFrame(failures)
    failure_df.to_csv(
        OUT / "boundary_failures.csv",
        index=False,
        encoding="utf-8-sig",
    )
    return {
        "status": "PASS" if not failures else "FAIL",
        "checks": len(rows),
        "failures": len(failures),
        "synthetic_rollovers": len(synthetic),
    }



def main() -> None:
    started = time.perf_counter()
    started_at = datetime.now(timezone.utc).isoformat()

    universe = build_universe()
    save_universe(universe)
    cmap = canonical_map(universe)

    all_rows: list[dict] = []
    failures: list[dict] = []
    status_counts: dict[str, int] = {}
    source_counts: dict[str, int] = {}

    codes = universe.query_code.tolist()
    current_codes = set(
        universe.loc[
            universe["source"] == "tdx_current", "query_code"
        ]
    )
    print(
        f"Workers={WORKERS}, delay={DELAY}s, query_codes={len(codes)}, "
        f"refresh_current={REFRESH_CURRENT}, current_codes={len(current_codes)}"
    )

    with ThreadPoolExecutor(max_workers=WORKERS) as pool:
        futures = {
            pool.submit(
                worker,
                code,
                force_network=REFRESH_CURRENT and code in current_codes,
            ): code
            for code in codes
        }
        for n, future in enumerate(as_completed(futures), 1):
            code, rows, status, source, error = future.result()
            status_counts[status] = status_counts.get(status, 0) + 1
            source_counts[source] = source_counts.get(source, 0) + 1

            stock_id = cmap.get(code, code)
            if status == "ok":
                for row in rows:
                    all_rows.append(
                        {
                            "index_id": row["index_id"],
                            "index_name": row["index_name"],
                            "stock_id": stock_id,
                            "query_code": code,
                            "start_date": row["start_date"],
                            "end_date": row["end_date"],
                            "raw_index_code": row["raw_index_code"],
                            "raw_index_name": row["raw_index_name"],
                        }
                    )
            else:
                failures.append(
                    {"query_code": code, "status": status, "error": error}
                )

            if n % 200 == 0 or n == len(codes):
                print(
                    f"B XiangGuan [{n}/{len(codes)}] rows={len(all_rows)} "
                    f"failures={len(failures)}",
                    flush=True,
                )

    raw_df = pd.DataFrame(all_rows)
    if raw_df.empty:
        raise RuntimeError("No target-index XiangGuan rows were produced")

    raw_df = raw_df.drop_duplicates(
        ["index_id", "stock_id", "query_code", "start_date", "end_date"]
    ).sort_values(
        ["index_id", "stock_id", "start_date", "end_date", "query_code"]
    )
    raw_df.to_csv(
        OUT / "raw_target_membership.csv",
        index=False,
        encoding="utf-8-sig",
    )

    repaired_input_df, placeholder_audit = repair_placeholder_starts(
        raw_df[["index_id", "stock_id", "start_date", "end_date"]].copy(),
    )
    placeholder_audit.to_csv(
        OUT / "placeholder_start_repairs.csv",
        index=False,
        encoding="utf-8-sig",
    )
    unresolved_placeholder_audit = placeholder_audit[
        placeholder_audit["status"] == "UNRESOLVED"
    ].copy()
    unresolved_placeholder_audit.to_csv(
        OUT / "placeholder_start_unresolved.csv",
        index=False,
        encoding="utf-8-sig",
    )
    unresolved_placeholders = int(
        (placeholder_audit["status"] != "REPAIRED").sum()
    ) if not placeholder_audit.empty else 0

    final_df = normalize_intervals(repaired_input_df)
    final_df.to_csv(
        OUT / "index_membership_history.csv",
        index=False,
        encoding="utf-8-sig",
    )
    final_df.to_parquet(
        OUT / "index_membership_history.parquet",
        index=False,
    )

    pd.DataFrame(failures).to_csv(
        OUT / "failed_query_codes.csv",
        index=False,
        encoding="utf-8-sig",
    )

    validation_errors = validate_intervals(final_df)
    missing_indexes = sorted(
        set(TARGET_INDEXES) - set(final_df.index_id.unique())
    )
    boundary = boundary_audit(final_df, raw_df)

    # Preserve a compact PIT smoke test set already validated by the feasibility test.
    pit_tests = [
        ("000300", "000895", "2005-04-08", True),
        ("000300", "000895", "2007-07-02", False),
        ("000300", "000562", "2005-06-30", True),
        ("000300", "000562", "2005-07-01", False),
        ("000300", "600118", "2021-12-12", True),
        ("000300", "600118", "2021-12-13", False),
    ]
    pit_failures = []
    for idx, stock, date, expected in pit_tests:
        actual = is_member(final_df, idx, stock, date)
        if actual != expected:
            pit_failures.append(
                {
                    "index_id": idx,
                    "stock_id": stock,
                    "date": date,
                    "actual": actual,
                    "expected": expected,
                }
            )
    pd.DataFrame(pit_failures).to_csv(
        OUT / "pit_failures.csv",
        index=False,
        encoding="utf-8-sig",
    )

    summary = {
        "started_at": started_at,
        "finished_at": datetime.now(timezone.utc).isoformat(),
        "elapsed_seconds": round(time.perf_counter() - started, 3),
        "workers": WORKERS,
        "delay": DELAY,
        "refresh_current": REFRESH_CURRENT,
        "cache_ttl_seconds": CACHE_TTL_SEC,
        "current_query_count": len(current_codes),
        "universe_count": len(universe),
        "source_counts": source_counts,
        "status_counts": status_counts,
        "failed_query_count": len(failures),
        "raw_target_rows": len(raw_df),
        "normalized_interval_count": len(final_df),
        "target_index_count": len(TARGET_INDEXES),
        "placeholder_start_rows_before_repair": int(
            (raw_df["start_date"] == "1900-01-01").sum()
        ),
        "placeholder_start_repairs": int(
            (placeholder_audit["status"] == "REPAIRED").sum()
        ) if not placeholder_audit.empty else 0,
        "placeholder_start_repairs_by_source": (
            placeholder_audit.loc[
                placeholder_audit["status"] == "REPAIRED", "source"
            ].value_counts().to_dict()
            if not placeholder_audit.empty else {}
        ),
        "placeholder_start_unresolved": unresolved_placeholders,
        "placeholder_start_unresolved_file": str(
            OUT / "placeholder_start_unresolved.csv"
        ),
        "placeholder_start_remaining": int(
            (final_df["start_date"] == "1900-01-01").sum()
        ),
        "missing_indexes": missing_indexes,
        "validation_errors": validation_errors,
        "boundary_audit": boundary,
        "pit_failures": pit_failures,
    }
    (OUT / "summary.json").write_text(
        json.dumps(summary, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )

    print("\n========== INDEX MEMBERSHIP FINAL ==========")
    print(f"Query universe:              {len(universe)}")
    print(f"Successful XiangGuan codes:  {status_counts.get('ok', 0)}")
    print(f"Failed XiangGuan codes:      {len(failures)}")
    print(f"Raw target rows:             {len(raw_df)}")
    print(f"Normalized intervals:        {len(final_df)}")
    print(f"Target indexes:              {len(TARGET_INDEXES)}")
    print(f"Missing indexes:              {missing_indexes}")
    print(f"Boundary audit:              {boundary['status']} ({boundary['failures']} failures)")
    print(f"PIT failures:                 {len(pit_failures)}")
    print(
        "Placeholder starts repaired:   "
        f"{int((placeholder_audit['status'] == 'REPAIRED').sum())}"
    )
    if not placeholder_audit.empty:
        print(
            "Placeholder repair sources:     "
            + str(
                placeholder_audit.loc[
                    placeholder_audit["status"] == "REPAIRED", "source"
                ].value_counts().to_dict()
            )
        )
    print(
        "Placeholder starts unresolved: "
        f"{unresolved_placeholders}"
    )
    print(
        "Placeholder starts remaining:  "
        f"{int((final_df['start_date'] == '1900-01-01').sum())}"
    )
    print("============================================")

    if (
        failures
        or validation_errors
        or missing_indexes
        or pit_failures
        or unresolved_placeholders
        or int((final_df["start_date"] == "1900-01-01").sum())
        or boundary["status"] != "PASS"
    ):
        print("PRODUCTION INDEX MEMBERSHIP BUILD: FAIL")
        sys.exit(1)

    print("PRODUCTION INDEX MEMBERSHIP BUILD: PASS")


if __name__ == "__main__":
    main()
