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
CACHE_VERSION = 4
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
    "930713": "930713", "980017": "980017", "980087": "980087",
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
    "980087": "国证人工智能精选", "399673": "创业板50",
    "399412": "国证新能源", "399005": "中小100",
    "399994": "中证信息安全", "399975": "证券公司",
    "399986": "中证银行", "399932": "中证消费", "399933": "中证医药",
    "399967": "中证军工", "399989": "中证医疗", "399971": "中证传媒",
    "399997": "中证白酒", "000928": "中证能源", "000929": "中证原材料",
    "399990": "煤炭等权", "930708": "中证有色", "399974": "国证国企",
}

SINA_COMPONENT_INDEX_IDS = {"000851": "932000"}

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


def fetch_history_component_page(
    session: requests.Session,
    index_id: str,
    page: int,
) -> str:
    query_index_id = SINA_COMPONENT_INDEX_IDS.get(index_id, index_id)
    url = (
        f"{BASE}/corp/view/vII_HistoryComponent.php"
        f"?page={page}&indexid={query_index_id}"
    )
    response = session.get(url, timeout=TIMEOUT)
    response.raise_for_status()
    response.encoding = "gb2312"
    return response.text


def parse_history_component_rows(html: str) -> list[dict]:
    """Parse Sina HistoryComponent rows with true inclusion/removal dates."""
    soup = BeautifulSoup(html, "html.parser")
    required = {"品种代码", "纳入日期", "剔除日期"}

    for table in soup.find_all("table"):
        rows = table.find_all("tr")
        header_idx = None
        headers: list[str] = []

        for idx, row in enumerate(rows[:6]):
            values = [
                x.get_text(" ", strip=True)
                for x in row.find_all(["th", "td"])
            ]
            if required.issubset(set(values)):
                header_idx = idx
                headers = values
                break

        if header_idx is None:
            continue

        pos = {name: i for i, name in enumerate(headers)}
        result: list[dict] = []

        for row in rows[header_idx + 1:]:
            values = [
                x.get_text(" ", strip=True)
                for x in row.find_all(["th", "td"])
            ]
            if len(values) < len(headers):
                continue

            code = normalize_code(values[pos["品种代码"]])
            start = values[pos["纳入日期"]].strip()
            end = values[pos["剔除日期"]].strip()

            if not re.fullmatch(r"\d{6}", code):
                continue
            if not re.fullmatch(r"\d{4}-\d{2}-\d{2}", start):
                continue
            if end in {"", "--", "-"}:
                end = ""
            elif not re.fullmatch(r"\d{4}-\d{2}-\d{2}", end):
                continue

            result.append({
                "stock_code": code,
                "start_date": start,
                "end_date": end,
            })

        if result:
            return result

    return []


def component_page_count(html: str) -> int:
    pages = [int(x) for x in re.findall(r"[?&]page=(\d+)", html)]
    return max(pages, default=1)


def repair_placeholder_starts(
    df: pd.DataFrame,
    universe: pd.DataFrame,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Replace Sina XiangGuan's 1900-01-01 placeholder starts.

    XiangGuan may use 1900-01-01 for the first historical membership
    interval. HistoryComponent provides true per-stock admission dates for
    part of the history. Its historical table is itself a limited view, so
    when an affected stock is absent there, fall back to the earliest valid
    admission date observed for that index.

    The repair is intentionally two-level:
      1. stock-specific HistoryComponent date, when available;
      2. index historical earliest date, only when stock-specific evidence
         is unavailable.

    Every repair is written to placeholder_start_repairs.csv with its source.
    """
    audit_columns = [
        "index_id",
        "stock_id",
        "old_start_date",
        "new_start_date",
        "history_rows",
        "source",
        "status",
    ]

    if df.empty:
        return df, pd.DataFrame(columns=audit_columns)

    affected = df[df["start_date"] == "1900-01-01"].copy()
    if affected.empty:
        return df, pd.DataFrame(columns=audit_columns)

    wanted: dict[str, set[str]] = {}
    for row in affected.itertuples(index=False):
        wanted.setdefault(row.index_id, set()).add(row.stock_id)

    cmap = universe.set_index("query_code")["stock_id"].to_dict()
    repaired = df.copy()
    audit_rows: list[dict] = []
    session = make_session()

    for index_id, stock_ids in wanted.items():
        all_history: list[dict] = []

        try:
            first = fetch_history_component_page(session, index_id, 1)
            pages = component_page_count(first)

            for page in range(1, pages + 1):
                html = (
                    first
                    if page == 1
                    else fetch_history_component_page(session, index_id, page)
                )
                all_history.extend(parse_history_component_rows(html))

            print(
                f"Placeholder repair {index_id}: "
                f"HistoryComponent pages={pages}, rows={len(all_history)}",
                flush=True,
            )
        except Exception as exc:
            for stock_id in sorted(stock_ids):
                audit_rows.append({
                    "index_id": index_id,
                    "stock_id": stock_id,
                    "old_start_date": "1900-01-01",
                    "new_start_date": "",
                    "history_rows": 0,
                    "source": "history_component_error",
                    "status": f"ERROR: {exc!r}",
                })
            continue

        valid_dates = [
            row["start_date"]
            for row in all_history
            if re.fullmatch(r"\d{4}-\d{2}-\d{2}", row["start_date"])
        ]
        index_earliest = min(valid_dates) if valid_dates else ""

        candidate_dates: dict[str, list[str]] = {}
        for row in all_history:
            stock_id = cmap.get(row["stock_code"], row["stock_code"])
            if stock_id in stock_ids:
                candidate_dates.setdefault(stock_id, []).append(
                    row["start_date"]
                )

        for stock_id in sorted(stock_ids):
            dates = sorted(set(candidate_dates.get(stock_id, [])))

            if dates:
                new_start = dates[0]
                source = "history_component_stock"
                history_rows = len(dates)
            elif index_earliest:
                new_start = index_earliest
                source = "history_component_index_earliest"
                history_rows = 0
            else:
                audit_rows.append({
                    "index_id": index_id,
                    "stock_id": stock_id,
                    "old_start_date": "1900-01-01",
                    "new_start_date": "",
                    "history_rows": 0,
                    "source": "history_component_unavailable",
                    "status": "UNRESOLVED",
                })
                continue

            mask = (
                (repaired["index_id"] == index_id)
                & (repaired["stock_id"] == stock_id)
                & (repaired["start_date"] == "1900-01-01")
            )
            repaired.loc[mask, "start_date"] = new_start

            audit_rows.append({
                "index_id": index_id,
                "stock_id": stock_id,
                "old_start_date": "1900-01-01",
                "new_start_date": new_start,
                "history_rows": history_rows,
                "source": source,
                "status": "REPAIRED",
            })

    audit = pd.DataFrame(audit_rows, columns=audit_columns)
    return repaired, audit


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
        on_ok = member_on(row.index_id, row.stock_id, end)
        ok = before_ok and not on_ok
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


def build_a_candidates() -> tuple[
    dict[str, set[str]],
    dict[str, set[str]],
    list[dict],
]:
    session = make_session()
    candidates: dict[str, set[str]] = {}
    newest_candidates: dict[str, set[str]] = {}
    errors: list[dict] = []

    def fetch_component(index_id: str, kind: str, page: int) -> str:
        path = (
            "vII_HistoryComponent.php"
            if kind == "history"
            else "vII_NewestComponent.php"
        )
        query_id = SINA_COMPONENT_INDEX_IDS.get(index_id, index_id)
        url = f"{BASE}/corp/view/{path}?page={page}&indexid={query_id}"
        response = session.get(url, timeout=TIMEOUT)
        response.raise_for_status()
        response.encoding = "gb2312"
        return response.text

    def page_count(html: str) -> int:
        pages = [int(x) for x in re.findall(r"[?&]page=(\d+)", html)]
        return max(pages, default=1)

    def parse_codes(html: str) -> set[str]:
        soup = BeautifulSoup(html, "html.parser")
        for table in soup.find_all("table"):
            rows = table.find_all("tr")
            for row in rows[:6]:
                headers = [
                    x.get_text(" ", strip=True)
                    for x in row.find_all(["th", "td"])
                ]
                if "品种代码" not in headers:
                    continue
                pos = headers.index("品种代码")
                codes = set()
                row_idx = rows.index(row)
                for data_row in rows[row_idx + 1:]:
                    vals = [
                        x.get_text(" ", strip=True)
                        for x in data_row.find_all(["th", "td"])
                    ]
                    if len(vals) <= pos:
                        continue
                    code = normalize_code(vals[pos])
                    if re.fullmatch(r"\d{6}", code):
                        codes.add(code)
                return codes
        return set()

    for index_id in TARGET_INDEXES:
        codes: set[str] = set()
        newest_codes: set[str] = set()
        for kind in ("history", "newest"):
            try:
                first = fetch_component(index_id, kind, 1)
                pages = page_count(first)
                for page in range(1, pages + 1):
                    html = (
                        first
                        if page == 1
                        else fetch_component(index_id, kind, page)
                    )
                    page_codes = parse_codes(html)
                    codes.update(page_codes)
                    if kind == "newest":
                        newest_codes.update(page_codes)
                print(
                    f"A candidates {index_id} {kind}: pages={pages} "
                    f"stocks={len(newest_codes if kind == 'newest' else codes)}",
                    flush=True,
                )
            except Exception as exc:
                errors.append(
                    {"index_id": index_id, "kind": kind, "error": repr(exc)}
                )
        candidates[index_id] = codes
        newest_candidates[index_id] = newest_codes
        print(
            f"A candidates {index_id}: union={len(codes)}, newest={len(newest_codes)}",
            flush=True,
        )

    return candidates, newest_candidates, errors



def audit_current_membership(
    final_df: pd.DataFrame,
    newest_candidates: dict[str, set[str]],
    universe: pd.DataFrame,
) -> dict:
    """Reconcile current XiangGuan membership against Sina NewestComponent."""
    cmap = universe.set_index("query_code")["stock_id"].to_dict()
    universe_codes = set(universe["query_code"])

    rows: list[dict] = []
    missing_rows: list[dict] = []
    extra_rows: list[dict] = []

    for index_id in TARGET_INDEXES:
        newest_all = newest_candidates.get(index_id, set())
        newest_in_universe = {
            cmap.get(code, code)
            for code in newest_all
            if code in universe_codes
        }

        b_current = set(
            final_df.loc[
                (final_df["index_id"] == index_id)
                & (final_df["end_date"] == ""),
                "stock_id",
            ]
        )

        missing = sorted(newest_in_universe - b_current)
        extra = sorted(b_current - newest_in_universe)

        for stock_id in missing:
            missing_rows.append({
                "index_id": index_id,
                "stock_id": stock_id,
            })
        for stock_id in extra:
            extra_rows.append({
                "index_id": index_id,
                "stock_id": stock_id,
            })

        rows.append({
            "index_id": index_id,
            "newest_component_codes": len(newest_all),
            "newest_in_universe": len(newest_in_universe),
            "b_current_open_intervals": len(b_current),
            "missing_from_xiangguan": len(missing),
            "extra_in_xiangguan": len(extra),
            "status": "PASS" if not missing and not extra else "FAIL",
        })

    audit_df = pd.DataFrame(rows)
    missing_df = pd.DataFrame(missing_rows, columns=["index_id", "stock_id"])
    extra_df = pd.DataFrame(extra_rows, columns=["index_id", "stock_id"])

    audit_df.to_csv(
        OUT / "current_membership_audit.csv",
        index=False,
        encoding="utf-8-sig",
    )
    missing_df.to_csv(
        OUT / "current_membership_missing.csv",
        index=False,
        encoding="utf-8-sig",
    )
    extra_df.to_csv(
        OUT / "current_membership_extra.csv",
        index=False,
        encoding="utf-8-sig",
    )

    failures = int((audit_df["status"] == "FAIL").sum()) if not audit_df.empty else 0
    print(
        f"Current membership reconciliation: indexes={len(audit_df)}, "
        f"failures={failures}, missing={len(missing_df)}, extra={len(extra_df)}",
        flush=True,
    )

    return {
        "status": "PASS" if failures == 0 else "FAIL",
        "index_failures": failures,
        "missing_total": len(missing_df),
        "extra_total": len(extra_df),
        "by_index": rows,
    }


def build_a_from_xiangguan(
    candidates: dict[str, set[str]],
    cmap: dict[str, str],
) -> tuple[pd.DataFrame, list[dict]]:
    wanted_by_code = {}
    for index_id, codes in candidates.items():
        for code in codes:
            wanted_by_code.setdefault(code, set()).add(index_id)

    all_codes = sorted(wanted_by_code)
    rows: list[dict] = []
    errors: list[dict] = []

    def one(code: str):
        parsed = load_parsed_cache(code)
        if parsed is None:
            session = make_session()
            html, _ = fetch_html(session, code)
            parsed_rows, status = parse_xiangguan(html, code)
            save_parsed_cache(code, parsed_rows, status)
        else:
            parsed_rows, status = parsed
        if status != "ok":
            return code, [], {"code": code, "status": status}
        stock_id = cmap.get(code, code)
        wanted = wanted_by_code[code]
        out = []
        for row in parsed_rows:
            if row["index_id"] in wanted:
                out.append(
                    {
                        "index_id": row["index_id"],
                        "stock_id": stock_id,
                        "start_date": row["start_date"],
                        "end_date": row["end_date"],
                    }
                )
        return code, out, None

    with ThreadPoolExecutor(max_workers=WORKERS) as pool:
        futures = {pool.submit(one, code): code for code in all_codes}
        for n, future in enumerate(as_completed(futures), 1):
            try:
                code, out, error = future.result()
                rows.extend(out)
                if error:
                    errors.append(error)
            except Exception as exc:
                errors.append(
                    {
                        "code": futures[future],
                        "status": "http_error",
                        "error": repr(exc),
                    }
                )
            if n % 200 == 0 or n == len(all_codes):
                print(
                    f"A XiangGuan [{n}/{len(all_codes)}] "
                    f"intervals={len(rows)} errors={len(errors)}",
                    flush=True,
                )

    result = pd.DataFrame(rows)
    if result.empty:
        result = pd.DataFrame(
            columns=["index_id", "stock_id", "start_date", "end_date"]
        )
    return normalize_intervals(result), errors


def ab_diff(a_df: pd.DataFrame, b_df: pd.DataFrame) -> dict:
    key = ["index_id", "stock_id", "start_date", "end_date"]
    records = []
    for index_id in TARGET_INDEXES:
        a = a_df[a_df.index_id == index_id][key].drop_duplicates()
        b = b_df[b_df.index_id == index_id][key].drop_duplicates()
        merged = a.merge(b, on=key, how="outer", indicator=True)
        a_only = merged[merged["_merge"] == "left_only"].drop(columns="_merge")
        b_only = merged[merged["_merge"] == "right_only"].drop(columns="_merge")
        records.append(
            {
                "index_id": index_id,
                "a_intervals": len(a),
                "b_intervals": len(b),
                "a_only_intervals": len(a_only),
                "b_only_intervals": len(b_only),
                "a_stocks": a.stock_id.nunique(),
                "b_stocks": b.stock_id.nunique(),
            }
        )
        if not a_only.empty:
            a_only.to_csv(
                OUT / f"ab_a_minus_b_{index_id}.csv",
                index=False,
                encoding="utf-8-sig",
            )
        if not b_only.empty:
            b_only.to_csv(
                OUT / f"ab_b_minus_a_{index_id}.csv",
                index=False,
                encoding="utf-8-sig",
            )

    summary = pd.DataFrame(records)
    summary.to_csv(
        OUT / "ab_diff_summary.csv",
        index=False,
        encoding="utf-8-sig",
    )
    return {
        "a_minus_b_intervals": int(summary.a_only_intervals.sum()),
        "b_minus_a_intervals": int(summary.b_only_intervals.sum()),
        "summary": records,
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
        universe,
    )
    placeholder_audit.to_csv(
        OUT / "placeholder_start_repairs.csv",
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
    missing_indexes = sorted(set(TARGET_INDEXES) - set(final_df.index_id.unique()))
    boundary = boundary_audit(final_df, raw_df)

    print("\nA/B completeness audit...", flush=True)
    a_candidates, newest_candidates, a_candidate_errors = build_a_candidates()
    a_df, a_xiangguan_errors = build_a_from_xiangguan(a_candidates, cmap)
    current_audit = audit_current_membership(
        final_df, newest_candidates, universe
    )
    a_df.to_parquet(
        OUT / "ab_a_index_membership_history.parquet",
        index=False,
    )
    ab = ab_diff(a_df, final_df)

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
        "placeholder_start_remaining": int(
            (final_df["start_date"] == "1900-01-01").sum()
        ),
        "missing_indexes": missing_indexes,
        "validation_errors": validation_errors,
        "boundary_audit": boundary,
        "pit_failures": pit_failures,
        "current_membership_audit": current_audit,
        "ab": {
            "candidate_errors": a_candidate_errors,
            "xiangguan_errors": a_xiangguan_errors,
            **ab,
        },
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
    print(
        f"A-B intervals:                {ab['a_minus_b_intervals']}"
    )
    print(
        f"B-A intervals:                {ab['b_minus_a_intervals']}"
    )
    print(
        "Current membership audit:       "
        f"{current_audit['status']} "
        f"(missing={current_audit['missing_total']}, "
        f"extra={current_audit['extra_total']})"
    )
    print("============================================")

    if (
        failures
        or validation_errors
        or missing_indexes
        or pit_failures
        or unresolved_placeholders
        or int((final_df["start_date"] == "1900-01-01").sum())
        or a_candidate_errors
        or a_xiangguan_errors
        or boundary["status"] != "PASS"
        or current_audit["status"] != "PASS"
    ):
        print("PRODUCTION INDEX MEMBERSHIP BUILD: FAIL")
        sys.exit(1)

    print("PRODUCTION INDEX MEMBERSHIP BUILD: PASS")


if __name__ == "__main__":
    main()
