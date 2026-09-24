#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Build the historical BSE stock universe for downstream index-membership crawling.

Production source strategy:
1. CNINFO historical full-text search, filtered by plate=bj, finds BSE
   "终止上市"/"摘牌" announcements.
2. Announcement titles are semantically filtered so risk-warning/proposed
   termination notices are not treated as final termination events.
3. BSE's official current stock list is the current-status cross-check.
4. BSE's official 248-row old/new code mapping is joined so all pre-2025
   BSE codes remain in the historical code universe.
5. The current BSE risk-warning board is fetched separately as an audit of
   currently risky / delisting-arrangement stocks.

No PDF download is required.
"""

from __future__ import annotations

import io
import json
import random
import re
import time
from datetime import date, datetime, timezone
from html import unescape
from html.parser import HTMLParser
from zoneinfo import ZoneInfo
from pathlib import Path
from typing import Any

import pandas as pd
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

OUTPUT_DIR = Path(__file__).resolve().parents[1] / "data" / "universe"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

BSE_BASE_URL = "https://www.bse.cn"
BSE_PAGE_URL = f"{BSE_BASE_URL}/disclosure/announcement.html"
BSE_CURRENT_LIST_URL = f"{BSE_BASE_URL}/nqxxController/nqxxCnzq.do"
BSE_RISK_API_URL = f"{BSE_BASE_URL}/nqxxController/getRiskWarningStock.do"
BSE_CODE_MAPPING_URL = f"{BSE_BASE_URL}/service/code_mapping.html"
CNINFO_TERMINATION_URL = "https://www.cninfo.com.cn/new/hisAnnouncement/query"
SSE_URL = "https://query.sse.com.cn/sseQuery/commonQuery.do"
SZSE_URL = "https://www.szse.cn/api/report/ShowReport"

COMMON_USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/150.0.0.0 Safari/537.36"
)

SSE_HEADERS = {
    "Accept": "*/*",
    "Accept-Encoding": "gzip, deflate",
    "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8,en-GB;q=0.6",
    "Cache-Control": "no-cache",
    "Connection": "keep-alive",
    "Host": "query.sse.com.cn",
    "Pragma": "no-cache",
    "Referer": "https://www.sse.com.cn/assortment/stock/list/delisting/",
    "User-Agent": COMMON_USER_AGENT,
}
SZSE_HEADERS = {
    "Accept": "*/*",
    "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8,en-GB;q=0.6",
    "Cache-Control": "no-cache",
    "Referer": "https://www.szse.cn/market/stock/suspend/index.html",
    "User-Agent": COMMON_USER_AGENT,
}


START_DATE = date(2021, 11, 15)
END_DATE = date.today()

REQUEST_TIMEOUT = (15, 60)
HTTP_RETRIES = 2
SOURCE_ATTEMPTS = 3
MAX_PAGES = 100
CNINFO_PAGE_SIZE = 100
MIN_CURRENT_BSE_STOCKS = 300

# These are hard source-integrity cases already independently verified.
KNOWN_TERMINATED_CODES = ("832317", "833874", "833994", "920680", "920305")

# Positive title patterns for an actual/final BSE termination event.
TERMINATION_PHRASES = (
    "股票终止上市暨摘牌",
    "股票在北京证券交易所终止上市",
    "股票因转板在北京证券交易所终止上市",
    "终止在北京证券交易所上市",
)
# These phrases identify process/risk notices, not the final termination event.
NON_FINAL_TERMINATION_PHRASES = (
    "风险提示",
    "风险警示",
    "可能被终止上市",
    "可能终止上市",
    "拟终止上市",
    "将被终止上市",
    "事先告知书",
    "筹划发行",
)

BSE_HEADERS = {
    "Accept": "text/javascript, application/javascript, application/ecmascript, */*; q=0.01",
    "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8,en-GB;q=0.6",
    "Cache-Control": "no-cache",
    "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
    "Origin": BSE_BASE_URL,
    "Pragma": "no-cache",
    "Referer": BSE_PAGE_URL,
    "User-Agent": COMMON_USER_AGENT,
    "X-Requested-With": "XMLHttpRequest",
}

CNINFO_HEADERS = {
    "Accept": "application/json, text/javascript, */*; q=0.01",
    "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
    "Origin": "https://www.cninfo.com.cn",
    "Referer": "https://www.cninfo.com.cn/new/commonUrl/pageOfSearch?url=disclosure/list/search",
    "User-Agent": BSE_HEADERS["User-Agent"],
    "X-Requested-With": "XMLHttpRequest",
}


def build_session() -> requests.Session:
    session = requests.Session()
    retry = Retry(
        total=HTTP_RETRIES,
        connect=HTTP_RETRIES,
        read=HTTP_RETRIES,
        status=HTTP_RETRIES,
        backoff_factor=2.0,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=frozenset({"GET", "POST"}),
        raise_on_status=False,
    )
    adapter = HTTPAdapter(
        max_retries=retry,
        pool_connections=8,
        pool_maxsize=8,
    )
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    return session


def request_with_retry(
    session: requests.Session,
    method: str,
    url: str,
    *,
    headers: dict[str, str],
    params: dict[str, str] | None = None,
    data: Any = None,
    label: str,
) -> requests.Response:
    last_error: Exception | None = None
    for attempt in range(1, HTTP_RETRIES + 1):
        try:
            response = session.request(
                method,
                url,
                params=params,
                data=data,
                headers=headers,
                timeout=REQUEST_TIMEOUT,
            )
            response.raise_for_status()
            if not response.content:
                raise RuntimeError(f"{label}: empty HTTP response")
            print(
                f"[{label}] HTTP {response.status_code}, "
                f"{len(response.content):,} bytes, http_attempt={attempt}"
            )
            return response
        except Exception as exc:
            last_error = exc
            if attempt == HTTP_RETRIES:
                break
            delay = min(30.0, 2 ** (attempt - 1) + random.random())
            print(
                f"[{label}] HTTP attempt {attempt}/{HTTP_RETRIES} failed: "
                f"{type(exc).__name__}: {exc}; retry in {delay:.1f}s"
            )
            time.sleep(delay)
    raise RuntimeError(f"{label}: all HTTP retries failed") from last_error



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


def normalize_exchange_codes(df, exchange):
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
    df, rejected = normalize_exchange_codes(df, "SSE")
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
    df, rejected = normalize_exchange_codes(df, "SZSE")
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


def validate_sse_szse(df):
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



def normalize_code(value: object) -> str:
    raw = str(value or "").strip()
    return raw.zfill(6) if re.fullmatch(r"\d{1,6}", raw) else ""


def to_announcement_date(value: object) -> str:
    """Convert CNINFO millisecond timestamp to Beijing calendar date."""
    raw = str(value or "").strip()
    if not raw:
        return ""
    try:
        timestamp = int(float(raw))
        dt = datetime.fromtimestamp(timestamp / 1000, tz=timezone.utc)
        return dt.astimezone(ZoneInfo("Asia/Shanghai")).date().isoformat()
    except (TypeError, ValueError, OverflowError, OSError):
        return ""


def strip_html(value: object) -> str:
    text = unescape(str(value or ""))
    return re.sub(r"<[^>]+>", "", text).strip()


def is_bse_code(code: str) -> bool:
    return bool(
        re.fullmatch(r"\d{6}", code)
        and code.startswith(("83", "87", "88", "92"))
    )


def is_final_termination_title(title: str) -> bool:
    clean = re.sub(r"\s+", "", title)
    if any(phrase in clean for phrase in NON_FINAL_TERMINATION_PHRASES):
        return False
    return any(phrase in clean for phrase in TERMINATION_PHRASES)


def fetch_cninfo_keyword(session: requests.Session, keyword: str) -> list[dict]:
    """Fetch every CNINFO BSE announcement for one keyword."""
    all_rows: list[dict] = []
    seen = set()

    for page_num in range(1, MAX_PAGES + 1):
        data = {
            "pageNum": str(page_num),
            "pageSize": str(CNINFO_PAGE_SIZE),
            "column": "",
            "tabName": "fulltext",
            "plate": "bj",
            "stock": "",
            "searchkey": keyword,
            "secid": "",
            "category": "",
            "trade": "",
            "seDate": f"{START_DATE.isoformat()}~{END_DATE.isoformat()}",
            "sortName": "announcementTime",
            "sortType": "-1",
            "isHLtitle": "true",
        }

        response = request_with_retry(
            session,
            "POST",
            CNINFO_TERMINATION_URL,
            headers=CNINFO_HEADERS,
            data=data,
            label=f"CNINFO keyword={keyword} page={page_num}",
        )
        payload = response.json()
        announcements = payload.get("announcements") or []
        total = int(payload.get("totalAnnouncement") or 0)

        print(
            f"[CNINFO] keyword={keyword!r} page={page_num} "
            f"returned={len(announcements)} total={total}"
        )

        for item in announcements:
            if not isinstance(item, dict):
                continue
            code = normalize_code(item.get("secCode"))
            title = strip_html(item.get("announcementTitle"))
            if not is_bse_code(code):
                continue
            key = (
                code,
                title,
                str(item.get("announcementTime") or ""),
                str(item.get("adjunctUrl") or ""),
            )
            if key in seen:
                continue
            seen.add(key)
            row = dict(item)
            row["secCode"] = code
            row["announcementTitle"] = title
            row["_keyword"] = keyword
            all_rows.append(row)

        if not announcements or page_num * CNINFO_PAGE_SIZE >= total:
            break
    else:
        raise RuntimeError(
            f"CNINFO keyword={keyword!r}: pagination exceeded {MAX_PAGES} pages"
        )

    return all_rows


def fetch_cninfo_termination_announcements(
    session: requests.Session,
) -> pd.DataFrame:
    rows: list[dict] = []
    for keyword in ("终止上市", "摘牌"):
        rows.extend(fetch_cninfo_keyword(session, keyword))

    output: list[dict] = []
    seen = set()
    for item in rows:
        code = str(item.get("secCode") or "")
        title = str(item.get("announcementTitle") or "")
        if not is_final_termination_title(title):
            continue

        key = (
            code,
            title,
            str(item.get("announcementTime") or ""),
        )
        if key in seen:
            continue
        seen.add(key)

        adjunct = str(item.get("adjunctUrl") or "").strip()
        announcement_url = (
            f"https://static.cninfo.com.cn/{adjunct.lstrip('/')}"
            if adjunct
            else ""
        )
        output.append(
            {
                "code": code,
                "name": str(item.get("secName") or "").strip(),
                "announcement_date": to_announcement_date(
                    item.get("announcementTime")
                ),
                "title": title,
                "keyword": str(item.get("_keyword") or "").strip(),
                "announcement_url": announcement_url,
            }
        )

    columns = [
        "code",
        "name",
        "announcement_date",
        "title",
        "keyword",
        "announcement_url",
    ]
    result = pd.DataFrame(output, columns=columns)
    if result.empty:
        raise RuntimeError("CNINFO: no final BSE termination announcements found")

    result = (
        result.drop_duplicates(
            subset=["code", "title", "announcement_date"],
            keep="first",
        )
        .sort_values(["announcement_date", "code", "title"], ascending=[False, True, True])
        .reset_index(drop=True)
    )

    unique_codes = result["code"].nunique()
    print(
        f"CNINFO termination validation PASS: "
        f"rows={len(result)} unique_codes={unique_codes}"
    )
    return result


def fetch_current_risk_board(session: requests.Session) -> pd.DataFrame:
    """Fetch current BSE risk-warning and delisting-arrangement snapshots."""
    rows: list[dict] = []

    for risk_type, label in ((0, "risk_warning"), (1, "delist_arrange")):
        for page in range(MAX_PAGES):
            response = request_with_retry(
                session,
                "POST",
                BSE_RISK_API_URL,
                headers=BSE_HEADERS,
                params={"callback": f"jQueryRisk{risk_type}"},
                data=[
                    ("page", str(page)),
                    ("pageSize", "20"),
                    ("type", str(risk_type)),
                ],
                label=f"BSE-RISK type={risk_type} page={page}",
            )
            text = response.text.strip()
            match = re.match(r"^[A-Za-z_$][\w$]*\((.*)\);?$", text, re.DOTALL)
            if match:
                text = match.group(1)
            payload = json.loads(text)
            root = payload[0] if isinstance(payload, list) and payload else payload
            if not isinstance(root, dict):
                raise RuntimeError(
                    f"BSE risk type={risk_type}: unsupported response root"
                )

            content = root.get("content") or []
            total_pages = int(root.get("totalPages") or 0)
            for item in content:
                if not isinstance(item, dict):
                    continue
                code = normalize_code(item.get("xxzqdm"))
                if not is_bse_code(code):
                    continue
                rows.append(
                    {
                        "code": code,
                        "name": str(item.get("xxzqjc") or "").strip(),
                        "risk_type": label,
                    }
                )

            print(
                f"[BSE-RISK] type={risk_type} page={page} "
                f"rows={len(content)} total_pages={total_pages}"
            )
            if not content or page >= max(total_pages - 1, 0):
                break
        else:
            raise RuntimeError(
                f"BSE risk type={risk_type}: pagination exceeded {MAX_PAGES} pages"
            )

    result = pd.DataFrame(rows, columns=["code", "name", "risk_type"]).drop_duplicates(
        subset=["code", "risk_type"]
    )
    print(
        f"BSE risk-board validation PASS: {len(result)} rows, "
        f"{result['code'].nunique() if not result.empty else 0} unique codes"
    )
    return result


def parse_current_list_payload(text: str) -> tuple[list[Any], int]:
    start = text.find("[")
    end = text.rfind("]")
    if start < 0 or end < start:
        raise RuntimeError(f"BSE current list: invalid response: {text[:160]!r}")
    payload = json.loads(text[start : end + 1])
    if not isinstance(payload, list) or not payload or not isinstance(payload[0], dict):
        raise RuntimeError("BSE current list: unsupported response shape")
    root = payload[0]
    content = root.get("content") or []
    if not isinstance(content, list):
        raise RuntimeError("BSE current list: content is not a list")
    return content, int(root.get("totalPages") or 0)


def fetch_current_bse_stocks(session: requests.Session) -> pd.DataFrame:
    payload = {
        "page": "0",
        "typejb": "T",
        "xxfcbj[]": "2",
        "xxzqdm": "",
        "sortfield": "xxzqdm",
        "sorttype": "asc",
    }

    all_rows: list[Any] = []
    total_pages = 0
    for page in range(MAX_PAGES):
        payload["page"] = str(page)
        response = request_with_retry(
            session,
            "POST",
            BSE_CURRENT_LIST_URL,
            headers=BSE_HEADERS,
            data=payload,
            label=f"BSE-CURRENT page={page}",
        )
        rows, page_total = parse_current_list_payload(response.text)
        total_pages = page_total or total_pages
        all_rows.extend(rows)
        print(
            f"[BSE-CURRENT] page={page} rows={len(rows)} "
            f"accumulated={len(all_rows)} total_pages={total_pages}"
        )
        if not rows or (total_pages and page >= total_pages - 1):
            break
    else:
        raise RuntimeError(
            f"BSE current list: pagination exceeded {MAX_PAGES} pages"
        )

    parsed: list[dict] = []
    seen = set()
    for row in all_rows:
        code = ""
        name = ""
        values: list[Any] = []

        if isinstance(row, dict):
            code = normalize_code(row.get("证券代码") or row.get("xxzqdm"))
            name = str(row.get("证券简称") or row.get("xxzqjc") or "").strip()
            values = list(row.values())
        elif isinstance(row, list):
            values = row
            code = normalize_code(row[20]) if len(row) > 20 else ""
            name = str(row[22] or "").strip() if len(row) > 22 else ""

        if not code:
            for value in values:
                candidate = normalize_code(value)
                if is_bse_code(candidate):
                    code = candidate
                    break

        if not is_bse_code(code) or code in seen:
            continue
        seen.add(code)
        parsed.append({"code": code, "name": name})

    if len(parsed) < MIN_CURRENT_BSE_STOCKS:
        raise RuntimeError(
            f"BSE current list: suspiciously small count {len(parsed)} "
            f"< {MIN_CURRENT_BSE_STOCKS}"
        )

    result = pd.DataFrame(parsed).sort_values("code").reset_index(drop=True)
    print(
        f"BSE current-list validation PASS: "
        f"{len(result)} unique listed stocks"
    )
    return result


class _TableParser(HTMLParser):
    """Extract table rows without depending on BSE CSS/JS implementation."""

    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self.rows: list[list[str]] = []
        self._row: list[str] | None = None
        self._cell_parts: list[str] | None = None

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        tag = tag.lower()
        if tag == "tr":
            self._row = []
            self._cell_parts = None
        elif tag in ("td", "th") and self._row is not None:
            self._cell_parts = []

    def handle_data(self, data: str) -> None:
        if self._cell_parts is not None:
            self._cell_parts.append(data)

    def handle_endtag(self, tag: str) -> None:
        tag = tag.lower()
        if tag in ("td", "th") and self._cell_parts is not None and self._row is not None:
            value = re.sub(r"\s+", " ", unescape("".join(self._cell_parts))).strip()
            self._row.append(value)
            self._cell_parts = None
        elif tag == "tr":
            if self._row:
                self.rows.append(self._row)
            self._row = None
            self._cell_parts = None


def fetch_bse_code_mapping(session: requests.Session) -> pd.DataFrame:
    """Fetch and validate all 248 rows from BSE's official code mapping page."""
    for attempt in range(1, SOURCE_ATTEMPTS + 1):
        response = session.get(
            BSE_CODE_MAPPING_URL,
            headers={
                **BSE_HEADERS,
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                "Referer": f"{BSE_BASE_URL}/service/guidance.html",
                "X-Requested-With": "",
            },
            timeout=REQUEST_TIMEOUT,
        )
        print(
            f"[BSE-CODE-MAPPING] HTTP {response.status_code}, "
            f"{len(response.content):,} bytes, http_attempt={attempt}"
        )
        if response.status_code != 200 or len(response.content) < 1000:
            if attempt < SOURCE_ATTEMPTS:
                time.sleep(1.5 * attempt)
                continue
            raise RuntimeError("BSE code mapping: invalid HTTP response")

        parser = _TableParser()
        parser.feed(response.text)
        parser.close()

        records: list[dict[str, str]] = []
        for row in parser.rows:
            date_idx = next(
                (
                    i
                    for i, value in enumerate(row)
                    if re.fullmatch(r"\d{4}[-/]\d{1,2}[-/]\d{1,2}", value)
                ),
                None,
            )
            if date_idx is None:
                continue
            code_idx = [
                i for i, value in enumerate(row) if re.fullmatch(r"\d{6}", value)
            ]
            code_idx = [i for i in code_idx if i > date_idx]
            if len(code_idx) < 2:
                continue
            old_code, new_code = row[code_idx[0]], row[code_idx[1]]
            if not re.fullmatch(r"\d{6}", old_code) or not is_bse_code(new_code):
                continue
            if not new_code.startswith("920") or old_code == new_code:
                continue
            records.append(
                {
                    "name": row[1] if len(row) > 1 else "",
                    "listing_date": row[date_idx].replace("/", "-"),
                    "old_code": old_code,
                    "new_code": new_code,
                }
            )

        mapping = pd.DataFrame(
            records,
            columns=["name", "listing_date", "old_code", "new_code"],
        ).drop_duplicates(subset=["old_code", "new_code"])

        if len(mapping) != 248:
            print(f"BSE code mapping parse: extracted {len(mapping)} rows, expected 248")
            if attempt < SOURCE_ATTEMPTS:
                time.sleep(1.5 * attempt)
                continue
            raise RuntimeError(
                f"BSE code mapping: expected exactly 248 rows, got {len(mapping)}"
            )

        if mapping["old_code"].duplicated().any() or mapping["new_code"].duplicated().any():
            raise RuntimeError("BSE code mapping: duplicate old/new code detected")
        if mapping["new_code"].nunique() != 248 or mapping["old_code"].nunique() != 248:
            raise RuntimeError("BSE code mapping: uniqueness validation failed")
        if not mapping["new_code"].str.startswith("920").all():
            raise RuntimeError("BSE code mapping: non-920 new code detected")

        mapping = mapping.sort_values(["listing_date", "old_code"]).reset_index(drop=True)
        print("BSE code-mapping validation PASS: 248 official old/new code aliases")
        return mapping

    raise AssertionError("unreachable")

def build_historical_codes(
    termination_announcements: pd.DataFrame,
    current: pd.DataFrame,
    mapping: pd.DataFrame,
) -> pd.DataFrame:
    current_codes = set(current["code"])
    terminated_codes = sorted(set(termination_announcements["code"]))

    latest = (
        termination_announcements.sort_values(
            ["announcement_date", "code"],
            ascending=[False, True],
        )
        .drop_duplicates("code", keep="first")
        .set_index("code")
    )

    rows: list[dict] = []

    # Every final BSE termination code must remain searchable, including
    # transfers that happened before the 2025 code migration.
    for code in terminated_codes:
        evidence = latest.loc[code]
        rows.append(
            {
                "code": code,
                "current_code": code,
                "name": evidence["name"],
                "code_type": "termination_code",
                "termination_announcement_date": evidence["announcement_date"],
                "current_bse": code in current_codes,
                "status": (
                    "current_after_termination_signal"
                    if code in current_codes
                    else "absent_from_current_bse"
                ),
            }
        )

    # Every one of the 248 official pre-switch codes is required for
    # historical Sina XiangGuan lookups, not only aliases belonging to
    # already-terminated stocks.
    for _, alias in mapping.iterrows():
        new_code = alias["new_code"]
        termination_date = ""
        if new_code in latest.index:
            termination_date = latest.loc[new_code, "announcement_date"]
        rows.append(
            {
                "code": alias["old_code"],
                "current_code": new_code,
                "name": alias["name"],
                "code_type": "old_code_alias",
                "termination_announcement_date": termination_date,
                "current_bse": alias["old_code"] in current_codes,
                "status": "historical_old_code",
            }
        )

    result = (
        pd.DataFrame(rows)
        .drop_duplicates(subset=["code"], keep="first")
        .sort_values(["code_type", "code"])
        .reset_index(drop=True)
    )
    return result

def validate_all_sources(
    sse: pd.DataFrame,
    szse: pd.DataFrame,
    bse_delisted: pd.DataFrame,
    merged: pd.DataFrame,
    termination_announcements: pd.DataFrame,
    mapping: pd.DataFrame,
    historical_codes: pd.DataFrame,
    current: pd.DataFrame,
    risk_board: pd.DataFrame,
) -> None:
    validate_sse_szse(pd.concat([sse, szse], ignore_index=True))
    if len(bse_delisted) != len(KNOWN_TERMINATED_CODES):
        raise RuntimeError(
            f"BSE: expected {len(KNOWN_TERMINATED_CODES)} final terminated codes, "
            f"got {len(bse_delisted)}"
        )
    if set(bse_delisted["code"]) != set(KNOWN_TERMINATED_CODES):
        raise RuntimeError(
            "BSE: final terminated code set mismatch: "
            f"{sorted(set(bse_delisted['code']) ^ set(KNOWN_TERMINATED_CODES))}"
        )
    if len(mapping) != 248:
        raise RuntimeError(f"BSE: official code mapping must contain 248 rows, got {len(mapping)}")
    if len(historical_codes) != 253:
        raise RuntimeError(f"BSE: expected 253 historical code rows, got {len(historical_codes)}")
    if current.empty or len(current) < MIN_CURRENT_BSE_STOCKS:
        raise RuntimeError("BSE: current list failed validation")
    if not termination_announcements["code"].isin(KNOWN_TERMINATED_CODES).all():
        raise RuntimeError("BSE: termination result contains unexpected codes")

    invalid_code = ~merged["code"].astype("string").str.fullmatch(r"\d{6}", na=False)
    if invalid_code.any():
        raise RuntimeError(
            f"All-delisted: invalid stock codes: {merged.loc[invalid_code, 'code'].tolist()[:20]}"
        )
    dup = merged[merged.duplicated(["exchange", "code"], keep=False)]
    if not dup.empty:
        raise RuntimeError(
            "All-delisted: duplicate exchange/code pairs remain: "
            f"{dup[['exchange','code']].to_dict('records')[:20]}"
        )
    expected = len(sse) + len(szse) + len(bse_delisted)
    if len(merged) != expected:
        raise RuntimeError(
            f"All-delisted: merged count mismatch, expected {expected}, got {len(merged)}"
        )
    if merged["exchange"].value_counts().to_dict().get("BSE", 0) != len(bse_delisted):
        raise RuntimeError("All-delisted: BSE rows missing from merged result")
    if merged["event_date"].isna().all():
        raise RuntimeError("All-delisted: no event dates available")

    print(
        f"ALL-DELISTED VALIDATION PASS: total={len(merged)}, "
        f"SSE={len(sse)}, SZSE={len(szse)}, BSE={len(bse_delisted)}"
    )

def terminated_announcements(df: pd.DataFrame) -> list[str]:
    return list(df["code"].dropna().astype(str).unique())


def main() -> None:
    print(f"All A-share delisted stock build: {START_DATE} -> {END_DATE}")
    print("SSE source=official SSE commonQuery.do")
    print("SZSE source=official SZSE ShowReport xlsx")
    print("BSE termination source=CNINFO plate=bj + BSE current-list crosscheck")
    print("BSE code mapping source=official BSE service/code_mapping.html")

    session = build_session()

    print("\nDownloading SSE delisted stocks...")
    sse, sse_rejected = fetch_source_with_retry(session, fetch_sse, "SSE")

    print("\nDownloading SZSE delisted stocks...")
    szse, szse_rejected = fetch_source_with_retry(session, fetch_szse, "SZSE")

    print("\nDownloading BSE final terminated stocks and historical-code audit...")
    termination_announcements = fetch_cninfo_termination_announcements(session)
    risk_board = fetch_current_risk_board(session)
    current = fetch_current_bse_stocks(session)
    mapping = fetch_bse_code_mapping(session)
    historical_codes = build_historical_codes(
        termination_announcements, current, mapping
    )

    current_codes = set(current["code"])
    delisted_bse = historical_codes[
        (historical_codes["code_type"] == "termination_code")
        & (~historical_codes["current_bse"])
    ].copy()
    aliases = historical_codes[
        historical_codes["code_type"] == "old_code_alias"
    ].copy()

    # Normalize three exchanges into one production schema.
    sse_out = sse.assign(
        current_code=sse["code"],
        termination_announcement_date=pd.NA,
        event_type="delist",
        event_date=pd.to_datetime(sse["delist_date"], errors="coerce"),
        status="terminated_listing",
        source="SSE official commonQuery",
    )
    szse_out = szse.assign(
        current_code=szse["code"],
        termination_announcement_date=pd.NA,
        event_type="delist",
        event_date=pd.to_datetime(szse["delist_date"], errors="coerce"),
        status="terminated_listing",
        source="SZSE official ShowReport",
    )
    bse_out = delisted_bse.assign(
        exchange="BSE",
        list_date=pd.NaT,
        delist_date=pd.NaT,
        event_type="terminate_listing",
        event_date=pd.to_datetime(
            delisted_bse["termination_announcement_date"], errors="coerce"
        ),
        source="CNINFO/BSE official",
    )

    unified_columns = [
        "exchange",
        "code",
        "current_code",
        "name",
        "list_date",
        "delist_date",
        "termination_announcement_date",
        "event_date",
        "event_type",
        "status",
        "source",
    ]
    merged = pd.concat(
        [sse_out[unified_columns], szse_out[unified_columns], bse_out[unified_columns]],
        ignore_index=True,
    )
    merged["event_date"] = pd.to_datetime(merged["event_date"], errors="coerce").dt.date
    merged = merged.sort_values(
        ["event_date", "exchange", "code"],
        ascending=[False, True, True],
        na_position="last",
    ).reset_index(drop=True)

    validate_all_sources(
        sse=sse,
        szse=szse,
        bse_delisted=delisted_bse,
        merged=merged,
        termination_announcements=termination_announcements,
        mapping=mapping,
        historical_codes=historical_codes,
        current=current,
        risk_board=risk_board,
    )

    # Source-specific outputs.
    sse.to_csv(OUTPUT_DIR / "sse_delisted.csv", index=False, encoding="utf-8-sig")
    szse.to_csv(OUTPUT_DIR / "szse_delisted.csv", index=False, encoding="utf-8-sig")
    pd.concat([sse_rejected, szse_rejected], ignore_index=True).to_csv(
        OUTPUT_DIR / "invalid_delisted_rows.csv",
        index=False,
        encoding="utf-8-sig",
    )

    termination_announcements.to_csv(
        OUTPUT_DIR / "bse_termination_announcements.csv",
        index=False,
        encoding="utf-8-sig",
    )
    risk_board.to_csv(
        OUTPUT_DIR / "bse_current_risk_stocks.csv",
        index=False,
        encoding="utf-8-sig",
    )
    current.to_csv(
        OUTPUT_DIR / "bse_current_stocks.csv",
        index=False,
        encoding="utf-8-sig",
    )
    mapping.to_csv(
        OUTPUT_DIR / "bse_code_mapping.csv",
        index=False,
        encoding="utf-8-sig",
    )
    historical_codes.to_csv(
        OUTPUT_DIR / "bse_historical_codes.csv",
        index=False,
        encoding="utf-8-sig",
    )
    delisted_bse.to_csv(
        OUTPUT_DIR / "bse_delisted.csv",
        index=False,
        encoding="utf-8-sig",
    )
    aliases.to_csv(
        OUTPUT_DIR / "bse_old_code_aliases.csv",
        index=False,
        encoding="utf-8-sig",
    )

    merged.to_csv(
        OUTPUT_DIR / "a_share_delisted_all.csv",
        index=False,
        encoding="utf-8-sig",
        date_format="%Y-%m-%d",
    )

    print("\n========== FINAL RESULT ==========")
    print(f"SSE delisted:                 {len(sse)}")
    print(f"SZSE delisted:                {len(szse)}")
    print(f"BSE final terminated:         {len(delisted_bse)}")
    print(f"ALL UNIQUE DELISTED:          {len(merged)}")
    print(f"BSE official code mappings:   {len(mapping)}")
    print(f"BSE historical code universe: {len(historical_codes)}")
    print("\n----- BSE FINAL TERMINATED -----")
    print(
        delisted_bse[
            ["code", "current_code", "name",
             "termination_announcement_date", "status"]
        ].to_string(index=False)
    )
    print("\n----- ALL DELISTED COUNTS BY EXCHANGE -----")
    print(merged.groupby("exchange").size().to_string())
    print(f"\nOUTPUT: {OUTPUT_DIR / 'a_share_delisted_all.csv'}")
    print("===================================")


if __name__ == "__main__":
    main()
