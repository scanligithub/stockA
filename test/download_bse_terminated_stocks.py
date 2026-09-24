#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Build the historical BSE stock universe for downstream index-membership crawling.

Production source strategy:
1. CNINFO historical full-text search, filtered by plate=bj, finds BSE
   "终止上市"/"摘牌" announcements.
2. Announcement titles are semantically filtered so risk-warning/proposed
   termination notices are not treated as final termination events.
3. BSE's official current stock list is the current-status cross-check.
4. BSE's official old/new code mapping is joined so pre-2025 BSE codes (for
   example 839680 -> 920680) remain in the historical code universe.
5. The current BSE risk-warning board is fetched separately as an audit of
   currently risky / delisting-arrangement stocks.

No PDF download is required.
"""

from __future__ import annotations

import json
import random
import re
import time
from datetime import date, datetime, timezone
from html import unescape
from zoneinfo import ZoneInfo
from pathlib import Path
from typing import Any

import pandas as pd
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

OUTPUT_DIR = Path(__file__).resolve().parent

BSE_BASE_URL = "https://www.bse.cn"
BSE_PAGE_URL = f"{BSE_BASE_URL}/disclosure/announcement.html"
BSE_CURRENT_LIST_URL = f"{BSE_BASE_URL}/nqxxController/nqxxCnzq.do"
BSE_RISK_API_URL = f"{BSE_BASE_URL}/nqxxController/getRiskWarningStock.do"
BSE_CODE_MAPPING_URL = f"{BSE_BASE_URL}/service/code_mapping.html"

CNINFO_TERMINATION_URL = "https://www.cninfo.com.cn/new/hisAnnouncement/query"

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
KNOWN_OLD_CODE_ALIASES = {"920680": "839680"}

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
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/150.0.0.0 Safari/537.36"
    ),
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


def parse_code_mapping_html(html: str) -> pd.DataFrame:
    """Parse the static BSE official old/new code mapping table."""
    rows: list[list[str]] = []
    for row_match in re.finditer(r"<tr\b[^>]*>(.*?)</tr>", html, re.I | re.S):
        cells = re.findall(
            r"<t[dh]\b[^>]*>(.*?)</t[dh]>",
            row_match.group(1),
            re.I | re.S,
        )
        cleaned = [
            re.sub(r"\s+", " ", strip_html(cell)).strip()
            for cell in cells
        ]
        if cleaned:
            rows.append(cleaned)

    header_index = -1
    for idx, row in enumerate(rows):
        joined = " ".join(row)
        if "旧代码" in joined and "新代码" in joined:
            header_index = idx
            break

    if header_index < 0:
        raise RuntimeError("BSE code mapping: header row not found")

    output: list[dict] = []
    for row in rows[header_index + 1 :]:
        if len(row) < 5:
            continue
        # Expected columns: 序号 / 证券简称 / 上市日期 / 旧代码 / 新代码
        old_code = normalize_code(row[-2])
        new_code = normalize_code(row[-1])
        if not is_bse_code(old_code) or not is_bse_code(new_code):
            continue
        output.append(
            {
                "name": row[1],
                "listing_date": row[2],
                "old_code": old_code,
                "new_code": new_code,
            }
        )

    if not output:
        raise RuntimeError("BSE code mapping: no mapping rows parsed")

    result = pd.DataFrame(output).drop_duplicates(
        subset=["old_code", "new_code"], keep="first"
    )
    print(
        f"BSE code-mapping validation PASS: "
        f"{len(result)} old/new code mappings"
    )
    return result


def fetch_bse_code_mapping(session: requests.Session) -> pd.DataFrame:
    response = request_with_retry(
        session,
        "GET",
        BSE_CODE_MAPPING_URL,
        headers=BSE_HEADERS,
        label="BSE-CODE-MAPPING",
    )
    return parse_code_mapping_html(response.text)


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
    for code in terminated_codes:
        evidence = latest.loc[code]
        current_code = code

        rows.append(
            {
                "code": code,
                "current_code": current_code,
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

        aliases = mapping[mapping["new_code"] == code]
        for _, alias in aliases.iterrows():
            rows.append(
                {
                    "code": alias["old_code"],
                    "current_code": code,
                    "name": alias["name"] or evidence["name"],
                    "code_type": "old_code_alias",
                    "termination_announcement_date": evidence["announcement_date"],
                    "current_bse": alias["old_code"] in current_codes,
                    "status": "historical_old_code",
                }
            )

    result = (
        pd.DataFrame(rows)
        .drop_duplicates(subset=["code", "current_code", "code_type"], keep="first")
        .sort_values(["code_type", "code"])
        .reset_index(drop=True)
    )
    return result


def validate(
    termination_announcements: pd.DataFrame,
    current: pd.DataFrame,
    mapping: pd.DataFrame,
    historical_codes: pd.DataFrame,
    risk_board: pd.DataFrame,
) -> None:
    if termination_announcements.empty:
        raise RuntimeError("BSE: no termination announcements")
    if current.empty:
        raise RuntimeError("BSE: current list empty")
    if mapping.empty:
        raise RuntimeError("BSE: code mapping empty")

    missing_known = [
        code
        for code in KNOWN_TERMINATED_CODES
        if code not in set(termination_announcements["code"])
    ]
    if missing_known:
        raise RuntimeError(
            "BSE: known termination codes missing from CNINFO final announcements: "
            f"{missing_known}"
        )

    current_codes = set(current["code"])
    still_current_known = [
        code for code in KNOWN_TERMINATED_CODES if code in current_codes
    ]
    if still_current_known:
        raise RuntimeError(
            "BSE: known terminated codes unexpectedly present in current BSE list: "
            f"{still_current_known}"
        )

    for new_code, old_code in KNOWN_OLD_CODE_ALIASES.items():
        hit = mapping[
            (mapping["new_code"] == new_code)
            & (mapping["old_code"] == old_code)
        ]
        if hit.empty:
            raise RuntimeError(
                f"BSE: expected old/new code mapping missing: {old_code} -> {new_code}"
            )

    if not risk_board.empty:
        bad = ~risk_board["code"].astype("string").str.fullmatch(
            r"\d{6}", na=False
        )
        if bad.any():
            raise RuntimeError("BSE: risk board contains invalid codes")

    print(
        f"BSE VALIDATION PASS: final termination announcement rows="
        f"{len(termination_announcements)}, unique terminated codes="
        f"{termination_announcements['code'].nunique()}"
    )
    print(
        f"BSE CURRENT-LIST CROSSCHECK: current stocks={len(current)}, "
        f"terminated codes still current={sum(code in current_codes for code in terminated_announcements(termination_announcements))}, "
        f"terminated codes absent={sum(code not in current_codes for code in termination_announcements['code'].unique())}"
    )
    print(
        f"BSE HISTORICAL CODE UNIVERSE: {len(historical_codes)} unique code rows"
    )


def terminated_announcements(df: pd.DataFrame) -> list[str]:
    return list(df["code"].dropna().astype(str).unique())


def main() -> None:
    print(
        f"BSE historical termination build: "
        f"{START_DATE} -> {END_DATE}"
    )
    print("termination_source=CNINFO hisAnnouncement/query, plate=bj")
    print("current_status_source=BSE nqxxController/nqxxCnzq.do")
    print("code_mapping_source=BSE service/code_mapping.html")
    print("final_title_filter=semantic; risk/proposed/H-share false positives excluded")

    session = build_session()

    termination_announcements = fetch_cninfo_termination_announcements(session)
    risk_board = fetch_current_risk_board(session)
    current = fetch_current_bse_stocks(session)
    mapping = fetch_bse_code_mapping(session)

    current_codes = set(current["code"])
    unique_terminated = set(termination_announcements["code"])
    terminated_current = sorted(unique_terminated & current_codes)
    terminated_absent = sorted(unique_terminated - current_codes)

    historical_codes = build_historical_codes(
        termination_announcements,
        current,
        mapping,
    )

    # The final delisted list is based on final termination announcements,
    # then cross-checked against today's BSE current-stock list.
    delisted = historical_codes[
        (historical_codes["code_type"] == "termination_code")
        & (~historical_codes["current_bse"])
    ].copy()

    aliases = historical_codes[
        (historical_codes["code_type"] == "old_code_alias")
    ].copy()

    validate(
        termination_announcements,
        current,
        mapping,
        historical_codes,
        risk_board,
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
    delisted.to_csv(
        OUTPUT_DIR / "bse_delisted.csv",
        index=False,
        encoding="utf-8-sig",
    )
    aliases.to_csv(
        OUTPUT_DIR / "bse_old_code_aliases.csv",
        index=False,
        encoding="utf-8-sig",
    )

    print("========== RESULT ==========")
    print(f"CNINFO final termination rows: {len(termination_announcements)}")
    print(f"CNINFO unique terminated codes: {len(unique_terminated)}")
    print(f"terminated codes still current: {len(terminated_current)}")
    print(f"terminated codes absent current: {len(terminated_absent)}")
    print(f"BSE current stocks: {len(current)}")
    print(f"BSE current risk-board rows: {len(risk_board)}")
    print(f"BSE code mappings: {len(mapping)}")
    print(f"historical code universe rows: {len(historical_codes)}")

    print("\n----- FINAL DELISTED CODES -----")
    print(
        delisted[
            [
                "code",
                "current_code",
                "name",
                "termination_announcement_date",
                "status",
            ]
        ].to_string(index=False)
    )

    print("\n----- OLD CODE ALIASES -----")
    print(
        aliases[
            [
                "code",
                "current_code",
                "name",
                "termination_announcement_date",
                "status",
            ]
        ].to_string(index=False)
    )

    print("\n----- CURRENT RISK BOARD -----")
    print(risk_board.to_string(index=False))

    print(f"\nOUTPUT: {OUTPUT_DIR / 'bse_delisted.csv'}")
    print(f"OUTPUT: {OUTPUT_DIR / 'bse_historical_codes.csv'}")
    print("=============================")


if __name__ == "__main__":
    main()
