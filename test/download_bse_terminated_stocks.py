#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Build the BSE historical delisting candidate list from risk notices + current stock list.

BSE does not provide a simple historical terminated-stock table equivalent to
SSE/SZSE. We therefore use BSE company announcements as the candidate source:
first remember stocks whose titles indicate delisting risk / possible
termination / termination, then query the official current BSE stock list.
A candidate still present in the current BSE list is NOT considered delisted;
only a candidate absent from the current BSE list is emitted as a delisted
candidate. This also handles securities that transferred from BSE to SSE/SZSE.

PDF downloads are intentionally not required: the announcement title is enough
to identify the risk/termination signal, while the current-list membership is
the deciding evidence for current BSE listing status.
"""

from __future__ import annotations

import json
import random
import re
import time
from datetime import date
from pathlib import Path

import pandas as pd
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

OUTPUT_DIR = Path(__file__).resolve().parent

BSE_PAGE_URL = "https://www.bse.cn/disclosure/announcement.html"
BSE_LIST_URL = "https://www.bse.cn/disclosureInfoController/initDisclosureList.do"
BSE_CURRENT_LIST_URL = "https://www.bse.cn/nqxxController/nqxxCnzq.do"
BSE_BASE_URL = "https://www.bse.cn"

START_DATE = date(2021, 11, 15)
END_DATE = date.today()

# Only one historical scan. The previous implementation repeatedly scanned
# five one-year windows and three keywords, causing hundreds of redundant
# requests against the same BSE announcement index.
SERVER_KEYWORD = ""

REQUEST_TIMEOUT = (15, 60)
HTTP_RETRIES = 2
SOURCE_ATTEMPTS = 3
MAX_PAGES = 200

RISK_KEYWORDS = ("退市风险", "退市风险警示", "可能被终止上市", "可能终止上市", "拟终止上市")
TERMINATION_KEYWORDS = ("股票终止上市", "终止上市暨摘牌", "终止在北京证券交易所上市", "因转板在北京证券交易所终止上市", "股票摘牌", "终止上市")
ALL_SIGNAL_KEYWORDS = RISK_KEYWORDS + TERMINATION_KEYWORDS

# Known historical BSE terminations/transfers used only as source-integrity checks.
KNOWN_TERMINATED_CODES = ("832317", "833874", "833994", "920680", "920305")
MIN_CURRENT_BSE_STOCKS = 100

HEADERS = {
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
        max_retries=HTTP_RETRIES,
        pool_connections=4,
        pool_maxsize=4,
    )
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    return session


def request_with_retry(
    session: requests.Session,
    method: str,
    url: str,
    *,
    params: dict | None = None,
    data=None,
    label: str,
) -> requests.Response:
    last_error = None
    for attempt in range(1, HTTP_RETRIES + 1):
        try:
            response = session.request(
                method,
                url,
                params=params,
                data=data,
                headers=HEADERS,
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
            delay = min(30, 2 ** (attempt - 1) + random.random())
            print(
                f"[{label}] http attempt {attempt}/{HTTP_RETRIES} failed: "
                f"{type(exc).__name__}: {exc}; retry in {delay:.1f}s"
            )
            time.sleep(delay)
    raise RuntimeError(f"{label}: all HTTP retries failed") from last_error


def parse_jsonp(text: str):
    payload = text.strip()
    match = re.match(r"^[A-Za-z_$][\w$]*\((.*)\);?$", payload, re.DOTALL)
    if match:
        payload = match.group(1)
    return json.loads(payload)


def normalize_code(value: object) -> str:
    raw = str(value or "").strip()
    if re.fullmatch(r"\d{1,6}", raw):
        return raw.zfill(6)
    return ""


def extract_records(payload) -> tuple[list[dict], int, int]:
    """Support both known BSE announcement response shapes."""
    root = payload[0] if isinstance(payload, list) and payload else payload
    if not isinstance(root, dict):
        raise RuntimeError("BSE: JSONP payload root is not an object")

    if isinstance(root.get("data"), dict):
        data = root["data"]
        blocks = data.get("content") or []
        records: list[dict] = []
        for block in blocks:
            if isinstance(block, dict) and isinstance(block.get("disclosures"), list):
                records.extend(
                    item for item in block["disclosures"]
                    if isinstance(item, dict)
                )
            elif isinstance(block, dict):
                records.append(block)
        return (
            records,
            int(data.get("totalPages") or 0),
            int(data.get("totalElements") or 0),
        )

    if isinstance(root.get("listInfo"), dict):
        info = root["listInfo"]
        content = info.get("content") or []
        records: list[dict] = []
        for item in content:
            if isinstance(item, dict) and isinstance(item.get("disclosures"), list):
                records.extend(
                    disclosure for disclosure in item["disclosures"]
                    if isinstance(disclosure, dict)
                )
            elif isinstance(item, dict):
                records.append(item)
        return (
            records,
            int(info.get("totalPages") or 0),
            int(info.get("totalElements") or 0),
        )

    if isinstance(root.get("content"), list):
        records = [r for r in root["content"] if isinstance(r, dict)]
        return (
            records,
            int(root.get("totalPages") or 0),
            int(root.get("totalElements") or 0),
        )

    raise RuntimeError(
        f"BSE: unsupported announcement JSON structure, keys={list(root)[:20]}"
    )


def fetch_page(
    session: requests.Session,
    page: int,
) -> tuple[list[dict], int, int]:
    callback = f"jQuery{int(time.time() * 1000)}_{page}"
    form_data = [
        ("siteId", "6"),
        ("flag", "1"),
        ("page", str(page)),
        ("companyCd", ""),
        ("isNewThree", "1"),
        ("keyword", SERVER_KEYWORD),
        ("date", f"{START_DATE.isoformat()} ~ {END_DATE.isoformat()}"),
        ("startTime", START_DATE.isoformat()),
        ("endTime", END_DATE.isoformat()),
        ("xxfcbj[]", "2"),
        ("needFields[]", "companyCd"),
        ("needFields[]", "companyName"),
        ("needFields[]", "disclosureTitle"),
        ("needFields[]", "disclosurePostTitle"),
        ("needFields[]", "destFilePath"),
        ("needFields[]", "publishDate"),
        ("needFields[]", "xxfcbj"),
        ("needFields[]", "fileExt"),
        ("needFields[]", "xxzrlx"),
        ("sortfield", "xxssdq"),
        ("sorttype", "asc"),
    ]
    response = request_with_retry(
        session,
        "POST",
        BSE_LIST_URL,
        params={"callback": callback},
        data=form_data,
        label=f"BSE-ANN page={page}",
    )
    try:
        payload = parse_jsonp(response.text)
    except Exception as exc:
        raise RuntimeError(
            f"BSE: invalid JSONP response: {response.text[:160]!r}"
        ) from exc
    return extract_records(payload)


def fetch_all(session: requests.Session) -> list[dict]:
    last_error: Exception | None = None

    for attempt in range(1, SOURCE_ATTEMPTS + 1):
        try:
            records: list[dict] = []
            page = 0
            total_pages = 0
            total_elements = 0

            while True:
                if page >= MAX_PAGES:
                    raise RuntimeError(
                        f"BSE: pagination exceeded {MAX_PAGES} pages"
                    )

                page_records, page_total, page_elements = fetch_page(session, page)
                total_pages = page_total or total_pages
                total_elements = page_elements or total_elements

                if not page_records:
                    break

                records.extend(page_records)
                print(
                    f"[BSE-ANN] page={page} records={len(page_records)} "
                    f"accumulated={len(records)} total={total_elements} "
                    f"total_pages={total_pages}"
                )

                if total_pages and page >= total_pages - 1:
                    break

                page += 1

            if not records:
                raise RuntimeError("BSE: announcement query returned no records")

            print(
                f"[BSE-ANN] source validation PASS, rows={len(records)}, "
                f"source_attempt={attempt}"
            )
            return records

        except Exception as exc:
            last_error = exc
            if attempt == SOURCE_ATTEMPTS:
                break
            delay = min(60, 5 * attempt + random.uniform(0, 3))
            print(
                f"[BSE-ANN] source attempt {attempt}/{SOURCE_ATTEMPTS} failed: "
                f"{type(exc).__name__}: {exc}; retry in {delay:.1f}s"
            )
            time.sleep(delay)

    raise RuntimeError(
        f"BSE: all {SOURCE_ATTEMPTS} source attempts failed"
    ) from last_error


def fetch_current_bse_stocks(session: requests.Session) -> pd.DataFrame:
    """Fetch the official current BSE listed-stock snapshot."""
    payload = {
        "page": "0",
        "typejb": "T",
        "xxfcbj[]": "2",
        "xxzqdm": "",
        "sortfield": "xxzqdm",
        "sorttype": "asc",
    }

    def parse_current(text: str) -> tuple[list[object], int]:
        start = text.find("[")
        end = text.rfind("]")
        if start < 0 or end < start:
            raise RuntimeError(f"BSE current list: invalid response: {text[:160]!r}")
        data = json.loads(text[start:end + 1])
        if not isinstance(data, list) or not data or not isinstance(data[0], dict):
            raise RuntimeError("BSE current list: unsupported response shape")
        root = data[0]
        content = root.get("content") or []
        if not isinstance(content, list):
            raise RuntimeError("BSE current list: content is not a list")
        return content, int(root.get("totalPages") or 0)

    all_rows: list[object] = []
    total_pages = 0
    page = 0
    while True:
        payload["page"] = str(page)
        response = request_with_retry(
            session,
            "POST",
            BSE_CURRENT_LIST_URL,
            data=payload,
            label=f"BSE-CURRENT page={page}",
        )
        rows, page_total = parse_current(response.text)
        total_pages = page_total or total_pages
        all_rows.extend(rows)
        print(
            f"[BSE-CURRENT] page={page} rows={len(rows)} "
            f"accumulated={len(all_rows)} total_pages={total_pages}"
        )
        if not rows or (total_pages and page >= total_pages - 1):
            break
        page += 1
        if page >= MAX_PAGES:
            raise RuntimeError(
                f"BSE current list: pagination exceeded {MAX_PAGES} pages"
            )

    codes: list[dict] = []
    seen: set[str] = set()
    for row in all_rows:
        code = ""
        name = ""

        if isinstance(row, dict):
            code = normalize_code(row.get("证券代码"))
            if not code:
                code = normalize_code(row.get("xxzqdm"))
            name = str(row.get("证券简称") or row.get("xxzqjc") or "").strip()
            values = list(row.values())
        elif isinstance(row, list):
            # The current BSE endpoint returns array rows. AKShare's official
            # parser maps index 20 -> 证券代码 and index 22 -> 证券简称.
            code = normalize_code(row[20]) if len(row) > 20 else ""
            name = str(row[22] or "").strip() if len(row) > 22 else ""
            values = row
        else:
            values = []

        if not code:
            for value in values:
                candidate = normalize_code(value)
                if candidate and candidate.startswith(("43", "83", "87", "88", "92")):
                    code = candidate
                    break

        if not code or code in seen:
            continue
        seen.add(code)
        codes.append({"code": code, "name": name})

    if not codes:
        raise RuntimeError("BSE current list: no valid stock codes parsed")
    if len(codes) < MIN_CURRENT_BSE_STOCKS:
        raise RuntimeError(
            f"BSE current list: suspiciously small stock count {len(codes)} "
            f"< {MIN_CURRENT_BSE_STOCKS}"
        )

    current = pd.DataFrame(codes).sort_values("code").reset_index(drop=True)
    print(f"BSE current-list validation PASS: {len(current)} unique listed stocks")
    return current


def build_candidates(records: list[dict]) -> pd.DataFrame:
    rows: list[dict] = []

    for record in records:
        title = str(record.get("disclosureTitle") or "").strip()
        post_title = str(record.get("disclosurePostTitle") or "").strip()
        combined_title = f"{title} {post_title}"

        signals = [keyword for keyword in ALL_SIGNAL_KEYWORDS if keyword in combined_title]
        if not signals:
            continue

        code = normalize_code(record.get("companyCd"))
        if not code:
            text_blob = " ".join(
                [
                    title,
                    post_title,
                    str(record.get("destFilePath") or ""),
                    str(record.get("companyName") or ""),
                ]
            )
            matches = re.findall(
                r"(?<!\d)(?:92|87|83|43|82)\d{4}(?!\d)",
                text_blob,
            )
            code = matches[0] if matches else ""

        if not code:
            continue

        is_risk = any(keyword in combined_title for keyword in RISK_KEYWORDS)
        is_termination = any(keyword in combined_title for keyword in TERMINATION_KEYWORDS)

        rows.append(
            {
                "code": code,
                "name": str(record.get("companyName") or "").strip(),
                "title": title,
                "post_title": post_title,
                "publish_time": str(record.get("publishDate") or "").strip(),
                "category": str(record.get("xxzrlx") or "").strip(),
                "source_url": BSE_PAGE_URL,
                "risk_signal": int(is_risk),
                "termination_signal": int(is_termination),
                "signal_keywords": ",".join(signals),
            }
        )

    columns = [
        "code", "name", "title", "post_title", "publish_time",
        "category", "source_url", "risk_signal", "termination_signal",
        "signal_keywords",
    ]
    if not rows:
        return pd.DataFrame(columns=columns)

    return (
        pd.DataFrame(rows)
        .drop_duplicates(
            subset=["code", "publish_time", "title"],
            keep="first",
        )
        .sort_values(["publish_time", "code", "title"])
        .reset_index(drop=True)
    )


def validate_candidates(df: pd.DataFrame, current: pd.DataFrame) -> None:
    if df.empty:
        raise RuntimeError(
            "BSE: no delisting-risk/termination announcement candidates found"
        )
    if current.empty:
        raise RuntimeError("BSE: current listed-stock snapshot is empty")

    invalid = ~df["code"].astype("string").str.fullmatch(r"\d{6}", na=False)
    if invalid.any():
        raise RuntimeError(
            f"BSE: invalid candidate codes: {df.loc[invalid, 'code'].tolist()[:20]}"
        )

    current_invalid = ~current["code"].astype("string").str.fullmatch(r"\d{6}", na=False)
    if current_invalid.any():
        raise RuntimeError(
            f"BSE: invalid current-list codes: {current.loc[current_invalid, 'code'].tolist()[:20]}"
        )

    current_codes = set(current["code"])
    candidate_codes = set(df["code"])
    present = df["code"].isin(current_codes)

    missing_known = [code for code in KNOWN_TERMINATED_CODES if code not in candidate_codes]
    still_current_known = [code for code in KNOWN_TERMINATED_CODES if code in current_codes]
    if missing_known:
        raise RuntimeError(
            "BSE: known historical termination codes missing from announcement scan: "
            f"{missing_known}"
        )
    if still_current_known:
        raise RuntimeError(
            "BSE: known historical termination codes still present in current list: "
            f"{still_current_known}"
        )

    print(
        f"BSE VALIDATION PASS: {len(df)} matched announcements, "
        f"{df['code'].nunique()} unique candidate stocks"
    )
    print(
        f"BSE CURRENT-LIST CROSSCHECK: current rows={int(present.sum())}, "
        f"absent rows={int((~present).sum())}"
    )
    print(
        "BSE KNOWN-CASE CHECK PASS: "
        f"{len(KNOWN_TERMINATED_CODES)} historical termination cases confirmed"
    )


def main() -> None:
    print(f"BSE announcement scan: {START_DATE} -> {END_DATE}")
    print(f"server_keyword={SERVER_KEYWORD}")
    print(f"signal_keywords={ALL_SIGNAL_KEYWORDS}")
    print("announcement_source=BSE company announcements (xxfcbj=2)")
    print("decision_rule=candidate absent from current BSE stock list => delisted")

    session = build_session()

    try:
        response = session.get(
            BSE_PAGE_URL,
            headers=HEADERS,
            timeout=REQUEST_TIMEOUT,
            allow_redirects=False,
        )
        print(
            f"[BSE-PAGE] HTTP {response.status_code}, "
            f"{len(response.content):,} bytes"
        )
    except Exception as exc:
        print(
            f"[BSE-PAGE] warm-up failed (continuing): "
            f"{type(exc).__name__}: {exc}"
        )

    raw_records = fetch_all(session)
    candidates = build_candidates(raw_records)
    current = fetch_current_bse_stocks(session)

    current_codes = set(current["code"])

    latest_rows = (
        candidates.sort_values(["publish_time", "code"])
        .drop_duplicates("code", keep="last")
        .copy()
    )
    latest_rows["current_bse"] = latest_rows["code"].isin(current_codes)
    latest_rows["status"] = latest_rows["current_bse"].map(
        {
            True: "current_bse_not_delisted",
            False: "absent_from_current_bse_delisted_candidate",
        }
    )

    candidates.to_csv(
        OUTPUT_DIR / "bse_delisting_candidates_all.csv",
        index=False,
        encoding="utf-8-sig",
    )
    current.to_csv(
        OUTPUT_DIR / "bse_current_stocks.csv",
        index=False,
        encoding="utf-8-sig",
    )

    classified = latest_rows[
        [
            "code",
            "name",
            "publish_time",
            "title",
            "risk_signal",
            "termination_signal",
            "signal_keywords",
            "current_bse",
            "status",
            "source_url",
        ]
    ].sort_values("code").reset_index(drop=True)

    delisted = classified[~classified["current_bse"]].copy()
    active_risk = classified[classified["current_bse"]].copy()

    validate_candidates(candidates, current)

    delisted.to_csv(
        OUTPUT_DIR / "bse_delisted.csv",
        index=False,
        encoding="utf-8-sig",
    )
    classified.to_csv(
        OUTPUT_DIR / "bse_delisting_candidates_classified.csv",
        index=False,
        encoding="utf-8-sig",
    )

    print("========== RESULT ==========")
    print(f"raw announcement records: {len(raw_records)}")
    print(f"matched announcement rows: {len(candidates)}")
    print(f"unique candidate stocks: {classified['code'].nunique()}")
    print(f"current BSE candidates:  {len(active_risk)}")
    print(f"delisted candidates:     {len(delisted)}")
    print("\n----- DELISTED CANDIDATES -----")
    print(delisted.to_string(index=False))
    print("\n----- STILL CURRENT / RISK -----")
    print(active_risk.to_string(index=False))
    print(f"\nOUTPUT: {OUTPUT_DIR / 'bse_delisted.csv'}")
    print("=============================")


if __name__ == "__main__":
    main()
