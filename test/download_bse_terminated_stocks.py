#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Download Beijing Stock Exchange terminated-listing stock candidates.

The BSE disclosure index already returns the security code/name and the
announcement title.  For building the historical candidate universe we only
need to identify stocks whose official announcement title indicates
termination/delisting. PDF downloads are intentionally not required.
"""

from __future__ import annotations

import json
import random
import re
import time
from datetime import date, timedelta
from pathlib import Path

import pandas as pd
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

OUTPUT_DIR = Path(__file__).resolve().parent

BSE_PAGE_URL = "https://www.bse.cn/disclosure/announcement.html"
BSE_LIST_URL = "https://www.bse.cn/disclosureInfoController/initDisclosureList.do"
BSE_BASE_URL = "https://www.bse.cn"

START_DATE = date(2021, 11, 15)
END_DATE = date.today()
WINDOW_DAYS = 365

REQUEST_TIMEOUT = (15, 60)
HTTP_RETRIES = 2
SOURCE_ATTEMPTS = 3
MAX_PAGES_PER_WINDOW = 200

KEYWORDS = ("终止上市", "摘牌", "退市")

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
        max_retries=retry,
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


def code_from_text(*values: object) -> str:
    for value in values:
        text = str(value or "")
        # Prefer current six-digit stock codes such as 920xxx/87xxxx/83xxxx.
        match = re.search(r"(?<!\d)(92\d{4}|87\d{4}|83\d{4}|43\d{4}|82\d{4}|\d{6})(?!\d)", text)
        if match:
            return match.group(1)
        # Also accept 4-digit legacy BJ codes if an announcement only exposes
        # a short code; normalize later when appropriate.
        match = re.search(r"(?<!\d)(\d{4})(?!\d)", text)
        if match:
            return match.group(1).zfill(6)
    return ""


def extract_records(payload) -> tuple[list[dict], int, int]:
    """Support the two known BSE announcement response shapes."""
    root = payload[0] if isinstance(payload, list) and payload else payload
    if not isinstance(root, dict):
        raise RuntimeError("BSE: JSONP payload root is not an object")

    if isinstance(root.get("listInfo"), dict):
        info = root["listInfo"]
        records = info.get("content") or []
        return (
            [r for r in records if isinstance(r, dict)],
            int(info.get("totalPages") or 0),
            int(info.get("totalElements") or 0),
        )

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
    start: date,
    end: date,
    page: int,
    keyword: str,
) -> tuple[list[dict], int, int]:
    callback = f"jQuery{int(time.time() * 1000)}_{page}"
    form_data = [
        ("siteId", "6"),
        ("flag", "0"),
        ("page", str(page)),
        ("companyCd", ""),
        ("isNewThree", "1"),
        ("keyword", keyword),
        ("date", f"{start.isoformat()} ~ {end.isoformat()}"),
        ("startTime", start.isoformat()),
        ("endTime", end.isoformat()),
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
        label=f"BSE-ANN keyword={keyword} page={page}",
    )
    try:
        payload = parse_jsonp(response.text)
    except Exception as exc:
        raise RuntimeError(
            f"BSE: invalid JSONP response: {response.text[:160]!r}"
        ) from exc
    return extract_records(payload)


def fetch_window(
    session: requests.Session,
    start: date,
    end: date,
    keyword: str,
) -> list[dict]:
    last_error: Exception | None = None
    for attempt in range(1, SOURCE_ATTEMPTS + 1):
        try:
            records: list[dict] = []
            page = 0
            total_pages = 0
            total_elements = 0

            while True:
                if page >= MAX_PAGES_PER_WINDOW:
                    raise RuntimeError(
                        f"BSE: pagination exceeded {MAX_PAGES_PER_WINDOW} pages "
                        f"for {start}..{end}, keyword={keyword}"
                    )

                page_records, page_total, page_elements = fetch_page(
                    session, start, end, page, keyword
                )
                total_pages = page_total or total_pages
                total_elements = page_elements or total_elements

                if not page_records:
                    break

                records.extend(page_records)
                print(
                    f"[BSE-ANN] keyword={keyword} window={start}..{end} "
                    f"page={page} records={len(page_records)} "
                    f"accumulated={len(records)} total={total_elements}"
                )

                if total_pages and page >= total_pages - 1:
                    break
                page += 1

            print(
                f"[BSE-ANN] source validation PASS keyword={keyword}, "
                f"rows={len(records)}, source_attempt={attempt}"
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
        f"BSE: all {SOURCE_ATTEMPTS} attempts failed for {start}..{end}, "
        f"keyword={keyword}"
    ) from last_error


def iter_windows(start: date, end: date):
    cursor = start
    while cursor <= end:
        window_end = min(cursor + timedelta(days=WINDOW_DAYS - 1), end)
        yield cursor, window_end
        cursor = window_end + timedelta(days=1)


def build_candidates(records: list[dict]) -> pd.DataFrame:
    rows: list[dict] = []
    for record in records:
        title = str(record.get("disclosureTitle") or "").strip()
        post_title = str(record.get("disclosurePostTitle") or "").strip()
        combined_title = f"{title} {post_title}"

        if not any(keyword in combined_title for keyword in KEYWORDS):
            continue

        code = normalize_code(record.get("companyCd")) or code_from_text(
            record.get("companyCd"),
            title,
            post_title,
            record.get("destFilePath"),
        )
        if not code:
            continue

        rows.append(
            {
                "code": code,
                "name": str(record.get("companyName") or "").strip(),
                "title": title,
                "post_title": post_title,
                "publish_time": str(record.get("publishDate") or "").strip(),
                "category": str(record.get("xxzrlx") or "").strip(),
                "source_url": BSE_PAGE_URL,
                "termination_signal": ",".join(
                    keyword for keyword in KEYWORDS if keyword in combined_title
                ),
            }
        )

    columns = [
        "code", "name", "title", "post_title", "publish_time",
        "category", "source_url", "termination_signal",
    ]
    if not rows:
        return pd.DataFrame(columns=columns)

    df = pd.DataFrame(rows)
    return (
        df.drop_duplicates(
            subset=["code", "publish_time", "title"],
            keep="first",
        )
        .sort_values(["publish_time", "code", "title"])
        .reset_index(drop=True)
    )


def validate_candidates(df: pd.DataFrame) -> None:
    if df.empty:
        raise RuntimeError("BSE: no terminated-listing announcement candidates found")

    invalid = ~df["code"].astype("string").str.fullmatch(r"\d{6}", na=False)
    if invalid.any():
        raise RuntimeError(
            f"BSE: invalid codes: {df.loc[invalid, 'code'].tolist()[:20]}"
        )

    companies = df["code"].nunique()
    if companies == 0:
        raise RuntimeError("BSE: candidate company count is zero")

    print(
        f"BSE VALIDATION PASS: {len(df)} matched announcements, "
        f"{companies} unique candidate stocks"
    )


def main() -> None:
    print(f"BSE announcement scan: {START_DATE} -> {END_DATE}")
    print(f"keywords={KEYWORDS}")

    session = build_session()

    try:
        response = session.get(
            BSE_PAGE_URL,
            headers=HEADERS,
            timeout=REQUEST_TIMEOUT,
            allow_redirects=False,
        )
        print(f"[BSE-PAGE] HTTP {response.status_code}, {len(response.content):,} bytes")
    except Exception as exc:
        print(f"[BSE-PAGE] warm-up failed (continuing): {type(exc).__name__}: {exc}")

    raw_records: list[dict] = []

    for keyword in KEYWORDS:
        for start, end in iter_windows(START_DATE, END_DATE):
            raw_records.extend(fetch_window(session, start, end, keyword))
            time.sleep(random.uniform(0.3, 0.8))

    candidates = build_candidates(raw_records)

    candidates.to_csv(
        OUTPUT_DIR / "bse_termination_announcements.csv",
        index=False,
        encoding="utf-8-sig",
    )

    # One row per stock is what the historical universe builder needs.
    stocks = (
        candidates.sort_values(["publish_time", "code"])
        .drop_duplicates("code", keep="last")
        [["code", "name", "publish_time", "title", "termination_signal", "source_url"]]
        .sort_values("code")
        .reset_index(drop=True)
    )
    validate_candidates(candidates)

    stocks.to_csv(
        OUTPUT_DIR / "bse_delisted.csv",
        index=False,
        encoding="utf-8-sig",
    )

    print("========== RESULT ==========")
    print(f"raw announcement records: {len(raw_records)}")
    print(f"matched announcements:    {len(candidates)}")
    print(f"unique candidate stocks:  {len(stocks)}")
    print(stocks.to_string(index=False))
    print(f"OUTPUT: {OUTPUT_DIR / 'bse_delisted.csv'}")
    print("=============================")


if __name__ == "__main__":
    main()
