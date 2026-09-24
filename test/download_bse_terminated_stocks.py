#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Download Beijing Stock Exchange terminated-listing stock candidates.

The BSE disclosure index returns the security code/name and announcement title.
For the historical candidate universe we only need to identify stocks whose
official announcement title indicates termination/delisting. PDF downloads are
not required.

The endpoint is scanned once across the complete historical period. We use
"终止上市" as the server-side search term to keep the response small, then
apply the broader ("终止上市", "摘牌", "退市") title test locally.
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
BSE_BASE_URL = "https://www.bse.cn"

START_DATE = date(2021, 11, 15)
END_DATE = date.today()

# Only one historical scan. The previous implementation repeatedly scanned
# five one-year windows and three keywords, causing hundreds of redundant
# requests against the same BSE announcement index.
SERVER_KEYWORD = "终止上市"

REQUEST_TIMEOUT = (15, 60)
HTTP_RETRIES = 2
SOURCE_ATTEMPTS = 3
MAX_PAGES = 200

TITLE_KEYWORDS = ("终止上市", "摘牌", "退市")

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
        ("flag", "0"),
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


def build_candidates(records: list[dict]) -> pd.DataFrame:
    rows: list[dict] = []

    for record in records:
        title = str(record.get("disclosureTitle") or "").strip()
        post_title = str(record.get("disclosurePostTitle") or "").strip()
        combined_title = f"{title} {post_title}"

        if not any(keyword in combined_title for keyword in TITLE_KEYWORDS):
            continue

        code = normalize_code(record.get("companyCd"))
        if not code:
            # Some BSE notices put the code only in the title/post-title/file name.
            text = " ".join(
                [
                    title,
                    post_title,
                    str(record.get("destFilePath") or ""),
                ]
            )
            matches = re.findall(
                r"(?<!\d)(?:92|87|83|43|82)\d{4}(?!\d)",
                text,
            )
            code = matches[0] if matches else ""

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
                    keyword for keyword in TITLE_KEYWORDS
                    if keyword in combined_title
                ),
            }
        )

    columns = [
        "code", "name", "title", "post_title", "publish_time",
        "category", "source_url", "termination_signal",
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


def validate_candidates(df: pd.DataFrame) -> None:
    if df.empty:
        raise RuntimeError(
            "BSE: no terminated-listing announcement candidates found"
        )

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
    print(f"server_keyword={SERVER_KEYWORD}")
    print(f"title_keywords={TITLE_KEYWORDS}")

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

    candidates.to_csv(
        OUTPUT_DIR / "bse_termination_announcements.csv",
        index=False,
        encoding="utf-8-sig",
    )

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
