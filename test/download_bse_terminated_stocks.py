#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Discover and verify Beijing Stock Exchange terminated-listing stocks.

The BSE website exposes its announcement search through a JSONP endpoint.
We use the official announcement index as the discovery source, then download
the matched official PDFs and extract the actual termination/delisting date.
"""

from __future__ import annotations

import json
import random
import re
import time
from datetime import date, datetime, timedelta
from io import BytesIO
from pathlib import Path
from urllib.parse import urljoin

import pandas as pd
import requests
from pypdf import PdfReader
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

OUTPUT_DIR = Path(__file__).resolve().parent

BSE_PAGE_URL = "https://www.bse.cn/disclosure/announcement.html"
BSE_LIST_URL = "https://www.bse.cn/disclosureInfoController/initDisclosureList.do"
BSE_BASE_URL = "https://www.bse.cn"

START_DATE = date(2021, 11, 15)  # BSE opening date
END_DATE = date.today()
WINDOW_DAYS = 365

REQUEST_TIMEOUT = (15, 60)
HTTP_RETRIES = 2
SOURCE_ATTEMPTS = 3
MAX_PAGES_PER_WINDOW = 200
PDF_DOWNLOAD_MAX = 100

HEADERS = {
    "Accept": "text/javascript, application/javascript, application/ecmascript, */*; q=0.01",
    "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
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


def parse_jsonp(text: str):
    payload = text.strip()
    match = re.match(r"^[A-Za-z_$][\w$]*\((.*)\);?$", payload, re.DOTALL)
    if match:
        payload = match.group(1)
    return json.loads(payload)


def request_with_retry(
    session: requests.Session,
    method: str,
    url: str,
    *,
    params: dict | None = None,
    data=None,
    headers: dict | None = None,
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
                headers=headers or HEADERS,
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


def normalize_code(value: object) -> str:
    raw = str(value or "").strip()
    if not raw:
        return ""
    if not re.fullmatch(r"\d{1,6}", raw):
        return ""
    return raw.zfill(6)


def extract_content_and_total(payload) -> tuple[list[dict], int, int]:
    """Support both BSE response shapes seen in the wild.

    Shape A:
      [{"listInfo": {"content": [...], "totalPages": N, "totalElements": M}}]

    Shape B:
      [{"data": {"content": [{"disclosures": [...]}], ...}}]
    """
    root = payload[0] if isinstance(payload, list) and payload else payload
    if not isinstance(root, dict):
        raise RuntimeError("BSE: JSONP payload root is not an object")

    if isinstance(root.get("listInfo"), dict):
        info = root["listInfo"]
        records = info.get("content") or []
        total_pages = int(info.get("totalPages") or 0)
        total_elements = int(info.get("totalElements") or 0)
        return [r for r in records if isinstance(r, dict)], total_pages, total_elements

    if isinstance(root.get("data"), dict):
        data = root["data"]
        blocks = data.get("content") or []
        records: list[dict] = []
        for block in blocks:
            if isinstance(block, dict) and isinstance(block.get("disclosures"), list):
                records.extend(
                    item for item in block["disclosures"] if isinstance(item, dict)
                )
            elif isinstance(block, dict):
                records.append(block)
        total_pages = int(data.get("totalPages") or 0)
        total_elements = int(data.get("totalElements") or 0)
        return records, total_pages, total_elements

    if isinstance(root.get("content"), list):
        records = [r for r in root["content"] if isinstance(r, dict)]
        return records, int(root.get("totalPages") or 0), int(root.get("totalElements") or 0)

    raise RuntimeError(f"BSE: unsupported announcement JSON structure, keys={list(root)[:20]}")


def fetch_announcement_page(
    session: requests.Session,
    start: date,
    end: date,
    page: int,
    keyword: str = "终止上市",
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
        headers=HEADERS,
        label=f"BSE-ANN page={page}",
    )
    try:
        payload = parse_jsonp(response.text)
    except Exception as exc:
        raise RuntimeError(
            f"BSE: announcement response is not valid JSONP: {response.text[:160]!r}"
        ) from exc

    return extract_content_and_total(payload)


def fetch_window(
    session: requests.Session,
    start: date,
    end: date,
) -> list[dict]:
    all_records: list[dict] = []
    last_error: Exception | None = None

    for attempt in range(1, SOURCE_ATTEMPTS + 1):
        try:
            window_records: list[dict] = []
            page = 0
            total_pages = 0
            total_elements = 0

            while True:
                if page >= MAX_PAGES_PER_WINDOW:
                    raise RuntimeError(
                        f"BSE: pagination exceeded {MAX_PAGES_PER_WINDOW} pages "
                        f"for {start}..{end}"
                    )

                records, page_total, page_elements = fetch_announcement_page(
                    session, start, end, page
                )
                total_pages = page_total or total_pages
                total_elements = page_elements or total_elements

                if not records:
                    break

                window_records.extend(records)
                print(
                    f"[BSE-ANN] window={start}..{end} page={page} "
                    f"records={len(records)} accumulated={len(window_records)} "
                    f"total={total_elements}"
                )

                if total_pages and page >= total_pages - 1:
                    break
                if not total_pages and len(records) == 0:
                    break

                page += 1

            print(
                f"[BSE-ANN] window validation PASS, "
                f"rows={len(window_records)}, source_attempt={attempt}"
            )
            return window_records
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
        f"BSE: all {SOURCE_ATTEMPTS} attempts failed for {start}..{end}"
    ) from last_error


def iter_windows(start: date, end: date):
    cursor = start
    while cursor <= end:
        window_end = min(cursor + timedelta(days=WINDOW_DAYS - 1), end)
        yield cursor, window_end
        cursor = window_end + timedelta(days=1)


def absolute_pdf_url(path: object) -> str:
    raw = str(path or "").strip()
    if not raw:
        return ""
    return urljoin(BSE_BASE_URL + "/", raw)


def deduplicate_announcements(records: list[dict]) -> pd.DataFrame:
    rows: list[dict] = []
    for record in records:
        code = normalize_code(record.get("companyCd"))
        title = str(record.get("disclosureTitle") or "").strip()
        post_title = str(record.get("disclosurePostTitle") or "").strip()
        publish_time = str(record.get("publishDate") or "").strip()
        pdf_url = absolute_pdf_url(record.get("destFilePath"))
        if not code or not title or not pdf_url:
            continue
        rows.append(
            {
                "code": code,
                "name": str(record.get("companyName") or "").strip(),
                "title": title,
                "post_title": post_title,
                "publish_time": publish_time,
                "pdf_url": pdf_url,
                "file_ext": str(record.get("fileExt") or "").strip(),
                "category": str(record.get("xxzrlx") or "").strip(),
            }
        )

    df = pd.DataFrame(rows)
    if df.empty:
        return pd.DataFrame(
            columns=[
                "code", "name", "title", "post_title",
                "publish_time", "pdf_url", "file_ext", "category"
            ]
        )

    return (
        df.drop_duplicates(
            subset=["code", "publish_time", "title", "pdf_url"],
            keep="first",
        )
        .sort_values(["publish_time", "code", "title"])
        .reset_index(drop=True)
    )


def extract_pdf_text(content: bytes) -> str:
    reader = PdfReader(BytesIO(content))
    parts: list[str] = []
    for page in reader.pages:
        try:
            text = page.extract_text() or ""
        except Exception:
            text = ""
        if text:
            parts.append(text)
    return "\n".join(parts)


def normalize_date_text(year: str, month: str, day: str) -> str:
    try:
        return date(int(year), int(month), int(day)).isoformat()
    except ValueError:
        return ""


def find_termination_dates(text: str) -> list[tuple[str, str]]:
    """Return (date, evidence) pairs, prioritizing explicit delisting wording."""
    text = re.sub(r"\s+", "", text or "")
    patterns = [
        (
            r"(?:终止上市暨摘牌日|终止上市日期|终止上市日|摘牌日期|摘牌日)[：:，,；;。]?"
            r"(?:为|是|定于|安排在|安排于)?"
            r"(20\d{2})[年\-/](\d{1,2})[月\-/](\d{1,2})日?",
            "explicit_date_label",
        ),
        (
            r"(?:将于|拟于|于)(20\d{2})年(\d{1,2})月(\d{1,2})日"
            r"(?:对公司股票)?(?:予以)?摘牌",
            "to_be_delisted",
        ),
        (
            r"(20\d{2})年(\d{1,2})月(\d{1,2})日"
            r"(?:对公司股票)?(?:予以)?摘牌",
            "delist_date_phrase",
        ),
        (
            r"(20\d{2})年(\d{1,2})月(\d{1,2})日"
            r"(?:公司股票)?终止上市",
            "termination_phrase",
        ),
    ]

    found: list[tuple[str, str]] = []
    for pattern, evidence in patterns:
        for match in re.finditer(pattern, text):
            normalized = normalize_date_text(*match.groups())
            if normalized:
                found.append((normalized, evidence))
    # Keep first occurrence per pair.
    return list(dict.fromkeys(found))


def classify_termination(text: str, title: str) -> str:
    combined = f"{title}\n{text}"
    if "转板" in combined:
        return "transfer"
    if "主动终止上市" in combined or "主动终止" in combined:
        return "voluntary"
    return "delisting"


def verify_announcements(
    session: requests.Session,
    announcements: pd.DataFrame,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    if announcements.empty:
        return pd.DataFrame(), pd.DataFrame()

    grouped: dict[str, list[dict]] = {}
    for row in announcements.to_dict("records"):
        grouped.setdefault(row["code"], []).append(row)

    confirmed: list[dict] = []
    unresolved: list[dict] = []
    processed_pdfs = 0

    for code, records in sorted(grouped.items()):
        all_dates: list[tuple[str, str, str, str]] = []
        transfer_seen = False
        max_name = records[0]["name"]

        # Prefer the newest matching announcements, but cap work per code.
        records = sorted(
            records,
            key=lambda r: (r["publish_time"], r["title"]),
            reverse=True,
        )[:PDF_DOWNLOAD_MAX]

        for record in records:
            if processed_pdfs >= PDF_DOWNLOAD_MAX * max(1, len(grouped)):
                break

            try:
                response = request_with_retry(
                    session,
                    "GET",
                    record["pdf_url"],
                    headers=HEADERS,
                    label=f"BSE-PDF {code}",
                )
                text = extract_pdf_text(response.content)
                if not text:
                    print(f"[BSE-PDF {code}] no extractable text: {record['pdf_url']}")
                    continue

                termination_type = classify_termination(text, record["title"])
                transfer_seen = transfer_seen or termination_type == "transfer"
                max_name = record["name"] or max_name

                for found_date, evidence in find_termination_dates(text):
                    all_dates.append(
                        (found_date, evidence, record["publish_time"], record["title"])
                    )
                processed_pdfs += 1
            except Exception as exc:
                print(
                    f"[BSE-PDF {code}] failed: {type(exc).__name__}: {exc}; "
                    f"title={record['title']!r}"
                )

        # The actual termination date should be the latest date explicitly tied
        # to termination/delisting language in the official PDFs.
        if all_dates:
            all_dates.sort(key=lambda item: (item[0], item[2], item[3]))
            final_date, evidence, publish_time, source_title = all_dates[-1]
            confirmed.append(
                {
                    "code": code,
                    "name": max_name,
                    "terminate_date": final_date,
                    "termination_type": "transfer" if transfer_seen else "delisting",
                    "evidence": evidence,
                    "source_publish_time": publish_time,
                    "source_title": source_title,
                }
            )
        else:
            unresolved.append(
                {
                    "code": code,
                    "name": max_name,
                    "reason": "no explicit termination/delisting date extracted from matched official PDFs",
                    "matched_announcements": len(records),
                }
            )

    confirmed_df = pd.DataFrame(confirmed)
    unresolved_df = pd.DataFrame(unresolved)

    if not confirmed_df.empty:
        confirmed_df = confirmed_df.drop_duplicates("code", keep="last").sort_values(
            ["terminate_date", "code"], ascending=[False, True]
        )
    return confirmed_df, unresolved_df


def validate_confirmed(df: pd.DataFrame) -> None:
    if df.empty:
        raise RuntimeError("BSE: no confirmed terminated-listing stocks")
    invalid = ~df["code"].astype("string").str.fullmatch(r"\d{6}", na=False)
    if invalid.any():
        raise RuntimeError(f"BSE: invalid codes: {df.loc[invalid, 'code'].tolist()[:20]}")
    if df["code"].duplicated().any():
        raise RuntimeError("BSE: duplicate confirmed stock codes remain")
    if df["terminate_date"].isna().any():
        raise RuntimeError("BSE: confirmed rows contain null terminate_date")
    print(f"BSE VALIDATION PASS: {len(df)} confirmed terminated-listing stocks")


def main() -> None:
    print(f"BSE announcement scan: {START_DATE} -> {END_DATE}")
    print(f"keyword=终止上市, window_days={WINDOW_DAYS}")

    session = build_session()
    raw_records: list[dict] = []

    # Establish the WAF/session cookie before calling the JSONP endpoint.
    try:
        landing = session.get(
            BSE_PAGE_URL,
            headers=HEADERS,
            timeout=REQUEST_TIMEOUT,
            allow_redirects=False,
        )
        print(f"[BSE-PAGE] HTTP {landing.status_code}, {len(landing.content):,} bytes")
    except Exception as exc:
        print(f"[BSE-PAGE] warm-up failed (continuing): {type(exc).__name__}: {exc}")

    for start, end in iter_windows(START_DATE, END_DATE):
        raw_records.extend(fetch_window(session, start, end))
        time.sleep(random.uniform(0.5, 1.2))

    announcements = deduplicate_announcements(raw_records)
    if announcements.empty:
        raise RuntimeError("BSE: announcement search returned no matching records")

    # Discovery is itself an important auditable artifact.
    announcements.to_csv(
        OUTPUT_DIR / "bse_termination_announcements.csv",
        index=False,
        encoding="utf-8-sig",
    )

    print("========== DISCOVERY ==========")
    print(f"raw announcement records: {len(raw_records)}")
    print(f"unique matched announcements: {len(announcements)}")
    print(f"candidate company codes: {announcements['code'].nunique()}")
    print("===============================")

    confirmed, unresolved = verify_announcements(session, announcements)

    unresolved.to_csv(
        OUTPUT_DIR / "bse_termination_unresolved.csv",
        index=False,
        encoding="utf-8-sig",
    )

    if confirmed.empty:
        raise RuntimeError(
            "BSE: no confirmed termination dates extracted; "
            "inspect bse_termination_announcements.csv and unresolved output"
        )

    validate_confirmed(confirmed)
    confirmed.to_csv(
        OUTPUT_DIR / "bse_delisted.csv",
        index=False,
        encoding="utf-8-sig",
    )

    print("========== RESULT ==========")
    print(f"Confirmed terminated listings: {len(confirmed)}")
    print(f"Unresolved companies:           {len(unresolved)}")
    print(
        "Termination type counts:",
        confirmed["termination_type"].value_counts(dropna=False).to_dict(),
    )
    print(
        "Date range:",
        f"{confirmed['terminate_date'].min()} -> {confirmed['terminate_date'].max()}",
    )
    print(f"OUTPUT: {OUTPUT_DIR / 'bse_delisted.csv'}")
    print("=============================")


if __name__ == "__main__":
    main()
