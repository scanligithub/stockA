#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Probe BSE disclosure endpoint parameters for terminated-listing notices."""

from __future__ import annotations

import json
import random
import re
import time

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

URL = "https://www.bse.cn/disclosureInfoController/initDisclosureList.do"
PAGE_URL = "https://www.bse.cn/disclosure/announcement.html"

HEADERS = {
    "Accept": "text/javascript, application/javascript, application/ecmascript, */*; q=0.01",
    "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
    "Cache-Control": "no-cache",
    "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
    "Origin": "https://www.bse.cn",
    "Referer": PAGE_URL,
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/150.0.0.0 Safari/537.36",
    "X-Requested-With": "XMLHttpRequest",
}

KNOWN_CODES = {"832317", "833994", "833874", "920680", "920305"}
TITLE_WORDS = ("终止上市", "转板", "摘牌")

session = requests.Session()
retry = Retry(
    total=2,
    connect=2,
    read=2,
    status=2,
    backoff_factor=1,
    status_forcelist=(429, 500, 502, 503, 504),
    allowed_methods=frozenset({"GET", "POST"}),
    raise_on_status=False,
)
adapter = HTTPAdapter(max_retries=retry)
session.mount("https://", adapter)


def parse_jsonp(text: str):
    text = text.strip()
    m = re.match(r"^[A-Za-z_$][\w$]*\((.*)\);?$", text, re.DOTALL)
    if m:
        text = m.group(1)
    return json.loads(text)


def extract_records(payload):
    root = payload[0] if isinstance(payload, list) else payload
    if not isinstance(root, dict):
        return [], 0, 0

    for key in ("data", "listInfo"):
        obj = root.get(key)
        if isinstance(obj, dict):
            content = obj.get("content") or []
            records = []
            for item in content:
                if isinstance(item, dict) and isinstance(item.get("disclosures"), list):
                    records.extend(x for x in item["disclosures"] if isinstance(x, dict))
                elif isinstance(item, dict):
                    records.append(item)
            return records, int(obj.get("totalPages") or 0), int(obj.get("totalElements") or 0)

    content = root.get("content")
    if isinstance(content, list):
        return [x for x in content if isinstance(x, dict)], int(root.get("totalPages") or 0), int(root.get("totalElements") or 0)
    return [], 0, 0


def probe(flag: str, keyword: str, company: str = ""):
    callback = f"jQuery{int(time.time() * 1000)}_{random.randint(100,999)}"
    data = [
        ("siteId", "6"),
        ("flag", flag),
        ("page", "0"),
        ("companyCd", company),
        ("isNewThree", "1"),
        ("keyword", keyword),
        ("date", "2021-11-15 ~ 2026-09-24"),
        ("startTime", "2021-11-15"),
        ("endTime", "2026-09-24"),
        ("xxfcbj[]", flag),
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
    r = session.post(URL, params={"callback": callback}, data=data, headers=HEADERS, timeout=(15, 60))
    r.raise_for_status()
    records, pages, total = extract_records(parse_jsonp(r.text))
    hits = []
    for x in records:
        code = str(x.get("companyCd") or "").strip()
        title = str(x.get("disclosureTitle") or "").strip()
        post_title = str(x.get("disclosurePostTitle") or "").strip()
        combined = f"{title} {post_title}"
        if code in KNOWN_CODES or any(w in combined for w in TITLE_WORDS):
            hits.append({
                "code": code,
                "name": x.get("companyName"),
                "title": title,
                "post_title": post_title,
                "publishDate": x.get("publishDate"),
            })
    print(
        f"flag/xxfcbj={flag!r}, keyword={keyword!r}, company={company!r}: "
        f"page_records={len(records)}, totalElements={total}, totalPages={pages}, "
        f"interesting_hits={len(hits)}"
    )
    for hit in hits[:20]:
        print("  ", hit)


def main():
    session.get(PAGE_URL, headers=HEADERS, timeout=(15, 60), allow_redirects=False)
    for flag in ("1", "2"):
        for keyword in ("", "终止上市"):
            probe(flag, keyword)
        probe(flag, "终止上市", "920305")


if __name__ == "__main__":
    main()
