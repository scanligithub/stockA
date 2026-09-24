#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Probe BSE disclosure pages to discover their actual iframe/JS data endpoints."""

from __future__ import annotations

import re
from pathlib import Path
from urllib.parse import urljoin

import requests

ROOT = Path(__file__).resolve().parent
OUT = ROOT / "bse_page_probe"
OUT.mkdir(exist_ok=True)

BASE = "https://www.bse.cn"
PAGES = {
    "vocational": f"{BASE}/disclosure/vocational.html",
    "announcement": f"{BASE}/disclosure/announcement.html",
    "risk_warning": f"{BASE}/disclosure/risk_warning_board.html",
    "select_stop": f"{BASE}/disclosure/select_stop/200028455.html",
    "listedcompany": f"{BASE}/nq/listedcompany.html",
}

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/150.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
}


def fetch(session: requests.Session, url: str) -> str:
    response = session.get(url, headers=HEADERS, timeout=(15, 60))
    response.raise_for_status()
    if not response.content:
        raise RuntimeError(f"empty response: {url}")
    print(f"[PAGE] {url} -> HTTP {response.status_code}, {len(response.content):,} bytes")
    return response.text


def extract_urls(html: str, base_url: str) -> list[str]:
    raw = re.findall(r"""(?:src|href|action)\s*=\s*["']([^"']+)["']""", html, re.I)
    urls: list[str] = []
    seen: set[str] = set()
    for value in raw:
        full = urljoin(base_url, value)
        if full.startswith(("https://www.bse.cn/", "https://static.bse.cn/")) and full not in seen:
            seen.add(full)
            urls.append(full)
    return urls


def extract_api_like(text: str) -> list[str]:
    patterns = [
        r"""https?://[^"'\\s<>]+(?:Controller|controller)[^"'\\s<>]*""",
        r"""["']([^"']+Controller[^"']*\.do[^"']*)["']""",
        r"""["']([^"']+\.do(?:\?[^"']*)?)["']""",
        r"""["']([^"']+(?:risk|Risk|stop|Stop|delist|Delist|disclosure|Disclosure)[^"']*)["']""",
    ]
    found: list[str] = []
    seen: set[str] = set()
    for pattern in patterns:
        for match in re.findall(pattern, text):
            value = match if isinstance(match, str) else match[0]
            if value and value not in seen:
                seen.add(value)
                found.append(value)
    return found


def print_hits(label: str, text: str) -> None:
    needles = (
        "iframe", "initDisclosureList", "风险警示", "退市整理",
        "终止上市", "risk", "delist", "select_stop", "Controller",
    )
    hits = []
    for line in text.splitlines():
        if any(n.lower() in line.lower() for n in needles):
            cleaned = line.strip()
            if cleaned:
                hits.append(cleaned[:1000])
    print(f"\n===== {label}: keyword hits ({len(hits)}) =====")
    for line in hits[:120]:
        print(line)


def main() -> None:
    session = requests.Session()
    all_linked: list[str] = []
    seen_linked: set[str] = set()

    for label, url in PAGES.items():
        try:
            html = fetch(session, url)
        except Exception as exc:
            print(f"[ERROR] {label}: {type(exc).__name__}: {exc}")
            continue

        path = OUT / f"{label}.html"
        path.write_text(html, encoding="utf-8")
        print(f"[SAVE] {path}")

        urls = extract_urls(html, url)
        print(f"[LINKS] {label}: {len(urls)} BSE/static links")
        for linked in urls:
            if linked not in seen_linked:
                seen_linked.add(linked)
                all_linked.append(linked)
            print(" ", linked)

        print_hits(label, html)

    js_urls = [
        url for url in all_linked
        if url.lower().split("?", 1)[0].endswith((".js", ".mjs"))
    ]

    print(f"\n===== JS TO SCAN: {len(js_urls)} =====")
    for index, url in enumerate(js_urls, 1):
        try:
            js = fetch(session, url)
        except Exception as exc:
            print(f"[JS-ERROR] {url}: {type(exc).__name__}: {exc}")
            continue

        js_path = OUT / f"js_{index:03d}.txt"
        js_path.write_text(js, encoding="utf-8")
        api_like = extract_api_like(js)
        interesting = any(
            n.lower() in js.lower()
            for n in ("risk", "delist", "select_stop", "initdisclosurelist", "退市", "风险警示")
        )
        print(f"[JS] {url} bytes={len(js):,} interesting={interesting} api_like={len(api_like)}")
        for item in api_like[:80]:
            print("  ", item)
        if interesting:
            print_hits(url, js)

    print("\n===== PROBE COMPLETE =====")
    print(f"saved under: {OUT}")


if __name__ == "__main__":
    main()
