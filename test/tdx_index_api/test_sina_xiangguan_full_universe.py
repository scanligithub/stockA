#!/usr/bin/env python3
"""
Full-universe Sina XiangGuan feasibility test.

Universe:
  TDX current A-share list (generated from the stockA TDX server mechanism)
  +
  Historical delisted stock codes supplied as a static test input

For every stock code, fetch the Sina XiangGuan page and aggregate
historical membership for the target indexes.

This is deliberately a feasibility test, not production code.
"""

from __future__ import annotations

import json
import os
import re
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from pathlib import Path

import pandas as pd
import requests
from bs4 import BeautifulSoup

BASE = "https://vip.stock.finance.sina.com.cn"
ROOT = Path(__file__).resolve().parent
CACHE = ROOT / "sina_xiangguan_cache"
OUT = ROOT / "sina_xiangguan_full_universe"
CACHE.mkdir(parents=True, exist_ok=True)
OUT.mkdir(parents=True, exist_ok=True)

INDEX_ALIASES = {
    "000300": "000300",
    "399300": "000300",
}

TARGET_INDEXES = {
    "000300": "沪深300",
    "000905": "中证500",
    "000852": "中证1000",
    "000688": "科创50",
    "000016": "上证50",
    "399006": "创业板指",
    "932000": "中证2000",
}

WORKERS = int(os.getenv("SINA_WORKERS", "8"))
DELAY = float(os.getenv("SINA_DELAY", "0.15"))
TIMEOUT = 20
RETRIES = 4
UA = (
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/130.0 Safari/537.36"
)


def normalize_code(value: str) -> str:
    s = str(value).strip()
    s = re.sub(r"^(sh|sz|bj)\.?", "", s, flags=re.I)
    return s.zfill(6)


def load_tdx_current() -> pd.DataFrame:
    path = ROOT / "stock_list_master.json"
    if not path.exists():
        raise FileNotFoundError(
            f"Missing {path}; build the TDX current-stock universe first"
        )
    data = json.loads(path.read_text(encoding="utf-8"))
    rows = []
    for item in data:
        code = normalize_code(item.get("code", ""))
        if len(code) == 6:
            rows.append({
                "code": code,
                "name": str(item.get("code_name", "")),
                "source": "tdx_current",
            })
    return pd.DataFrame(rows).drop_duplicates("code")


def load_delisted() -> pd.DataFrame:
    """Load the user-supplied historical delisted stock universe.

    This test deliberately does not call AKShare.  The delisted-code file is
    checked into the test directory and is therefore deterministic/offline.
    """
    path = ROOT / "delisted_stock_codes.txt"
    if not path.exists():
        raise FileNotFoundError(f"Missing supplied delisted list: {path}")

    rows = []
    for line in path.read_text(encoding="utf-8").splitlines():
        code = line.strip()
        if not code or code.startswith("#"):
            continue
        code = normalize_code(code)
        if len(code) != 6 or not code.isdigit():
            raise ValueError(f"Invalid delisted stock code: {code!r}")
        rows.append({
            "code": code,
            "name": "",
            "source": "delisted",
            "list_date": "",
            "delist_date": "",
        })

    return (
        pd.DataFrame(rows)
        .drop_duplicates(subset=["code"])
        .reset_index(drop=True)
    )

