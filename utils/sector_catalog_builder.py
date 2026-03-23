import concurrent.futures
import time
from collections import Counter
from datetime import datetime, timedelta

import baostock as bs
import requests
from requests.adapters import HTTPAdapter
from tqdm import tqdm

from utils.cf_proxy import EastMoneyProxy

UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
STOCK_SECTOR_API = "https://push2.eastmoney.com/api/qt/slist/get?spt=3&ut=fa5fd1943c09a822273714f23b58f2d0&pi=0&pz=100&po=1&np=1&fields=f12,f14&secid={secid}"
EM_SEED_API = "https://push2.eastmoney.com/api/qt/clist/get?pn=1&pz=6000&po=1&np=1&ut=bd1d9ddb04089700cf9c27f6f7426281&fltt=2&invt=2&fid=f3&fs=m:0+t:6,m:0+t:80,m:1+t:2,m:1+t:23,m:0+t:81+s:2048&fields=f12,f14"
BASEINFO_API = "https://quote.eastmoney.com/newapi/baseinfo/90.{code}"
UNIVERSE_WORKERS = 15
BASEINFO_WORKERS = 5

BASEINFO_TYPE_MAP = {
    "1": "Region",
    "2": "Industry",
    "3": "Concept",
}


def create_session() -> requests.Session:
    session = requests.Session()
    adapter = HTTPAdapter(pool_connections=100, pool_maxsize=100, max_retries=1)
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    session.headers.update({
        "User-Agent": UA,
        "Referer": "https://quote.eastmoney.com/",
        "Connection": "keep-alive",
    })
    return session


def get_json(session: requests.Session, url: str, params=None, timeout=20, retries=3):
    for attempt in range(retries):
        try:
            resp = session.get(url, params=params, timeout=timeout)
            if resp.status_code == 200:
                return resp.json()
        except Exception:
            pass
        if attempt < retries - 1:
            time.sleep(1.2 + attempt * 0.8)
    return None


def get_stock_seeds_from_baostock():
    try:
        bs.login()
        yesterday = datetime.now() - timedelta(days=1)
        start_str = (datetime.now() - timedelta(days=15)).strftime("%Y-%m-%d")
        end_str = yesterday.strftime("%Y-%m-%d")

        rs_dates = bs.query_trade_dates(start_date=start_str, end_date=end_str)
        trade_dates = []
        while rs_dates.error_code == '0' and rs_dates.next():
            row = rs_dates.get_row_data()
            if row[1] == '1':
                trade_dates.append(row[0])

        for target_date in reversed(trade_dates):
            rs_stocks = bs.query_all_stock(day=target_date)
            stocks = []
            while rs_stocks.error_code == '0' and rs_stocks.next():
                row = rs_stocks.get_row_data()
                code = row[0]
                code_name = row[2] if len(row) > 2 else code
                if code.startswith(("sh.", "sz.", "bj.")) and code_name and code_name.strip():
                    stocks.append((code, code_name.strip()))
            if stocks:
                print(f"[*] 成功从 Baostock 获取种子 (日期: {target_date}, 数量: {len(stocks)})")
                return stocks
    except Exception:
        pass
    finally:
        try:
            bs.logout()
        except Exception:
            pass
    return []


def get_stock_seeds_from_eastmoney(session: requests.Session):
    data = get_json(session, EM_SEED_API, timeout=20, retries=2)
    if not data:
        return []

    diff = data.get("data", {}).get("diff", [])
    items = list(diff.values()) if isinstance(diff, dict) else diff
    stocks = []
    for item in items:
        pure_code = item.get("f12", "")
        name = item.get("f14", "")
        if not pure_code or not name:
            continue
        if pure_code.startswith("6"):
            prefix = "sh"
        elif pure_code.startswith(("4", "8")):
            prefix = "bj"
        else:
            prefix = "sz"
        stocks.append((f"{prefix}.{pure_code}", name.strip()))

    if stocks:
        print(f"[*] 成功从东财种子接口获取种子 (数量: {len(stocks)})")
    return stocks


def bs_code_to_secid(bs_code: str) -> str:
    pure_code = bs_code.split(".")[1]
    if bs_code.startswith("sh"):
        prefix = "1."
    elif bs_code.startswith("sz"):
        prefix = "0."
    else:
        prefix = "0."
    return f"{prefix}{pure_code}"


def fetch_stock_sector_relations(session: requests.Session, stock_info):
    bs_code, stock_name = stock_info
    secid = bs_code_to_secid(bs_code)
    data = get_json(session, STOCK_SECTOR_API.format(secid=secid), timeout=20, retries=2)
    if not data:
        return []

    diff = data.get("data", {}).get("diff", [])
    items = list(diff.values()) if isinstance(diff, dict) else diff
    results = []
    for item in items:
        sector_code = item.get("f12", "")
        sector_name = item.get("f14", "")
        if sector_code.startswith("BK") and sector_name:
            results.append({
                "stock_code": bs_code.split(".")[1],
                "stock_name": stock_name,
                "sector_code": sector_code,
                "sector_name": sector_name.strip(),
            })
    return results


def build_sector_universe():
    session = create_session()
    stocks = get_stock_seeds_from_baostock()
    if not stocks:
        print("[!] Baostock 获取种子失败，切换东财种子接口...")
        stocks = get_stock_seeds_from_eastmoney(session)

    if not stocks:
        raise RuntimeError("无法获取任何个股种子")

    sector_map = {}
    print(f"[*] 开始从 {len(stocks)} 只股票反推完整板块列表...")
    with concurrent.futures.ThreadPoolExecutor(max_workers=UNIVERSE_WORKERS) as executor:
        futures = {executor.submit(fetch_stock_sector_relations, session, stock): stock[0] for stock in stocks}
        for future in tqdm(concurrent.futures.as_completed(futures), total=len(futures), desc="个股->板块"):
            try:
                items = future.result()
                for item in items:
                    code = item["sector_code"]
                    if code not in sector_map:
                        sector_map[code] = {
                            "code": code,
                            "market": 90,
                            "name": item["sector_name"],
                        }
            except Exception:
                pass

    print(f"[+] 个股反推得到唯一板块: {len(sector_map)}")
    return sector_map


def fetch_single_dimension(proxy: EastMoneyProxy, fs_code, fid, po):
    return proxy.get_sector_list(fs_code, fid=fid, po=po, pn=1, pz=100)


def scan_category_types(proxy: EastMoneyProxy, fs_code, label):
    print(f"[*] 正在对 [{label}] 执行 20 维度并发探测...")
    seen_codes = {}
    fids = [
        "f12", "f3", "f2", "f6", "f5", "f4", "f17", "f18", "f8", "f10",
        "f15", "f16", "f11", "f9", "f23", "f20", "f21", "f22", "f24", "f25"
    ]
    tasks = [(fid, po) for fid in fids for po in [1, 0]]

    with concurrent.futures.ThreadPoolExecutor(max_workers=15) as executor:
        futures = {executor.submit(fetch_single_dimension, proxy, fs_code, fid, po): (fid, po) for fid, po in tasks}
        for future in concurrent.futures.as_completed(futures):
            try:
                items = future.result()
                if not items:
                    continue
                for item in items:
                    code = item["f12"]
                    if code not in seen_codes:
                        seen_codes[code] = {
                            "code": code,
                            "market": item.get("f13", 90),
                            "name": item["f14"],
                            "type": label,
                        }
            except Exception:
                pass

    print(f"    [✓] [{label}] 扫描完成，捕获: {len(seen_codes)} 个唯一板块")
    return seen_codes


def build_catalog_type_map(proxy: EastMoneyProxy):
    targets = {
        "Industry": "m:90 t:2",
        "Concept": "m:90 t:3",
        "Region": "m:90 t:1",
    }
    typed_map = {}
    for label, fs_code in targets.items():
        typed_map.update(scan_category_types(proxy, fs_code, label))
    print(f"[+] 目录扫描得到已分类板块: {len(typed_map)}")
    return typed_map


def map_baseinfo_type(data):
    if not data:
        return None
    for key in ("Type111", "JYS", "Type182"):
        value = str(data.get(key, "")).strip()
        if value in BASEINFO_TYPE_MAP:
            return BASEINFO_TYPE_MAP[value]
    return None


def fetch_baseinfo_type(session: requests.Session, sector_code: str):
    data = get_json(
        session,
        BASEINFO_API.format(code=sector_code),
        timeout=15,
        retries=3,
    )
    return map_baseinfo_type(data)


def fill_missing_types(universe_map, typed_map):
    missing_codes = [code for code in universe_map if code not in typed_map]
    if not missing_codes:
        print("[+] 目录扫描已完成全部板块分类")
        return typed_map

    print(f"[*] 目录扫描仍缺失 {len(missing_codes)} 个板块类型，使用 baseinfo 补全...")
    session = create_session()
    with concurrent.futures.ThreadPoolExecutor(max_workers=BASEINFO_WORKERS) as executor:
        futures = {executor.submit(fetch_baseinfo_type, session, code): code for code in missing_codes}
        for future in tqdm(concurrent.futures.as_completed(futures), total=len(futures), desc="baseinfo补分类"):
            code = futures[future]
            try:
                sector_type = future.result()
                if sector_type:
                    typed_map[code] = {
                        "code": code,
                        "market": universe_map[code].get("market", 90),
                        "name": universe_map[code]["name"],
                        "type": sector_type,
                    }
            except Exception:
                pass

    unresolved = [code for code in universe_map if code not in typed_map]
    if unresolved:
        print(f"[!] 仍有 {len(unresolved)} 个板块无法分类，已标记为 Unknown")
    else:
        print("[+] baseinfo 已补齐全部剩余板块类型")
    return typed_map


def build_sector_catalog(proxy: EastMoneyProxy | None = None):
    proxy = proxy or EastMoneyProxy()

    universe_map = build_sector_universe()
    typed_map = build_catalog_type_map(proxy) if proxy.worker_url else {}
    typed_map = fill_missing_types(universe_map, typed_map)

    all_sectors = []
    for code in sorted(universe_map.keys()):
        sector = universe_map[code]
        sector_type = typed_map.get(code, {}).get("type", "Unknown")
        all_sectors.append({
            "code": code,
            "market": sector.get("market", 90),
            "name": sector["name"],
            "type": sector_type,
        })

    counts = Counter(item["type"] for item in all_sectors)
    print(f"[+] 三步式板块目录构建完成，总数: {len(all_sectors)} | 分类统计: {dict(counts)}")
    return all_sectors
