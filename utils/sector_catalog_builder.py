import concurrent.futures
import time
import gc
from collections import Counter
from datetime import datetime, timedelta

import baostock as bs
import requests
from requests.adapters import HTTPAdapter
from tqdm import tqdm

from utils.cf_proxy import EastMoneyProxy

UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
# 个股所属板块 API
STOCK_SECTOR_API = "https://push2.eastmoney.com/api/qt/slist/get?spt=3&ut=fa5fd1943c09a822273714f23b58f2d0&pi=0&pz=100&po=1&np=1&fields=f12,f14&secid={secid}"
# 东财轻量级种子接口
EM_SEED_API = "https://push2.eastmoney.com/api/qt/clist/get?pn=1&pz=6000&po=1&np=1&ut=bd1d9ddb04089700cf9c27f6f7426281&fltt=2&invt=2&fid=f3&fs=m:0+t:6,m:0+t:80,m:1+t:2,m:1+t:23,m:0+t:81+s:2048&fields=f12,f14"
# 板块详情接口 (仅作最后兜底)
BASEINFO_API = "https://quote.eastmoney.com/newapi/baseinfo/90.{code}"

UNIVERSE_WORKERS = 40
BASEINFO_WORKERS = 40

BASEINFO_TYPE_MAP = {
    "1": "Region",
    "2": "Industry",
    "3": "Concept",
}

def create_session() -> requests.Session:
    session = requests.Session()
    # 增大连接池，并禁用 Keep-Alive 防止线程池关闭时挂起
    adapter = HTTPAdapter(pool_connections=200, pool_maxsize=200, max_retries=1)
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    session.headers.update({
        "User-Agent": UA,
        "Referer": "https://quote.eastmoney.com/",
        "Connection": "close",  # 💥 关键：请求完立即关闭连接，防止句柄泄露导致线程卡死
    })
    return session

def get_json(session: requests.Session, url: str, params=None, timeout=15, retries=2):
    for attempt in range(retries):
        try:
            resp = session.get(url, params=params, timeout=timeout)
            if resp.status_code == 200:
                return resp.json()
        except Exception:
            pass
        if attempt < retries - 1:
            time.sleep(1)
    return None

def get_stock_seeds_from_baostock():
    """获取全量个股种子列表"""
    try:
        bs.login()
        # 探测最近的交易日
        for i in range(15):
            target_date = (datetime.now() - timedelta(days=i)).strftime("%Y-%m-%d")
            rs = bs.query_all_stock(day=target_date)
            stocks = []
            if rs.error_code == '0':
                while rs.next():
                    row = rs.get_row_data()
                    code, name = row[0], row[2] if len(row) > 2 else ""
                    if code.startswith(("sh.", "sz.", "bj.")) and name:
                        stocks.append((code, name.strip()))
                if stocks:
                    print(f"[*] 成功从 Baostock 获取种子 (日期: {target_date}, 数量: {len(stocks)})")
                    return stocks
    except: pass
    finally:
        try: bs.logout()
        except: pass
    return []

def get_stock_seeds_from_eastmoney(session: requests.Session):
    """东财备选种子接口"""
    data = get_json(session, EM_SEED_API)
    if not data: return []
    diff = data.get("data", {}).get("diff", [])
    items = list(diff.values()) if isinstance(diff, dict) else diff
    stocks = []
    for item in items:
        c, n = item.get("f12", ""), item.get("f14", "")
        if not c: continue
        prefix = "sh" if c.startswith("6") else "bj" if c.startswith(("4","8")) else "sz"
        stocks.append((f"{prefix}.{c}", n.strip()))
    return stocks

def fetch_stock_sector_relations(session: requests.Session, stock_info):
    """单只股票所属板块查询"""
    bs_code, stock_name = stock_info
    pure_code = bs_code.split(".")[1]
    secid = f"1.{pure_code}" if bs_code.startswith("sh") else f"0.{pure_code}"
    
    data = get_json(session, STOCK_SECTOR_API.format(secid=secid))
    if not data: return []
    
    diff = data.get("data", {}).get("diff", [])
    items = list(diff.values()) if isinstance(diff, dict) else diff
    return [{"sector_code": x["f12"], "sector_name": x["f14"].strip()} 
            for x in items if x.get("f12", "").startswith("BK")]

def build_sector_universe():
    """核心：自下而上反推板块全集"""
    session = create_session()
    stocks = get_stock_seeds_from_baostock() or get_stock_seeds_from_eastmoney(session)
    if not stocks: raise RuntimeError("无法获取任何个股种子")

    sector_map = {}
    print(f"[*] 开始从 {len(stocks)} 只股票反推完整板块列表...")
    
    # 手动管理线程池，避免 with 块隐式等待挂起
    executor = concurrent.futures.ThreadPoolExecutor(max_workers=UNIVERSE_WORKERS)
    try:
        futures = {executor.submit(fetch_stock_sector_relations, session, s): s for s in stocks}
        
        for future in tqdm(concurrent.futures.as_completed(futures), total=len(futures), desc="个股->板块"):
            try:
                relations = future.result()
                for rel in relations:
                    code = rel["sector_code"]
                    if code not in sector_map:
                        sector_map[code] = {"code": code, "name": rel["sector_name"]}
            except: pass

        # 💥 解决卡顿核心：在退出前显式销毁连接和引用
        session.close()
        del futures
        gc.collect() 
        executor.shutdown(wait=False) # 不再死等残留请求
        
    except Exception as e:
        print(f"[-] 扫描中断: {e}")
    finally:
        session.close()

    print(f"[+] 个股反推得到唯一板块: {len(sector_map)}")
    return sector_map

def scan_category_types(proxy: EastMoneyProxy, fs_code, label):
    """全量分类探测 (pz=3000 一次性拿全)"""
    print(f"[*] 获取 [{label}] 分类目录 (一次性扫描)...")
    seen_codes = {}
    # 使用 pz=3000 确保 1009 个板块一次性全部在列表内，不再需要 20 维包抄
    try:
        items = proxy.get_sector_list(fs_code, fid="f3", po=1, pn=1, pz=3000)
        if items:
            for item in items:
                code = item["f12"]
                seen_codes[code] = {
                    "code": code,
                    "market": item.get("f13", 90),
                    "name": item["f14"],
                    "type": label
                }
    except Exception as e:
        print(f"[-] 扫描 [{label}] 失败: {e}")
    return seen_codes

def fetch_baseinfo_type(session: requests.Session, code: str):
    """最后的抢救：通过 baseinfo 详情页获取类型"""
    data = get_json(session, BASEINFO_API.format(code=code))
    if not data: return None
    for key in ("Type111", "JYS", "Type182"):
        val = str(data.get(key, "")).strip()
        if val in BASEINFO_TYPE_MAP: return BASEINFO_TYPE_MAP[val]
    return None

def build_sector_catalog(proxy: EastMoneyProxy | None = None):
    """三步走构建完整板块目录"""
    proxy = proxy or EastMoneyProxy()

    # 1. 自下而上反推 (确保不漏)
    universe_map = build_sector_universe()
    
    # 2. 官方目录扫描 (确保分类)
    targets = {"Industry": "m:90 t:2", "Concept": "m:90 t:3", "Region": "m:90 t:1"}
    typed_map = {}
    if proxy.worker_url:
        for label, fs_code in targets.items():
            typed_map.update(scan_category_types(proxy, fs_code, label))

    # 3. 补全缺失类型 (如果有的话)
    missing_codes = [c for c in universe_map if c not in typed_map]
    if missing_codes:
        print(f"[*] 仍有 {len(missing_codes)} 个板块缺失类型，启动抢救模式...")
        session = create_session()
        with concurrent.futures.ThreadPoolExecutor(max_workers=BASEINFO_WORKERS) as executor:
            f_map = {executor.submit(fetch_baseinfo_type, session, c): c for c in missing_codes}
            for f in tqdm(concurrent.futures.as_completed(f_map), total=len(f_map), desc="补分类"):
                code = f_map[f]
                try:
                    t = f.result()
                    if t:
                        typed_map[code] = {
                            "code": code,
                            "name": universe_map[code]["name"],
                            "type": t
                        }
                except: pass
        session.close()

    # 4. 汇总
    all_sectors = []
    for code, info in universe_map.items():
        all_sectors.append({
            "code": code,
            "name": info["name"],
            "type": typed_map.get(code, {}).get("type", "Unknown")
        })
    
    counts = Counter(x["type"] for x in all_sectors)
    print(f"[+] 板块目录构建完成 | 总数: {len(all_sectors)} | 统计: {dict(counts)}")
    return all_sectors
