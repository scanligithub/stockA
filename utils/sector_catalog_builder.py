import concurrent.futures
import time
import gc
from collections import Counter
from datetime import datetime, timedelta

import random

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
    # 扩大连接池，但【不禁用 Keep-Alive】，恢复底层的 TCP/TLS 通道复用，维持 60it/s 的极速
    adapter = HTTPAdapter(pool_connections=200, pool_maxsize=200, max_retries=1)
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    session.headers.update({
        "User-Agent": UA,
        "Referer": "https://quote.eastmoney.com/",
        # 注意：这里去掉了 "Connection": "close"，恢复极速复用
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
    
    # 显式控制线程池生命周期
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

    except Exception as e:
        print(f"[-] 扫描中断: {e}")
    finally:
        # 💥 极速强杀防卡死组合拳：
        # 1. 主动关闭 session，销毁底层的 TCP Socket 连接，向服务器发送 FIN。
        session.close()
        # 2. 清理大量 Future 对象引用，减轻垃圾回收压力
        try: del futures 
        except: pass
        gc.collect() 
        # 3. wait=False 确保主线程绝不在此死等残留的僵尸线程
        executor.shutdown(wait=False)

    print(f"[+] 个股反推得到唯一板块: {len(sector_map)}")
    return sector_map

def fetch_single_dimension(proxy: EastMoneyProxy, fs_code, fid, po):
    """单维度前100名探测：采用极速'游击战'策略，遇阻即撤，绝不死等"""
    params = {
        "pn": 1, "pz": 100, "po": po, "np": 1,
        "ut": "bd1d9ddb04089700cf9c27f6f7426281",
        "fltt": 2, "invt": 2, "fid": fid,
        "fs": fs_code, "fields": "f12,f13,f14",
        "target_func": "list",
        "_cb": str(int(time.time() * 1000)) # 穿透缓存
    }
    
    try:
        # 💥 核心修改：绕过 proxy._request 的 3次重试和 20秒死等。
        # 强制设置 timeout=3.5秒。只要东财 3.5 秒没算出来，说明是慢查询维度，立刻抛弃！
        resp = proxy.session.get(proxy.worker_url, params=params, timeout=3.5)
        
        if resp.status_code == 200:
            res = resp.json()
            if res and res.get('data') and res['data'].get('diff'):
                items = res['data']['diff']
                return list(items.values()) if isinstance(items, dict) else items
    except Exception:
        # 发生超时或网络错误，一声不吭直接放弃，绝不重试阻塞线程池
        pass
        
    return []

def scan_category_types(proxy: EastMoneyProxy, fs_code, label):
    """20维度正反序全向包抄法 (极速非阻塞版)"""
    print(f"[*] 正在对 [{label}] 执行 20 维度并发探测...")
    seen_codes = {}
    
    fids = [
        "f12", "f3", "f2", "f6", "f5", "f4", "f17", "f18", "f8", "f10",
        "f15", "f16", "f11", "f9", "f23", "f20", "f21", "f22", "f24", "f25"
    ]
    tasks = [(fid, po) for fid in fids for po in [1, 0]]

    # 40 个任务瞬间打出
    with concurrent.futures.ThreadPoolExecutor(max_workers=15) as executor:
        futures = {executor.submit(fetch_single_dimension, proxy, fs_code, fid, po): (fid, po) for fid, po in tasks}
        
        # 即使有倒霉任务卡住，最多 3.5 秒后就会被强制剔除
        for future in concurrent.futures.as_completed(futures):
            try:
                items = future.result()
                if not items: continue
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
