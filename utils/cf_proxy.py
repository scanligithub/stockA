import os
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import logging
from time import sleep

class EastMoneyProxy:
    def __init__(self):
        raw_url = os.getenv("CF_WORKER_URL", "").strip()
        if not raw_url:
            logging.warning("⚠️ CF_WORKER_URL 未设置！")
            self.worker_url = None
        else:
            self.worker_url = f"https://{raw_url}" if not raw_url.startswith("http") else raw_url
            
        self.headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/120.0.0.0 Safari/537.36",
            "Connection": "keep-alive"
        }
        self.session = requests.Session()
        self.session.headers.update(self.headers)
        
        # 核心修复1：拦截 CF 特有的 520 未知错误，并引入指数退避重试
        retry_strategy = Retry(
            total=3,
            backoff_factor=1,
            status_forcelist=[429, 500, 502, 503, 504, 520, 522], 
            allowed_methods=["GET"]
        )
        
        adapter = HTTPAdapter(pool_connections=20, pool_maxsize=20, max_retries=retry_strategy)
        self.session.mount('https://', adapter)
        self.session.mount('http://', adapter)

    def _request(self, target_func, params):
        if not self.worker_url:
            return None
            
        payload = params.copy()
        payload["target_func"] = target_func
        
        try:
            # timeout=(10, 45) -> 10秒连通性探测，45秒等待东财数据库慢查询
            resp = self.session.get(self.worker_url, params=payload, timeout=(10, 45))
            
            if resp.status_code == 200:
                return resp.json()
            else:
                logging.error(f"CF Proxy Response Error: {resp.status_code} for {target_func}")
                return None
                
        except requests.exceptions.ReadTimeout:
            logging.error(f"CF Proxy Read Timeout (45s reached) for {target_func}")
            return None
        except Exception as e:
            logging.error(f"CF Proxy Request Failed: {e}")
            return None

    def get_sector_list(self, fs_code):
        all_items = []
        page = 1
        while True:
            params = {
                "pn": page, "pz": 100, "po": 1, "np": 1, 
                "ut": "bd1d9ddb04089700cf9c27f6f7426281",
                "fltt": 2, "invt": 2, "fid": "f3", "fs": fs_code, "fields": "f12,f13,f14" 
            }
            res = self._request("list", params)
            if res and res.get('data') and res['data'].get('diff'):
                items = res['data']['diff']
                batch = list(items.values()) if isinstance(items, dict) else items
                all_items.extend(batch)
                if len(batch) < 100: break
                page += 1
                if page > 100: break # 防御性阻断
            else: 
                break
        return all_items

    def get_sector_kline(self, secid, beg="19900101", end="20500101"):
        params = {"secid": secid, "fields1": "f1,f2,f3,f4,f5,f6", "fields2": "f51,f52,f53,f54,f55,f56,f57,f58", 
                  "klt": "101", "fqt": "1", "beg": beg, "end": end, "lmt": "1000000"}
        return self._request("kline", params)

    def get_sector_constituents(self, sector_code):
        """
        核心修复2：东财对 pz=3000 的大单查询容易直接切断连接报 520
        改为 pz=500 分页拉取，模拟普通用户的行为。
        """
        all_items = []
        page = 1
        while True:
            params = {
                "pn": page, "pz": 500, "po": 1, "np": 1, 
                "ut": "bd1d9ddb04089700cf9c27f6f7426281",
                "fltt": 2, "invt": 2, "fid": "f3", "fs": f"b:{sector_code}", "fields": "f12,f13,f14"
            }
            res = self._request("constituents", params)
            if res and res.get('data') and res['data'].get('diff'):
                items = res['data']['diff']
                batch = list(items.values()) if isinstance(items, dict) else items
                all_items.extend(batch)
                if len(batch) < 500: break
                page += 1
                if page > 50: break # 防止死循环，A股没有哪个板块超25000只股
            else:
                break
                
        # 包装成与原先单次查询一致的字典结构返回
        if all_items:
            return {'data': {'diff': all_items}}
        return None
