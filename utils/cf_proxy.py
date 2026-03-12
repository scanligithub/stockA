import os
import requests
import time
import random
import logging
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

class EastMoneyProxy:
    def __init__(self):
        raw_url = os.getenv("CF_WORKER_URL", "").strip()
        if not raw_url:
            self.worker_url = None
        else:
            self.worker_url = f"https://{raw_url}" if not raw_url.startswith("http") else raw_url
            
        self.headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/120.0.0.0 Safari/537.36",
            "Connection": "keep-alive"
        }
        self.session = requests.Session()
        self.session.headers.update(self.headers)
        
        # 核心改进：增大 backoff_factor (1->2)，遇到 520 时等待更久
        retry_strategy = Retry(
            total=5, # 增加重试次数
            backoff_factor=2, 
            status_forcelist=[429, 500, 502, 503, 504, 520, 522],
            allowed_methods=["GET"]
        )
        
        adapter = HTTPAdapter(pool_connections=50, pool_maxsize=100, max_retries=retry_strategy)
        self.session.mount('https://', adapter)
        self.session.mount('http://', adapter)

    def _request(self, target_func, params):
        if not self.worker_url: return None
        payload = params.copy()
        payload["target_func"] = target_func
        
        try:
            # 增加随机抖动延迟，错开 Matrix Job 的并发峰值
            time.sleep(random.uniform(0.1, 0.3))
            resp = self.session.get(self.worker_url, params=payload, timeout=(10, 60))
            if resp.status_code == 200:
                return resp.json()
        except Exception as e:
            logging.error(f"CF Proxy Error [{target_func}]: {e}")
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
                # 核心改进：翻页之间强行休眠，防止被东财 WAF 封锁
                time.sleep(random.uniform(0.5, 1.0))
            else: 
                break
        return all_items

    def get_sector_kline(self, secid, beg="19900101", end="20500101"):
        params = {"secid": secid, "fields1": "f1,f2,f3,f4,f5,f6", "fields2": "f51,f52,f53,f54,f55,f56,f57,f58", 
                  "klt": "101", "fqt": "1", "beg": beg, "end": end, "lmt": "1000000"}
        return self._request("kline", params)

    def get_sector_constituents(self, sector_code):
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
                time.sleep(random.uniform(0.3, 0.6))
            else: break
        if all_items: return {'data': {'diff': all_items}}
        return None
