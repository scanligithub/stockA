import os
import requests
import pandas as pd
from time import sleep

class EastMoneyProxy:
    def __init__(self):
        raw_url = os.getenv("CF_WORKER_URL", "").strip()
        if raw_url and not raw_url.startswith("http"):
            self.worker_url = f"https://{raw_url}"
        else:
            self.worker_url = raw_url
            
        self.headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/120.0.0.0 Safari/537.36",
            "Connection": "keep-alive"
        }
        self.session = requests.Session()
        self.session.headers.update(self.headers)

    def _request(self, target_func, params, retries=3):
        if not self.worker_url:
            print("❌ Error: CF_WORKER_URL not set.")
            return None
            
        payload = params.copy()
        payload["target_func"] = target_func
        
        for i in range(retries):
            try:
                resp = self.session.get(self.worker_url, params=payload, timeout=30)
                if resp.status_code == 200:
                    try: return resp.json()
                    except: return None
                else:
                    sleep(1)
            except:
                sleep(1)
        return None

    def get_sector_list(self, fs_code):
        all_items = []
        page = 1
        while True:
            params = {
                "pn": page, "pz": 100, "po": 1, "np": 1, 
                "ut": "bd1d9ddb04089700cf9c27f6f7426281",
                "fltt": 2, "invt": 2, "fid": "f3", "fs": fs_code,
                "fields": "f12,f13,f14" 
            }
            res = self._request("list", params)
            if res and res.get('data') and res['data'].get('diff'):
                items = res['data']['diff']
                batch = list(items.values()) if isinstance(items, dict) else items
                all_items.extend(batch)
                if len(batch) < 100: break
                page += 1
                if page > 100: break
            else:
                break
        return all_items

    def get_sector_kline(self, secid, beg="19900101", end="20500101"):
        params = {
            "secid": secid,
            "fields1": "f1,f2,f3,f4,f5,f6",
            "fields2": "f51,f52,f53,f54,f55,f56,f57,f58", 
            "klt": "101", "fqt": "1", "beg": beg, "end": end, "lmt": "1000000"
        }
        return self._request("kline", params)

    def get_sector_constituents(self, sector_code):
        params = {
            "pn": 1, "pz": 3000, "po": 1, "np": 1,
            "ut": "bd1d9ddb04089700cf9c27f6f7426281",
            "fltt": 2, "invt": 2, "fid": "f3",
            "fs": f"b:{sector_code}",
            "fields": "f12,f13,f14"
        }
        return self._request("constituents", params)
