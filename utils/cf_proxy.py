import os
import requests
import time
import random
from requests.adapters import HTTPAdapter

class EastMoneyProxy:
    def __init__(self):
        raw_url = os.getenv("CF_WORKER_URL", "").strip()
        self.worker_url = f"https://{raw_url}" if raw_url and not raw_url.startswith("http") else raw_url
            
        self.session = requests.Session()
        adapter = HTTPAdapter(pool_connections=50, pool_maxsize=50, max_retries=1)
        self.session.mount('http://', adapter)
        self.session.mount('https://', adapter)

    def _request(self, target_func, params, retries=3):
        if not self.worker_url: return None
        
        payload = params.copy()
        payload["target_func"] = target_func
        
        for i in range(retries):
            try:
                payload["_cb"] = f"{int(time.time() * 1000)}_{i}"
                resp = self.session.get(self.worker_url, params=payload, timeout=15)
                
                if resp.status_code == 200:
                    try:
                        data = resp.json()
                        # 东财风控阻断时，常返回没有 data 只有 rc 报错的情况
                        if data.get("rc") not in [0, "0", None] and not data.get("data"):
                            time.sleep(random.uniform(0.5, 1.5))
                            continue
                        return data
                    except:
                        pass
                        
                time.sleep(random.uniform(1.0, 2.0))
            except Exception:
                time.sleep(random.uniform(1.5, 2.5))
                
        return None

    def get_sector_list(self, fs_code, fid="f3", po=1, pz=100, pn=1):
        params = {
            "pn": pn, "pz": pz, "po": po, "np": 1, 
            "ut": "bd1d9ddb04089700cf9c27f6f7426281",
            "fltt": 2, "invt": 2, "fid": fid, "fs": fs_code, "fields": "f12,f13,f14" 
        }
        res = self._request("list", params)
        if res and res.get('data') and res['data'].get('diff'):
            items = res['data']['diff']
            return list(items.values()) if isinstance(items, dict) else items
        return []

    def get_sector_kline(self, secid, beg="19900101", end="20500101"):
        params = {
            "secid": secid,
            "fields1": "f1,f2,f3,f4,f5,f6",
            "fields2": "f51,f52,f53,f54,f55,f56,f57,f58", 
            "klt": "101", 
            # 💥 核心修复：板块指数没有分红配股，强行 fqt=1 (前复权) 会导致东财部分网关返回 null 或错误码
            "fqt": "0", 
            "beg": beg, 
            "end": end, 
            "lmt": "10000" 
        }
        return self._request("kline", params)

    def get_sector_constituents(self, sector_code):
        all_items, pn, pz = [], 1, 500
        while True:
            params = {
                "pn": pn, "pz": pz, "po": 1, "np": 1,
                "ut": "bd1d9ddb04089700cf9c27f6f7426281",
                "fltt": 2, "invt": 2, "fid": "f3",
                "fs": f"b:{sector_code}", "fields": "f12,f13,f14"
            }
            res = self._request("constituents", params)
            if not res or not res.get('data') or not res['data'].get('diff'):
                break
                
            diff = res['data']['diff']
            items = list(diff.values()) if isinstance(diff, dict) else diff
            all_items.extend(items)
            
            if len(items) < pz: break
            pn += 1
            time.sleep(0.2)
            
        return {"data": {"diff": all_items}} if all_items else None
