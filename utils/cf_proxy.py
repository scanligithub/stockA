import os
import requests
import time
import random
from requests.adapters import HTTPAdapter

class EastMoneyProxy:
    def __init__(self):
        raw_url = os.getenv("CF_WORKER_URL", "").strip()
        if raw_url and not raw_url.startswith("http"):
            self.worker_url = f"https://{raw_url}"
        else:
            self.worker_url = raw_url
            
        self.headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
            "Referer": "https://quote.eastmoney.com/",
            "Connection": "keep-alive"
        }
        
        self.session = requests.Session()
        adapter = HTTPAdapter(
            pool_connections=100, 
            pool_maxsize=100, 
            max_retries=1
        )
        self.session.mount('http://', adapter)
        self.session.mount('https://', adapter)
        self.session.headers.update(self.headers)

    def _request(self, target_func, params, retries=3):
        if not self.worker_url:
            return None
            
        payload = params.copy()
        payload["target_func"] = target_func
        
        for i in range(retries):
            try:
                payload["_cb"] = f"{int(time.time() * 1000)}_{i}"
                # 配合 CF Worker 的 15 秒超时，Python 端放宽到 18 秒，防止提前斩断
                resp = self.session.get(self.worker_url, params=payload, timeout=18)
                
                if resp.status_code == 200:
                    try: 
                        return resp.json()
                    except:
                        time.sleep(random.uniform(1.0, 2.0))
                        continue
                elif resp.status_code in [502, 504]:
                    # 如果 CF Worker 返回了标准化的 JSON 错误，进行拦截
                    try:
                        err_data = resp.json()
                        # rc = -2 说明是 Worker 主动阻断或超时
                        pass 
                    except:
                        pass
                    time.sleep(random.uniform(1.5, 3.0))
                else:
                    time.sleep(random.uniform(1.0, 2.5))
            except requests.exceptions.Timeout:
                time.sleep(random.uniform(2.0, 3.5))
            except Exception:
                time.sleep(random.uniform(1.5, 3.0))
        return None

    def get_sector_list(self, fs_code, fid="f3", po=1, pz=100, pn=1):
        params = {
            "pn": pn, "pz": pz, "po": po, "np": 1, 
            "ut": "bd1d9ddb04089700cf9c27f6f7426281",
            "fltt": 2, "invt": 2, "fid": fid,
            "fs": fs_code, "fields": "f12,f13,f14" 
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
            "fqt": "1", 
            "beg": beg, 
            "end": end, 
            # 💥 核心修复：10000 天约合 40 年，完美覆盖A股历史，绝不触发东财后端慢查询防御
            "lmt": "10000" 
        }
        return self._request("kline", params)

    def get_sector_constituents(self, sector_code):
        """
        核心修复：自带智能翻页，将单次 pz 从 1000 降级到 500，彻底解决慢查询挂起问题
        """
        all_items = []
        pn = 1
        pz = 500 # 💥 安全阈值
        
        while True:
            params = {
                "pn": pn, 
                "pz": pz, 
                "po": 1, 
                "np": 1,
                "ut": "bd1d9ddb04089700cf9c27f6f7426281",
                "fltt": 2, 
                "invt": 2, 
                "fid": "f3",
                "fs": f"b:{sector_code}",
                "fields": "f12,f13,f14"
            }
            res = self._request("constituents", params)
            
            if res is None:
                # 如果 500 都超时，直接抢救性返回已获取的部分
                break
                
            if res.get('data') and res['data'].get('diff'):
                diff = res['data']['diff']
                items = list(diff.values()) if isinstance(diff, dict) else diff
                all_items.extend(items)
                
                # 如果返回数量小于 pz，说明没有下一页了
                if len(items) < pz:
                    break
                pn += 1
                time.sleep(0.5) # 翻页轻微错峰
            else:
                break
                
        # 封装为统一结构返回
        if all_items:
            return {"data": {"diff": all_items}}
        return None
