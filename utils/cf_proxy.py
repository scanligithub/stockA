import os
import requests
from time import sleep

class EastMoneyProxy:
    def __init__(self):
        # 自动从环境变量获取 CF Worker URL
        raw_url = os.getenv("CF_WORKER_URL", "").strip()
        if raw_url and not raw_url.startswith("http"):
            self.worker_url = f"https://{raw_url}"
        else:
            self.worker_url = raw_url
            
        self.headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Referer": "https://quote.eastmoney.com/",
            "Connection": "keep-alive"
        }
        self.session = requests.Session()
        self.session.headers.update(self.headers)

    def _request(self, target_func, params, retries=3):
        """
        统一的底层请求转发逻辑
        """
        if not self.worker_url:
            print("❌ Error: CF_WORKER_URL is not set.")
            return None
            
        payload = params.copy()
        payload["target_func"] = target_func
        
        for i in range(retries):
            try:
                # 统一通过 CF Worker 代理发送 GET 请求
                resp = self.session.get(self.worker_url, params=payload, timeout=30)
                if resp.status_code == 200:
                    try:
                        return resp.json()
                    except:
                        return None
                else:
                    # 遇到 403 或其他非 200 状态码，进行简单退避
                    sleep(1.5)
            except Exception:
                sleep(2)
        return None

    def get_sector_list(self, fs_code, po=1, pz=500, pn=1):
        """
        获取板块列表。
        针对截断风险优化：
        1. 默认 pz=500 (尝试一次性获取最大上限)
        2. 支持 po 参数进行双向排序对冲 (1: 跌幅降序, 0: 涨幅升序)
        3. 支持 pn 手动翻页
        """
        params = {
            "pn": pn, 
            "pz": pz, 
            "po": po, 
            "np": 1, 
            "ut": "bd1d9ddb04089700cf9c27f6f7426281",
            "fltt": 2, 
            "invt": 2, 
            "fid": "f3", 
            "fs": fs_code,
            "fields": "f12,f13,f14" 
        }
        res = self._request("list", params)
        if res and res.get('data') and res['data'].get('diff'):
            items = res['data']['diff']
            # 兼容东财有时返回 dict (以索引为 key) 或 list 的情况
            return list(items.values()) if isinstance(items, dict) else items
        return []

    def get_sector_kline(self, secid, beg="19900101", end="20500101"):
        """
        获取板块历史日K线数据
        fields2 定义: f51(日期), f52(开盘), f53(收盘), f54(最高), f55(最低), f56(成交量), f57(成交额), f58(振幅)
        """
        params = {
            "secid": secid,
            "fields1": "f1,f2,f3,f4,f5,f6",
            "fields2": "f51,f52,f53,f54,f55,f56,f57,f58", 
            "klt": "101", 
            "fqt": "1", 
            "beg": beg, 
            "end": end, 
            "lmt": "1000000" # 尽力拉取全量历史
        }
        return self._request("kline", params)

    def get_sector_constituents(self, sector_code):
        """
        获取板块成份股列表。
        pz=5000 确保大概念板块（如华为概念）也能一次性拿全，无需翻页。
        """
        params = {
            "pn": 1, 
            "pz": 5000, 
            "po": 1, 
            "np": 1,
            "ut": "bd1d9ddb04089700cf9c27f6f7426281",
            "fltt": 2, 
            "invt": 2, 
            "fid": "f3",
            "fs": f"b:{sector_code}",
            "fields": "f12,f13,f14"
        }
        return self._request("constituents", params)
