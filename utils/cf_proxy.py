import os
import requests
import time
from requests.adapters import HTTPAdapter

class EastMoneyProxy:
    def __init__(self):
        # 1. 自动从环境变量获取 CF Worker URL，并确保格式正确
        raw_url = os.getenv("CF_WORKER_URL", "").strip()
        if raw_url and not raw_url.startswith("http"):
            self.worker_url = f"https://{raw_url}"
        else:
            self.worker_url = raw_url
            
        # 2. 模拟真实 Chrome 浏览器的请求头，增加伪装度
        self.headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Referer": "https://quote.eastmoney.com/",
            "Connection": "keep-alive" # 启用长连接，提升并发效率
        }
        
        self.session = requests.Session()
        
        # 100个连接池，保障 20 并发通道畅通，防止本地端口耗尽
        adapter = HTTPAdapter(
            pool_connections=100, 
            pool_maxsize=100, 
            max_retries=1
        )
        self.session.mount('http://', adapter)
        self.session.mount('https://', adapter)
        
        self.session.headers.update(self.headers)

    def _request(self, target_func, params, retries=3):
        """
        统一底层请求逻辑：通过 CF Worker 转发请求
        """
        if not self.worker_url:
            print("❌ Error: 环境变量 CF_WORKER_URL 未设置。")
            return None
            
        payload = params.copy()
        payload["target_func"] = target_func
        
        for i in range(retries):
            try:
                # 【防线1：缓存击穿 Cache Buster】
                # 附加毫秒级时间戳+轮次，强制穿透所有中间 CDN 节点，直达东财真实数据库
                payload["_cb"] = f"{int(time.time() * 1000)}_{i}"
                
                # 超时设置 20 秒，给 CF Worker 留出足够的边缘转发时间
                resp = self.session.get(self.worker_url, params=payload, timeout=20)
                
                if resp.status_code == 200:
                    try:
                        return resp.json()
                    except:
                        # 极端情况下（如被网关拦截返回验证码HTML）解析 JSON 失败
                        time.sleep(1.5)
                        continue
                else:
                    # 遇到 403, 502 等非 200 响应，进行简单退避重试
                    time.sleep(1.5)
            except Exception:
                time.sleep(2)
        return None

    def get_sector_list(self, fs_code, fid="f3", po=1, pz=100, pn=1):
        params = {
            "pn": pn, 
            "pz": pz, 
            "po": po, 
            "np": 1, 
            "ut": "bd1d9ddb04089700cf9c27f6f7426281",
            "fltt": 2, 
            "invt": 2, 
            "fid": fid,
            "fs": fs_code,
            "fields": "f12,f13,f14" 
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
            "lmt": "1000000"
        }
        return self._request("kline", params)

    def get_sector_constituents(self, sector_code, pz=5000, pn=1):
        """
        获取板块成份股列表。
        带有智能降级机制，对付“社保重仓”等慢查询策略板块。
        """
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
        
        # 【防线2：慢查询降级 Fallback】
        # 策略板块(如社保重仓)请求 5000 条会触发东财数据库超时。
        # 如果返回 None 且当前是 5000，立刻降级为 500 再次尝试抢救。
        if res is None and pz == 5000:
            return self.get_sector_constituents(sector_code, pz=500, pn=1)
            
        return res
