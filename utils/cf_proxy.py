import os
import requests
from time import sleep

class EastMoneyProxy:
    def __init__(self):
        # 从环境变量获取 CF Worker URL，确保前缀正确
        raw_url = os.getenv("CF_WORKER_URL", "").strip()
        if raw_url and not raw_url.startswith("http"):
            self.worker_url = f"https://{raw_url}"
        else:
            self.worker_url = raw_url
            
        # 模拟真实 Chrome 浏览器的请求头，增加伪装度
        self.headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Referer": "https://quote.eastmoney.com/",
            "Connection": "keep-alive" # 启用长连接，提升并发效率
        }
        self.session = requests.Session()
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
                # 统一使用 GET 方式，由 CF Worker 转发至东财 API
                resp = self.session.get(self.worker_url, params=payload, timeout=30)
                if resp.status_code == 200:
                    try:
                        return resp.json()
                    except:
                        # 某些极端情况下（如被网关拦截返回错误HTML）解析 JSON 会失败
                        return None
                else:
                    # 遇到 403, 502 等非 200 响应，进行简单退避重试
                    sleep(1.5)
            except Exception:
                sleep(2)
        return None

    def get_sector_list(self, fs_code, fid="f3", po=1, pz=100, pn=1):
        """
        获取板块列表。
        针对硬截断优化：
        1. 锁定 pz=100，这是网关最信任的单页大小。
        2. 开放 fid (排序字段) 和 po (排序方向)，用于实现多维度包抄。
        """
        params = {
            "pn": pn, 
            "pz": pz, 
            "po": po, 
            "np": 1, 
            "ut": "bd1d9ddb04089700cf9c27f6f7426281",
            "fltt": 2, 
            "invt": 2, 
            "fid": fid,  # 核心参数：根据涨跌、价格、代码等不同维度排序
            "fs": fs_code,
            "fields": "f12,f13,f14" # 仅取代码、市场ID、名称
        }
        res = self._request("list", params)
        if res and res.get('data') and res['data'].get('diff'):
            items = res['data']['diff']
            # 容错处理：东财 API 可能会返回 list 或以索引为 key 的 dict
            return list(items.values()) if isinstance(items, dict) else items
        return []

    def get_sector_kline(self, secid, beg="19900101", end="20500101"):
        """
        获取板块历史日K线数据
        fields2 映射：f51(日期), f52(开), f53(收), f54(高), f55(低), f56(量), f57(额), f58(振幅)
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
        pz=5000 确保无需翻页即可拿走全量股票。
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
