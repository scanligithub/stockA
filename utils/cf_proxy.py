import os
import requests
from time import sleep

class EastMoneyProxy:
    def __init__(self):
        # 自动从环境变量获取 CF Worker URL，并确保格式正确
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
        核心请求逻辑：通过 CF Worker 转发请求到东方财富 API
        """
        if not self.worker_url:
            print("❌ Error: CF_WORKER_URL 环境变量未设置。")
            return None
            
        payload = params.copy()
        payload["target_func"] = target_func
        
        for i in range(retries):
            try:
                # 统一使用 GET 方式，由 CF Worker 接收并转发
                resp = self.session.get(self.worker_url, params=payload, timeout=30)
                if resp.status_code == 200:
                    try:
                        return resp.json()
                    except:
                        # 某些极端情况下返回 200 但内容不是 JSON (例如被 CDN 劫持)
                        return None
                else:
                    # 遇到非 200 状态码（如 403, 502），进行重试退避
                    sleep(1.5)
            except Exception:
                sleep(2)
        return None

    def get_sector_list(self, fs_code, fid="f3", po=1, pz=100, pn=1):
        """
        获取板块列表。
        
        参数说明：
        - fs_code: 过滤字符串，如 "m:90 t:2" (行业板块)
        - fid: 排序字段。f3:涨跌幅, f2:最新价, f12:代码, f6:成交额
        - po: 排序方向。1:降序, 0:升序
        - pz: 每页数量。建议固定 100 以对齐浏览器正常特征
        - pn: 页码。由于网关限流，建议配合脚本多维度请求 pn=1
        """
        params = {
            "pn": pn, 
            "pz": pz, 
            "po": po, 
            "np": 1, 
            "ut": "bd1d9ddb04089700cf9c27f6f7426281",
            "fltt": 2, 
            "invt": 2, 
            "fid": fid,  # 动态排序字段
            "fs": fs_code,
            "fields": "f12,f13,f14" # 仅请求代码、市场、名称，减少流量
        }
        res = self._request("list", params)
        if res and res.get('data') and res['data'].get('diff'):
            items = res['data']['diff']
            # 兼容处理：东财 API 有时返回 dict，有时返回 list
            return list(items.values()) if isinstance(items, dict) else items
        return []

    def get_sector_kline(self, secid, beg="19900101", end="20500101"):
        """
        获取板块历史日K线数据
        
        字段映射 (fields2): 
        f51(日期), f52(开盘), f53(收盘), f54(最高), f55(最低), f56(成交量), f57(成交额), f58(振幅)
        """
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

    def get_sector_constituents(self, sector_code):
        """
        获取板块成份股列表
        pz=5000 确保无需翻页即可拿走全量股票
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
