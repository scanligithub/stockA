import json
import random
import requests
import pandas as pd
import time
import os
import polars as pl
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm

# 全局标准浏览器请求头，绕过云节点风控
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Referer": "https://quote.eastmoney.com/"
}

REPORT_NAME = "RPT_LICO_FN_CPD"
WORKING_FILTER = None

# 37 个原始大写字段到本地标准小写字段的映射字典
FIELD_MAPPING = {
    "SECURITY_CODE": "code",
    "SECURITY_NAME_ABBR": "name",
    "TRADE_MARKET_CODE": "trade_market_code",
    "TRADE_MARKET": "trade_market",
    "SECURITY_TYPE_CODE": "security_type_code",
    "SECURITY_TYPE": "security_type",
    "UPDATE_DATE": "update_date",
    "REPORTDATE": "report_date",
    "BASIC_EPS": "basic_eps",
    "DEDUCT_BASIC_EPS": "deduct_basic_eps",
    "TOTAL_OPERATE_INCOME": "total_operate_income",
    "PARENT_NETPROFIT": "parent_netprofit",
    "WEIGHTAVG_ROE": "weightavg_roe",
    "YSTZ": "ystz",
    "SJLTZ": "sjltz",
    "BPS": "bps",
    "MGJYXJJE": "mgjyxjje",
    "XSMLL": "xsmll",
    "YSHZ": "yshz",
    "SJLHZ": "sjlhz",
    "ASSIGNDSCRPT": "assigndscrpt",
    "PAYYEAR": "payyear",
    "PUBLISHNAME": "publishname",
    "ZXGXL": "zxgxl",
    "NOTICE_DATE": "notice_date",
    "ORG_CODE": "org_code",
    "TRADE_MARKET_ZJG": "trade_market_zjg",
    "ISNEW": "isnew",
    "QDATE": "qdate",
    "DATATYPE": "datatype",
    "DATAYEAR": "datayear",
    "DATEMMDD": "datemmdd",
    "EITIME": "eitime",
    "SECUCODE": "secucode",
    "BOARD_NAME": "board_name",
    "ORI_BOARD_CODE": "ori_board_code",
    "BOARD_CODE": "board_code"
}

def safe_float(val):
    if val is None: return None
    try: return float(val)
    except: return None

def safe_int(val):
    if val is None: return None
    try: return int(val)
    except: return None

def safe_str(val):
    if val is None: return ""
    return str(val).strip()

def safe_date(val):
    """剔除日期后面多余的 00:00:00 尾巴"""
    if val is None: return ""
    val_str = str(val).strip()
    return val_str[:10] if len(val_str) >= 10 else val_str

def explore_sql_syntax(pure_code, tdx_code):
    """🕵️ 语法对齐探测，建立可复用过滤器模板"""
    global WORKING_FILTER
    flt = f'(SECURITY_CODE="{pure_code}")'
    url = "https://datacenter.eastmoney.com/api/data/v1/get"
    params = {
        "sortColumns": "SECURITY_CODE",
        "sortTypes": "-1",
        "pageSize": "3",
        "pageNumber": "1",
        "reportName": REPORT_NAME,
        "columns": "ALL",
        "filter": flt,
        "client": "WEB"
    }
    try:
        res = requests.get(url, params=params, headers=HEADERS, timeout=10).json()
        if res.get("code") == 0:
            WORKING_FILTER = flt
            return True
    except:
        pass
    return False

def fetch_f10_datacenter_with_retry(tdx_item):
    """安全拉取单个个股的全量 F10 财务历史"""
    global WORKING_FILTER
    tdx_code = tdx_item['code'].replace('.', '').lower()         
    name = tdx_item.get('code_name', '')
    pure_code = tdx_code[2:]
    
    actual_filter = WORKING_FILTER.replace("PROBE_CODE", pure_code)
    url = "https://datacenter.eastmoney.com/api/data/v1/get"
    params = {
        "sortColumns": "REPORTDATE", # 🛡️ 使用紧凑的 REPORTDATE 字段进行严格的时间正序排列
        "sortTypes": "1",           # 1代表升序（从小到大，1995-2026）
        "pageSize": "150",          # 支持单股 37 年全历史季度
        "pageNumber": "1",
        "reportName": REPORT_NAME,
        "columns": "ALL",           # 🌟 核心：拉取 37 个全量物理列，绝不遗漏
        "filter": actual_filter,
        "client": "WEB" 
    }
    
    max_retries = 3
    for attempt in range(max_retries):
        try:
            res = requests.get(url, params=params, headers=HEADERS, timeout=8)
            data = res.json()
            if data and data.get("result") and data["result"].get("data"):
                return pure_code, name, data["result"]["data"], None
            
            code = data.get('code')
            if code == 911 or "无数据" in data.get('message', ''):
                return pure_code, name, [], "NO_DATA"
                
            err_msg = f"API返回码: {code}, 消息: {data.get('message')}"
            if attempt < max_retries - 1:
                time.sleep(1.0 * (attempt + 1))
                continue
            return pure_code, name, [], err_msg
        except Exception as e:
            if attempt < max_retries - 1:
                time.sleep(1.5 * (attempt + 1))
                continue
            return pure_code, name, [], str(e)

def main():
    global WORKING_FILTER
    start_time = time.time()
    
    print("\n" + "="*70)
    print("[*] 东方财富全市场全量基本面同步引擎 | 启动")
    print("="*70)
    
    print("📋 正在载入本地 Master 股票列表...")
    master_json_path = "stock_list_master.json"
    if not os.path.exists(master_json_path):
        print(f"❌ 严重错误：未在根目录下找到股票种子文件 {master_json_path}，请确认前置 TDX 扫描步骤。")
        return

    try:
        with open(master_json_path, 'r', encoding='utf-8') as f:
            master_list = json.load(f)
    except Exception as e:
        print(f"❌ 读取股票种子文件失败: {e}")
        return

    # 精准清洗过滤出有效的 A 股市场股票作为扫描基准
    valid_stocks = []
    for s in master_list:
        code = s['code'].replace('.', '').lower()
        if code.startswith('sh') and code[2:4] in ['60', '68']:
            valid_stocks.append(s)
        elif code.startswith('sz') and code[2:4] in ['00', '30']:
            valid_stocks.append(s)
        elif code.startswith('bj') and code[2:4] in ['43', '83', '87', '92']:
            valid_stocks.append(s)

    total_stocks = len(valid_stocks)
    print(f"[+] 种子数据解析完毕。全市场待扫描 A 股标的共计: {total_stocks} 只")
    
    # 随机抓取一个种子进行过滤器适配
    sample_item = random.choice(valid_stocks)
    sample_pure = sample_item['code'].replace('.', '').lower()[2:]
    if not explore_sql_syntax(sample_pure, sample_item['code']):
        print("❌ 错误：SQL 语句对齐探测失败！")
        return
        
    WORKING_FILTER = WORKING_FILTER.replace(sample_pure, "PROBE_CODE")
    print(f"[+] SQL 过滤器模板构建成功: {WORKING_FILTER}")
    print(f"🚀 启动 30 线程温和并发，开始全量抓取 5200+ A 股全维度财报...")
    
    all_flat_rows = []
    success_count = 0
    no_data_count = 0
    fail_count = 0
    
    with ThreadPoolExecutor(max_workers=30) as executor:
        futures = {executor.submit(fetch_f10_datacenter_with_retry, s): s for s in valid_stocks}
        for future in tqdm(as_completed(futures), total=total_stocks, desc="同步财报"):
            pure_code, name, data, err = future.result()
            if data:
                success_count += 1
                for row in data:
                    # 🛡️ 工业级对齐卫检：遍历 37 个原始字段，若缺失则置为 None，防止空键异常引发崩溃
                    cleaned_row = {}
                    for orig_col, target_col in FIELD_MAPPING.items():
                        val = row.get(orig_col)
                        
                        # 按字段类型执行极致精度清洗
                        if target_col in ["code", "name", "trade_market", "security_type_code", "security_type", 
                                          "assigndscrpt", "payyear", "publishname", "org_code", "trade_market_zjg", 
                                          "datatype", "secucode", "board_name", "ori_board_code", "board_code"]:
                            cleaned_row[target_col] = safe_str(val)
                        elif target_col in ["update_date", "report_date", "notice_date", "qdate", "eitime"]:
                            cleaned_row[target_col] = safe_date(val)
                        elif target_col in ["trade_market_code", "isnew", "datayear"]:
                            cleaned_row[target_col] = safe_int(val)
                        else:
                            cleaned_row[target_col] = safe_float(val)
                    
                    # 强制写入经过解析清洗的小写 code
                    cleaned_row["code"] = pure_code
                    all_flat_rows.append(cleaned_row)
            elif err == "NO_DATA":
                no_data_count += 1
            else:
                fail_count += 1
                print(f"❌ 抓取失败: {name}({pure_code}) - {err}")
                
    print("\n" + "="*70)
    print(f"📊 同步统计总结：")
    print(f"    - 目标股票总数: {total_stocks} 只")
    print(f"    - 同步成功标的: {success_count} 只")
    print(f"    - 无财务数据股: {no_data_count} 只 (通常为极新上市股票)")
    print(f"    - 网络及服务器失败: {fail_count} 只")
    print(f"    - 累计捕获财报行: {len(all_flat_rows):,} 行")
    print("="*70 + "\n")
    
    if all_flat_rows:
        print("💾 正在调用 Polars 引擎对 37 个字段执行极致压缩落盘...")
        df = pl.DataFrame(all_flat_rows)
        
        # 强制约束 Polars 的物理 Schema 列类型，保证对齐一致性
        type_constraints = {
            "code": pl.Utf8, "name": pl.Utf8, "trade_market_code": pl.Int32, "trade_market": pl.Utf8,
            "security_type_code": pl.Utf8, "security_type": pl.Utf8, "update_date": pl.Utf8, "report_date": pl.Utf8,
            "basic_eps": pl.Float64, "deduct_basic_eps": pl.Float64, "total_operate_income": pl.Float64,
            "parent_netprofit": pl.Float64, "weightavg_roe": pl.Float64, "ystz": pl.Float64, "sjltz": pl.Float64,
            "bps": pl.Float64, "mgjyxjje": pl.Float64, "xsmll": pl.Float64, "yshz": pl.Float64, "sjlhz": pl.Float64,
            "assigndscrpt": pl.Utf8, "payyear": pl.Utf8, "publishname": pl.Utf8, "zxgxl": pl.Float64,
            "notice_date": pl.Utf8, "org_code": pl.Utf8, "trade_market_zjg": pl.Utf8, "isnew": pl.Int32,
            "qdate": pl.Utf8, "datatype": pl.Utf8, "datayear": pl.Int32, "datemmdd": pl.Utf8, "eitime": pl.Utf8,
            "secucode": pl.Utf8, "board_name": pl.Utf8, "ori_board_code": pl.Utf8, "board_code": pl.Utf8
        }
        
        # 应用严格约束并按 code、时间轴正序升向重组
        df = df.with_columns([pl.col(col).cast(dtype) for col, dtype in type_constraints.items()]).sort(["code", "report_date"])
        
        os.makedirs("output", exist_ok=True)
        out_path = "output/all_stocks_f10_raw.parquet"
        df.write_parquet(out_path, compression="zstd")
        
        # -------------------------------------------------------------
        # 📈 量化数据多维度深度质量审计 (Data Quality Audit)
        # -------------------------------------------------------------
        print("📊 正在生成量化基本面质量审计与时延时滞分析报告...")
        pdf = df.to_pandas()
        
        min_rep, max_rep = pdf['report_date'].min(), pdf['report_date'].max()
        min_not, max_not = pdf['notice_date'].min(), pdf['notice_date'].max()
        
        null_profit = pdf['parent_netprofit'].isnull().sum()
        null_bps = pdf['bps'].isnull().sum()
        pct_null_profit = (null_profit / len(pdf)) * 100
        pct_null_bps = (null_bps / len(pdf)) * 100
        
        pdf['md'] = pdf['report_date'].str[-5:]
        q_dist = pdf['md'].value_counts()
        q_report_md = {
            "03-31": "一季报 (Q1)",
            "06-30": "半年报 (H1)",
            "09-30": "三季报 (Q3)",
            "12-31": "年报 (FY)"
        }
        
        lens = pdf.groupby('code').size()
        avg_len = lens.mean()
        max_len = lens.max()
        min_len = lens.min()
        
        top5 = pdf.groupby(['code', 'name']).size().reset_index(name='count').sort_values('count', ascending=False).head(5)
        
        pdf['dt_report'] = pd.to_datetime(pdf['report_date'], errors='coerce')
        pdf['dt_notice'] = pd.to_datetime(pdf['notice_date'], errors='coerce')
        delays = (pdf['dt_notice'] - pdf['dt_report']).dt.days
        valid_delays = delays[delays >= 0].dropna()
        avg_delay = valid_delays.mean() if not valid_delays.empty else 0
        max_delay = valid_delays.max() if not valid_delays.empty else 0
        
        # 构建精美 Markdown 总结，自动注入 GitHub Step Summary
        md = [
            "# 📊 A-Share F10 全量财务数据库深度审计报告",
            f"生成时间: `{time.strftime('%Y-%m-%d %H:%M:%S')}`\n",
            "## 1. 基础吞吐与存储层校验",
            f"- **扫描尝试 A 股总数:** `{total_stocks}` 只",
            f"- **财务数据获取成功率:** `{success_count/total_stocks*100:.2f}%` (`{success_count}` / `{total_stocks}`)",
            f"- **无财务记录新股:** `{no_data_count}` 只",
            f"- **拉取失败异常标的:** `{fail_count}` 只",
            f"- **数据库累计存储单元格数:** `{(len(pdf)*37):,}` 个",
            f"- **全维度 37 字段物理 Parquet 文件大小:** `{os.path.getsize(out_path)/(1024*1024):.2f} MB` (得益于 ZSTD 列式极致压缩)\n",
            
            "## 2. 覆盖跨度与时间线",
            f"- **报告期覆盖区间:** `{min_rep}` 至 `{max_rep}`",
            f"- **披露日覆盖区间:** `{min_not}` 至 `{max_not}`",
            f"- **平均单股历史季度数:** `{avg_len:.1f}` 个季度",
            f"- **单股最长季度跨度:** `{max_len}` 个季度",
            f"- **单股最短季度跨度:** `{min_len}` 个季度\n",
            
            "## 3. 核心字段缺失率分析",
            f"- **归母净利润缺失行数:** `{null_profit:,}` 行 (缺失率: `{pct_null_profit:.3f}%`)",
            f"- **每股净资产缺失行数:** `{null_bps:,}` 行 (缺失率: `{pct_null_bps:.3f}%`)\n",
            
            "## 4. 报告期季节分布",
            "| 季度财务报告 | 匹配报告期月日 | 累计行数 | 占比百分比 |",
            "| :--- | :--- | :--- | :--- |"
        ]
        for md_key, label in q_report_md.items():
            cnt = q_dist.get(md_key, 0)
            md.append(f"| {label} | `{md_key}` | `{cnt:,}` | `{cnt/len(pdf)*100:.2f}%` |")
        md.append("")
        
        md.extend([
            "## 5. 披露时滞深度计算 (Reporting Delay/Lag)",
            f"- **平均披露延迟:** `{avg_delay:.1f}` 天 (指财报从截止日期到市场获知的真实平均滞后天数)",
            f"- **最长披露延迟:** `{max_delay:,}` 天 (部分重组/ST/延期披露股)\n",
            
            "## 6. 历史季度跨度 Top 5 股票",
            "| 证券代码 | 股票名称 | 捕获财报季度数 |",
            "| :--- | :--- | :--- |"
        ])
        for _, row in top5.iterrows():
            md.append(f"| `{row['code']}` | `{row['name']}` | `{row['count']}` |")
            
        report_md_str = "\n".join(md)
        with open("output/f10_audit_summary.md", "w", encoding="utf-8") as f:
            f.write(report_md_str)
            
        print("\n" + "="*70)
        print("📊 [全量同步审计成功]")
        print(f"    - 数据已写出: {out_path}")
        print(f"    - 审计指标分析已保存: output/f10_audit_summary.md")
        print(f"    - 总耗时: {time.time() - start_time:.1f} 秒")
        print("="*70 + "\n")

if __name__ == "__main__":
    main()
