# FILE: scripts/fetch_f10_mainbus.py
import json
import os
import time
import requests
import polars as pl
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
    "Referer": "https://xueqiu.com/",
    "Connection": "keep-alive"
}

def ms_to_date_str(ms):
    if not ms: return ""
    try:
        return time.strftime('%Y-%m-%d', time.localtime(int(ms) / 1000))
    except:
        return ""

def fetch_single_stock_mainbus(session, s_item):
    """拉取单只股票雪球全量历史主营及占比"""
    raw_code = s_item['code'].replace('.', '').upper() 
    name = s_item.get('code_name', '')
    pure_code = raw_code[2:]
    
    url = f"https://stock.xueqiu.com/v5/stock/finance/cn/business.json?symbol={raw_code}&count=100"
    
    max_retries = 3
    for attempt in range(max_retries):
        try:
            res = session.get(url, timeout=10)
            if res.status_code == 200:
                data = res.json()
                if data.get("error_code") == 0 and data.get("data"):
                    return pure_code, name, data["data"].get("list", []), None
                else:
                    return pure_code, name, [], data.get("error_description", "API 内部未知错误")
            elif res.status_code == 400:
                if attempt < max_retries - 1:
                    time.sleep(1.0 * (attempt + 1))
                    continue
                return pure_code, name, [], "HTTP 400 Bad Request"
        except Exception as e:
            if attempt < max_retries - 1:
                time.sleep(1.0 * (attempt + 1))
                continue
            return pure_code, name, [], str(e)
            
    return pure_code, name, [], "Exceeded maximum retries"

def main():
    start_time = time.time()
    print("\n" + "="*70)
    print("[*] 雪球 A 股历史主营业务构成全量深度同步引擎 | 启动")
    print("="*70)
    
    session = requests.Session()
    session.headers.update(HEADERS)
    
    xq_token = os.getenv("XQ_A_TOKEN", "").strip()
    if xq_token:
        print("🔑 检测到 XQ_A_TOKEN 安全凭证，正在注入 Session...")
        session.cookies.set("xq_a_token", xq_token, domain=".xueqiu.com")
    else:
        print("❌ 严重错误: 未检测到环境变量中的 XQ_A_TOKEN 密钥，任务终止。")
        return

    master_json_path = "stock_list_master.json"
    if not os.path.exists(master_json_path):
        print(f"❌ 严重错误：未找到股票种子文件 {master_json_path}")
        return

    with open(master_json_path, 'r', encoding='utf-8') as f:
        master_list = json.load(f)

    # 清洗出有效的 A 股市场股票
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
    print(f"[+] 有效 A 股标的共计: {total_stocks} 只")
    print(f"🚀 启动 30 线程，并发拉取全历史主营构成明细...")
    
    all_flat_rows = []
    success_count = 0
    fail_count = 0
    
    with ThreadPoolExecutor(max_workers=30) as executor:
        futures = {executor.submit(fetch_single_stock_mainbus, session, s): s for s in valid_stocks}
        for future in tqdm(as_completed(futures), total=total_stocks, desc="同步主营"):
            pure_code, name, records, err = future.result()
            if records:
                success_count += 1
                for rec in records:
                    report_date = ms_to_date_str(rec.get("report_date"))
                    report_name = rec.get("report_name", "")
                    class_list = rec.get("class_list", [])
                    
                    for clazz in class_list:
                        class_standard = clazz.get("class_standard") # 1=行业, 2=产品, 3=地区
                        if class_standard not in [1, 2]:
                            continue
                            
                        bus_list = clazz.get("business_list", [])
                        for bus in bus_list:
                            ratio = float(bus.get("income_ratio") or 0.0)
                            if ratio < 1.0: 
                                ratio = ratio * 100.0
                                
                            margin = float(bus.get("gross_profit_rate") or 0.0)
                            if margin < 1.0 and margin > 0:
                                margin = margin * 100.0

                            all_flat_rows.append({
                                "code": pure_code,
                                "name": name,
                                "report_date": report_date,
                                "report_name": report_name,
                                "item_type": int(class_standard),
                                "item_name": str(bus.get("project_announced_name", "")).strip(),
                                "income": float(bus.get("prime_operating_income") or 0.0),
                                "income_ratio": ratio,
                                "gross_margin": margin
                            })
            else:
                fail_count += 1

    print("\n" + "="*70)
    print(f"📊 同步统计总结：")
    print(f"    - 同步成功标的: {success_count} / {total_stocks}")
    print(f"    - 累计捕获主营明细: {len(all_flat_rows):,} 行")
    print("="*70 + "\n")

    if all_flat_rows:
        print("💾 正在调用 Polars 引擎强制类型对齐输出 Parquet...")
        schema = {
            "code": pl.Utf8, "name": pl.Utf8, "report_date": pl.Utf8, "report_name": pl.Utf8,
            "item_type": pl.Int32, "item_name": pl.Utf8, "income": pl.Float64,
            "income_ratio": pl.Float64, "gross_margin": pl.Float64
        }
        df = pl.DataFrame(all_flat_rows, schema=schema).sort(["code", "report_date"])
        os.makedirs("output", exist_ok=True)
        out_path = "output/all_stocks_mainbus_raw.parquet"
        df.write_parquet(out_path, compression="zstd")
        print(f"✅ 文件已落盘: {out_path} ({os.path.getsize(out_path)/(1024*1024):.2f} MB)")

        # -------------------------------------------------------------
        # 📈 主营业务及产品占比深度数据质量审计 (Polars Engine)
        # -------------------------------------------------------------
        print("📊 正在生成主营业务构成与产业链标签质量审计报告...")
        
        total_rows = len(df)
        unique_stocks = df["code"].n_unique()
        min_rep, max_rep = df["report_date"].min(), df["report_date"].max()
        
        # 统计行业分类 (Type 1) 与产品分类 (Type 2) 行数
        type_counts = df.group_by("item_type").agg(pl.len().alias("count"))
        
        type_1_cnt = 0
        type_2_cnt = 0
        for r in type_counts.iter_rows():
            if r[0] == 1: type_1_cnt = r[1]
            elif r[0] == 2: type_2_cnt = r[1]
            
        # 过滤出产品级明细（item_type == 2），统计常见产品名
        df_product = df.filter(pl.col("item_type") == 2)
        top5_products = (
            df_product.group_by("item_name")
            .agg(pl.len().alias("count"))
            .sort("count", descending=True)
            .head(5)
        )
        
        # 计算单只股票拥有的平均历史报告期数
        reports_per_stock = df.group_by("code").agg(pl.col("report_date").n_unique().alias("rep_cnt"))
        avg_reps = reports_per_stock["rep_cnt"].mean()
        max_reps = reports_per_stock["rep_cnt"].max()
        min_reps = reports_per_stock["rep_cnt"].min()
        
        # 寻找历史跨度最长的 Top 5 股票（最深可追溯至 2000 年左右）
        top5_history = reports_per_stock.sort("rep_cnt", descending=True).head(5)
        
        # 行业及板块中文名称对齐
        stock_name_map = {}
        for row in df.select(["code", "name"]).unique().iter_rows():
            stock_name_map[row[0]] = row[1]

        # 编写 Markdown 审计总结
        md = [
            "# ❄️ A-Share 主营业务构成及产品占比深度审计报告",
            f"生成时间: `{time.strftime('%Y-%m-%d %H:%M:%S')}`\n",
            "## 1. 基础吞吐与存储层校验",
            f"- **扫描尝试 A 股总数:** `{total_stocks}` 只",
            f"- **主营数据获取成功率:** `{success_count/total_stocks*100:.2f}%` (`{success_count}` / `{total_stocks}`)",
            f"- **数据库累计存储构成行数:** `{total_rows:,}` 行",
            f"- **纯净产品级 (Type 2) 物理行:** `{type_2_cnt:,}` 行 (占比: `{type_2_cnt/total_rows*100:.2f}%`)",
            f"- **行业归纳级 (Type 1) 物理行:** `{type_1_cnt:,}` 行 (占比: `{type_1_cnt/total_rows*100:.2f}%`)",
            f"- **主营明细 Parquet 存储大小:** `{os.path.getsize(out_path)/(1024*1024):.2f} MB` (采用 ZSTD 极致压缩)\n",
            
            "## 2. 覆盖跨度与时间线",
            f"- **历史报告期最远追溯:** `{min_rep}`",
            f"- **历史报告期最近更新:** `{max_rep}`",
            f"- **平均单股历史报告期数:** `{avg_reps:.1f}` 个季度 (主要为半年报/年报切片)",
            f"- **单股最长季度跨度:** `{max_reps}` 个报告期",
            f"- **单股最短季度跨度:** `{min_reps}` 个报告期\n",
            
            "## 3. 最常见 A 股主营产品明细 Top 5",
            "| 排名 | 产品/业务明细名称 | 累计出现次数 | 占比 (占所有产品级行数) |",
            "| :--- | :--- | :--- | :--- |"
        ]
        
        for idx, row in enumerate(top5_products.iter_rows()):
            p_name, cnt = row[0], row[1]
            md.append(f"| {idx+1} | `{p_name}` | `{cnt:,}` | `{cnt/type_2_cnt*100:.2f}%` |")
            
        md.extend([
            "\n## 4. 历史主营数据期数最深 Top 5 股票",
            "| 证券代码 | 股票名称 | 捕获历史主营报告期数 |",
            "| :--- | :--- | :--- |"
        ])
        for row in top5_history.iter_rows():
            code, count = row[0], row[1]
            name = stock_name_map.get(code, "未知")
            md.append(f"| `{code}` | `{name}` | `{count}` |")
            
        report_md_str = "\n".join(md)
        mainbus_summary_path = "output/mainbus_audit_summary.md"
        with open(mainbus_summary_path, "w", encoding="utf-8") as f:
            f.write(report_md_str)
            
        print("\n" + "="*70)
        print("📊 [主营业务数据深度审计成功]")
        print(f"    - 审计分析已保存: {mainbus_summary_path}")
        print(f"    - 总耗时: {time.time() - start_time:.1f} 秒")
        print("="*70 + "\n")

if __name__ == "__main__":
    main()
