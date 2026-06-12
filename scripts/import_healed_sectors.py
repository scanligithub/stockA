import os
import zipfile
import glob
import json
import polars as pl
from datetime import datetime
import shutil

def main():
    zip_path = "Full_Sector_Klines.zip"
    extract_dir = "extracted_sectors"
    output_dir = "temp_parts"
    os.makedirs(output_dir, exist_ok=True)

    if not os.path.exists(zip_path):
        raise FileNotFoundError(f"❌ 错误: 未在根目录下找到输入压缩包: {zip_path}，请确认工作流下载步骤。")

    print("📂 正在解压板块自愈历史数据集...")
    with zipfile.ZipFile(zip_path, 'r') as zip_ref:
        zip_ref.extractall(extract_dir)

    # 1. 构建官方分类映射字典与板块中文名映射字典
    print("🏷️ 正在构建板块官方维度与中文名映射表...")
    type_map = {}
    sector_name_map = {}
    
    meta_configs = [
        ("regions.json", "地域板块"), 
        ("industries.json", "行业板块"), 
        ("concepts.json", "概念板块")
    ]
    for cat, alias in meta_configs:
        cat_path = os.path.join(extract_dir, "metadata", cat)
        if os.path.exists(cat_path):
            with open(cat_path, 'r', encoding='utf-8') as f:
                try:
                    cat_data = json.load(f)
                    for item in cat_data:
                        code = item["sid"].split(".")[-1]  # "90.BK1043" -> "BK1043"
                        type_map[code] = alias
                        sector_name_map[code] = item.get("name", "")
                except Exception as e:
                    print(f"⚠️ 解析元数据 {cat} 失败: {e}")

    # 2. 遍历解压出来的所有 K 线 JSON 文件
    print("⚡ 正在使用 Polars 极速解析全量板块历史 K 线...")
    kline_rows = []
    json_files = glob.glob(os.path.join(extract_dir, "90.BK*.json"))

    for fpath in json_files:
        try:
            with open(fpath, 'r', encoding='utf-8') as f:
                data = json.load(f)
            if not data or 'klines' not in data or not data['klines']:
                continue
            code = data['code']
            name = data['name']
            sector_name_map[code] = name  # 实时更新更精确的板块名称
            sector_type = type_map.get(code, "概念板块")

            for kl in data['klines']:
                parts = kl.split(',')
                if len(parts) >= 8:
                    kline_rows.append({
                        "date": parts[0],
                        "open": float(parts[1]),
                        "close": float(parts[2]),
                        "high": float(parts[3]),
                        "low": float(parts[4]),
                        "volume": float(parts[5]),
                        "amount": float(parts[6]),
                        "amplitude": parts[7],  # 💥 核心修改：保持原始 String 类型，不转 float，完美兼容历史文件
                        "code": code,
                        "name": name,
                        "type": sector_type
                    })
        except Exception as e:
            print(f"⚠️ 读取 K 线文件 {fpath} 失败: {e}")

    if not kline_rows:
        raise ValueError("❌ 转换失败: 未能解析出任何有效的 K 线数据行！")

    # 转换为 Polars DataFrame，严格对齐 stockA 历史 Parquet Schema
    df_k = pl.DataFrame(kline_rows)
    df_k = df_k.with_columns([
        pl.col("date").cast(pl.Utf8),
        pl.col("open").cast(pl.Float32),
        pl.col("close").cast(pl.Float32),
        pl.col("high").cast(pl.Float32),
        pl.col("low").cast(pl.Float32),
        pl.col("volume").cast(pl.Float64),
        pl.col("amount").cast(pl.Float64),
        pl.col("amplitude").cast(pl.Utf8),  # 💥 核心修改：强制映射为 Utf8(String)，避免 VStack 类型冲突崩溃
        pl.col("code").cast(pl.Utf8),
        pl.col("name").cast(pl.Utf8),
        pl.col("type").cast(pl.Utf8)
    ])
    
    # 全局去重 & 升序排序
    df_k = df_k.unique(subset=["date", "code"], keep="last").sort(["code", "date"])
    
    # 写入 Parquet (采用 zstd 高性能无损压缩)
    kline_out_path = os.path.join(output_dir, "sector_kline_full.parquet")
    df_k.write_parquet(kline_out_path, compression="zstd")
    print(f"✅ [K线转换完成] 共有 {len(df_k):,} 行数据成功落盘 (Float32精度与历史String对齐)。")

    # 3. 转换成分股关系映射表
    comp_path = os.path.join(extract_dir, "metadata", "components.json")
    if os.path.exists(comp_path):
        print("🧱 正在解析转换板块成分股最新对应关系...")
        try:
            with open(comp_path, 'r', encoding='utf-8') as f:
                comp_data = json.load(f)
            transformed_comp = []
            for item in comp_data:
                sector_code = item["sector_id"].split(".")[-1]  # "90.BK1043" -> "BK1043"
                stock_id = item["stock_id"]
                stock_code = stock_id[-6:]  # "SH600000" -> "600000"
                transformed_comp.append({
                    "sector_code": sector_code,
                    "stock_code": stock_code,
                    "sector_name": sector_name_map.get(sector_code, "")
                })

            if transformed_comp:
                df_c = pl.DataFrame(transformed_comp)
                today_str = datetime.now().strftime('%Y-%m-%d')
                df_c = df_c.with_columns([
                    pl.lit(today_str).alias("date"),
                    pl.col("sector_code").cast(pl.Utf8),
                    pl.col("stock_code").cast(pl.Utf8),
                    pl.col("sector_name").cast(pl.Utf8)
                ])
                # 基于板块与个股关系去重
                df_c = df_c.unique(subset=["sector_code", "stock_code"], keep="last").sort(["sector_code", "stock_code"])
                
                const_out_path = os.path.join(output_dir, "sector_constituents_latest.parquet")
                df_c.write_parquet(const_out_path, compression="zstd")
                print(f"✅ [成分股转换完成] 共有 {len(df_c):,} 条关系映射成功落盘。")
        except Exception as e:
            print(f"⚠️ 转换成分股数据失败: {e}")

    # 清理临时解压目录
    try:
        shutil.rmtree(extract_dir)
        print("🧹 临时解压目录清理完毕。")
    except Exception as e:
        print(f"⚠️ 临时目录清理失败: {e}")

if __name__ == "__main__":
    main()
