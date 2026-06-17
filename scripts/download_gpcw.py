import os
import sys
import datetime
import requests
import zipfile
import concurrent.futures
import time

# 通达信官方专业财务数据包根地址
BASE_URL = "http://www.tdx.com.cn/products/data/data/dbf/"
OUTPUT_DIR = "gpcw_zips"

# 确保输出目录存在
os.makedirs(OUTPUT_DIR, exist_ok=True)

def generate_quarter_dates():
    """
    生成从 START_YEAR 到当前年份的所有季度报告期
    支持通过环境变量 START_YEAR 动态调整，默认 2005 年（兼容 stockA 历史起点）
    """
    start_year_env = os.getenv("START_YEAR", "2005")
    try:
        start_year = int(start_year_env)
    except ValueError:
        start_year = 2005
        
    current_year = datetime.datetime.now().year
    quarters = ["0331", "0630", "0930", "1231"]
    dates = []
    for year in range(start_year, current_year + 1):
        for q in quarters:
            dates.append(f"{year}{q}")
    return dates

def verify_zip(file_path):
    """
    验证下载的 ZIP 文件是否完整且无损坏
    """
    if not os.path.exists(file_path):
        return False
    if os.path.getsize(file_path) == 0:
        return False
    try:
        with zipfile.ZipFile(file_path, "r") as zip_ref:
            # testzip 会检测损坏的块，返回 None 代表文件完好无损
            return zip_ref.testzip() is None
    except Exception:
        return False

def download_single_gpcw(date_str):
    """
    下载单个季度的 gpcw 压缩包，内置强力重试、增量跳过和自愈逻辑
    """
    filename = f"gpcw{date_str}.zip"
    url = f"{BASE_URL}{filename}"
    local_path = os.path.join(OUTPUT_DIR, filename)

    # 1. 增量机制：若本地已存在且校验通过，则跳过
    if os.path.exists(local_path):
        if verify_zip(local_path):
            return date_str, "SKIP", None
        else:
            try:
                os.remove(local_path)
            except Exception:
                pass

    # 2. 执行带重试的下载逻辑（应对 GitHub Actions 虚拟机偶尔的抖动）
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
    }
    
    max_retries = 3
    for attempt in range(max_retries):
        try:
            response = requests.get(url, headers=headers, timeout=30)
            
            # 404 代表通达信尚未发布该季度的财报数据（属于正常现象）
            if response.status_code == 404:
                return date_str, "NOT_FOUND", None
            
            if response.status_code != 200:
                if attempt < max_retries - 1:
                    time.sleep(2)
                    continue
                return date_str, "FAIL", f"HTTP {response.status_code}"

            with open(local_path, "wb") as f:
                f.write(response.content)

            # 校验完整性
            if verify_zip(local_path):
                return date_str, "SUCCESS", None
            else:
                try:
                    os.remove(local_path)
                except Exception:
                    pass
                if attempt < max_retries - 1:
                    time.sleep(2)
                    continue
                return date_str, "CORRUPTED", "ZIP完整性校验失败"

        except Exception as e:
            if attempt < max_retries - 1:
                time.sleep(3)
                continue
            return date_str, "ERROR", str(e)

def main():
    start_time = datetime.datetime.now()
    dates = generate_quarter_dates()
    total_tasks = len(dates)
    
    print(f"\n{'='*70}")
    print(f"[*] GitHub Actions 财务包下载引擎 | 启动时间: {start_time}")
    print(f"[*] 目标年份起点: {os.getenv('START_YEAR', '2005')} | 需探测季度数: {total_tasks}")
    print(f"{'='*70}\n")

    success_count = 0
    skip_count = 0
    not_found_count = 0
    fail_count = []
    
    completed_tasks = 0

    # 💥 针对 Actions 虚拟机优化的并发设置（12 线程能最大化带宽并兼顾 CPU）
    MAX_WORKERS = 12

    with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {executor.submit(download_single_gpcw, d): d for d in dates}
        
        for future in concurrent.futures.as_completed(futures):
            date_str = futures[future]
            completed_tasks += 1
            
            try:
                d_date, status, err_msg = future.result()
                if status == "SUCCESS":
                    success_count += 1
                elif status == "SKIP":
                    skip_count += 1
                elif status == "NOT_FOUND":
                    not_found_count += 1
                else:
                    fail_count.append((d_date, err_msg))
            except Exception as e:
                fail_count.append((date_str, str(e)))
            
            # 🚀 针对 Actions 优化的非阻塞日志输出机制（每完成 10% 打印一行，防止日志刷屏）
            if completed_tasks % max(1, total_tasks // 10) == 0 or completed_tasks == total_tasks:
                percentage = (completed_tasks / total_tasks) * 100
                print(f"[#] 任务进度: {completed_tasks}/{total_tasks} ({percentage:.1f}%) | 当前新增: {success_count} | 当前跳过: {skip_count}")

    # 打印最终统计总结
    print(f"\n{'='*70}")
    print("[+] Actions 同步任务运行结束。数据汇总如下：")
    print(f"    - 本地已存（跳过）: {skip_count} 个")
    print(f"    - 新增成功下载  : {success_count} 个")
    print(f"    - 官方未发布(404): {not_found_count} 个")
    print(f"    - 下载失败(需重试): {len(fail_count)} 个")
    
    if fail_count:
        print("\n[!] 失败明细 (前 10 个)：")
        for d, err in fail_count[:10]:
            print(f"    - 报告期 {d} | 原因: {err}")
            
    # 计算落盘包体积
    total_size = sum(
        os.path.getsize(os.path.join(OUTPUT_DIR, f)) 
        for f in os.listdir(OUTPUT_DIR) if f.endswith(".zip")
    )
    print(f"\n[*] 财务包存储目录: {os.path.abspath(OUTPUT_DIR)}")
    print(f"[*] 磁盘占用总量: {total_size / (1024*1024):.2f} MB")
    print(f"[*] 本次总耗时: {datetime.datetime.now() - start_time}")
    print(f"{'='*70}\n")

    # 如果有重要失败（非 404 导致），以非 0 状态码退出以让 Actions 报错提醒
    # 若仅是未发布（NOT_FOUND），属于正常现象，允许安全通过
    if len(fail_count) > 0:
        print("[!] 检测到未预期的数据下载失败，引发 Actions 报错中断。")
        sys.exit(1)

if __name__ == "__main__":
    main()
