import os
import sys
import datetime
import requests
import zipfile
import concurrent.futures
import time
import re

# 🌟 修正为通达信官方专用的财务下载节点与文件索引
BASE_URL = "http://down.tdx.com.cn:8001/tdxfin/"
INDEX_URL = "http://down.tdx.com.cn:8001/tdxfin/gpcw.txt"
OUTPUT_DIR = "gpcw_zips"

os.makedirs(OUTPUT_DIR, exist_ok=True)

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
            return zip_ref.testzip() is None
    except Exception:
        return False

def discover_remote_gpcw_files():
    """
    🌟 核心修改：通过下载通达信官方标准的 gpcw.txt 文件索引，获取最纯净、最精准的物理文件清单
    """
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
    }
    
    # 财务清单是核心入口，重试 3 次确保拿到
    for attempt in range(3):
        try:
            print(f"[*] 正在尝试下载官方文件索引 gpcw.txt (第 {attempt + 1} 次尝试)...")
            response = requests.get(INDEX_URL, headers=headers, timeout=20)
            if response.status_code == 200:
                files = []
                for line in response.text.splitlines():
                    line = line.strip()
                    if not line:
                        continue
                    # 标准格式：gpcw20240331.zip,f4af4a6bc6d013...,1234567
                    parts = line.split(",")
                    name = parts[0].strip()
                    if name.lower().endswith(".zip"):
                        files.append(name)
                return sorted(files)
        except Exception as e:
            print(f"[!] 下载官方索引异常: {e}")
            time.sleep(2)
            
    return []

def filter_files_by_year(file_list):
    """
    根据设定的起始年份筛选出需要下载的财务包
    支持 gpcw20240331.zip, gpcw2403.zip 等变体的智能年份提取
    """
    start_year_env = os.getenv("START_YEAR", "2005")
    try:
        start_year = int(start_year_env)
    except ValueError:
        start_year = 2005

    filtered = []
    for fname in file_list:
        nums = re.findall(r'\d+', fname)
        if not nums:
            continue
        
        date_num_str = nums[0]
        # 适配 4 位年份 (如 20240331)
        if len(date_num_str) >= 4:
            file_year = int(date_num_str[:4])
        # 适配 2 位简写年份 (如 2403)
        elif len(date_num_str) in [2, 3]:
            file_year = 2000 + int(date_num_str[:2])
        else:
            file_year = start_year

        if file_year >= start_year:
            filtered.append(fname)
            
    return filtered

def download_single_gpcw(filename):
    """
    下载单个特定的 gpcw 压缩包
    """
    url = f"{BASE_URL}{filename}"
    local_path = os.path.join(OUTPUT_DIR, filename)

    # 1. 增量跳过逻辑（物理存在且 Zip 校验通过则直接跳过）
    if os.path.exists(local_path):
        if verify_zip(local_path):
            return filename, "SKIP", None
        else:
            try:
                os.remove(local_path)
            except Exception:
                pass

    # 2. 温和重试下载
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
    }
    
    max_retries = 3
    for attempt in range(max_retries):
        try:
            response = requests.get(url, headers=headers, timeout=25)
            if response.status_code != 200:
                if attempt < max_retries - 1:
                    time.sleep(2)
                    continue
                return filename, "FAIL", f"HTTP {response.status_code}"

            with open(local_path, "wb") as f:
                f.write(response.content)

            if verify_zip(local_path):
                return filename, "SUCCESS", None
            else:
                try:
                    os.remove(local_path)
                except Exception:
                    pass
                if attempt < max_retries - 1:
                    time.sleep(2)
                    continue
                return filename, "CORRUPTED", "ZIP完整性校验失败"

        except Exception as e:
            if attempt < max_retries - 1:
                time.sleep(3)
                continue
            return filename, "ERROR", str(e)

def main():
    start_time = datetime.datetime.now()
    
    print(f"\n{'='*70}")
    print(f"[*] GitHub Actions 财务包增量同步引擎 | 启动时间: {start_time}")
    print(f"[*] 目标年份起点: {os.getenv('START_YEAR', '2005')}")
    print(f"{'='*70}\n")

    # 1. 探测官方索引
    remote_files = discover_remote_gpcw_files()
    if not remote_files:
        print("❌ 严重错误: 无法获取通达信官方 gpcw.txt 索引，任务被迫终止。")
        sys.exit(1)
        
    print(f"[+] 索引同步成功！通达信官方目前共记录有 {len(remote_files)} 个历史财务包。")

    # 2. 筛选目标
    target_files = filter_files_by_year(remote_files)
    total_tasks = len(target_files)
    print(f"[+] 经年份过滤后，锁定本期目标下载文件数: {total_tasks} 个\n")

    if total_tasks == 0:
        print("⚠️ 提示: 没有符合年份要求的下载目标。")
        sys.exit(0)

    success_count = 0
    skip_count = 0
    fail_count = []
    completed_tasks = 0

    # 3. 温和并发同步（降低并发至 6 线程，防止 GitHub 节点大流量触发防刷策略）
    MAX_WORKERS = 6
    with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {executor.submit(download_single_gpcw, f): f for f in target_files}
        
        for future in concurrent.futures.as_completed(futures):
            filename = futures[future]
            completed_tasks += 1
            
            try:
                d_file, status, err_msg = future.result()
                if status == "SUCCESS":
                    success_count += 1
                elif status == "SKIP":
                    skip_count += 1
                else:
                    fail_count.append((d_file, err_msg))
            except Exception as e:
                fail_count.append((filename, str(e)))
            
            # Actions 友好型进度日志（10% 频率）
            if completed_tasks % max(1, total_tasks // 10) == 0 or completed_tasks == total_tasks:
                percentage = (completed_tasks / total_tasks) * 100
                print(f"[#] 同步进度: {completed_tasks}/{total_tasks} ({percentage:.1f}%) | 增量跳过: {skip_count} | 本期成功: {success_count}")

    # 4. 统计总结
    print(f"\n{'='*70}")
    print("[+] Actions 同步任务运行结束。数据汇总如下：")
    print(f"    - 本地已存（跳过）: {skip_count} 个")
    print(f"    - 本期成功拉取    : {success_count} 个")
    print(f"    - 临时下载失败    : {len(fail_count)} 个")
    
    if fail_count:
        print(f"\n[⚠️] 共有 {len(fail_count)} 个财务包在本次 Actions 运行中偶发性下载失败 (前 10 个)：")
        for f, err in fail_count[:10]:
            print(f"        - {f} | 原因: {err}")
            
    # 计算物理占用
    total_size = sum(
        os.path.getsize(os.path.join(OUTPUT_DIR, f)) 
        for f in os.listdir(OUTPUT_DIR) if f.endswith(".zip")
    )
    print(f"\n[*] 财务包存储目录: {os.path.abspath(OUTPUT_DIR)}")
    print(f"[*] 磁盘占用总量: {total_size / (1024*1024):.2f} MB")
    print(f"[*] 本次总耗时: {datetime.datetime.now() - start_time}")
    print(f"{'='*70}\n")

    # 🌟 防御性自愈退出的黄金准则：
    # 我们拒绝因为网络波动导致个别历史季度数据下载不成功就引发 Actions 构建红牌报错中断。
    # 只要整体“本地已存 + 本期成功”占“目标任务数”的比例超过 80% (或成功数 > 0)，就允许构建绿牌通过，并在下一次 Actions 运行时自动重试增量补齐！
    overall_success_rate = ((success_count + skip_count) / total_tasks) * 100 if total_tasks > 0 else 0
    print(f"[*] 综合同步成功率: {overall_success_rate:.2f}%")
    
    if overall_success_rate >= 80.0:
        print("[+] 综合同步成功率处于健康区间，工作流绿牌安全通过。未同步成功的包将在后续运行中自动补齐。")
        sys.exit(0)
    else:
        print("❌ 错误: 同步成功率低于 80% 警戒线，判定网络或磁盘出现重大系统性故障，工作流报错退出。")
        sys.exit(1)

if __name__ == "__main__":
    main()
