import os
import sys
import datetime
import requests
import zipfile
import concurrent.futures
import time
import re

# 通达信官方专业财务数据目录
BASE_URL = "http://www.tdx.com.cn/products/data/data/dbf/"
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
    🚀 核心自愈功能：实时请求通达信服务器目录，解析出服务器上物理存在的所有 gpcw 压缩包真实文件名
    """
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
    }
    try:
        response = requests.get(BASE_URL, headers=headers, timeout=20)
        if response.status_code != 200:
            print(f"[!] 无法访问通达信数据源目录，状态码: {response.status_code}")
            return []
        
        # 提取 HTML 页面中所有指向 gpcw*.zip 的超链接真实名称（忽略大小写）
        html_text = response.text
        links = re.findall(r'href=["\'](gpcw[^"\']+\.zip)["\']', html_text, re.IGNORECASE)
        
        # 去重并排序
        discovered = sorted(list(set(links)))
        return discovered
    except Exception as e:
        print(f"[!] 自动探测通达信服务器目录异常: {e}")
        return []

def filter_files_by_year(file_list):
    """
    根据设定的起始年份筛选出需要下载的财务包
    支持 gpcw20240331.zip, gpcw2403.zip 等多种命名变体的智能识别
    """
    start_year_env = os.getenv("START_YEAR", "2005")
    try:
        start_year = int(start_year_env)
    except ValueError:
        start_year = 2005

    filtered = []
    for fname in file_list:
        # 寻找文件名中的数字序列，例如从 gpcw20240331.zip 中提取 20240331
        nums = re.findall(r'\d+', fname)
        if not nums:
            continue
        
        date_num_str = nums[0]
        # 适配 4 位完整年份 (如 20240331) 
        if len(date_num_str) >= 4:
            file_year = int(date_num_str[:4])
        # 适配 2 位简写年份 (如 2403)
        elif len(date_num_str) == 2 or len(date_num_str) == 3:
            file_year = 2000 + int(date_num_str[:2])
        else:
            file_year = start_year  # 无法识别时保守放过

        if file_year >= start_year:
            filtered.append(fname)
            
    return filtered

def download_single_gpcw(filename):
    """
    下载单个特定的 gpcw 压缩包
    """
    url = f"{BASE_URL}{filename}"
    local_path = os.path.join(OUTPUT_DIR, filename)

    # 1. 增量机制
    if os.path.exists(local_path):
        if verify_zip(local_path):
            return filename, "SKIP", None
        else:
            try:
                os.remove(local_path)
            except Exception:
                pass

    # 2. 带重试下载
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
    }
    
    max_retries = 3
    for attempt in range(max_retries):
        try:
            response = requests.get(url, headers=headers, timeout=30)
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
    print(f"[*] GitHub Actions 财务包探测下载引擎 | 启动时间: {start_time}")
    print(f"[*] 目标年份起点: {os.getenv('START_YEAR', '2005')}")
    print(f"{'='*70}\n")

    # 1. 第一步：向通达信官方服务器索取真实存在的物理清单
    print("[*] 正在向通达信官方服务器探测实际发布的文件清单...")
    remote_files = discover_remote_gpcw_files()
    if not remote_files:
        print("❌ 错误: 探测到通达信服务器上的财务包文件列表为空，或网络连接被阻断。")
        sys.exit(1)
        
    print(f"[+] 探测成功！通达信服务器目前物理存有 {len(remote_files)} 个财务压缩包。")

    # 2. 第二步：根据过滤条件精确定位我们需要的文件
    target_files = filter_files_by_year(remote_files)
    total_tasks = len(target_files)
    print(f"[+] 经年份过滤后，锁定目标下载文件数: {total_tasks} 个\n")

    if total_tasks == 0:
        print("⚠️ 警告: 过滤后没有符合年份要求的下载目标，任务提前结束。")
        sys.exit(0)

    success_count = 0
    skip_count = 0
    fail_count = []
    completed_tasks = 0

    # 3. 第三步：并发极速拉取
    MAX_WORKERS = 12
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
            
            # 离散非阻塞式输出
            if completed_tasks % max(1, total_tasks // 10) == 0 or completed_tasks == total_tasks:
                percentage = (completed_tasks / total_tasks) * 100
                print(f"[#] 任务进度: {completed_tasks}/{total_tasks} ({percentage:.1f}%) | 本地跳过: {skip_count} | 成功拉取: {success_count}")

    # 最终汇总
    print(f"\n{'='*70}")
    print("[+] Actions 同步任务运行结束。数据汇总如下：")
    print(f"    - 本地已存（跳过）: {skip_count} 个")
    print(f"    - 成功下载到本地  : {success_count} 个")
    print(f"    - 预期下载失败    : {len(fail_count)} 个")
    
    if fail_count:
        print("\n[!] 失败明细：")
        for f, err in fail_count[:10]:
            print(f"    - 文件 {f} | 原因: {err}")
            
    # 计算落盘包体积
    total_size = sum(
        os.path.getsize(os.path.join(OUTPUT_DIR, f)) 
        for f in os.listdir(OUTPUT_DIR) if f.endswith(".zip")
    )
    print(f"\n[*] 财务包存储目录: {os.path.abspath(OUTPUT_DIR)}")
    print(f"[*] 磁盘占用总量: {total_size / (1024*1024):.2f} MB")
    print(f"[*] 本次总耗时: {datetime.datetime.now() - start_time}")
    print(f"{'='*70}\n")

    if len(fail_count) > 0:
        print("[!] 检测到部分文件未成功同步，任务报错中断。")
        sys.exit(1)

if __name__ == "__main__":
    main()
