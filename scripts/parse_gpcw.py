import os
import glob
import zipfile
import tempfile
import re
from datetime import datetime
import pandas as pd
from pytdx.reader import HistoryFinancialReader

def get_safe_publish_date(report_date):
    """
    根据实际报告期，推算最保守的发布截点，绝对规避回测时的未来函数 (Look-ahead Bias)
    """
    if report_date.month == 3: return report_date.replace(month=4, day=30)
    elif report_date.month == 6: return report_date.replace(month=8, day=31)
    elif report_date.month == 9: return report_date.replace(month=10, day=31)
    elif report_date.month == 12: return report_date.replace(year=report_date.year + 1, month=4, day=30)
    return report_date

def main():
    print("🚀 [Parse GPCW] 启动本地 TDX 历史财务二进制包极速解析引擎...")
    reader = HistoryFinancialReader()
    all_dfs = []
    
    zip_files = glob.glob("gpcw_zips/*.zip")
    if not zip_files:
        print("⚠️ 未找到任何 GPCW zip 文件，将生成空占位符文件。")
        pd.DataFrame(columns=['code','publish_date','net_profit_ttm','net_assets']).to_parquet("finance_master.parquet")
        return

    for zf in zip_files:
        # 提取报告期，如 gpcw20231231.zip
        match = re.search(r'\d{8}', zf)
        if not match: continue
        report_date = datetime.strptime(match.group(), "%Y%m%d")
        publish_date = get_safe_publish_date(report_date)
        
        # 内存解压提取 dat
        with zipfile.ZipFile(zf, 'r') as z:
            dat_names = [n for n in z.namelist() if n.endswith('.dat')]
            if not dat_names: continue
            with tempfile.NamedTemporaryFile(delete=False) as tmp:
                tmp.write(z.read(dat_names[0]))
                tmp_path = tmp.name
        
        try:
            df = reader.get_df(tmp_path)
            if df is not None and not df.empty:
                # 转换代码格式 600519 -> sh.600519
                def map_code(c):
                    c = str(c).zfill(6)
                    if c.startswith('6'): return f"sh.{c}"
                    elif c.startswith(('4','8')): return f"bj.{c}"
                    else: return f"sz.{c}"
                    
                df['code'] = df['code'].apply(map_code)
                df['report_date'] = report_date
                df['publish_date'] = publish_date
                
                # TDX 标准字段: jlr (净利润, 万元), gdqy (股东权益, 万元)
                keep_cols = ['code', 'report_date', 'publish_date']
                if 'jlr' in df.columns: keep_cols.append('jlr')
                if 'gdqy' in df.columns: keep_cols.append('gdqy')
                
                all_dfs.append(df[keep_cols])
        except Exception as e:
            print(f"⚠️ 解析异常 {zf}: {e}")
        finally:
            os.remove(tmp_path)
            
    if not all_dfs:
        pd.DataFrame(columns=['code','publish_date','net_profit_ttm','net_assets']).to_parquet("finance_master.parquet")
        return
        
    master = pd.concat(all_dfs, ignore_index=True)
    if 'jlr' not in master.columns: master['jlr'] = 0.0
    if 'gdqy' not in master.columns: master['gdqy'] = 0.0
    
    master['jlr'] = pd.to_numeric(master['jlr'], errors='coerce').fillna(0.0)
    master['gdqy'] = pd.to_numeric(master['gdqy'], errors='coerce').fillna(0.0)
    master = master.sort_values(['code', 'report_date'])
    
    print("🧠 [Parse GPCW] 正在进行 TTM 跨期滚动推导计算...")
    master.set_index(['code', 'report_date'], inplace=True)
    
    ttm_list = []
    for (code, rdate), row in master.iterrows():
        current_ytd = row['jlr']
        # 年报的 TTM 就是当前值
        if rdate.month == 12:
            ttm_list.append(current_ytd)
            continue
            
        last_year_q4_date = rdate.replace(year=rdate.year - 1, month=12, day=31)
        last_year_same_q_date = rdate.replace(year=rdate.year - 1)
        
        try:
            ly_q4 = master.loc[(code, last_year_q4_date), 'jlr']
            ly_sq = master.loc[(code, last_year_same_q_date), 'jlr']
            if isinstance(ly_q4, pd.Series): ly_q4 = ly_q4.iloc[0]
            if isinstance(ly_sq, pd.Series): ly_sq = ly_sq.iloc[0]
            
            # TTM = 本期累计 + (去年全年 - 去年同期累计)
            ttm = current_ytd + (ly_q4 - ly_sq)
            ttm_list.append(ttm)
        except KeyError:
            ttm_list.append(0.0) # 缺失时硬拦截
            
    master['net_profit_ttm'] = ttm_list
    master = master.reset_index()
    master.rename(columns={'gdqy': 'net_assets'}, inplace=True)
    
    # 按照发布日期排序，为 AsOf 拼接做准备
    master = master.sort_values(['code', 'publish_date']).dropna(subset=['publish_date'])
    master['publish_date'] = pd.to_datetime(master['publish_date'])
    
    final_df = master[['code', 'publish_date', 'net_profit_ttm', 'net_assets']]
    final_df.to_parquet("finance_master.parquet", index=False)
    print(f"✅ [Parse GPCW] 主体财务降维合并成功，输出 Artifact 大小: {len(final_df)} 行。")

if __name__ == "__main__":
    main()
