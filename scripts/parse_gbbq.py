import sys
import os
import pandas as pd
from pytdx.reader import GbbqReader

def main():
    gbbq_path = "gbbq.dat"
    out_path = "gbbq_clean.csv"
    if len(sys.argv) > 1:
        gbbq_path = sys.argv[1]
    if len(sys.argv) > 2:
        out_path = sys.argv[2]
        
    print(f"📖 Parsing {gbbq_path} using PyTDX GbbqReader...")
    if not os.path.exists(gbbq_path):
        print(f"❌ Error: {gbbq_path} not found!")
        sys.exit(1)
        
    # 调用 Python 社区成熟的解密内核直读
    reader = GbbqReader()
    df = reader.get_df(gbbq_path)
    
    # 过滤并提取核心字段：代码,日期,类别,分红,配股价,送股数,配股数
    df = df[['code', 'date', 'category', 'fenhong', 'peijia', 'songgu', 'peigu']]
    df.to_csv(out_path, index=False)
    print(f"✅ GBBQ parsed successfully! Saved to {out_path}, total rows: {len(df)}")

if __name__ == "__main__":
    main()
