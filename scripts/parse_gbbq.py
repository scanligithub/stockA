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
        
    reader = GbbqReader()
    try:
        df = reader.get_df(gbbq_path)
    except Exception as e:
        print(f"❌ PyTDX Reader failed with error: {e}")
        sys.exit(1)
        
    if df is None:
        print("❌ Error: PyTDX returned None!")
        sys.exit(1)
        
    print("📊 Successfully parsed GBBQ DataFrame!")
    print(f"   - Row count: {len(df)}")
    print(f"   - Shape: {df.shape}")
    print(f"   - Columns: {df.columns.tolist()}") # 🎯 核心诊断：打印出实际拥有的列名
    
    if df.empty:
        print("❌ Error: DataFrame is empty!")
        sys.exit(1)
        
    print("📋 First 5 rows of decoded data:")
    print(df.head()) # 🎯 核心诊断：打印前 5 行解密数据样例
    
    # 智能安全落盘：不再进行过滤，直接输出
    df.to_csv(out_path, index=False)
    print(f"✅ Saved clean GBBQ to {out_path}")

if __name__ == "__main__":
    main()
