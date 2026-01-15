import baostock as bs
import pandas as pd
import json
import math
import random
import datetime

NUM_CHUNKS = 20

def main():
    bs.login()
    data = []
    for i in range(10):
        d = (datetime.datetime.now() - datetime.timedelta(days=i)).strftime("%Y-%m-%d")
        rs = bs.query_all_stock(day=d)
        if rs.error_code == '0':
            while rs.next():
                row = rs.get_row_data()
                # 过滤：code_name 不能为空
                if row[2] and row[2].strip():
                    data.append(row)
            if data: break
    bs.logout()
    
    if not data: return

    # 过滤：只留 A 股
    valid_codes = [x[0] for x in data if x[0].startswith(('sh.', 'sz.', 'bj.'))]
    random.shuffle(valid_codes)
    
    print(f"Total valid stocks: {len(valid_codes)}")

    chunk_size = math.ceil(len(valid_codes) / NUM_CHUNKS)
    chunks = []
    for i in range(NUM_CHUNKS):
        subset = valid_codes[i * chunk_size : (i + 1) * chunk_size]
        if subset:
            chunks.append({"index": i, "codes": subset})

    with open("stock_matrix.json", "w") as f:
        json.dump(chunks, f)

if __name__ == "__main__":
    main()
