import baostock as bs
import pandas as pd
import json
import math
import random
import datetime

NUM_CHUNKS = 20

def main():
    bs.login()
    # 尝试回溯最近交易日获取列表
    data = []
    for i in range(10):
        d = (datetime.datetime.now() - datetime.timedelta(days=i)).strftime("%Y-%m-%d")
        rs = bs.query_all_stock(day=d)
        while rs.next(): data.append(rs.get_row_data())
        if data: break
    bs.logout()
    
    # 过滤指数
    all_codes = [x[0] for x in data if x[0].startswith(('sh.', 'sz.', 'bj.'))]
    random.shuffle(all_codes) # 随机打乱
    print(f"Total stocks: {len(all_codes)}, shuffling...")

    chunk_size = math.ceil(len(all_codes) / NUM_CHUNKS)
    chunks = []
    for i in range(NUM_CHUNKS):
        subset = all_codes[i * chunk_size : (i + 1) * chunk_size]
        if subset: chunks.append({"index": i, "codes": subset})

    with open("stock_matrix.json", "w") as f:
        json.dump(chunks, f)

if __name__ == "__main__":
    main()
