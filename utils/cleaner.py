import pandas as pd
import numpy as np

class DataCleaner:
    @staticmethod
    def clean_stock_kline(df: pd.DataFrame) -> pd.DataFrame:
        if df.empty: return df
        
        # 1. 强制类型转换 (Float32/64)
        # 价格类、比率类 -> float32 (足够精度，节省空间)
        f32_cols = ['open', 'high', 'low', 'close', 'turn', 'pctChg', 'peTTM', 'pbMRQ', 'adjustFactor']
        for c in f32_cols:
            if c in df.columns: 
                df[c] = pd.to_numeric(df[c], errors='coerce').astype('float32')
            
        # 成交量、成交额、股本、市值 -> float64 (防止大盘股和万亿市值溢出)
        f64_cols = ['volume', 'amount', 'total_shares', 'float_shares', 'total_mv', 'float_mv']
        for c in f64_cols:
            if c in df.columns: 
                df[c] = pd.to_numeric(df[c], errors='coerce').astype('float64')

        if 'isST' in df.columns:
            df['isST'] = pd.to_numeric(df['isST'], errors='coerce').fillna(0).astype('int8')

        # 2. 日期格式化 YYYY-MM-DD
        df['date'] = pd.to_datetime(df['date']).dt.strftime('%Y-%m-%d')
        
        # 3. 去重 (保留最新)
        df = df.drop_duplicates(subset=['date', 'code'], keep='last')
        
        # 4. 排序
        return df.sort_values(['code', 'date'])

    @staticmethod
    def clean_money_flow(df: pd.DataFrame) -> pd.DataFrame:
        if df.empty: return df
        
        # 1. 严格筛选列 (White-list)
        target_cols = ['date', 'code', 'net_amount', 'main_net', 'super_net', 'large_net', 'medium_net', 'small_net']
        
        for col in target_cols:
            if col not in df.columns:
                if col not in ['date', 'code']:
                    df[col] = 0.0
                    
        df = df[target_cols].copy()
        
        # 2. 数值处理
        value_cols = ['net_amount', 'main_net', 'super_net', 'large_net', 'medium_net', 'small_net']
        for col in value_cols:
            s = pd.to_numeric(df[col], errors='coerce').fillna(0.0)
            s = s / 10000.0  # 元 -> 万元
            df[col] = s.astype('float32')
            
        df['date'] = pd.to_datetime(df['date']).dt.strftime('%Y-%m-%d')
        df = df.drop_duplicates(subset=['date', 'code'], keep='last')
        return df.sort_values(['code', 'date'])
    
    @staticmethod
    def clean_sector_kline(df: pd.DataFrame) -> pd.DataFrame:
        if df.empty: return df
        
        # 板块指数价格 -> float32
        for c in ['open','high','low','close']: 
            if c in df.columns:
                df[c] = pd.to_numeric(df[c], errors='coerce').astype('float32')
                
        # 板块成交量额 -> float64
        for c in ['volume','amount']:
            if c in df.columns:
                df[c] = pd.to_numeric(df[c], errors='coerce').astype('float64')
                
        df['date'] = pd.to_datetime(df['date']).dt.strftime('%Y-%m-%d')
        
        if 'code' in df.columns:
            df = df.drop_duplicates(subset=['date', 'code'], keep='last')
            return df.sort_values(['code', 'date'])
        else:
            return df.sort_values(['date'])
