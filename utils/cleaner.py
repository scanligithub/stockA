import pandas as pd
import numpy as np

class DataCleaner:
    @staticmethod
    def clean_stock_kline(df: pd.DataFrame) -> pd.DataFrame:
        if df.empty: return df
        
        # 1. 强制类型转换 (Float32/64)
        f32_cols = ['open', 'high', 'low', 'close', 'turn', 'pctChg', 'peTTM', 'pbMRQ', 'adjustFactor']
        for c in f32_cols:
            if c in df.columns: df[c] = pd.to_numeric(df[c], errors='coerce').astype('float32')
            
        f64_cols = ['volume', 'amount']
        for c in f64_cols:
            if c in df.columns: df[c] = pd.to_numeric(df[c], errors='coerce').astype('float64')

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
        
        cols = ['net_amount', 'main_net', 'super_net', 'large_net', 'medium_net', 'small_net']
        for c in cols:
            df[c] = pd.to_numeric(df[c], errors='coerce').fillna(0.0).astype('float64')
            
        df['date'] = pd.to_datetime(df['date']).dt.strftime('%Y-%m-%d')
        df = df.drop_duplicates(subset=['date', 'code'], keep='last')
        return df.sort_values(['code', 'date'])
    
    @staticmethod
    def clean_sector_kline(df: pd.DataFrame) -> pd.DataFrame:
        if df.empty: return df
        for c in ['open','high','low','close']: 
            df[c] = pd.to_numeric(df[c]).astype('float32')
        for c in ['volume','amount']:
            df[c] = pd.to_numeric(df[c]).astype('float64')
        df['date'] = pd.to_datetime(df['date']).dt.strftime('%Y-%m-%d')
        return df.sort_values(['code', 'date'])
