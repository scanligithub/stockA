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
            
        # 成交量、成交额 -> float64 (防止大盘股溢出或精度丢失，虽然float32也勉强够，但k线数据核心字段建议保真)
        f64_cols = ['volume', 'amount']
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
        """
        资金流向数据清洗与瘦身
        策略：单位转为'万元'，并使用 float32 存储
        """
        if df.empty: return df
        
        cols = ['net_amount', 'main_net', 'super_net', 'large_net', 'medium_net', 'small_net']
        for col in cols:
            if col in df.columns:
                # 1. 转为数字，填充0
                s = pd.to_numeric(df[col], errors='coerce').fillna(0.0)
                
                # 2. 【瘦身策略】单位换算：元 -> 万元
                s = s / 10000.0
                
                # 3. 【瘦身策略】降级为 float32
                # 对于万元级数据，float32 精度足够，能减少 50% 存储空间
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
        # 板块去重逻辑
        if 'code' in df.columns:
            df = df.drop_duplicates(subset=['date', 'code'], keep='last')
            return df.sort_values(['code', 'date'])
        else:
            return df.sort_values(['date'])
