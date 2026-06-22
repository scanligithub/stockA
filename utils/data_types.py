import pyarrow as pa

class AShareDataSchema:
    DATE = 'date'
    CODE = 'code'
    
    OPEN = 'open'
    HIGH = 'high'
    LOW = 'low'
    CLOSE = 'close'
    VOLUME = 'volume'
    AMOUNT = 'amount'
    TURN = 'turn'
    PCT_CHG = 'pctChg'
    PE_TTM = 'peTTM'
    PB_MRQ = 'pbMRQ'
    ADJ_FACTOR = 'adjustFactor'
    IS_ST = 'isST'
    
    # 💥 全面并轨基本面核心列
    TOTAL_MV = 'total_mv'
    FLOAT_MV = 'float_mv'
    TOTAL_SHARES = 'totalShares'
    FLOAT_SHARES = 'floatShares'

    NET_FLOW = 'net_amount'
    MAIN_FLOW = 'main_net'
    SUPER_FLOW = 'super_net'
    LARGE_FLOW = 'large_net'
    MEDIUM_FLOW = 'medium_net'
    SMALL_FLOW = 'small_net'

    NAME = 'name'
    TYPE = 'type' 

    @staticmethod
    def get_stock_kline_schema():
        return pa.schema([
            (AShareDataSchema.DATE, pa.string()),
            (AShareDataSchema.CODE, pa.string()),
            (AShareDataSchema.OPEN, pa.float32()),
            (AShareDataSchema.HIGH, pa.float32()),
            (AShareDataSchema.LOW, pa.float32()),
            (AShareDataSchema.CLOSE, pa.float32()),
            (AShareDataSchema.VOLUME, pa.float64()),
            (AShareDataSchema.AMOUNT, pa.float64()),
            (AShareDataSchema.TURN, pa.float32()),
            (AShareDataSchema.PCT_CHG, pa.float32()),
            (AShareDataSchema.PE_TTM, pa.float32()),
            (AShareDataSchema.PB_MRQ, pa.float32()),
            (AShareDataSchema.ADJ_FACTOR, pa.float32()),
            (AShareDataSchema.IS_ST, pa.int8()),
            (AShareDataSchema.TOTAL_MV, pa.float64()),
            (AShareDataSchema.FLOAT_MV, pa.float64()),
            (AShareDataSchema.TOTAL_SHARES, pa.float64()),
            (AShareDataSchema.FLOAT_SHARES, pa.float64())
        ])

    @staticmethod
    def get_money_flow_schema():
        return pa.schema([
            (AShareDataSchema.DATE, pa.string()),
            (AShareDataSchema.CODE, pa.string()),
            (AShareDataSchema.NET_FLOW, pa.float64()),
            (AShareDataSchema.MAIN_FLOW, pa.float64()),
            (AShareDataSchema.SUPER_FLOW, pa.float64()),
            (AShareDataSchema.LARGE_FLOW, pa.float64()),
            (AShareDataSchema.MEDIUM_FLOW, pa.float64()),
            (AShareDataSchema.SMALL_FLOW, pa.float64())
        ])
