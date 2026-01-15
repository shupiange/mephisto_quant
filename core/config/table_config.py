

TABLE_FIELDS_CONFIG = {

    # 30分钟级别股票数据表字段及其数据类型
    'quant.stock_data_30_minute_fields': [
        'date','code', 'open', 'close', 'high', 'low', 'volume', 'amount', 
        'time', 'time_rank'
    ],
    'quant.stock_data_30_minute': {
        'date': str, 
        'code': str, 
        'open': float, 
        'close': float, 
        'high': float, 
        'low': float, 
        'volume': int, 
        'amount': float, 
        'time': int, 
        'time_rank': int
    },

    # 5分钟级别股票数据表字段及其数据类型
    'quant.stock_data_5_minute_fields': [
        'date','code', 'open', 'close', 'high', 'low', 'volume', 'amount', 
        'time', 'time_rank'
    ],
    'quant.stock_data_5_minute': {
        'date': str, 
        'code': str, 
        'open': float, 
        'close': float, 
        'high': float, 
        'low': float, 
        'volume': int, 
        'amount': float, 
        'time': int, 
        'time_rank': int
    },
    
    # 日线级别股票数据表字段及其数据类型
    'quant.stock_data_1_day_fields': [
        'date', 'code', 'open', 'close', 'high', 'low', 'volume', 'amount', 
        'turn', 'pct_chg', 'pe_ttm', 'pb', 'ps_ttm', 'pcf_ttm', 'trade_status'
    ],
    'quant.stock_data_1_day': {
        'date': str, 
        'code': str, 
        'open': float, 
        'close': float, 
        'high': float, 
        'low': float, 
        'volume': int, 
        'amount': float, 
        'turn': float, 
        'pct_chg': float, 
        'pe_ttm': float, 
        'pb': float,
        'ps_ttm': float,
        'pcf_ttm': float,
        'trade_status': float
    },
}