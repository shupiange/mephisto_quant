

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
    
    # 日线级别股票数据表字段及其数据类型
    'quant.stock_data_1_day_fields': [
        'date', 'code', 'open', 'close', 'high', 'low', 'volume', 'amount', 'turn'
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
    },

    # 日线级别股票指标表字段及其数据类型
    'quant.stock_indicators_1_day_fields': [
        'date', 'code', 'diff', 'dea', 'macd', 'k', 'd', 'j', 'cci', 'mfi',
        'ma3', 'ma5', 'ma10', 'ma20', 'ma30', 'ma60', 'ma90',
        'boll_upper', 'boll_middle', 'boll_lower'
    ],
    'quant.stock_indicators_1_day': {
        'date': str, 
        'code': str,
        'diff': float,
        'dea': float,
        'macd': float,
        'k': float,
        'd': float,
        'j': float,
        'cci': float,
        'mfi': float,
        'ma3': float,
        'ma5': float,
        'ma10': float,
        'ma20': float,
        'ma30': float,
        'ma60': float,
        'ma90': float,
        'boll_upper': float,
        'boll_middle': float,
        'boll_lower': float
    },

    # 30分钟级别股票指标表字段及其数据类型
    'quant.stock_indicators_30_minute_fields': [
        'date', 'code', 'time', 'diff', 'dea', 'macd', 'k', 'd', 'j', 'cci', 'mfi',
        'ma3', 'ma5', 'ma10', 'ma20', 'ma30', 'ma60', 'ma90',
        'boll_upper', 'boll_middle', 'boll_lower'
    ],
    'quant.stock_indicators_30_minute': {
        'date': str, 
        'code': str,
        'time': int,
        'diff': float,
        'dea': float,
        'macd': float,
        'k': float,
        'd': float,
        'j': float,
        'cci': float,
        'mfi': float,
        'ma3': float,
        'ma5': float,
        'ma10': float,
        'ma20': float,
        'ma30': float,
        'ma60': float,
        'ma90': float,
        'boll_upper': float,
        'boll_middle': float,
        'boll_lower': float
    },
}