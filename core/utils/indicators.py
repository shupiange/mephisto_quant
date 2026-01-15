try:
    import pandas_ta_classic as ta
except ImportError:
    try:
        import pandas_ta as ta
    except ImportError:
        ta = None

def get_indicators_library():
    """获取技术指标库"""
    return ta

class IndicatorHelper:
    """
    技术指标助手类，封装常用指标计算。
    """
    @staticmethod
    def add_sma(df, length=20, append=True):
        if ta:
            return df.ta.sma(length=length, append=append)
        return None

    @staticmethod
    def add_rsi(df, length=14, append=True):
        if ta:
            return df.ta.rsi(length=length, append=append)
        return None

    @staticmethod
    def add_macd(df, fast=12, slow=26, signal=9, append=True):
        if ta:
            return df.ta.macd(fast=fast, slow=slow, signal=signal, append=append)
        return None

    @staticmethod
    def add_bbands(df, length=20, std=2, append=True):
        if ta:
            return df.ta.bbands(length=length, std=std, append=append)
        return None

    @staticmethod
    def add_adx(df, length=14, lensig=14, scalar=100, append=True):
        if ta:
            # ADX requires 'high', 'low', 'close' columns (case insensitive usually)
            return df.ta.adx(length=length, lensig=lensig, scalar=scalar, append=append)
        return None

    @staticmethod
    def add_atr(df, length=14, append=True):
        """添加 ATR (Average True Range) 指标"""
        if ta:
            return df.ta.atr(length=length, append=append)
        return None
