import pandas as pd

from core.engine.event import MarketEvent, SignalEvent, SignalType
from core.engine.strategy import Strategy
from core.utils.indicators import IndicatorHelper


class MeanReversionStrategy(Strategy):
    """
    均值回归策略 (Mean Reversion Strategy) - 震荡市利器

    策略逻辑：
    1. 布林带 (Bollinger Bands): 利用价格偏离均线的程度来判断超买超卖。
       - 价格跌破下轨 -> 超卖 -> 潜在买入机会。
       - 价格突破上轨 -> 超买 -> 潜在卖出机会。
    2. RSI (Relative Strength Index): 辅助确认反转信号。
       - RSI < 30 -> 确认超卖。
       - RSI > 70 -> 确认超买。
    
    适用场景: 震荡市、盘整期 (价格在区间内波动)。
    风险: 在单边强趋势中可能会逆势操作导致亏损，因此需要严格的止损。
    """

    def __init__(self, bb_length=20, bb_std=2.0, rsi_length=14):
        super().__init__()
        # 布林带参数
        self.bb_length = bb_length
        self.bb_std = bb_std
        
        # RSI 参数
        self.rsi_length = rsi_length
        self.rsi_buy_threshold = 30
        self.rsi_sell_threshold = 70
        
        # 止损参数 (固定比例)
        self.stop_loss_pct = 0.05 # 5% 止损

        # 数据缓存
        self.history = {}
        # 记录入场价格以便计算止损
        self.entry_prices = {} 

    def calculate_signals(self, event: MarketEvent):
        symbol = event.symbol
        
        if symbol not in self.history:
            self.history[symbol] = {"high": [], "low": [], "close": []}
            
        # 1. 更新数据
        self.history[symbol]["high"].append(event.data["high"])
        self.history[symbol]["low"].append(event.data["low"])
        self.history[symbol]["close"].append(event.data["close"])
        
        # 保持数据长度
        if len(self.history[symbol]["close"]) > 100:
             self.history[symbol]["high"] = self.history[symbol]["high"][-100:]
             self.history[symbol]["low"] = self.history[symbol]["low"][-100:]
             self.history[symbol]["close"] = self.history[symbol]["close"][-100:]

        close_list = self.history[symbol]["close"]
        # 至少需要 bb_length + 一些缓冲
        if len(close_list) < self.bb_length + 2:
            return

        df = pd.DataFrame(self.history[symbol])
        
        # 2. 计算指标
        # Bollinger Bands
        bb_df = IndicatorHelper.add_bbands(df, length=self.bb_length, std=self.bb_std)
        if bb_df is None or bb_df.empty: return
        
        # BBL_20_2.0 (Lower), BBM_20_2.0 (Mid), BBU_20_2.0 (Upper)
        # 列名可能略有不同，pandas_ta 通常返回: BBL, BBM, BBU, BBB, BBP
        # 我们假设列顺序或通过名称获取
        lower_band = bb_df.iloc[-1, 0] # 通常第一列是 Lower
        mid_band = bb_df.iloc[-1, 1]   # Mid
        upper_band = bb_df.iloc[-1, 2] # Upper
        
        # RSI
        rsi_series = IndicatorHelper.add_rsi(df, length=self.rsi_length)
        curr_rsi = rsi_series.iloc[-1] if rsi_series is not None else 50
        
        close = close_list[-1]
        
        # 3. 信号逻辑
        
        # 检查是否持有仓位 (这里简单通过 entry_prices 判断，实际应由 Portfolio 确认，但策略层需要知道状态)
        has_position = symbol in self.entry_prices and self.entry_prices[symbol] > 0
        
        if has_position:
            # 持仓状态: 检查卖出或止损
            entry_price = self.entry_prices[symbol]
            
            # 止损检查 (Stop Loss)
            if close < entry_price * (1 - self.stop_loss_pct):
                print(f"[Strategy] {symbol} 触发止损 (均值回归失败). Price: {close:.2f} < Entry: {entry_price:.2f}")
                self.put_event(SignalEvent(symbol, str(pd.Timestamp.now()), SignalType.SHORT)) # 清仓
                self.entry_prices[symbol] = 0
                return

            # 卖出条件: 触及上轨 OR RSI 超买
            if close > upper_band or curr_rsi > self.rsi_sell_threshold:
                print(f"[Strategy] {symbol} 均值回归获利了结. Price: {close:.2f} > UpperBand({upper_band:.2f}) | RSI={curr_rsi:.1f}")
                self.put_event(SignalEvent(symbol, str(pd.Timestamp.now()), SignalType.SHORT))
                self.entry_prices[symbol] = 0
                
            # 回归均值卖出 (可选: 触及中轨就减仓? 这里简单点，只在上轨卖)
            
        else:
            # 空仓状态: 检查买入
            # 买入条件: 跌破下轨 AND RSI 超卖
            if close < lower_band and curr_rsi < self.rsi_buy_threshold:
                print(f"[Strategy] {symbol} 均值回归买点! Price: {close:.2f} < LowerBand({lower_band:.2f}) | RSI={curr_rsi:.1f}")
                self.put_event(SignalEvent(symbol, str(pd.Timestamp.now()), SignalType.LONG))
                self.entry_prices[symbol] = close

    def predict_next(self, symbol):
        if symbol not in self.history or len(self.history[symbol]["close"]) < self.bb_length:
            return "数据不足"
            
        df = pd.DataFrame(self.history[symbol])
        bb_df = IndicatorHelper.add_bbands(df, length=self.bb_length, std=self.bb_std)
        if bb_df is None or bb_df.empty: return "指标计算失败"
        
        lower = bb_df.iloc[-1, 0]
        upper = bb_df.iloc[-1, 2]
        close = self.history[symbol]["close"][-1]
        
        rsi_series = IndicatorHelper.add_rsi(df, length=self.rsi_length)
        rsi = rsi_series.iloc[-1] if rsi_series is not None else 0
        
        status = "观望区间"
        if close < lower: status = "超卖区域 (关注买点)"
        elif close > upper: status = "超买区域 (注意风险)"
        
        return f"当前价: {close:.2f}\n  布林带: [{lower:.2f}, {upper:.2f}]\n  RSI: {rsi:.1f}\n  状态: {status}"
