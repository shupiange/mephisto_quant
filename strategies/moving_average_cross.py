import pandas as pd

from core.engine.event import MarketEvent, SignalEvent, SignalType
from core.engine.strategy import Strategy
from core.utils.indicators import IndicatorHelper


class TrendFollowingStrategy(Strategy):
    """
    趋势跟踪策略 (Trend Following Strategy) - 2026 Pro Upgrade

    策略逻辑：
    1. 趋势识别: 使用 MACD (12, 26, 9) 判断主趋势。
       - MACD > Signal 且 MACD > 0: 强势多头区域。
    2. 入场信号:
       - 必须满足趋势条件 (MACD 金叉或多头排列)。
       - 辅助确认: ADX > 25 (确保有趋势)，RSI < 70 (避免追高)。
       - 价格 > MA60 (长期趋势线)。
    3. 出场信号:
       - MACD 死叉。
       - 跌破 ATR 动态止损线 (Trailing Stop based on ATR)。
    """

    def __init__(self, atr_period=14, atr_multiplier=2.5):
        super().__init__()
        # MACD 参数
        self.fast_period = 12
        self.slow_period = 26
        self.signal_period = 9

        # 过滤器参数
        self.ma_trend_window = 60
        self.adx_threshold = 25
        self.rsi_overbought = 70

        # ATR 止损参数
        self.atr_period = atr_period
        self.atr_multiplier = atr_multiplier

        # 数据缓存
        self.history = {}

        # 动态止损线缓存 {symbol: stop_price}
        self.trailing_stops = {}

    def calculate_signals(self, event: MarketEvent):
        symbol = event.symbol

        if symbol not in self.history:
            self.history[symbol] = {"high": [], "low": [], "close": []}

        # 1. 更新数据
        self.history[symbol]["high"].append(event.data["high"])
        self.history[symbol]["low"].append(event.data["low"])
        self.history[symbol]["close"].append(event.data["close"])

        # 保持数据长度适中，避免内存无限增长 (取最近 200 天即可)
        if len(self.history[symbol]["close"]) > 200:
            self.history[symbol]["high"] = self.history[symbol]["high"][-200:]
            self.history[symbol]["low"] = self.history[symbol]["low"][-200:]
            self.history[symbol]["close"] = self.history[symbol]["close"][-200:]

        close_list = self.history[symbol]["close"]
        if len(close_list) < self.ma_trend_window:
            return

        df = pd.DataFrame(self.history[symbol])

        # 2. 计算指标
        # MACD
        macd_df = IndicatorHelper.add_macd(
            df, self.fast_period, self.slow_period, self.signal_period
        )
        if macd_df is None or macd_df.empty:
            return
        macd = macd_df.iloc[-1, 0]  # MACD line
        signal = macd_df.iloc[-1, 2]  # Signal line
        macd_prev = macd_df.iloc[-2, 0]
        signal_prev = macd_df.iloc[-2, 2]

        # ATR
        atr_series = IndicatorHelper.add_atr(df, self.atr_period)
        atr = atr_series.iloc[-1] if atr_series is not None else 0

        # ADX & RSI
        adx_df = IndicatorHelper.add_adx(df)
        curr_adx = adx_df.iloc[-1, 0] if adx_df is not None and not adx_df.empty else 0
        rsi_series = IndicatorHelper.add_rsi(df)
        curr_rsi = rsi_series.iloc[-1] if rsi_series is not None else 50

        # Trend MA
        trend_ma = sum(close_list[-self.ma_trend_window :]) / self.ma_trend_window
        close = close_list[-1]

        # 3. 信号逻辑

        # 更新动态止损线 (Chandelier Exit 逻辑)
        # 如果持有仓位(这里假设外部Portfolio持有，策略层只负责发信号，但为了计算 trailing stop，我们需要假设做多)
        # 简单处理：每次价格创新高，提升止损线。
        # 这里我们维护一个虚拟的 trailing stop
        prev_stop = self.trailing_stops.get(symbol, 0)
        new_stop = close - (atr * self.atr_multiplier)

        # 如果新止损线更高，上移；否则保持 (仅针对多头)
        # 注意：这需要知道当前是否在多头状态。如果处于空仓，则重置。
        # 我们用 MACD > Signal 作为多头状态的近似
        is_bullish = macd > signal

        if is_bullish:
            if new_stop > prev_stop:
                self.trailing_stops[symbol] = new_stop
            else:
                # 保持原止损线，除非价格跌破它
                self.trailing_stops[symbol] = prev_stop
        else:
            # 修改：不直接重置，只在触发卖出信号时重置。
            # 但如果 MACD 死叉，通常会触发卖出信号。
            # 这里保留原值，交给下面的信号逻辑判断是否卖出。
            if prev_stop > 0:
                self.trailing_stops[symbol] = prev_stop
            else:
                self.trailing_stops[symbol] = 0

        current_stop = self.trailing_stops.get(symbol, 0)

        # --- 生成信号 ---

        # 买入条件:
        # 1. MACD 金叉 (本周期或刚发生)
        # 2. 价格 > MA60 (趋势向上)
        # 3. ADX > 25 (趋势强劲)
        # 4. RSI < 70 (未超买)

        macd_gold_cross = (macd_prev <= signal_prev) and (macd > signal)

        if macd_gold_cross:
            if (
                close > trend_ma
                and curr_adx > self.adx_threshold
                and curr_rsi < self.rsi_overbought
            ):
                print(
                    f"[Strategy] {symbol} 趋势买点触发! MACD金叉 | Price({close:.2f})>MA60 | ADX={curr_adx:.1f}"
                )
                self.put_event(SignalEvent(symbol, str(pd.Timestamp.now()), SignalType.LONG))
                # 初始化止损线
                self.trailing_stops[symbol] = close - (atr * self.atr_multiplier)
                return

        # 卖出条件:
        # 1. MACD 死叉
        # 2. 或者 跌破 ATR 动态止损线

        macd_death_cross = (macd_prev >= signal_prev) and (macd < signal)

        if macd_death_cross:
            print(f"[Strategy] [{symbol}] MACD死叉触发卖出 | Price: {close:.2f}")
            self.put_event(SignalEvent(symbol, str(pd.Timestamp.now()), SignalType.SHORT))
            self.trailing_stops[symbol] = 0

        elif current_stop > 0 and close < current_stop:
            print(
                f"[Strategy] [{symbol}] 触发ATR移动止损 | Price: {close:.2f} < Stop: {current_stop:.2f}"
            )
            self.put_event(
                SignalEvent(symbol, str(pd.Timestamp.now()), SignalType.EXIT)
            )  # EXIT 代表清仓
            self.trailing_stops[symbol] = 0

    def predict_next(self, symbol, current_position=0):
        if symbol not in self.history or len(self.history[symbol]["close"]) < self.ma_trend_window:
            return "数据不足: 需要更多K线数据"

        # 临时构建 DataFrame 用于计算指标 (与 calculate_signals 保持一致)
        df = pd.DataFrame(self.history[symbol])

        # 1. 计算指标
        # MACD
        macd_df = IndicatorHelper.add_macd(
            df, self.fast_period, self.slow_period, self.signal_period
        )
        if macd_df is None or len(macd_df) < 2:
            return "指标计算失败 (MACD数据不足)"

        macd = macd_df.iloc[-1, 0]
        signal = macd_df.iloc[-1, 2]
        macd_prev = macd_df.iloc[-2, 0]
        signal_prev = macd_df.iloc[-2, 2]

        # ATR
        atr_series = IndicatorHelper.add_atr(df, self.atr_period)
        atr = atr_series.iloc[-1] if atr_series is not None else 0

        # ADX
        adx_df = IndicatorHelper.add_adx(df)
        curr_adx = adx_df.iloc[-1, 0] if adx_df is not None and not adx_df.empty else 0

        # RSI
        rsi_series = IndicatorHelper.add_rsi(df)
        curr_rsi = rsi_series.iloc[-1] if rsi_series is not None else 50

        # Trend MA
        close_list = self.history[symbol]["close"]
        trend_ma = sum(close_list[-self.ma_trend_window :]) / self.ma_trend_window
        close = close_list[-1]

        # 2. 判断信号
        msg_lines = []
        msg_lines.append(f"当前价: {close:.2f}")
        msg_lines.append(
            f"趋势均线(MA{self.ma_trend_window}): {trend_ma:.2f} ({'多头' if close > trend_ma else '空头'})"
        )
        msg_lines.append(f"MACD: {macd:.2f} / Signal: {signal:.2f}")
        msg_lines.append(
            f"ADX: {curr_adx:.1f} (>{self.adx_threshold}?) | RSI: {curr_rsi:.1f} (<{self.rsi_overbought}?)"
        )

        # 检查买入信号 (MACD金叉 + 过滤条件)
        macd_gold_cross = (macd_prev <= signal_prev) and (macd > signal)
        # 如果不是刚好金叉，但也处于多头区域，可以提示
        is_bullish_zone = macd > signal
        # 检查卖出信号 (MACD死叉)
        macd_death_cross = (macd_prev >= signal_prev) and (macd < signal)

        is_buy_signal = False
        if macd_gold_cross:
            if (
                close > trend_ma
                and curr_adx > self.adx_threshold
                and curr_rsi < self.rsi_overbought
            ):
                is_buy_signal = True

        # 检查现有持仓止损
        stop = self.trailing_stops.get(symbol, 0)

        # 如果传入了实际持仓，且 stop 为 0 (可能被重置了)，尝试恢复一个临时的止损显示 (ATR止损)
        # 或者仅仅是用来判断状态
        if current_position > 0 and stop == 0:
            # 如果策略认为 stop 为 0，通常意味着之前触发了卖出信号。
            pass

        msg_lines.append("-" * 30)

        if is_buy_signal:
            msg_lines.append("★ 交易信号: 【建议买入】")
            if current_position > 0:
                msg_lines.append(f"  (注意: 当前已有持仓 {current_position}，请根据风控加仓)")

            # 计算建议仓位
            # 假设单笔交易风险金额 (R) 为账户权益的 1% 或 固定金额 (例如 1000 元)
            # 这里仅作展示，使用固定风险金额 2000 元作为示例
            risk_budget = 2000
            risk_per_share = atr * self.atr_multiplier
            suggested_stop = close - risk_per_share

            if risk_per_share > 0.01:
                suggested_qty = int(risk_budget / risk_per_share)
                # 向下取整到 100 股 (A股手数)
                suggested_qty = (suggested_qty // 100) * 100
                if suggested_qty < 100:
                    suggested_qty = 100  # 至少1手

                msg_lines.append(f"  建议止损价: {suggested_stop:.2f}")
                msg_lines.append(f"  建议买入数量: {suggested_qty} 股")
                msg_lines.append(f"  (基于2000元风险预算估算, 每股风险 {risk_per_share:.2f})")
            else:
                msg_lines.append("  波动率过低，无法计算合理仓位")

        elif current_position > 0:
            # 有实际持仓
            msg_lines.append("★ 状态: 【持仓中】 (实际持仓)")

            if macd_death_cross:
                msg_lines.append("  [警告] MACD刚刚死叉，建议卖出!")

            if stop > 0:
                msg_lines.append(f"  ATR动态止损: {stop:.2f}")
                if close < stop:
                    msg_lines.append("  [警告] 现价跌破止损线，建议立即卖出!")
                else:
                    distance = ((close - stop) / close) * 100
                    msg_lines.append(f"  安全垫: {distance:.1f}%")
            else:
                # 策略端没有止损线 (可能之前触发了卖出但没成交，或者刚初始化)
                # 我们可以计算一个临时的 ATR 止损给用户参考
                temp_stop = close - (atr * self.atr_multiplier)
                msg_lines.append(f"  (策略止损线未激活，参考 ATR 支撑位: {temp_stop:.2f})")
                if not is_bullish_zone:
                    msg_lines.append("  当前处于空头区域 (MACD < Signal)，注意风险")

        elif stop > 0:
            # 策略认为有持仓 (有止损线)，但实际持仓为 0 (可能刚卖出，或数据不同步)
            msg_lines.append("★ 状态: 【策略持有中】 (实际空仓)")
            msg_lines.append(f"  ATR动态止损: {stop:.2f}")
            msg_lines.append("  (注: 实际账户无持仓，可能是信号未成交或已手动平仓)")

        else:
            msg_lines.append("★ 状态: 【空仓观望】")
            if not is_bullish_zone:
                msg_lines.append("  等待 MACD 金叉...")
            elif close <= trend_ma:
                msg_lines.append("  等待价格站上 MA60...")
            elif curr_adx <= self.adx_threshold:
                msg_lines.append(f"  趋势强度不足 (ADX {curr_adx:.1f} <= {self.adx_threshold})")
            elif curr_rsi >= self.rsi_overbought:
                msg_lines.append(f"  RSI超买 ({curr_rsi:.1f})，暂停追高")
            else:
                msg_lines.append("  趋势良好，但未触发新入场信号 (错过金叉点)")

        return "\n".join(msg_lines)
