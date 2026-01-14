from strategy.holds import Hold
import pandas as pd
import datetime

class Context:
    """
    策略运行上下文，提供账户交互接口
    """
    def __init__(self, hold: Hold):
        self.hold = hold
        self.current_prices = {} # {code: price}
        self.current_time = None

    def update_env(self, prices, time):
        self.current_prices = prices
        self.current_time = time

    def buy(self, code, volume):
        """买入下单 (立即成交模式)"""
        price = self.current_prices.get(code)
        if price is None:
            return False, f"Price for {code} not available"
        return self.hold.buy(code, price, volume)

    def sell(self, code, volume):
        """卖出下单 (立即成交模式)"""
        price = self.current_prices.get(code)
        if price is None:
            return False, f"Price for {code} not available"
        return self.hold.sell(code, price, volume)

    @property
    def cash(self):
        return self.hold.cash

    @property
    def positions(self):
        return self.hold.positions

class Strategy:
    """
    策略基类
    """
    def initialize(self, context):
        pass

    def on_open(self, context, quotes):
        """
        每日开盘竞价时调用
        quotes: dict {code: {'open': ..., 'date': ...}}
        """
        pass

    def on_close(self, context, quotes):
        """
        每日收盘竞价时调用
        quotes: dict {code: {'close': ..., 'date': ...}}
        """
        pass
