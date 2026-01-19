
class Context:
    def __init__(self, account, engine):
        self.account = account
        self.engine = engine
        self.current_time = None
        self.current_prices = {} # {code: price}

    def buy(self, code, volume, price=None):
        """
        买入接口
        price: 如果为 None,使用当前收盘价/当前价
        """
        if price is None:
            price = self.current_prices.get(code)
        
        if price is None:
            return False, f"No price available for {code}"
            
        return self.account.buy(code, price, volume)

    def sell(self, code, volume, price=None):
        """
        卖出接口
        """
        if price is None:
            price = self.current_prices.get(code)
            
        if price is None:
            return False, f"No price available for {code}"
            
        return self.account.sell(code, price, volume)

    @property
    def cash(self):
        return self.account.cash

    @property
    def positions(self):
        return self.account.positions

class Strategy:
    def initialize(self, context):
        """
        初始化策略,只执行一次
        """
        pass

    def on_bar(self, context, bar_dict):
        """
        每个时间步调用一次
        bar_dict: {code: series/dict},包含 open, close, high, low, volume, date 等
        """
        pass

    def on_day_start(self, context, date):
        """
        每日开盘前调用 (可选)
        """
        pass

    def on_day_end(self, context, date):
        """
        每日收盘后调用 (可选)
        """
        pass
