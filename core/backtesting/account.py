
class Position:
    def __init__(self, code, initial_volume=0, initial_price=0.0):
        self.code = code
        self.total_volume = initial_volume      # 总持仓
        self.available_volume = initial_volume  # 可卖持仓 (T+1规则)
        self.avg_cost = initial_price           # 持仓成本
        self.frozen_volume = 0                  # 卖出冻结（已挂单未成交部分，如果是立即成交模式则不需要）

    def on_buy(self, volume, price):
        """
        买入成交更新
        如果余额不够，按照最多可买数量成交，且必须是100的整数倍
        """
        if volume <= 0:
            return
        
        # 必须是100的整数倍
        volume = (volume // 100) * 100
        if volume <= 0:
            return
        
        # 更新成本
        total_cost = self.total_volume * self.avg_cost + volume * price
        self.total_volume += volume
        self.avg_cost = total_cost / self.total_volume
        # available_volume 不变，等待结算

    def on_sell(self, volume, price):
        """
        卖出成交更新
        如果 volume 大于 available_volume,按照 available_volume 成交
        """
        if volume <= 0:
            return
        
        # 如果请求卖出量大于可卖量，则按可卖量成交
        if volume > self.available_volume:
            volume = self.available_volume
        
        self.total_volume -= volume
        self.available_volume -= volume
        
        if self.total_volume == 0:
            self.avg_cost = 0.0
            
    def settle(self):
        """
        日终/日初结算，处理 T+1
        """
        self.available_volume = self.total_volume

class Account:
    def __init__(self, initial_cash=1000000.0, commission_rate=0.0002):
        self.cash = initial_cash
        self.initial_cash = initial_cash
        self.positions = {} # code -> Position
        self.commission_rate = commission_rate
        self.total_value = initial_cash

    def get_position(self, code):
        if code not in self.positions:
            self.positions[code] = Position(code)
        return self.positions[code]

    def buy(self, code, price, volume):
        """
        执行买入
        Returns: (success, message)
        """
        if volume <= 0:
            return False, "Volume must be positive"

        cost = price * volume
        commission = cost * self.commission_rate
        # A股最低佣金通常是5元，这里暂简化
        total_cost = cost + commission

        if self.cash < total_cost:
            return False, f"Not enough cash. Need {total_cost:.2f}, Have {self.cash:.2f}"

        pos = self.get_position(code)
        pos.on_buy(volume, price)
        self.cash -= total_cost
        return True, "Buy success"

    def sell(self, code, price, volume):
        """
        执行卖出
        Returns: (success, message)
        """
        if volume <= 0:
            return False, "Volume must be positive"
        
        if code not in self.positions:
            return False, "No position found"

        pos = self.positions[code]
        if pos.available_volume < volume:
            return False, f"Not enough available shares. Available: {pos.available_volume}"

        revenue = price * volume
        commission = revenue * self.commission_rate
        # 印花税等暂忽略或包含在 commission 中
        net_revenue = revenue - commission

        try:
            pos.on_sell(volume, price)
        except ValueError as e:
            return False, str(e)
            
        self.cash += net_revenue
        
        if pos.total_volume == 0:
            del self.positions[code]
            
        return True, "Sell success"

    def settle(self):
        """
        每日结算调用
        """
        for pos in self.positions.values():
            pos.settle()

    def update_market_value(self, current_prices):
        """
        更新账户总市值
        current_prices: {code: price}
        """
        market_value = 0.0
        for code, pos in self.positions.items():
            price = current_prices.get(code, pos.avg_cost)
            market_value += pos.total_volume * price
        self.total_value = self.cash + market_value
        return self.total_value
