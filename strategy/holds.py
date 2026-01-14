
TRADE_PRICE_RATE = 0.0002

class Position:
    def __init__(self, code):
        self.code = code
        self.total_volume = 0      # 总持仓
        self.available_volume = 0  # 可卖持仓 (T+1规则)
        self.avg_cost = 0.0        # 持仓成本

    def update_after_buy(self, price, volume):
        """
        买入后更新：
        1. 重新计算持仓成本
        2. 增加总持仓
        3. 可卖持仓不变 (T+1)
        """
        if self.total_volume + volume > 0:
            total_cost = self.total_volume * self.avg_cost + price * volume
            self.total_volume += volume
            self.avg_cost = total_cost / self.total_volume
        else:
            # 理论上买入 volume 必须 > 0，这里防守一下
            self.total_volume += volume

    def update_after_sell(self, price, volume):
        """
        卖出后更新：
        1. 扣除总持仓
        2. 扣除可卖持仓
        3. 成本一般也可以选择不更新（移动加权平均），或者实现具体会计准则
           这里简单处理：成本价保持不变，视为按比例减少持仓
        """
        if volume > self.available_volume:
            raise ValueError(f"Not enough available shares to sell. Available: {self.available_volume}, Request: {volume}")
        
        self.total_volume -= volume
        self.available_volume -= volume
        
        if self.total_volume == 0:
            self.avg_cost = 0.0

    def settle(self):
        """
        每日结算：将所有持仓变为可卖 (T+1 解冻)
        """
        self.available_volume = self.total_volume

class Hold:
    def __init__(self, initial_cash=100000.0):
        self.cash = initial_cash
        self.positions = {} # code -> Position

    def settle(self):
        """
        每日盘前调用, 处理T+1解冻
        """
        for code, pos in self.positions.items():
            pos.settle()

    def buy(self, code, price, volume):
        """
        买入操作
        返回: (success, message)
        """
        if volume <= 0:
            return False, "Volume must be positive"

        cost = price * volume
        fee = cost * TRADE_PRICE_RATE
        # 最低佣金等细节暂忽略，仅按比例
        total_cost = cost + fee

        if self.cash < total_cost:
            return False, f"Not enough cash. Need {total_cost}, Have {self.cash}"

        if code not in self.positions:
            self.positions[code] = Position(code)

        self.positions[code].update_after_buy(price, volume)
        self.cash -= total_cost
        return True, "Buy success"

    def sell(self, code, price, volume):
        """
        卖出操作
        返回: (success, message)
        """
        if volume <= 0:
            return False, "Volume must be positive"
        
        if code not in self.positions:
            return False, "Position not found"

        pos = self.positions[code]
        if pos.available_volume < volume:
            return False, f"Not enough available shares (T+1 rule). Available: {pos.available_volume}"

        revenue = price * volume
        fee = revenue * TRADE_PRICE_RATE
        total_revenue = revenue - fee

        pos.update_after_sell(price, volume)
        self.cash += total_revenue
        
        # 如果卖光了，可以选择清理 key，也可以保留空对象
        if pos.total_volume == 0:
            del self.positions[code]
            
        return True, "Sell success"

    def get_total_value(self, current_prices):
        """
        计算账户总市值 (现金 + 持仓市值)
        current_prices: dict {code: price}
        """
        market_value = 0.0
        for code, pos in self.positions.items():
            price = current_prices.get(code, pos.avg_cost) # 如果没有当前价，暂用成本价估计（或报错）
            market_value += pos.total_volume * price
        return self.cash + market_value
