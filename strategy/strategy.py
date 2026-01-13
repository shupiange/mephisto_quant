from database.load_dataset import load_dataset
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

class BacktestEngine:
    def __init__(self, strategy_cls, codes, start_date, end_date, initial_cash=100000.0):
        self.strategy_cls = strategy_cls
        self.codes = codes
        self.start_date = start_date
        self.end_date = end_date
        self.initial_cash = initial_cash
        self.hold = Hold(initial_cash)
        self.context = Context(self.hold)
        self.daily_data = {} # date -> {code -> {open, close}}

    def _parse_custom_date(self, date_str):
        # 格式: 20251201093000000 -> YYYYMMDD
        # 只需要提取日期部分进行分组
        return str(date_str)[:8]

    def load_data(self):
        print("Loading data...")
        all_dfs = []
        for code in self.codes:
            # 假设 load_dataset 返回 DataFrame
            # 列: open, close, max, min, date
            df = load_dataset(code, start_date=self.start_date, end_date=self.end_date)
            if df is None or df.empty:
                print(f"Warning: No data for {code}")
                continue
            
            df = df.copy()
            df['code'] = code
            df['day_str'] = df['date'].apply(self._parse_custom_date)
            all_dfs.append(df)
        
        if not all_dfs:
            raise ValueError("No data loaded")

        combined_df = pd.concat(all_dfs)
        
        # 按日期分组处理
        # 我们需要每一天的 Open (09:30 bar's open) 和 Close (15:00 bar's close)
        # 假设数据是按时间排序的
        
        grouped = combined_df.groupby(['day_str', 'code'])
        
        processed_data = {} # day_str -> {code -> {open_price, close_price}}
        
        for (day, code), group in grouped:
            # 按 date 字符串排序确保时间顺序
            group = group.sort_values('date')
            
            # 取第一根K线的Open作为当日Open
            day_open = group.iloc[0]['open']
            # 取最后一根K线的Close作为当日Close
            day_close = group.iloc[-1]['close']
            
            if day not in processed_data:
                processed_data[day] = {}
            
            processed_data[day][code] = {
                'open': day_open,
                'close': day_close,
                'date': day # use the date string as ID
            }
            
        self.daily_data = processed_data
        print(f"Data processed. Total trading days: {len(self.daily_data)}")

    def run(self):
        self.load_data()
        
        strategy = self.strategy_cls()
        strategy.initialize(self.context)
        
        sorted_days = sorted(self.daily_data.keys())
        
        history = []
        
        for day in sorted_days:
            day_quotes = self.daily_data[day]
            
            # 1. 每日结算 (T+1 解锁)
            self.hold.settle()
            
            # 2. 开盘竞价交易
            # 构造 current_prices 用于 context
            current_opens = {code: data['open'] for code, data in day_quotes.items()}
            self.context.update_env(current_opens, f"{day} Open")
            
            strategy.on_open(self.context, day_quotes)
            
            # 3. 收盘竞价交易
            current_closes = {code: data['close'] for code, data in day_quotes.items()}
            self.context.update_env(current_closes, f"{day} Close")
            
            strategy.on_close(self.context, day_quotes)
            
            # 4. 记录当日净值
            total_value = self.hold.get_total_value(current_closes)
            history.append({
                'date': day,
                'total_value': total_value,
                'cash': self.hold.cash
            })
            # print(f"Day {day} end. Value: {total_value:.2f}")

        return pd.DataFrame(history)

# ==========================================
# 示例策略与运行代码
# ==========================================
if __name__ == "__main__":
    # 简单的均线策略示例 (由于没有历史数据计算均线，这里仅演示简单的买卖逻辑)
    # 策略：第一天开盘买入，第二天收盘卖出
    
    class DemoStrategy(Strategy):
        def initialize(self, context):
            self.bought = False

        def on_open(self, context, quotes):
            # 简单的示例：如果有钱就买第一个代码
            if not self.bought:
                for code in quotes:
                    price = quotes[code]['open']
                    # 买入 1000 股
                    # 检查钱够不够
                    cost = price * 1000 * (1 + 0.0002)
                    if context.cash >= cost:
                        res, msg = context.buy(code, 1000)
                        if res:
                            print(f"[{context.current_time}] Buy {code} 1000 @ {price}")
                            self.bought = True
                        else:
                            print(f"[{context.current_time}] Buy failed: {msg}")
                    break
        
        def on_close(self, context, quotes):
            # 如果持仓超过1天（简单模拟），则卖出
            # 这里简化为：只要有持仓且能卖就卖
            for code, pos in context.positions.items():
                if pos.available_volume > 0:
                    price = quotes[code]['close']
                    res, msg = context.sell(code, pos.available_volume)
                    if res:
                        print(f"[{context.current_time}] Sell {code} {pos.total_volume} @ {price}")

    # 注意：由于无法真正连接用户的MySQL，这里只是定义了类。
    # 用户可以在自己的环境中运行此类。
    # 为了演示，我们可以 mock load_dataset
    
    import sys
    from unittest.mock import MagicMock
    
    # Mock data for demonstration if running directly
    def mock_load_dataset(code, start_date, end_date):
        # 构造假数据 2天
        dates = [
            '20251201093000000', '20251201100000000', '20251201150000000',
            '20251202093000000', '20251202100000000', '20251202150000000'
        ]
        opens = [10.0, 10.1, 10.5, 10.6, 10.7, 11.0]
        closes = [10.1, 10.2, 10.6, 10.7, 10.8, 11.1]
        
        return pd.DataFrame({
            'date': dates,
            'open': opens,
            'close': closes,
            'max': [x + 0.1 for x in opens],
            'min': [x - 0.1 for x in opens]
        })

    # 替换掉真实的 load_dataset 以便测试运行
    # import database.load_dataset 
    # database.load_dataset.load_dataset = mock_load_dataset
    
    # print("Running Test Backtest...")
    # engine = BacktestEngine(DemoStrategy, ['000001'], '20250101', '20250105')
    # # Monkey patch load_dataset for this instance
    # global load_dataset
    # load_dataset = mock_load_dataset
    # result = engine.run()
    # print(result)
