
from core.backtesting.strategy_base import Strategy

class DemoStrategy(Strategy):
    """
    一个简单的示例策略：
    1. 第一天(20250101)：开盘买入
    2. 第二天(20250102)：持仓不动
    3. 第三天(20250103)：开盘卖出
    """
    def initialize(self, context):
        print(">>> 策略初始化成功")
        self.invested = False

    def on_day_start(self, context, date):
        print(f"--- 日始: {date} ---")

    def on_bar(self, context, bar_dict):
        # 获取当前时间戳
        ts = context.current_time
        # 获取股票代码 (假设只有一个)
        code = '000001'
        
        if code not in bar_dict:
            return
            
        price = bar_dict[code]['close']
        
        # 简单的择时逻辑
        if '2025-01-01' in ts and not self.invested:
            # 第一天有钱就买
            vol = 1000
            print(f"[{ts}] 信号触发：买入 {code} {vol}股 @ {price}")
            success, msg = context.buy(code, vol)
            if success:
                self.invested = True
                print(f"   成交: {msg}")
            else:
                print(f"   失败: {msg}")

        elif '2025-01-03' in ts and self.invested:
            # 第三天卖出
            # 注意：由于是 T+1,第一天买入的第二天就可以卖,这里演示第三天卖
            pos = context.positions.get(code)
            if pos and pos.available_volume > 0:
                print(f"[{ts}] 信号触发：卖出 {code} {pos.available_volume}股 @ {price}")
                success, msg = context.sell(code, pos.available_volume)
                if success:
                    self.invested = False # 简单标记
                    print(f"   成交: {msg}")
                else:
                    print(f"   失败: {msg}")

    def on_day_end(self, context, date):
        # 打印每日净值
        val = context.account.total_value
        cash = context.account.cash
        print(f"--- 日终: {date}, 账户总值: {val:.2f}, 现金: {cash:.2f} ---")
