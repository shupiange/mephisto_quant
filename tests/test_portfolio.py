import unittest
from datetime import datetime
from queue import Queue
import sys
import os

# 将项目根目录添加到 sys.path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from core.portfolio.position import NaivePortfolio
from core.engine.event_engine import EventEngine
from core.engine.event import MarketEvent, FillEvent, OrderDirection, SignalEvent, SignalType, OrderEvent

class MockEventEngine:
    def __init__(self):
        self.queue = Queue()
    
    def put(self, event):
        self.queue.put(event)

class TestNaivePortfolio(unittest.TestCase):
    def setUp(self):
        self.engine = MockEventEngine()
        self.portfolio = NaivePortfolio(self.engine, initial_capital=100000.0)
        # 设置测试用的风控参数
        self.portfolio.stop_loss_pct = 0.1
        self.portfolio.trailing_stop_pct = 0.1
        self.portfolio.take_profit_pct = 10.0 # 禁用止盈
        self.portfolio.buy_pct = 0.5
        self.portfolio.sell_pct = 1.0

    def test_buy_order_generation(self):
        """测试买入订单生成逻辑"""
        # 1. 发送行情更新价格
        market_event = MarketEvent("000001", {"close": 10.0, "high": 10.0, "low": 10.0, "open": 10.0, "volume": 100})
        self.portfolio.update_market_value(market_event)
        
        # 2. 发送买入信号
        signal = SignalEvent("000001", str(datetime.now()), SignalType.LONG)
        self.portfolio.update_signal(signal)
        
        # 3. 检查生成的订单
        self.assertFalse(self.engine.queue.empty())
        order = self.engine.queue.get()
        self.assertEqual(order.symbol, "000001")
        self.assertEqual(order.direction, OrderDirection.BUY)
        
        # 计算逻辑:
        # 1. 初始资金 100,000
        # 2. buy_pct = 0.5 -> 50,000 用于买入
        # 3. max_single_pos_pct = 0.2 (默认) -> 单只股票最多 20,000 市值
        # 4. 价格 10.0
        # 5. 预期数量: min(50000/10, 20000/10) = 2000 股
        self.assertEqual(order.quantity, 2000)

    def test_position_update(self):
        """测试持仓更新逻辑"""
        # 模拟买入成交
        fill = FillEvent(
            symbol="000001",
            datetime=str(datetime.now()),
            quantity=1000,
            direction=OrderDirection.BUY,
            fill_cost=10.0,
            commission=5.0
        )
        self.portfolio.update_fill(fill)
        
        self.assertEqual(self.portfolio.current_positions["000001"], 1000)
        self.assertEqual(self.portfolio.position_costs["000001"], 10.0)
        self.assertAlmostEqual(self.portfolio.current_cash, 100000.0 - 10000.0 - 5.0)

    def test_stop_loss(self):
        """测试止损触发"""
        # 1. 先建立持仓: 成本 10.0
        fill = FillEvent("000001", str(datetime.now()), 1000, OrderDirection.BUY, 10.0, 5.0)
        self.portfolio.update_fill(fill)
        
        # 2. 价格下跌 15% (超过 10% 止损线) -> 8.5
        market_event = MarketEvent("000001", {"close": 8.5, "high": 8.5, "low": 8.5, "open": 8.5, "volume": 100})
        self.portfolio.update_market_value(market_event)
        
        # 3. 检查是否生成卖出订单
        found_exit = False
        while not self.engine.queue.empty():
            event = self.engine.queue.get()
            if isinstance(event, OrderEvent) and event.direction == OrderDirection.SELL:
                found_exit = True
                self.assertEqual(event.quantity, 1000)
                break
        
        self.assertTrue(found_exit, "应触发止损卖出订单")

    def test_negative_position_prevention(self):
        """测试防止负持仓"""
        # 1. 建立 1000 股持仓
        fill_buy = FillEvent("000001", str(datetime.now()), 1000, OrderDirection.BUY, 10.0, 5.0)
        self.portfolio.update_fill(fill_buy)
        
        # 2. 尝试卖出 2000 股 (异常情况)
        fill_sell = FillEvent("000001", str(datetime.now()), 2000, OrderDirection.SELL, 11.0, 5.0)
        self.portfolio.update_fill(fill_sell)
        
        # 3. 检查持仓是否为 0 而不是 -1000
        self.assertEqual(self.portfolio.current_positions["000001"], 0)

if __name__ == '__main__':
    unittest.main()
