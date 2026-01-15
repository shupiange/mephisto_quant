import unittest
from queue import Queue
import sys
import os
import pandas as pd
import numpy as np
from unittest.mock import patch, MagicMock

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from strategies.moving_average_cross import MACrossStrategy
from core.engine.event import MarketEvent, SignalEvent, SignalType

class MockEventEngine:
    def __init__(self):
        self.queue = Queue()
    
    def put(self, event):
        self.queue.put(event)

class TestMACrossStrategy(unittest.TestCase):
    def setUp(self):
        self.engine = MockEventEngine()
        # 使用极短的窗口方便测试
        self.strategy = MACrossStrategy(short_window=2, long_window=3)
        self.strategy.set_engine(self.engine)
        self.strategy.filter_window = 5 # 趋势过滤窗口
        self.strategy.adx_threshold = 20 
        self.strategy.rsi_overbought = 80

    def _generate_market_data(self, prices):
        """辅助函数：批量生成行情数据"""
        events = []
        for p in prices:
            data = {
                "high": p + 1.0,
                "low": p - 1.0,
                "close": p,
                "open": p,
                "volume": 1000
            }
            events.append(MarketEvent("000001", data))
        return events

    @patch('core.utils.indicators.IndicatorHelper.add_adx')
    @patch('core.utils.indicators.IndicatorHelper.add_rsi')
    def test_golden_cross_signal(self, mock_rsi, mock_adx):
        """测试金叉信号生成"""
        # Mock 指标返回值，使其满足过滤条件
        # ADX > 20
        mock_adx.return_value = pd.DataFrame({'ADX': [30.0] * 100})
        # RSI < 80
        mock_rsi.return_value = pd.Series([50.0] * 100)

        # 构造: 跌 -> 盘整 -> 涨
        # Index:  0   1   2   3   4   5   6   7   8   9
        # Close: 20  19  18  17  16  15  15  15  16  20
        # MA2:       19.5 ...        15.5 15  15  15.5 18
        # MA3:           19 ...      16   15.3 15 15.3 17
        
        # 在 Index 8 (Close=16): MA2=15.5, MA3=15.3 (金叉!)
        # 且 Close(16) > MA5(16+15+15+15+16 / 5 = 15.4) -> 满足趋势过滤
        
        prices = [20, 19, 18, 17, 16, 15, 15, 15, 16, 20] 
        events = self._generate_market_data(prices)
        
        for event in events:
            self.strategy.calculate_signals(event)
            
        # 检查是否有买入信号
        found_buy = False
        while not self.engine.queue.empty():
            event = self.engine.queue.get()
            if isinstance(event, SignalEvent) and event.signal_type == SignalType.LONG:
                found_buy = True
                print(f"Found Signal at {event.datetime}")
                break
        
        self.assertTrue(found_buy, "应触发金叉买入信号")

    def test_not_enough_data(self):
        """测试数据不足时不产生信号"""
        event = MarketEvent("000001", {"close": 10.0, "high": 11, "low": 9, "open": 10, "volume": 100})
        self.strategy.calculate_signals(event)
        self.assertTrue(self.engine.queue.empty())

if __name__ == '__main__':
    unittest.main()
