import os
import sys

import pandas as pd

# 添加项目根目录到 sys.path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from core import (
    EventEngine,
    FillEvent,
    MarketEvent,
    NaivePortfolio,
    OrderDirection,
    SignalEvent,
    SignalType,
)


def main():
    """
    演示 NaivePortfolio 的风险控制功能，包括：
    1. 资金检查 (Cash Check)
    2. 仓位上限限制 (Position Limit)
    3. 止损逻辑 (Stop Loss)
    4. 止盈逻辑 (Take Profit)
    5. 权益曲线追踪 (Equity Tracking)
    """
    print("--- NaivePortfolio 风险控制功能演示 ---")

    # 1. 初始化
    engine = EventEngine()
    # 初始资金 100,000
    portfolio = NaivePortfolio(engine, initial_capital=100000.0)

    # 设置自定义风控参数
    portfolio.max_single_pos_pct = 0.3  # 单股最高 30% 仓位
    portfolio.stop_loss_pct = 0.05       # 5% 止损
    portfolio.take_profit_pct = 0.15     # 15% 止盈

    symbol = "600036"

    # 2. 模拟买入信号与成交
    print("\n[步骤 1] 产生买入信号并成交")
    # 假设当前价格 100
    portfolio.latest_prices[symbol] = 100.0
    signal = SignalEvent(symbol, "2024-01-01", SignalType.LONG)
    portfolio.update_signal(signal)

    # 模拟成交：买入 100 股，价格 100
    fill = FillEvent(symbol, "2024-01-01", 100, OrderDirection.BUY, 100.0, commission=5.0)
    portfolio.update_fill(fill)

    # 3. 演示仓位上限检查
    print("\n[步骤 2] 仓位上限检查演示")
    # 再次尝试买入大量股票 (比如 300 股，价格 100 -> 30,000，超过 30% 限制)
    # 注意：NaivePortfolio 默认每次 100 股，这里我们手动触发多次或调整价格
    portfolio.latest_prices[symbol] = 300.0 # 提高价格使市值占比增加
    signal_large = SignalEvent(symbol, "2024-01-02", SignalType.LONG)
    portfolio.update_signal(signal_large)

    # 4. 演示止损触发
    print("\n[步骤 3] 止损触发演示")
    # 恢复价格并模拟下跌
    portfolio.latest_prices[symbol] = 100.0
    # 成本 100，跌到 94 (跌幅 6% > 5% 止损线)
    market_data = {"close": 94.0, "name": "2024-01-03"}
    market_event = MarketEvent(symbol, market_data)
    portfolio.update_market_value(market_event)

    # 5. 演示权益记录
    print("\n[步骤 4] 查看账户权益记录")
    equity_df = portfolio.get_equity_curve()
    if not equity_df.empty:
        print(equity_df[['datetime', 'cash', 'market_value', 'total']].tail())

if __name__ == "__main__":
    main()
