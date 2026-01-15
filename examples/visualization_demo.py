import os
import sys
import pandas as pd
import numpy as np

# 添加项目根目录到 sys.path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from core import Visualizer, IndicatorHelper

def main():
    """
    演示如何使用 Visualizer 进行结果可视化。
    """
    # 1. 生成 100 天的模拟数据
    dates = pd.date_range(start='2024-01-01', periods=100)
    close_prices = 100 + np.cumsum(np.random.randn(100))
    df = pd.DataFrame({
        'open': close_prices - np.random.rand(100),
        'high': close_prices + np.random.rand(100),
        'low': close_prices - np.random.rand(100) - 1,
        'close': close_prices,
        'volume': np.random.randint(1000, 5000, size=100)
    }, index=dates)

    # 2. 添加指标用于显示
    IndicatorHelper.add_sma(df, length=10)
    IndicatorHelper.add_sma(df, length=30)

    # 3. 绘制 K 线图
    print("正在绘制 K 线图...")
    # 注意：在没有图形界面的服务器环境下，此操作可能不会显示窗口，但代码逻辑已跑通
    Visualizer.plot_candlestick(df, title="Visualization Demo: K-Line & Indicators", indicators=['SMA_10', 'SMA_30'])

    # 4. 模拟账户权益曲线
    equity_df = pd.DataFrame({
        'total': 100000 * (1 + 0.002 * np.arange(100) + 0.02 * np.random.randn(100))
    }, index=dates)

    # 5. 绘制权益曲线
    print("正在绘制权益曲线...")
    Visualizer.plot_equity_curve(equity_df, title="Backtest Result: Equity Curve")

    # 6. 绘制回撤图
    print("正在绘制回撤图...")
    Visualizer.plot_drawdown(equity_df)

if __name__ == "__main__":
    main()
