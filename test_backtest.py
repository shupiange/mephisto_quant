import pandas as pd
import numpy as np
from backtesting.backtest import SimpleBacktest


def test_simple_backtest_runs_and_returns_metrics():
    # create deterministic fake data
    dates = pd.date_range("2020-01-01", periods=60, freq='B')
    np.random.seed(42)
    prices = np.cumprod(1 + np.random.normal(0, 0.001, len(dates))) * 100
    df = pd.DataFrame({'open': prices, 'high': prices * 1.01, 'low': prices * 0.99, 'close': prices}, index=dates)

    def sma_strategy(dframe: pd.DataFrame):
        short = dframe['close'].rolling(3).mean()
        long = dframe['close'].rolling(10).mean()
        sig = pd.Series(0, index=dframe.index)
        sig[short > long] = 1
        sig[short < long] = 0
        return sig

    bt = SimpleBacktest(df, sma_strategy, initial_capital=10000, size=1.0, commission=0.0, slippage=0.0)
    bt.run()

    trades = bt.get_trades()
    eq = bt.get_equity_curve()
    summary = bt.summary()

    # Basic assertions
    assert not eq.empty, "equity curve should not be empty"
    assert 'total_value' in eq.columns
    assert isinstance(trades, pd.DataFrame)
    assert 'initial_capital' in summary
    assert summary['initial_capital'] == 10000
    assert summary['final_capital'] >= 0
