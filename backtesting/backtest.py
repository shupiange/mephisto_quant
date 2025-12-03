import pandas as pd
import numpy as np
from typing import Dict, Callable, Optional, List


class SimpleBacktest:
    """A simple backtest engine for single stock tickers.

    - Expects `data` to be a pandas DataFrame with a DatetimeIndex and columns: ['open','high','low','close'].
    - `strategy` is a callable that receives the same DataFrame and returns a pandas Series of signals
      with values: 1 (go long), 0 (flat), -1 (go short) — executed on the next bar's open price.

    Behavior:
    - Executes trades at the next bar's open price, applying commission and slippage.
    - Supports position sizing by fixed `size` fraction of capital (0 < size <= 1).
    - Tracks trade list, position, cash, and equity curve.
    - Computes basic performance metrics.
    """

    def __init__(
        self,
        data: pd.DataFrame,
        strategy: Callable[[pd.DataFrame], pd.Series],
        initial_capital: float = 100_000.0,
        size: float = 1.0,
        commission: float = 0.0,
        slippage: float = 0.0,
        allow_short: bool = False,
    ):
        if not isinstance(data, pd.DataFrame):
            raise ValueError("data must be a pandas DataFrame")
        for col in ["open", "high", "low", "close"]:
            if col not in data.columns:
                raise ValueError(f"data must contain '{col}' column")

        self.data = data.sort_index().copy()
        self.strategy = strategy
        self.initial_capital = float(initial_capital)
        self.size = float(size)
        self.commission = float(commission)
        self.slippage = float(slippage)
        self.allow_short = bool(allow_short)

        # runtime state
        self.signals: Optional[pd.Series] = None
        self.trades: List[Dict] = []
        self.equity_curve: Optional[pd.DataFrame] = None
        self.position_shares = 0.0
        self.position_side = 0  # -1 short, 0 flat, 1 long
        self.cash = float(initial_capital)
        self.total_value = float(initial_capital)

    def _execute_trade(self, date, action: str, price: float, shares: int):
        """Record execution and update cash/position values.

        action: 'BUY', 'SELL', 'SHORT', 'COVER'
        """
        trade_value = price * shares
        commission_cost = abs(trade_value) * self.commission
        slippage_cost = abs(trade_value) * self.slippage
        total_cost = trade_value + commission_cost + slippage_cost if action in ("BUY", "COVER") else -trade_value + commission_cost + slippage_cost

        # Update position and cash based on action
        if action == "BUY":
            self.position_shares += shares
            self.cash -= trade_value + commission_cost + slippage_cost
            self.position_side = 1
        elif action == "SELL":
            self.position_shares -= shares
            # if fully closed, set side to 0
            if self.position_shares == 0:
                self.position_side = 0
            self.cash += trade_value - commission_cost - slippage_cost
        elif action == "SHORT":
            self.position_shares -= shares
            self.cash += trade_value - commission_cost - slippage_cost
            self.position_side = -1
        elif action == "COVER":
            self.position_shares += shares
            if self.position_shares == 0:
                self.position_side = 0
            self.cash -= trade_value + commission_cost + slippage_cost
        else:
            raise ValueError("unsupported action")

        self.total_value = self.cash + self.position_shares * price

        self.trades.append(
            {
                "datetime": date,
                "action": action,
                "price": price,
                "shares": shares,
                "cash": self.cash,
                "total_value": self.total_value,
                "commission": commission_cost,
                "slippage": slippage_cost,
            }
        )

    def run(self):
        """Run backtest with the provided strategy.

        - The strategy is evaluated on the available data. The returned `signals` series is aligned to the input index.
        - Trades execute at the next index 'open' price. If no next bar exists, trade is ignored.
        """
        self.signals = self.strategy(self.data).reindex(self.data.index).fillna(0).astype(int)

        equity = []
        # iterate bars; execute orders at next bar open
        for i in range(len(self.data)):
            current_index = self.data.index[i]
            current_close = self.data['close'].iloc[i]
            current_signal = int(self.signals.iloc[i])

            # find next bar for execution
            if i + 1 < len(self.data):
                exec_index = self.data.index[i + 1]
                exec_price = self.data['open'].iloc[i + 1]
            else:
                exec_index = current_index
                exec_price = self.data['close'].iloc[i]

            # decide trade based on current position and signal
            if self.position_side == 0:
                # flat -> enter if signal != 0
                if current_signal == 1:
                    # go long
                    alloc = self.cash * self.size
                    shares = int(np.floor(alloc / (exec_price * (1 + self.slippage))))
                    if shares > 0:
                        self._execute_trade(exec_index, "BUY", exec_price * (1 + self.slippage), shares)
                elif current_signal == -1 and self.allow_short:
                    # go short
                    alloc = self.total_value * self.size
                    shares = int(np.floor(alloc / (exec_price * (1 + self.slippage))))
                    if shares > 0:
                        self._execute_trade(exec_index, "SHORT", exec_price * (1 + self.slippage), shares)
            elif self.position_side == 1:
                # we're long; exit if signal != 1
                if current_signal != 1:
                    shares = int(self.position_shares)
                    if shares > 0:
                        self._execute_trade(exec_index, "SELL", exec_price * (1 - self.slippage), shares)
            elif self.position_side == -1:
                # we're short; exit if signal != -1
                if current_signal != -1:
                    shares = int(abs(self.position_shares))
                    if shares > 0:
                        self._execute_trade(exec_index, "COVER", exec_price * (1 - self.slippage), shares)

            # update equity for day using close price
            price_for_valuation = self.data['close'].iloc[i]
            self.total_value = self.cash + self.position_shares * price_for_valuation
            equity.append({"datetime": current_index, "cash": self.cash, "position_shares": self.position_shares, "price": price_for_valuation, "total_value": self.total_value})

        self.equity_curve = pd.DataFrame(equity).set_index('datetime')

    def get_signals(self) -> pd.Series:
        if self.signals is None:
            raise ValueError("Please run the `run` method first to generate signals and trades.")
        return self.signals

    def get_trades(self) -> pd.DataFrame:
        return pd.DataFrame(self.trades)

    def get_equity_curve(self) -> pd.DataFrame:
        return self.equity_curve.copy() if self.equity_curve is not None else pd.DataFrame()

    def summary(self) -> Dict:
        """Return a dictionary with basic performance metrics.

        Metrics computed: total_return, cagr (annualized), max_drawdown, sharpe (assuming daily returns),
        num_trades, win_rate, avg_return_per_trade.
        """
        if self.equity_curve is None:
            raise ValueError("Please run the backtest first (run) before requesting a summary.")

        eq = self.equity_curve.copy()
        eq['returns'] = eq['total_value'].pct_change().fillna(0)
        total_return = eq['total_value'].iloc[-1] / eq['total_value'].iloc[0] - 1
        n_days = (eq.index[-1] - eq.index[0]).days
        if n_days > 0:
            cagr = (1 + total_return) ** (365.0 / n_days) - 1
        else:
            cagr = 0.0

        # max drawdown
        eq['cummax'] = eq['total_value'].cummax()
        eq['drawdown'] = (eq['total_value'] - eq['cummax']) / eq['cummax']
        max_drawdown = float(eq['drawdown'].min())

        # sharpe (simple): mean(daily returns) / std(daily returns) * sqrt(252)
        mean_ret = eq['returns'].mean()
        std_ret = eq['returns'].std()
        sharpe = float((mean_ret / std_ret) * np.sqrt(252.0)) if std_ret > 0 else 0.0

        trades_df = pd.DataFrame(self.trades)
        num_trades = len(trades_df)
        win_rate = None
        avg_return_per_trade = None
        if num_trades > 0:
            # compute trade returns by pairing entries and exits
            # We'll compute returns per round-trip pair (entry->exit) for simplicity
            trip_returns = []
            entry = None
            entry_price = None
            entry_action = None
            for tr in self.trades:
                if tr['action'] in ('BUY', 'SHORT') and entry is None:
                    entry = tr
                    entry_price = tr['price']
                    entry_action = tr['action']
                elif tr['action'] in ('SELL', 'COVER') and entry is not None:
                    # compute return relative to entry_price
                    exit_price = tr['price']
                    if entry_action == 'BUY' and entry_price > 0:
                        trip_returns.append((exit_price - entry_price) / entry_price)
                    elif entry_action == 'SHORT' and entry_price > 0:
                        trip_returns.append((entry_price - exit_price) / entry_price)
                    entry = None
            if len(trip_returns) > 0:
                trip_returns = np.array(trip_returns)
                win_rate = float((trip_returns > 0).mean())
                avg_return_per_trade = float(trip_returns.mean())

        return {
            'initial_capital': self.initial_capital,
            'final_capital': float(self.equity_curve['total_value'].iloc[-1]),
            'total_return': float(total_return),
            'cagr': float(cagr),
            'max_drawdown': float(max_drawdown),
            'sharpe': float(sharpe),
            'num_trades': num_trades,
            'win_rate': win_rate,
            'avg_return_per_trade': avg_return_per_trade,
        }


if __name__ == "__main__":
    # small example usage: 5/20 SMA crossover
    def sma_strategy(df: pd.DataFrame) -> pd.Series:
        short = df['close'].rolling(5).mean()
        long = df['close'].rolling(20).mean()
        sig = pd.Series(0, index=df.index)
        sig[short > long] = 1
        sig[short < long] = 0
        return sig

    # create fake data
    dates = pd.date_range("2020-01-01", periods=200, freq='B')
    np.random.seed(42)
    prices = np.cumprod(1 + np.random.normal(0, 0.01, len(dates))) * 100
    df = pd.DataFrame({'open': prices, 'high': prices * 1.01, 'low': prices * 0.99, 'close': prices}, index=dates)

    bt = SimpleBacktest(df, sma_strategy, initial_capital=10000, size=1.0, commission=0.0005, slippage=0.0001)
    bt.run()
    print(bt.get_trades().head())
    print(bt.summary())