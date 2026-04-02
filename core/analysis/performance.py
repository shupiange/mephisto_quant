import pandas as pd
import numpy as np
from typing import List, Tuple


class PerformanceAnalyzer:
    def __init__(self, equity_df, trades_df, initial_cash,
                 risk_free_rate=0.03, trading_days_per_year=242):
        """
        equity_df: DataFrame with columns [date, total_value, cash]
        trades_df: DataFrame from TradeLogger.to_dataframe()
        initial_cash: 初始资金
        risk_free_rate: 无风险利率 (年化, 默认 3%)
        trading_days_per_year: A股年交易日数 (默认 242)
        """
        self.equity_df = equity_df.copy()
        self.trades_df = trades_df.copy()
        self.initial_cash = float(initial_cash)
        self.risk_free_rate = risk_free_rate
        self.trading_days_per_year = trading_days_per_year

        self._equity_series = self.equity_df['total_value'].astype(float)
        self._round_trips = None

    # ── 收益指标 ──

    def total_return(self) -> float:
        if self._equity_series.empty:
            return 0.0
        final = self._equity_series.iloc[-1]
        return (final - self.initial_cash) / self.initial_cash

    def annualized_return(self) -> float:
        n_days = len(self._equity_series)
        if n_days <= 1:
            return 0.0
        total = self.total_return()
        years = n_days / self.trading_days_per_year
        if years <= 0 or total <= -1:
            return 0.0
        return (1 + total) ** (1 / years) - 1

    def daily_returns(self) -> pd.Series:
        return self._equity_series.pct_change().dropna()

    # ── 风险指标 ──

    def max_drawdown(self) -> float:
        dd = self.drawdown_series()
        if dd.empty:
            return 0.0
        return dd.max()

    def max_drawdown_duration(self) -> int:
        dd = self.drawdown_series()
        if dd.empty:
            return 0
        in_dd = dd > 0
        if not in_dd.any():
            return 0
        groups = (~in_dd).cumsum()
        dd_groups = groups[in_dd]
        if dd_groups.empty:
            return 0
        return dd_groups.value_counts().max()

    def drawdown_series(self) -> pd.Series:
        if self._equity_series.empty:
            return pd.Series(dtype=float)
        running_max = self._equity_series.cummax()
        dd = (running_max - self._equity_series) / running_max
        return dd

    def volatility(self) -> float:
        dr = self.daily_returns()
        if dr.empty:
            return 0.0
        return dr.std() * np.sqrt(self.trading_days_per_year)

    # ── 风险调整收益 ──

    def sharpe_ratio(self) -> float:
        vol = self.volatility()
        if vol == 0:
            return 0.0
        return (self.annualized_return() - self.risk_free_rate) / vol

    def sortino_ratio(self) -> float:
        dr = self.daily_returns()
        if dr.empty:
            return 0.0
        downside = dr[dr < 0]
        if downside.empty:
            return float('inf') if self.annualized_return() > self.risk_free_rate else 0.0
        downside_std = downside.std() * np.sqrt(self.trading_days_per_year)
        if downside_std == 0:
            return 0.0
        return (self.annualized_return() - self.risk_free_rate) / downside_std

    def calmar_ratio(self) -> float:
        mdd = self.max_drawdown()
        if mdd == 0:
            return float('inf') if self.annualized_return() > 0 else 0.0
        return self.annualized_return() / mdd

    # ── 交易统计 ──

    def _match_round_trips(self) -> List[dict]:
        """FIFO 配对 BUY/SELL，计算每笔交易盈亏"""
        if self._round_trips is not None:
            return self._round_trips

        if self.trades_df.empty:
            self._round_trips = []
            return self._round_trips

        round_trips = []
        # 每只股票独立配对
        open_buys = {}  # code -> list of {price, volume, date, remaining}

        for _, trade in self.trades_df.iterrows():
            code = trade['code']
            if trade['direction'] == 'BUY':
                if code not in open_buys:
                    open_buys[code] = []
                open_buys[code].append({
                    'price': trade['price'],
                    'volume': trade['volume'],
                    'date': trade['date'],
                    'remaining': trade['volume'],
                })
            elif trade['direction'] == 'SELL':
                sell_vol = trade['volume']
                sell_price = trade['price']
                sell_date = trade['date']

                if code not in open_buys:
                    continue

                while sell_vol > 0 and open_buys[code]:
                    buy = open_buys[code][0]
                    matched = min(sell_vol, buy['remaining'])

                    pnl = (sell_price - buy['price']) * matched
                    round_trips.append({
                        'code': code,
                        'buy_date': buy['date'],
                        'sell_date': sell_date,
                        'buy_price': buy['price'],
                        'sell_price': sell_price,
                        'volume': matched,
                        'pnl': pnl,
                        'return_pct': (sell_price - buy['price']) / buy['price'],
                    })

                    buy['remaining'] -= matched
                    sell_vol -= matched

                    if buy['remaining'] <= 0:
                        open_buys[code].pop(0)

                if not open_buys[code]:
                    del open_buys[code]

        self._round_trips = round_trips
        return self._round_trips

    def total_trades(self) -> int:
        return len(self._match_round_trips())

    def win_rate(self) -> float:
        rts = self._match_round_trips()
        if not rts:
            return 0.0
        wins = sum(1 for r in rts if r['pnl'] > 0)
        return wins / len(rts)

    def profit_factor(self) -> float:
        rts = self._match_round_trips()
        if not rts:
            return 0.0
        gross_profit = sum(r['pnl'] for r in rts if r['pnl'] > 0)
        gross_loss = abs(sum(r['pnl'] for r in rts if r['pnl'] < 0))
        if gross_loss == 0:
            return float('inf') if gross_profit > 0 else 0.0
        return gross_profit / gross_loss

    def avg_win(self) -> float:
        rts = self._match_round_trips()
        wins = [r['pnl'] for r in rts if r['pnl'] > 0]
        return np.mean(wins) if wins else 0.0

    def avg_loss(self) -> float:
        rts = self._match_round_trips()
        losses = [r['pnl'] for r in rts if r['pnl'] < 0]
        return np.mean(losses) if losses else 0.0

    def avg_holding_days(self) -> float:
        rts = self._match_round_trips()
        if not rts:
            return 0.0
        days = []
        for r in rts:
            try:
                buy_dt = pd.to_datetime(str(r['buy_date']))
                sell_dt = pd.to_datetime(str(r['sell_date']))
                days.append((sell_dt - buy_dt).days)
            except Exception:
                days.append(0)
        return np.mean(days) if days else 0.0

    def max_consecutive_wins(self) -> int:
        return self._max_consecutive(True)

    def max_consecutive_losses(self) -> int:
        return self._max_consecutive(False)

    def _max_consecutive(self, is_win: bool) -> int:
        rts = self._match_round_trips()
        if not rts:
            return 0
        max_count = 0
        count = 0
        for r in rts:
            if (r['pnl'] > 0) == is_win:
                count += 1
                max_count = max(max_count, count)
            else:
                count = 0
        return max_count

    # ── 汇总 ──

    def summary(self) -> dict:
        return {
            'total_return': self.total_return(),
            'annualized_return': self.annualized_return(),
            'max_drawdown': self.max_drawdown(),
            'max_drawdown_duration': self.max_drawdown_duration(),
            'sharpe_ratio': self.sharpe_ratio(),
            'sortino_ratio': self.sortino_ratio(),
            'calmar_ratio': self.calmar_ratio(),
            'volatility': self.volatility(),
            'total_trades': self.total_trades(),
            'win_rate': self.win_rate(),
            'profit_factor': self.profit_factor(),
            'avg_win': self.avg_win(),
            'avg_loss': self.avg_loss(),
            'avg_holding_days': self.avg_holding_days(),
            'max_consecutive_wins': self.max_consecutive_wins(),
            'max_consecutive_losses': self.max_consecutive_losses(),
        }

    def print_report(self):
        s = self.summary()
        from core.analysis.report import ReportFormatter
        print(ReportFormatter.text_report(s))
