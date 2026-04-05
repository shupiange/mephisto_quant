from dataclasses import dataclass

from core.backtesting.strategy_base import Strategy


@dataclass
class TrendMacdDailyParams:
    min_amount: float = 5000000.0
    max_holdings: int = 10
    position_pct: float = 0.15
    stop_loss_pct: float = 0.08
    take_profit_pct: float = 0.20
    lot_size: int = 100


class TrendMacdDailyStrategy(Strategy):
    numeric_fields = {
        'open', 'close', 'high', 'low', 'volume', 'amount',
        'diff', 'dea', 'macd', 'ma10', 'ma20', 'ma60'
    }

    def __init__(self, params=None):
        self.params = params or TrendMacdDailyParams()
        self.pending_buys = []
        self.last_signal_date = None
        self.last_candidates = []

    def initialize(self, context):
        self.pending_buys = []
        self.last_signal_date = None
        self.last_candidates = []

    def on_bar(self, context, bar_dict):
        snapshot = self._prepare_snapshot(bar_dict)
        if not snapshot:
            self.pending_buys = []
            self.last_candidates = []
            return

        self._execute_exit_orders(context, snapshot)
        self._execute_pending_buys(context, snapshot)
        self.last_candidates = self._rank_candidates(context, snapshot)
        self.pending_buys = [item['code'] for item in self.last_candidates]
        self.last_signal_date = next(iter(snapshot.values())).get('date')

    def _prepare_snapshot(self, bar_dict):
        snapshot = {}
        for code, raw_bar in bar_dict.items():
            if hasattr(raw_bar, "to_dict"):
                bar = raw_bar.to_dict()
            else:
                bar = dict(raw_bar)
            for field in self.numeric_fields:
                if field in bar and bar[field] is not None:
                    try:
                        bar[field] = float(bar[field])
                    except Exception:
                        pass
            snapshot[str(code)] = bar
        return snapshot

    def _execute_exit_orders(self, context, snapshot):
        for code, position in list(context.positions.items()):
            bar = snapshot.get(code)
            if not bar or position.available_volume <= 0:
                continue
            if self._should_sell(position.avg_cost, bar):
                context.sell(code, position.available_volume, bar['close'])

    def _execute_pending_buys(self, context, snapshot):
        if not self.pending_buys:
            return

        total_value = context.account.total_value or context.account.update_market_value(context.current_prices)
        max_new_positions = max(self.params.max_holdings - len(context.positions), 0)
        if max_new_positions <= 0:
            return

        for code in list(self.pending_buys):
            if max_new_positions <= 0:
                break

            if code in context.positions:
                continue

            bar = snapshot.get(code)
            if not bar:
                continue

            volume = self._calculate_buy_volume(context, total_value, bar['open'])
            if volume <= 0:
                continue

            success, _ = context.buy(code, volume, bar['open'])
            if success:
                max_new_positions -= 1

    def _calculate_buy_volume(self, context, total_value, open_price):
        target_value = total_value * self.params.position_pct
        affordable_value = min(target_value, context.cash)
        volume = int(affordable_value / open_price)
        volume = (volume // self.params.lot_size) * self.params.lot_size
        return max(volume, 0)

    def _rank_candidates(self, context, snapshot):
        slots = max(self.params.max_holdings - len(context.positions), 0)
        if slots <= 0:
            return []

        ranked = []
        for code, bar in snapshot.items():
            if code in context.positions:
                continue
            if not self._passes_pool_filter(bar):
                continue
            if not self._should_buy(bar):
                continue
            score = self._score_candidate(bar)
            ranked.append({'code': code, 'score': score})

        ranked.sort(key=lambda item: item['score'], reverse=True)
        return ranked[:slots]

    def _passes_pool_filter(self, bar):
        return (
            self._is_valid_number(bar.get('amount'))
            and self._is_valid_number(bar.get('close'))
            and self._is_valid_number(bar.get('ma60'))
            and bar['amount'] > self.params.min_amount
            and bar['close'] > bar['ma60']
        )

    def _should_buy(self, bar):
        required_fields = ['close', 'ma20', 'ma60', 'macd', 'diff', 'dea']
        if not all(self._is_valid_number(bar.get(field)) for field in required_fields):
            return False
        return (
            bar['close'] > bar['ma20']
            and bar['ma20'] > bar['ma60']
            and bar['macd'] > 0
            and bar['diff'] > bar['dea']
        )

    def _should_sell(self, avg_cost, bar):
        required_fields = ['close', 'ma10', 'macd']
        if not all(self._is_valid_number(bar.get(field)) for field in required_fields):
            return False
        price_change = 0.0
        if self._is_valid_number(avg_cost) and avg_cost > 0:
            price_change = (bar['close'] - avg_cost) / avg_cost
        return (
            bar['close'] < bar['ma10']
            or bar['macd'] < 0
            or price_change <= -self.params.stop_loss_pct
            or price_change >= self.params.take_profit_pct
        )

    def _score_candidate(self, bar):
        ma20_strength = (bar['close'] - bar['ma20']) / bar['ma20'] if bar['ma20'] else 0.0
        ma60_strength = (bar['ma20'] - bar['ma60']) / bar['ma60'] if bar['ma60'] else 0.0
        macd_strength = bar['diff'] - bar['dea']
        return float(ma20_strength + ma60_strength + macd_strength + bar['macd'])

    def _is_valid_number(self, value):
        if value is None:
            return False
        try:
            return value == value
        except Exception:
            return False
