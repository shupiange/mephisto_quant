from dataclasses import dataclass

from core.backtesting.strategy_base import Strategy


@dataclass
class PullbackBreakoutDailyParams:
    min_amount: float = 8000000.0
    max_holdings: int = 8
    position_pct: float = 0.12
    stop_loss_pct: float = 0.07
    trail_stop_pct: float = 0.06
    min_gain_to_trail_pct: float = 0.10
    lot_size: int = 100
    history_window: int = 30
    prior_strength_lookback: int = 20
    min_prior_runup_pct: float = 0.15
    pullback_from_high_min_pct: float = 0.03
    pullback_from_high_max_pct: float = 0.18
    ma20_proximity_pct: float = 0.03
    rebound_volume_ratio: float = 1.05


class PullbackBreakoutDailyStrategy(Strategy):
    numeric_fields = {
        'open', 'close', 'high', 'low', 'volume', 'amount',
        'diff', 'dea', 'macd', 'ma10', 'ma20', 'ma60'
    }

    def __init__(self, params=None):
        self.params = params or PullbackBreakoutDailyParams()
        self.pending_buys = []
        self.last_signal_date = None
        self.last_candidates = []
        self.price_history = {}
        self.peak_prices = {}

    def initialize(self, context):
        self.pending_buys = []
        self.last_signal_date = None
        self.last_candidates = []
        self.price_history = {}
        self.peak_prices = {}

    def on_bar(self, context, bar_dict):
        snapshot = self._prepare_snapshot(bar_dict)
        if not snapshot:
            self.pending_buys = []
            self.last_candidates = []
            return

        self._append_history(snapshot)
        self._update_peak_prices(context, snapshot)
        self._execute_exit_orders(context, snapshot)
        self._cleanup_position_state(context)
        self._execute_pending_buys(context, snapshot)
        self._refresh_new_position_peaks(context, snapshot)
        self.last_candidates = self._rank_candidates(context, snapshot)
        self.pending_buys = [item['code'] for item in self.last_candidates]
        self.last_signal_date = next(iter(snapshot.values())).get('date')

    def _prepare_snapshot(self, bar_dict):
        snapshot = {}
        for code, raw_bar in bar_dict.items():
            if hasattr(raw_bar, 'to_dict'):
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

    def _append_history(self, snapshot):
        for code, bar in snapshot.items():
            items = self.price_history.setdefault(code, [])
            items.append(dict(bar))
            if len(items) > self.params.history_window:
                del items[:-self.params.history_window]

    def _update_peak_prices(self, context, snapshot):
        for code, position in context.positions.items():
            if position.total_volume <= 0:
                continue
            bar = snapshot.get(code)
            if not bar:
                continue
            peak_candidate = bar.get('high')
            if not self._is_valid_number(peak_candidate):
                peak_candidate = bar.get('close')
            if not self._is_valid_number(peak_candidate):
                continue
            existing_peak = self.peak_prices.get(code, position.avg_cost)
            self.peak_prices[code] = max(existing_peak, peak_candidate)

    def _execute_exit_orders(self, context, snapshot):
        for code, position in list(context.positions.items()):
            bar = snapshot.get(code)
            if not bar or position.available_volume <= 0:
                continue
            peak_price = self.peak_prices.get(code, position.avg_cost)
            if self._should_sell(position.avg_cost, peak_price, bar):
                context.sell(code, position.available_volume, bar['close'])

    def _cleanup_position_state(self, context):
        active_codes = set(context.positions.keys())
        for code in list(self.peak_prices.keys()):
            if code not in active_codes:
                self.peak_prices.pop(code, None)

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
            if not bar or not self._is_valid_number(bar.get('open')):
                continue
            volume = self._calculate_buy_volume(context, total_value, bar['open'])
            if volume <= 0:
                continue
            success, _ = context.buy(code, volume, bar['open'])
            if success:
                max_new_positions -= 1
                self.peak_prices[code] = bar['open']

    def _refresh_new_position_peaks(self, context, snapshot):
        for code, position in context.positions.items():
            if position.total_volume <= 0:
                continue
            if code not in self.peak_prices:
                bar = snapshot.get(code)
                if not bar:
                    continue
                peak_candidate = bar.get('high')
                if not self._is_valid_number(peak_candidate):
                    peak_candidate = bar.get('close')
                if self._is_valid_number(peak_candidate):
                    self.peak_prices[code] = max(position.avg_cost, peak_candidate)

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
            history = self.price_history.get(code, [])
            if not self._passes_pool_filter(history, bar):
                continue
            if not self._should_buy(history, bar):
                continue
            ranked.append({'code': code, 'score': self._score_candidate(history, bar)})

        ranked.sort(key=lambda item: item['score'], reverse=True)
        return ranked[:slots]

    def _passes_pool_filter(self, history, bar):
        required = ['amount', 'close', 'ma20', 'ma60', 'macd']
        if not all(self._is_valid_number(bar.get(field)) for field in required):
            return False
        return (
            len(history) >= max(10, self.params.prior_strength_lookback)
            and bar['amount'] > self.params.min_amount
            and bar['close'] > bar['ma60']
            and bar['ma20'] > bar['ma60']
            and bar['macd'] > 0
        )

    def _should_buy(self, history, bar):
        if len(history) < max(10, self.params.prior_strength_lookback):
            return False
        if not self._has_prior_strength(history):
            return False
        if not self._is_pullback_ready(history, bar):
            return False
        if not all(self._is_valid_number(bar.get(field)) for field in ['open', 'close', 'diff', 'dea']):
            return False
        recent_amount_avg = self._recent_average(history, 'amount', 5)
        return (
            bar['close'] > bar['open']
            and bar['close'] >= bar['ma20']
            and bar['diff'] > bar['dea']
            and bar['amount'] >= max(self.params.min_amount, recent_amount_avg * self.params.rebound_volume_ratio)
        )

    def _should_sell(self, avg_cost, peak_price, bar):
        required = ['close', 'ma10', 'diff', 'dea']
        if not all(self._is_valid_number(bar.get(field)) for field in required):
            return False
        if not self._is_valid_number(avg_cost) or avg_cost <= 0:
            return False
        current_return = (bar['close'] - avg_cost) / avg_cost
        trailing_drawdown = 0.0
        if self._is_valid_number(peak_price) and peak_price > 0:
            trailing_drawdown = (peak_price - bar['close']) / peak_price
        return (
            current_return <= -self.params.stop_loss_pct
            or (
                current_return >= self.params.min_gain_to_trail_pct
                and trailing_drawdown >= self.params.trail_stop_pct
            )
            or bar['close'] < bar['ma10']
            or bar['diff'] < bar['dea']
        )

    def _has_prior_strength(self, history):
        recent = history[-self.params.prior_strength_lookback:]
        closes = [item.get('close') for item in recent if self._is_valid_number(item.get('close'))]
        if len(closes) < 5:
            return False
        min_close = min(closes)
        max_close = max(closes)
        if min_close <= 0:
            return False
        runup = (max_close - min_close) / min_close
        return runup >= self.params.min_prior_runup_pct

    def _is_pullback_ready(self, history, bar):
        recent = history[-self.params.prior_strength_lookback:]
        highs = [item.get('high') if self._is_valid_number(item.get('high')) else item.get('close') for item in recent]
        highs = [item for item in highs if self._is_valid_number(item)]
        if not highs:
            return False
        recent_high = max(highs)
        if recent_high <= 0 or not self._is_valid_number(bar.get('close')) or not self._is_valid_number(bar.get('ma20')):
            return False
        pullback_pct = (recent_high - bar['close']) / recent_high
        proximity_pct = abs(bar['close'] - bar['ma20']) / bar['ma20'] if bar['ma20'] else 1.0
        stable_near_ma20 = bar['close'] >= bar['ma20'] * 0.98 and proximity_pct <= self.params.ma20_proximity_pct
        touched_ma20 = any(
            self._is_valid_number(item.get('low')) and self._is_valid_number(item.get('ma20')) and item['low'] <= item['ma20'] * 1.01
            for item in history[-5:]
        )
        return (
            self.params.pullback_from_high_min_pct <= pullback_pct <= self.params.pullback_from_high_max_pct
            and stable_near_ma20
            and touched_ma20
        )

    def _score_candidate(self, history, bar):
        recent = history[-self.params.prior_strength_lookback:]
        closes = [item.get('close') for item in recent if self._is_valid_number(item.get('close'))]
        highs = [item.get('high') if self._is_valid_number(item.get('high')) else item.get('close') for item in recent]
        highs = [item for item in highs if self._is_valid_number(item)]
        min_close = min(closes) if closes else bar['close']
        recent_high = max(highs) if highs else bar['close']
        prior_runup = (recent_high - min_close) / min_close if min_close else 0.0
        pullback_pct = (recent_high - bar['close']) / recent_high if recent_high else 0.0
        volume_ratio = 1.0
        recent_amount_avg = self._recent_average(history, 'amount', 5)
        if recent_amount_avg > 0:
            volume_ratio = bar['amount'] / recent_amount_avg
        proximity_score = 1.0 - min(abs(bar['close'] - bar['ma20']) / bar['ma20'], 1.0)
        momentum_score = (bar['diff'] - bar['dea']) + max(bar['macd'], 0.0)
        return float(
            prior_runup * 4.0
            + volume_ratio * 1.5
            + proximity_score * 2.0
            + momentum_score
            - pullback_pct
        )

    def _recent_average(self, history, field, window):
        values = [item.get(field) for item in history[-window:] if self._is_valid_number(item.get(field))]
        if not values:
            return 0.0
        return sum(values) / len(values)

    def _is_valid_number(self, value):
        if value is None:
            return False
        try:
            return value == value
        except Exception:
            return False
