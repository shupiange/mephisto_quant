from dataclasses import dataclass
import math

from core.backtesting.strategy_base import Strategy


@dataclass
class RelativeStrengthPullbackDailyParams:
    """
    第三版策略参数：
    核心思想是先在全市场里筛出“近期强势、长期也强势”的股票，
    再从里面寻找“回调到均线附近、重新放量启动”的个股。
    """
    min_amount: float = 10000000.0
    max_holdings: int = 6
    position_pct: float = 0.12
    stop_loss_pct: float = 0.07
    trail_stop_pct: float = 0.05
    min_gain_to_trail_pct: float = 0.08
    lot_size: int = 100
    history_window: int = 60
    prior_strength_lookback: int = 30
    short_strength_lookback: int = 10
    min_prior_runup_pct: float = 0.18
    min_short_return_pct: float = 0.03
    pullback_from_high_min_pct: float = 0.03
    pullback_from_high_max_pct: float = 0.15
    ma20_proximity_pct: float = 0.025
    rebound_volume_ratio: float = 1.1
    breakout_lookback: int = 5
    top_rank_pct: float = 0.15


class RelativeStrengthPullbackDailyStrategy(Strategy):
    """
    第三版策略：Relative Strength Pullback Daily

    这版不是严格意义上的“强势板块 + 强势股”，
    因为当前项目里还没有完整的行业/概念/板块映射数据。
    所以这里先用“全市场横截面里的相对强势股”做代理：

    1. 先要求个股在中期内已经走出一段明显上涨
    2. 再要求短期仍保持相对强势
    3. 然后等待价格回调到 ma20 附近
    4. 最后只在重新突破、重新放量时介入

    卖出部分采用：
    - 固定百分比止损
    - 浮盈后移动止盈
    - 趋势破坏退出
    """
    numeric_fields = {
        'open', 'close', 'high', 'low', 'volume', 'amount',
        'diff', 'dea', 'macd', 'ma10', 'ma20', 'ma60'
    }

    def __init__(self, params=None):
        # params 用来承接 run_research.py 传进来的策略参数
        self.params = params or RelativeStrengthPullbackDailyParams()
        # 前一日收盘后挑选出来、准备在下一交易日开盘执行的待买列表
        self.pending_buys = []
        # 当天收盘后打分最高的候选股，用于调试与分析
        self.last_candidates = []
        # 最近一次产生信号的日期
        self.last_signal_date = None
        # 每只股票的最近若干日历史 bar，用来判断“前强”“回调”“突破”
        self.price_history = {}
        # 持仓后的最高价，用来做移动止盈
        self.peak_prices = {}

    def initialize(self, context):
        # 回测开始时清空所有运行态状态
        self.pending_buys = []
        self.last_candidates = []
        self.last_signal_date = None
        self.price_history = {}
        self.peak_prices = {}

    def on_bar(self, context, bar_dict):
        """
        日线模式下每天只会调用一次 on_bar。

        执行顺序非常重要：
        1. 先把当日 bar 转成统一 dict，并转数值类型
        2. 更新历史窗口
        3. 先处理已有持仓的退出
        4. 再执行前一天挂起、今天开盘要买的股票
        5. 然后基于今天收盘后的形态，挑出下一交易日候选股
        """
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
        """
        将引擎传入的 pandas.Series / dict 统一整理成普通 dict。
        同时把数值字段显式转成 float，避免后续比较和计算时报类型问题。
        """
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
        """
        为每只股票维护一个固定长度的历史窗口。

        这个窗口是整个策略判断的核心数据基础：
        - 看过去一段时间是否足够强
        - 看是否发生回调
        - 看是否重新突破
        """
        for code, bar in snapshot.items():
            items = self.price_history.setdefault(code, [])
            items.append(dict(bar))
            if len(items) > self.params.history_window:
                del items[:-self.params.history_window]

    def _update_peak_prices(self, context, snapshot):
        """
        更新持仓股票自买入以来的最高价。

        这个最高价不是用来选股的，
        而是专门用于“从最高点回落多少比例就止盈”的移动止盈逻辑。
        """
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
        """
        先处理卖出，再考虑买入。

        这样做的好处是：
        - 先释放仓位
        - 先回收现金
        - 避免当天既该卖又因为仓位限制买不进去
        """
        for code, position in list(context.positions.items()):
            bar = snapshot.get(code)
            if not bar or position.available_volume <= 0:
                continue
            peak_price = self.peak_prices.get(code, position.avg_cost)
            if self._should_sell(position.avg_cost, peak_price, bar):
                context.sell(code, position.available_volume, bar['close'])

    def _cleanup_position_state(self, context):
        # 持仓已经清掉的股票，要同步删掉其最高价状态，避免脏数据残留
        active_codes = set(context.positions.keys())
        for code in list(self.peak_prices.keys()):
            if code not in active_codes:
                self.peak_prices.pop(code, None)

    def _execute_pending_buys(self, context, snapshot):
        """
        执行前一个交易日收盘后选出来的待买股票。

        这里默认使用今天 open 价格买入，
        对应“昨日收盘形成信号，今日开盘执行”的日级别交易模型。
        """
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
        """
        对当天新建仓的股票补上初始最高价。

        通常买入后会先把最高价设成开盘价，
        这里再尝试提升到当天 high，保证移动止盈从更合理的价格开始追踪。
        """
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
        """
        按目标仓位计算买入数量，并约束为 A 股 100 股整数倍。
        """
        target_value = total_value * self.params.position_pct
        affordable_value = min(target_value, context.cash)
        volume = int(affordable_value / open_price)
        volume = (volume // self.params.lot_size) * self.params.lot_size
        return max(volume, 0)

    def _rank_candidates(self, context, snapshot):
        """
        对所有候选股打分并排序。

        这里的“相对强弱”不是行业/概念板块意义上的，
        而是股票之间的横截面对比：
        - 谁过去涨得更强
        - 谁短期更强
        - 谁回调更标准
        - 谁启动更像样
        """
        slots = max(self.params.max_holdings - len(context.positions), 0)
        if slots <= 0:
            return []

        candidates = []
        for code, bar in snapshot.items():
            if code in context.positions:
                continue
            history = self.price_history.get(code, [])
            if not self._passes_pool_filter(history, bar):
                continue
            if not self._should_buy(history, bar):
                continue
            candidates.append({'code': code, 'score': self._score_candidate(history, bar)})

        if not candidates:
            return []

        candidates.sort(key=lambda item: item['score'], reverse=True)
        top_n = max(slots, int(math.ceil(len(candidates) * self.params.top_rank_pct)))
        top_n = min(len(candidates), max(top_n, slots * 2))
        return candidates[:min(top_n, slots)]

    def _passes_pool_filter(self, history, bar):
        """
        第一层过滤：基础股票池。

        只保留：
        - 有足够历史数据的股票
        - 成交额足够大的股票
        - 中长期趋势向上的股票
        - MACD 已经站上 0 轴的股票
        """
        required = ['amount', 'close', 'ma20', 'ma60', 'macd']
        if not all(self._is_valid_number(bar.get(field)) for field in required):
            return False
        return (
            len(history) >= max(20, self.params.prior_strength_lookback)
            and bar['amount'] > self.params.min_amount
            and bar['close'] > bar['ma60']
            and bar['ma20'] > bar['ma60']
            and bar['macd'] > 0
        )

    def _should_buy(self, history, bar):
        """
        第二层过滤：真正的买点确认。

        要求同时满足：
        - 前面确实强过
        - 最近短期仍强
        - 回调到均线附近但没走坏
        - 今天重新突破
        - 今天收阳、量能不差、MACD 重新发力
        """
        if len(history) < max(20, self.params.prior_strength_lookback):
            return False
        if not self._has_prior_strength(history):
            return False
        if not self._has_short_strength(history):
            return False
        if not self._is_pullback_ready(history, bar):
            return False
        if not self._is_breakout_ready(history, bar):
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

    def _is_breakout_ready(self, history, bar):
        """
        突破确认：
        当前 close 至少要站上最近若干天的高点，
        否则只能视为“回调中”，不能视为“重新启动”。
        """
        if len(history) <= self.params.breakout_lookback:
            return False
        recent = history[-(self.params.breakout_lookback + 1):-1]
        prev_highs = [item.get('high') if self._is_valid_number(item.get('high')) else item.get('close') for item in recent]
        prev_highs = [item for item in prev_highs if self._is_valid_number(item)]
        if not prev_highs or not self._is_valid_number(bar.get('close')):
            return False
        return bar['close'] >= max(prev_highs)

    def _should_sell(self, avg_cost, peak_price, bar):
        """
        卖出逻辑分三层：
        1. 固定止损：防止单笔亏损失控
        2. 移动止盈：有利润后，从最高点回撤达到阈值就止盈
        3. 结构止损：跌破 ma10 或 diff < dea，视为趋势被破坏
        """
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
        """
        判断“前强”：
        在 prior_strength_lookback 观察窗口里，
        从低点到高点至少出现过一段最小涨幅。
        """
        recent = history[-self.params.prior_strength_lookback:]
        closes = [item.get('close') for item in recent if self._is_valid_number(item.get('close'))]
        if len(closes) < 5:
            return False
        min_close = min(closes)
        max_close = max(closes)
        if min_close <= 0:
            return False
        return (max_close - min_close) / min_close >= self.params.min_prior_runup_pct

    def _has_short_strength(self, history):
        """
        判断“最近仍强”：
        在 short_strength_lookback 窗口里，
        当前价格相对窗口起点仍保持正向涨幅。
        """
        recent = history[-self.params.short_strength_lookback:]
        first_close = None
        last_close = None
        for item in recent:
            value = item.get('close')
            if self._is_valid_number(value):
                if first_close is None:
                    first_close = value
                last_close = value
        if not self._is_valid_number(first_close) or not self._is_valid_number(last_close) or first_close <= 0:
            return False
        return (last_close - first_close) / first_close >= self.params.min_short_return_pct

    def _is_pullback_ready(self, history, bar):
        """
        判断“回调后稳定在均线附近”：
        - 相对前高已经有一定回落
        - 回落不能太深，太深通常意味着趋势坏了
        - 当前 close 靠近 ma20
        - 最近几天最好碰过 ma20，说明完成了均线回踩
        """
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
        stable_near_ma20 = bar['close'] >= bar['ma20'] * 0.985 and proximity_pct <= self.params.ma20_proximity_pct
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
        """
        候选打分：
        分数越高，说明越像“强势股回调后重新启动”。

        这里综合考虑：
        - 中期涨幅
        - 短期涨幅
        - 量能放大
        - 离 ma20 的贴近程度
        - MACD 动能
        - 对回调深度做轻微惩罚
        """
        long_return = self._window_return(history, self.params.prior_strength_lookback)
        short_return = self._window_return(history, self.params.short_strength_lookback)
        highs = [item.get('high') if self._is_valid_number(item.get('high')) else item.get('close') for item in history[-self.params.prior_strength_lookback:]]
        highs = [item for item in highs if self._is_valid_number(item)]
        recent_high = max(highs) if highs else bar['close']
        pullback_pct = (recent_high - bar['close']) / recent_high if recent_high else 0.0
        recent_amount_avg = self._recent_average(history, 'amount', 5)
        volume_ratio = bar['amount'] / recent_amount_avg if recent_amount_avg > 0 else 1.0
        proximity_score = 1.0 - min(abs(bar['close'] - bar['ma20']) / bar['ma20'], 1.0)
        momentum_score = max(bar['diff'] - bar['dea'], 0.0) + max(bar['macd'], 0.0)
        return float(
            long_return * 3.0
            + short_return * 4.0
            + volume_ratio * 1.5
            + proximity_score * 2.0
            + momentum_score
            - pullback_pct
        )

    def _window_return(self, history, window):
        # 计算一个时间窗口内的首尾收益率，用于衡量阶段强度
        recent = history[-window:]
        first_close = None
        last_close = None
        for item in recent:
            value = item.get('close')
            if self._is_valid_number(value):
                if first_close is None:
                    first_close = value
                last_close = value
        if not self._is_valid_number(first_close) or not self._is_valid_number(last_close) or first_close <= 0:
            return 0.0
        return (last_close - first_close) / first_close

    def _recent_average(self, history, field, window):
        # 计算最近窗口均值，常用于和当天成交额做比较
        values = [item.get(field) for item in history[-window:] if self._is_valid_number(item.get(field))]
        if not values:
            return 0.0
        return sum(values) / len(values)

    def _is_valid_number(self, value):
        # 用最简单稳妥的方式过滤 None / NaN
        if value is None:
            return False
        try:
            return value == value
        except Exception:
            return False
