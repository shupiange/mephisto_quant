from typing import List, Tuple


class RiskRule:
    """风控规则基类"""

    def check_order(self, code, direction, volume, price, context) -> Tuple[bool, int, str]:
        """
        检查订单是否允许执行。
        Returns: (approved, adjusted_volume, reason)
        """
        return True, volume, ""

    def on_day_check(self, context) -> List[dict]:
        """
        每日开盘前检查，返回需要执行的动作列表。
        [{'action': 'FORCE_SELL', 'code': 'xxx', 'volume': n, 'reason': '...'}]
        """
        return []


class PositionSizeRule(RiskRule):
    """单票仓位上限"""

    def __init__(self, max_position_pct=0.20):
        self.max_position_pct = max_position_pct

    def check_order(self, code, direction, volume, price, context):
        if direction != 'BUY':
            return True, volume, ""

        total_value = context.account.total_value
        if total_value <= 0:
            return False, 0, "Account value is zero"

        max_value = total_value * self.max_position_pct

        # 已有持仓市值
        existing_value = 0.0
        pos = context.account.positions.get(code)
        if pos:
            current_price = context.current_prices.get(code, pos.avg_cost)
            existing_value = pos.total_volume * current_price

        proposed_value = volume * price
        allowed_value = max_value - existing_value

        if allowed_value <= 0:
            return False, 0, f"Position in {code} already at limit ({self.max_position_pct:.0%})"

        if proposed_value > allowed_value:
            adjusted = int(allowed_value / price)
            adjusted = (adjusted // 100) * 100
            if adjusted <= 0:
                return False, 0, f"Position size rule: cannot buy more {code}"
            return True, adjusted, f"Volume reduced to {adjusted} by position size rule"

        return True, volume, ""


class StopLossRule(RiskRule):
    """止损规则：持仓亏损超过阈值时强制卖出"""

    def __init__(self, stop_loss_pct=0.08):
        self.stop_loss_pct = stop_loss_pct

    def on_day_check(self, context) -> List[dict]:
        actions = []
        for code, pos in list(context.account.positions.items()):
            if pos.avg_cost <= 0 or pos.available_volume <= 0:
                continue
            current_price = context.current_prices.get(code)
            if current_price is None:
                continue
            loss_pct = (current_price - pos.avg_cost) / pos.avg_cost
            if loss_pct <= -self.stop_loss_pct:
                actions.append({
                    'action': 'FORCE_SELL',
                    'code': code,
                    'volume': pos.available_volume,
                    'reason': f"Stop loss triggered: {code} loss {loss_pct:.2%} >= {self.stop_loss_pct:.0%}",
                })
        return actions


class TakeProfitRule(RiskRule):
    """止盈规则：持仓盈利超过阈值时强制卖出"""

    def __init__(self, take_profit_pct=0.20):
        self.take_profit_pct = take_profit_pct

    def on_day_check(self, context) -> List[dict]:
        actions = []
        for code, pos in list(context.account.positions.items()):
            if pos.avg_cost <= 0 or pos.available_volume <= 0:
                continue
            current_price = context.current_prices.get(code)
            if current_price is None:
                continue
            gain_pct = (current_price - pos.avg_cost) / pos.avg_cost
            if gain_pct >= self.take_profit_pct:
                actions.append({
                    'action': 'FORCE_SELL',
                    'code': code,
                    'volume': pos.available_volume,
                    'reason': f"Take profit triggered: {code} gain {gain_pct:.2%} >= {self.take_profit_pct:.0%}",
                })
        return actions


class DrawdownLimitRule(RiskRule):
    """组合回撤熔断：超限禁止买入，可选强制清仓"""

    def __init__(self, max_drawdown_pct=0.15, liquidate_on_breach=False):
        self.max_drawdown_pct = max_drawdown_pct
        self.liquidate_on_breach = liquidate_on_breach
        self._peak_value = None

    def _update_peak(self, context):
        current_value = context.account.total_value
        if self._peak_value is None or current_value > self._peak_value:
            self._peak_value = current_value

    def _current_drawdown(self, context) -> float:
        self._update_peak(context)
        if self._peak_value is None or self._peak_value <= 0:
            return 0.0
        return (self._peak_value - context.account.total_value) / self._peak_value

    def check_order(self, code, direction, volume, price, context):
        if direction != 'BUY':
            return True, volume, ""

        dd = self._current_drawdown(context)
        if dd >= self.max_drawdown_pct:
            return False, 0, f"Drawdown limit: current drawdown {dd:.2%} >= {self.max_drawdown_pct:.0%}, buying disabled"

        return True, volume, ""

    def on_day_check(self, context) -> List[dict]:
        if not self.liquidate_on_breach:
            return []

        dd = self._current_drawdown(context)
        if dd < self.max_drawdown_pct:
            return []

        actions = []
        for code, pos in list(context.account.positions.items()):
            if pos.available_volume > 0:
                actions.append({
                    'action': 'FORCE_SELL',
                    'code': code,
                    'volume': pos.available_volume,
                    'reason': f"Drawdown liquidation: {dd:.2%} >= {self.max_drawdown_pct:.0%}",
                })
        return actions


class MaxHoldingsRule(RiskRule):
    """最大持仓股票数限制"""

    def __init__(self, max_holdings=10):
        self.max_holdings = max_holdings

    def check_order(self, code, direction, volume, price, context):
        if direction != 'BUY':
            return True, volume, ""

        # 已持有的股票可以加仓
        if code in context.account.positions:
            return True, volume, ""

        if len(context.account.positions) >= self.max_holdings:
            return False, 0, f"Max holdings limit: already holding {len(context.account.positions)} stocks (max {self.max_holdings})"

        return True, volume, ""


class RiskManager:
    """风控管理器，编排多个风控规则"""

    def __init__(self):
        self._rules: List[RiskRule] = []

    def add_rule(self, rule: RiskRule):
        self._rules.append(rule)
        return self

    def check_order(self, code, direction, volume, price, context) -> Tuple[bool, int, str]:
        """串行执行所有规则，AND 组合"""
        current_volume = volume
        for rule in self._rules:
            approved, adjusted_vol, reason = rule.check_order(
                code, direction, current_volume, price, context
            )
            if not approved:
                return False, 0, reason
            current_volume = min(current_volume, adjusted_vol)

        # 确保 100 股整数倍
        current_volume = (current_volume // 100) * 100
        if current_volume <= 0:
            return False, 0, "Volume reduced to zero by risk rules"

        return True, current_volume, ""

    def daily_check(self, context) -> List[dict]:
        """聚合所有规则的每日检查动作"""
        actions = []
        for rule in self._rules:
            actions.extend(rule.on_day_check(context))
        return actions
