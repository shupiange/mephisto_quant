from abc import ABC, abstractmethod
from typing import Dict, List, Optional

import pandas as pd

from ..engine.event import (
    FillEvent,
    MarketEvent,
    OrderDirection,
    OrderEvent,
    OrderType,
    SignalEvent,
    SignalType,
)
from ..engine.event_engine import EventEngine


class Portfolio(ABC):
    """
    Portfolio 是所有持仓和风险管理类的基类。
    它负责接收信号并生成订单，同时跟踪持仓和账户价值。

    基类实现了通用的风控校验逻辑，子类只需关注策略特定的数量计算逻辑。
    """

    def __init__(self, events: EventEngine, initial_capital: float = 100000.0):
        self.events = events
        self.initial_capital = initial_capital

        # 核心账户状态
        self.current_cash = initial_capital
        self.locked_cash = 0.0
        self.current_positions: Dict[str, int] = {}
        self.pending_sells: Dict[str, int] = {}

        # 市场数据缓存
        self.latest_prices: Dict[str, float] = {}

        # 账户历史记录
        self.all_holdings: List[Dict] = []
        self.trade_history: List[Dict] = []

        # 基础风控参数 (子类可覆盖)
        self.max_single_pos_pct = 1.0  # 默认不限制单只仓位
        self.allow_short = False  # 默认禁止做空

    @abstractmethod
    def update_signal(self, event: SignalEvent):
        """接收信号事件并生成订单"""
        pass

    @abstractmethod
    def update_fill(self, event: FillEvent):
        """接收成交事件并更新持仓"""
        pass

    @abstractmethod
    def update_market_value(self, event: MarketEvent):
        """接收行情事件并更新账户市值"""
        pass

    def get_available_cash(self) -> float:
        """获取可用资金 (当前现金 - 冻结资金)"""
        return self.current_cash - self.locked_cash

    def get_available_position(self, symbol: str) -> int:
        """获取可用持仓 (当前持仓 - 冻结持仓)"""
        return self.current_positions.get(symbol, 0) - self.pending_sells.get(symbol, 0)

    def validate_buy(self, symbol: str, price: float, quantity: int) -> bool:
        """
        通用买入校验逻辑
        1. 价格有效性
        2. 资金充足性
        3. 仓位上限检查
        """
        # 1. 价格检查
        if price <= 0:
            print(f"[Portfolio] 错误: {symbol} 价格无效 ({price})")
            return False

        if quantity <= 0:
            print(f"[Portfolio] 错误: {symbol} 买入数量无效 ({quantity})")
            return False

        # 2. 资金检查
        estimated_cost = price * quantity
        available_cash = self.get_available_cash()
        if available_cash < estimated_cost:
            print(
                f"[Portfolio] 拒绝买入: {symbol} 资金不足 (需 {estimated_cost:.2f}, 可用 {available_cash:.2f})"
            )
            return False

        # 3. 仓位上限检查
        # 计算当前总资产 (现金 + 持仓市值)
        total_assets = self.current_cash + sum(
            self.current_positions.get(s, 0) * self.latest_prices.get(s, 0)
            for s in self.current_positions
        )

        current_pos_val = self.current_positions.get(symbol, 0) * price
        target_pos_val = current_pos_val + estimated_cost
        max_allowed_val = total_assets * self.max_single_pos_pct

        # 如果已经是满仓状态（总资产约等于0时保护），则跳过
        if total_assets > 0 and target_pos_val > max_allowed_val:
            print(
                f"[Portfolio] 拒绝买入: {symbol} 将超过单标的仓位上限 "
                f"(目标占比 {target_pos_val/total_assets:.1%}, 上限 {self.max_single_pos_pct:.1%})"
            )
            return False

        return True

    def validate_sell(self, symbol: str, quantity: int) -> bool:
        """
        通用卖出校验逻辑
        1. 持仓充足性
        2. 做空权限检查
        """
        if quantity <= 0:
            print(f"[Portfolio] 错误: {symbol} 卖出数量无效 ({quantity})")
            return False

        available_pos = self.get_available_position(symbol)

        # 如果不允许做空，必须有足够持仓
        if not self.allow_short:
            if available_pos < quantity:
                print(
                    f"[Portfolio] 拒绝卖出: {symbol} 可用持仓不足 (需 {quantity}, 可用 {available_pos})"
                )
                return False

        return True

    def create_order(
        self, symbol: str, direction: OrderDirection, quantity: int, price_snapshot: float = 0.0
    ) -> bool:
        """
        统一的订单创建入口，包含完整的校验流程。
        子类应调用此方法来发送订单，而不是直接 put event。
        """
        if direction == OrderDirection.BUY:
            if self.validate_buy(symbol, price_snapshot, quantity):
                # 预冻结资金
                self.locked_cash += quantity * price_snapshot
                order = OrderEvent(symbol, OrderType.MARKET, quantity, direction)
                self.events.put(order)
                return True
        elif direction == OrderDirection.SELL:
            if self.validate_sell(symbol, quantity):
                # 预冻结持仓
                current_pending = self.pending_sells.get(symbol, 0)
                self.pending_sells[symbol] = current_pending + quantity
                order = OrderEvent(symbol, OrderType.MARKET, quantity, direction)
                self.events.put(order)
                return True

        return False

    def record_holdings(self, dt):
        """记录当前账户快照"""
        market_value = 0.0
        positions_value = {}

        for symbol, pos in self.current_positions.items():
            if pos > 0:
                price = self.latest_prices.get(symbol, 0.0)
                val = pos * price
                market_value += val
                positions_value[f"{symbol}_val"] = val
            else:
                positions_value[f"{symbol}_val"] = 0.0

        total_equity = self.current_cash + market_value

        holdings = {
            "datetime": dt,
            "cash": self.current_cash,
            "market_value": market_value,
            "total": total_equity,
        }
        holdings.update(positions_value)
        for symbol, pos in self.current_positions.items():
            holdings[f"{symbol}_pos"] = pos

        self.all_holdings.append(holdings)

    def get_equity_curve(self) -> pd.DataFrame:
        """获取权益曲线"""
        if not self.all_holdings:
            return pd.DataFrame()

        df = pd.DataFrame(self.all_holdings)
        if "datetime" in df.columns:
            df["datetime"] = pd.to_datetime(df["datetime"])
            df.set_index("datetime", inplace=True)
        return df

    def get_trade_log(self) -> pd.DataFrame:
        """获取交易记录 DataFrame"""
        return pd.DataFrame(self.trade_history)


class NaivePortfolio(Portfolio):
    """
    NaivePortfolio 简单的持仓管理。
    继承自 Portfolio，复用其基础风控逻辑。
    """

    def __init__(self, events: EventEngine, initial_capital: float = 100000.0):
        super().__init__(events, initial_capital)

        # 风险控制参数设置
        self.max_single_pos_pct = 0.2
        self.allow_short = False
        self.default_quantity = 100
        self.buy_pct = 0.2
        self.sell_pct = 0.2
        self.stop_loss_pct = 0.1
        self.take_profit_pct = 0.3
        self.trailing_stop_pct = 0.15

        # 成本与高水位线记录
        self.position_costs: Dict[str, float] = {}
        self.high_water_marks: Dict[str, float] = {}

    def update_market_value(self, event: MarketEvent):
        """
        处理行情事件，更新最新价格、检查风险控制并记录账户价值。
        """
        symbol = event.symbol
        latest_data = event.data
        if not symbol or latest_data is None:
            return

        # 1. 更新最新价格
        close_price = latest_data["close"]
        self.latest_prices[symbol] = close_price

        # 更新高水位线 (仅当持仓 > 0 时)
        if self.current_positions.get(symbol, 0) > 0:
            current_high = self.high_water_marks.get(symbol, 0.0)
            if close_price > current_high:
                self.high_water_marks[symbol] = close_price

        # 2. 风险检查：止损/止盈/移动止盈
        self._check_risk_exit(symbol, close_price)

        # 3. 记录账户价值
        self.record_holdings(getattr(latest_data, "name", "unknown"))

    def _check_risk_exit(self, symbol: str, current_price: float):
        """检查并执行止损止盈"""
        pos = self.current_positions.get(symbol, 0)
        if pos == 0:
            return

        avg_cost = self.position_costs.get(symbol, 0)
        if avg_cost == 0:
            return

        # 计算收益率
        returns = (
            (current_price - avg_cost) / avg_cost
            if pos > 0
            else (avg_cost - current_price) / avg_cost
        )

        # 1. 触发固定止损
        if returns <= -self.stop_loss_pct:
            print(f"[RiskControl] {symbol} 触发固定止损: 当前收益 {returns:.2%}")
            self._generate_exit_order(SignalEvent(symbol, "", SignalType.EXIT))
            return

        # 2. 触发移动止盈 (Trailing Stop)
        high_mark = self.high_water_marks.get(symbol, avg_cost)
        if high_mark > 0:
            drawdown = (high_mark - current_price) / high_mark
            if drawdown >= self.trailing_stop_pct and returns > 0:
                print(
                    f"[RiskControl] {symbol} 触发移动止盈/回撤止损: 高点 {high_mark:.2f} -> 现价 {current_price:.2f} (回撤 {drawdown:.2%})"
                )
                self._generate_exit_order(SignalEvent(symbol, "", SignalType.EXIT))
                return

        # 3. 触发固定止盈
        if self.take_profit_pct < 10.0 and returns >= self.take_profit_pct:
            print(f"[RiskControl] {symbol} 触发固定止盈: 当前收益 {returns:.2%}")
            self._generate_exit_order(SignalEvent(symbol, "", SignalType.EXIT))

    def update_signal(self, event: SignalEvent):
        """
        根据信号生成订单。
        利用基类 create_order 进行校验和发送。
        """
        price = self.latest_prices.get(event.symbol, 0.0)

        if event.signal_type == SignalType.EXIT:
            self._generate_exit_order(event)
            return

        direction = (
            OrderDirection.BUY if event.signal_type == SignalType.LONG else OrderDirection.SELL
        )

        quantity = 0

        if direction == OrderDirection.BUY:
            if price <= 0:
                return

            available_cash = self.get_available_cash()
            if available_cash <= 0:
                print(f"[Portfolio] 忽略买入信号: 可用资金不足 ({available_cash:.2f})")
                return

            # 计算初步数量
            raw_quantity = (available_cash * self.buy_pct) / price
            quantity = int(raw_quantity // 100) * 100

            # 最小一手检查
            if quantity < 100 and available_cash >= price * 100:
                quantity = 100

            # 仓位上限调整 (Pre-adjustment)
            # 虽然 create_order 会拒绝超限订单，但我们最好在这里先调整好数量，而不是直接被拒
            total_assets = self.current_cash + sum(
                self.current_positions.get(s, 0) * self.latest_prices.get(s, 0)
                for s in self.current_positions
            )
            max_pos_val = total_assets * self.max_single_pos_pct
            curr_pos_val = self.current_positions.get(event.symbol, 0) * price

            if curr_pos_val + quantity * price > max_pos_val:
                allowed_val = max(0, max_pos_val - curr_pos_val)
                quantity = int((allowed_val / price) // 100) * 100

        elif direction == OrderDirection.SELL:
            available_pos = self.get_available_position(event.symbol)
            if available_pos > 0:
                raw_sell_qty = self.current_positions.get(event.symbol, 0) * self.sell_pct
                quantity = int(raw_sell_qty // 100) * 100

                # 限制不超过可用持仓
                if quantity > available_pos:
                    quantity = int(available_pos // 100) * 100

                # 至少卖一手
                if quantity < 100 and available_pos >= 100:
                    quantity = 100
            else:
                print(f"[Portfolio] 忽略卖出信号: 无可用持仓")
                return

        # 调用基类方法统一创建订单 (包含最终校验)
        if quantity > 0:
            self.create_order(event.symbol, direction, quantity, price)

    def _generate_exit_order(self, event: SignalEvent):
        """生成清仓订单"""
        # 清仓通常指卖出所有可用持仓
        available_pos = self.get_available_position(event.symbol)

        if available_pos > 0:
            # 直接调用 create_order，它会处理冻结逻辑
            self.create_order(event.symbol, OrderDirection.SELL, available_pos, 0.0)
            print(f"[Portfolio] 生成清仓订单: {event.symbol} 数量: {available_pos}")

        elif self.current_positions.get(event.symbol, 0) < 0 and self.allow_short:
            # 平空单
            qty = abs(self.current_positions.get(event.symbol, 0))
            self.create_order(
                event.symbol, OrderDirection.BUY, qty, self.latest_prices.get(event.symbol, 0.0)
            )

    def update_fill(self, event: FillEvent):
        """
        成交后更新持仓、现金和成本。
        """
        curr_pos = self.current_positions.get(event.symbol, 0)
        curr_cost = self.position_costs.get(event.symbol, 0.0)

        trade_record = {
            "datetime": event.datetime,
            "symbol": event.symbol,
            "direction": event.direction.value,
            "price": event.fill_cost,
            "quantity": event.quantity,
            "commission": event.commission,
            "cash_before": self.current_cash,
        }

        if event.direction == OrderDirection.BUY:
            fill_cost = event.quantity * event.fill_cost
            total_cost = fill_cost + event.commission

            # 解冻资金
            estimated_locked = fill_cost  # 假设锁定等于成交额(不含佣金)
            if self.locked_cash >= estimated_locked:
                self.locked_cash -= estimated_locked
            else:
                self.locked_cash = 0.0

            if self.current_cash < total_cost:
                print(
                    f"[Portfolio] 警告: 账户发生透支! 现金: {self.current_cash:.2f} -> 支出: {total_cost:.2f}"
                )

            self.current_cash -= total_cost

            # 更新平均成本
            new_pos = curr_pos + event.quantity
            if new_pos > 0 and curr_pos >= 0:
                self.position_costs[event.symbol] = (curr_pos * curr_cost + fill_cost) / new_pos

                # 更新高水位线
                if curr_pos == 0:
                    self.high_water_marks[event.symbol] = event.fill_cost
                else:
                    self.high_water_marks[event.symbol] = max(
                        self.high_water_marks.get(event.symbol, 0), event.fill_cost
                    )

            self.current_positions[event.symbol] = new_pos

            trade_record["realized_pnl"] = 0.0
            trade_record["pnl_pct"] = 0.0

        elif event.direction == OrderDirection.SELL:
            fill_cost = event.quantity * event.fill_cost
            self.current_cash += (fill_cost - event.commission)

            # 解冻持仓 (create_order 锁定的 pending_sells)
            # 注意: 如果是部分成交，这里逻辑需要更复杂。但目前回测假设全部成交。
            pending = self.pending_sells.get(event.symbol, 0)
            if pending >= event.quantity:
                self.pending_sells[event.symbol] = pending - event.quantity
            else:
                self.pending_sells[event.symbol] = 0

            # 计算盈亏
            realized_pnl = (event.fill_cost - curr_cost) * event.quantity - event.commission
            pnl_pct = (event.fill_cost - curr_cost) / curr_cost if curr_cost > 0 else 0.0

            trade_record["realized_pnl"] = realized_pnl
            trade_record["pnl_pct"] = pnl_pct

            new_pos = curr_pos - event.quantity
            if new_pos < 0:
                print(f"[Portfolio] 严重错误: {event.symbol} 卖出后持仓为负 ({new_pos})，重置为0")
                new_pos = 0

            if new_pos == 0:
                self.position_costs[event.symbol] = 0.0
                self.high_water_marks.pop(event.symbol, None)

            self.current_positions[event.symbol] = new_pos

        trade_record["cash_after"] = self.current_cash
        self.trade_history.append(trade_record)

        # 将交易记录写入CSV
        df = pd.DataFrame(self.trade_history)
        df.to_csv("trade_log.csv", index=False)
