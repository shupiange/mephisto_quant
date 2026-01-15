import datetime
from abc import ABC, abstractmethod

from .event import EventType, FillEvent, MarketEvent, OrderDirection, OrderEvent
from .event_engine import EventEngine


class ExecutionHandler(ABC):
    """
    ExecutionHandler 是所有执行类的基类。
    它负责接收订单并将其发送给交易所或模拟撮合。
    """
    def __init__(self):
        self.latest_prices = {}  # 缓存每个品种的最新的市场价格: {symbol: price}，用于模拟撮合成交
        self.current_time = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    def update_price(self, event: MarketEvent):
        """记录最新价格以便撮合"""
        self.latest_prices[event.symbol] = event.data["close"]
        # 如果 MarketEvent 中有时间信息，更新当前时间
        if hasattr(event.data, "name"):
            self.current_time = str(event.data.name)
        elif "date" in event.data:
            self.current_time = str(event.data["date"])

    def _log_fill(
        self,
        symbol: str,
        direction: OrderDirection,
        quantity: int,
        price: float,
        trade_value: float,
        total_costs: float,
        settlement_amount: float,
        currency: str = "CNY",
        original_price: float = None,
        original_currency: str = None,
    ):
        """统一的成交日志打印"""
        price_str = f"{price:.2f} {currency}"
        if original_price is not None and original_currency:
            price_str = f"{original_price:.2f} {original_currency} -> {price:.2f} {currency}"

        print(
            f"[Execution] [{symbol}] 成交: {direction.value} | "
            f"价格: {price_str} | "
            f"数量: {quantity} | "
            f"金额: {trade_value:.2f} {currency} | "
            f"总费用: {total_costs:.2f} {currency} | "
            f"最终结算: {settlement_amount:.2f} {currency}"
        )

    @abstractmethod
    def execute_order(self, event: OrderEvent):
        """
        执行订单
        """
        pass


class ChinaStockExecutionHandler(ExecutionHandler):
    """
    ChinaStockExecutionHandler 模拟 A 股市场的执行处理器。
    严格遵循 A 股交易规则：
    1. 交易单位：买入必须是 100 股的整数倍（一手）。
    2. 手续费逻辑：
        - 佣金：默认万分之三 (0.0003)，双向收取，最低 5 元。
        - 印花税：默认千分之零点五 (0.0005)，仅卖出时收取。
        - 过户费：默认万分之零点一 (0.00001)，双向收取。
    """

    def __init__(self, events: EventEngine):
        super().__init__()
        self.events = events  # 事件引擎

        # A 股交易费率设置
        self.commission_rate = 0.0003  # 佣金率 (万三)
        self.min_commission = 5.0  # 最低佣金 (5元)
        self.stamp_duty_rate = 0.0005  # 印花税率 (卖方单边, 千零点五)
        self.transfer_fee_rate = 0.00001  # 过户费率 (双向, 万零点一)

    def execute_order(self, event: OrderEvent):
        if event.type == EventType.ORDER:
            # A 股买入合规性检查：必须是100的整数倍
            if event.direction == OrderDirection.BUY and event.quantity % 100 != 0:
                print(
                    f"[Execution] 拒绝订单: A 股买入数量必须是 100 的整数倍 (当前: {event.quantity})"
                )
                return

            # 从缓存中获取最新价作为成交价
            fill_price = self.latest_prices.get(event.symbol, 0.0)
            if fill_price == 0:
                print(f"[Execution] 警告: {event.symbol} 未获取到价格，无法成交")
                return

            trade_value = fill_price * event.quantity

            # 计算费用
            total_transaction_cost = 0.0

            # A 股费用
            # 1. 计算佣金 (买卖双向，最低 5 元)
            commission = max(self.min_commission, trade_value * self.commission_rate)

            # 2. 计算印花税 (仅卖出收取)
            stamp_duty = 0.0
            if event.direction == OrderDirection.SELL:
                stamp_duty = trade_value * self.stamp_duty_rate

            # 3. 计算过户费 (买卖双向)
            transfer_fee = trade_value * self.transfer_fee_rate

            total_transaction_cost = commission + stamp_duty + transfer_fee

            fill_event = FillEvent(
                symbol=event.symbol,
                datetime=self.current_time,
                quantity=event.quantity,
                direction=event.direction,
                fill_cost=fill_price,
                commission=total_transaction_cost,
            )

            # 计算最终结算金额 (Settlement Amount)
            settlement_amount = 0.0
            if event.direction == OrderDirection.BUY:
                settlement_amount = trade_value + total_transaction_cost
            elif event.direction == OrderDirection.SELL:
                settlement_amount = trade_value - total_transaction_cost

            self._log_fill(
                symbol=event.symbol,
                direction=event.direction,
                quantity=event.quantity,
                price=fill_price,
                trade_value=trade_value,
                total_costs=total_transaction_cost,
                settlement_amount=settlement_amount,
            )
            self.events.put(fill_event)


class HongKongStockConnectExecutionHandler(ExecutionHandler):
    """
    HongKongStockConnectExecutionHandler 模拟港股通交易执行器。
    只负责港股通相关逻辑。

    主要特点：
    1. 资金账户为人民币，交易港股时会自动进行汇率换算。
    2. 费率包含：
       - 佣金：境内券商收取（默认万三，最低5元人民币）。
       - 印花税：香港政府收取（0.1%，双边），按汇率折算。
       - 交易征费/交易费/财汇局征费：合计约 0.0085%，按汇率折算。
       - 汇率损耗：模拟买卖时的汇率价差（默认单边 0.5%）。
    """

    def __init__(self, events: EventEngine, exchange_rate: float = 0.92):
        super().__init__()
        self.events = events
        self.exchange_rate = exchange_rate  # 港币对人民币汇率参考值
        self.exchange_slippage = 0.002  # 汇率损耗/滑点 (0.5%)

    def execute_order(self, event: OrderEvent):
        if event.type == EventType.ORDER:
            is_hk = len(event.symbol) == 5

            if not is_hk:
                return

            # --- 港股通逻辑 ---

            # 1. 获取港币价格
            price_hkd = self.latest_prices.get(event.symbol, 0.0)
            if price_hkd == 0:
                print(f"[Execution] 警告: {event.symbol} 未获取到价格，无法成交")
                return

            # 2. 汇率换算 (港币 -> 人民币)
            # 买入时，需要更多人民币（汇率上浮）；卖出时，换回更少人民币（汇率下浮）
            # 这里我们通过调整成交价或费用来体现。
            # 为了 Portfolio 计算简单，我们将 fill_cost 设为基础汇率换算后的人民币价格，
            # 将汇率损耗计入 commission (费用) 中。

            price_cny = price_hkd * self.exchange_rate
            trade_value_cny = price_cny * event.quantity

            # 3. 费用计算

            # A. 汇率损耗 (模拟换汇成本)
            exchange_cost = trade_value_cny * self.exchange_slippage

            # B. 佣金 (境内券商，人民币计价)
            commission_cny = max(5.0, trade_value_cny * 0.0003)

            # C. 印花税 (香港政府 0.1% HKD -> CNY)
            # 规则：不足1港元按1港元算。
            import math
            stamp_duty_hkd = math.ceil(price_hkd * event.quantity * 0.001)
            stamp_duty_cny = stamp_duty_hkd * self.exchange_rate

            # D. 监管费用 (SFC + HKEX + AFRC ≈ 0.0085%)
            reg_fees_hkd = price_hkd * event.quantity * 0.000085
            reg_fees_cny = reg_fees_hkd * self.exchange_rate

            # E. 证券组合费 (忽略)

            total_transaction_cost_cny = (
                commission_cny + stamp_duty_cny + reg_fees_cny + exchange_cost
            )

            # 4. 生成成交事件 (全部为人民币单位)
            fill_event = FillEvent(
                symbol=event.symbol,
                datetime=self.current_time,
                quantity=event.quantity,
                direction=event.direction,
                fill_cost=price_cny,  # 人民币价格
                commission=total_transaction_cost_cny,  # 人民币总费用
            )

            # 计算最终结算金额 (Settlement Amount)
            settlement_amount_cny = 0
            if event.direction == OrderDirection.BUY:
                settlement_amount_cny = trade_value_cny + total_transaction_cost_cny
            elif event.direction == OrderDirection.SELL:
                settlement_amount_cny = trade_value_cny - total_transaction_cost_cny

            self._log_fill(
                symbol=event.symbol,
                direction=event.direction,
                quantity=event.quantity,
                price=price_cny,
                trade_value=trade_value_cny,
                total_costs=total_transaction_cost_cny,
                settlement_amount=settlement_amount_cny,
                currency="CNY",
                original_price=price_hkd,
                original_currency="HKD",
            )
            self.events.put(fill_event)


class CompositeExecutionHandler(ExecutionHandler):
    """
    CompositeExecutionHandler 组合式执行器。
    根据股票代码类型，自动将订单路由给对应的执行器。

    支持：
    1. A股 -> ChinaStockExecutionHandler
    2. 港股 -> HongKongStockConnectExecutionHandler
    """
    def __init__(self, events: EventEngine):
        super().__init__()
        self.events = events
        # 初始化子执行器
        self.a_share_handler = ChinaStockExecutionHandler(events)
        self.hk_share_handler = HongKongStockConnectExecutionHandler(events)

    def update_price(self, event: MarketEvent):
        """
        分发价格更新事件给所有子执行器
        """
        super().update_price(event)
        self.a_share_handler.update_price(event)
        self.hk_share_handler.update_price(event)

    def execute_order(self, event: OrderEvent):
        """
        根据 symbol 长度路由订单
        """
        if event.type == EventType.ORDER:
            if len(event.symbol) == 5:
                # 5位代码 -> 港股
                self.hk_share_handler.execute_order(event)
            else:
                # 6位或其他 -> A股
                self.a_share_handler.execute_order(event)
