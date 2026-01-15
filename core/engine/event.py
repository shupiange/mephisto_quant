from dataclasses import dataclass
from enum import Enum
from typing import Any, Dict


class EventType(Enum):
    """
    事件类型
    """
    MARKET = "MARKET"  # 行情事件
    SIGNAL = "SIGNAL"  # 信号事件
    ORDER = "ORDER"  # 订单事件
    FILL = "FILL"  # 成交事件


class SignalType(Enum):
    LONG = "LONG"  # 做多
    SHORT = "SHORT"  # 做空
    EXIT = "EXIT"  # 清仓/平仓


class OrderDirection(Enum):
    """
    订单方向
    """

    BUY = "BUY"  # 买入
    SELL = "SELL"  # 卖出


class OrderType(Enum):
    """
    订单类型
    """

    MARKET = "MARKET"  # 市价单
    LIMIT = "LIMIT"  # 限价单


@dataclass
class Event:
    """
    事件基类
    """
    type: EventType

@dataclass
class MarketEvent(Event):
    """
    市场行情事件，当 DataHandler 收到新的行情时触发
    """
    symbol: str = None  # 股票代码/交易对名称
    data: Any = None  # 包含最新的行情数据 (如收盘价、开盘价等 Series 或 Dict)

    def __init__(self, symbol: str = None, data: Any = None):
        self.type = EventType.MARKET  # 事件类型固定为行情
        self.symbol = symbol
        self.data = data

@dataclass
class SignalEvent(Event):
    """
    信号事件，当 Strategy 产生交易信号时触发
    """
    symbol: str  # 股票代码
    datetime: str  # 信号产生的时间点
    signal_type: SignalType  # 信号类型枚举
    strength: float = 1.0  # 信号强度，可用于计算下单仓位权重

    def __init__(self, symbol: str, datetime: str, signal_type: SignalType, strength: float = 1.0):
        self.type = EventType.SIGNAL  # 事件类型固定为信号
        self.symbol = symbol
        self.datetime = datetime
        self.signal_type = signal_type
        self.strength = strength


@dataclass
class OrderEvent(Event):
    """
    订单事件，当 Portfolio 决定下单时触发
    """
    symbol: str  # 股票代码
    order_type: OrderType  # 订单类型枚举
    quantity: int  # 交易数量(股/手)
    direction: OrderDirection  # 交易方向枚举

    def __init__(
        self, symbol: str, order_type: OrderType, quantity: int, direction: OrderDirection
    ):
        self.type = EventType.ORDER  # 事件类型固定为订单
        self.symbol = symbol
        self.order_type = order_type
        self.quantity = quantity
        self.direction = direction

    def print_order(self):
        print(
            f"Order: Symbol={self.symbol}, Type={self.order_type.value}, Quantity={self.quantity}, Direction={self.direction.value}"
        )

@dataclass
class FillEvent(Event):
    """
    成交事件，当 ExecutionHandler 执行订单后触发
    """
    symbol: str  # 股票代码
    datetime: str  # 成交具体时间
    quantity: int  # 成交数量
    direction: OrderDirection  # 成交方向枚举
    fill_cost: float  # 成交价格
    commission: float = 0.0  # 交易佣金/手续费支出

    def __init__(
        self,
        symbol: str,
        datetime: str,
        quantity: int,
        direction: OrderDirection,
        fill_cost: float,
        commission: float = 0.0,
    ):
        self.type = EventType.FILL  # 事件类型固定为成交
        self.symbol = symbol
        self.datetime = datetime
        self.quantity = quantity
        self.direction = direction
        self.fill_cost = fill_cost
        self.commission = commission
