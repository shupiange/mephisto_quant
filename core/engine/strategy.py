from abc import ABC, abstractmethod

import pandas as pd

from .event import MarketEvent, SignalEvent


class Strategy(ABC):
    """
    Strategy 是所有策略类的基类。
    它负责接收行情数据，并根据逻辑产生交易信号。
    """

    def __init__(self, engine=None):
        self.engine = engine
        self.indicators = {}  # 存储计算好的指标数据

    def set_engine(self, engine):
        """设置事件引擎"""
        self.engine = engine

    @abstractmethod
    def calculate_signals(self, event: MarketEvent):
        """
        计算交易信号
        """
        pass

    def put_event(self, event):
        """将事件放入引擎队列"""
        if self.engine:
            self.engine.put(event)
        else:
            print("警告: 策略未关联引擎，无法发送事件")
