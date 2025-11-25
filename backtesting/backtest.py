import pandas as pd
import numpy as np
from typing import Dict, Callable


class SimpleBacktest:

    def __init__(self, data: pd.DataFrame, strategy: Callable[[pd.DataFrame], pd.Series]):
        """
        初始化回测系统。

        :param data: 包含价格数据的 DataFrame，必须包含 'open', 'high', 'low', 'close' 列。
        :param strategy: 策略函数，接受 DataFrame 并返回一个包含买入信号的 Series。
        """
        self.data = data
        self.strategy = strategy
        self.signals = None

    def run(self):
        """运行回测并生成交易信号。"""
        self.signals = self.strategy(self.data)

    def get_signals(self) -> pd.Series:
        """获取交易信号。"""
        if self.signals is None:
            raise ValueError("请先运行回测 (run) 方法。")
        return self.signals