import os
import pandas as pd
from ..engine.event import MarketEvent

class BacktestDataHandler:
    """
    BacktestDataHandler 负责读取本地 CSV 数据并模拟行情推送到事件队列。
    """
    def __init__(self, event_engine, csv_dir, symbol_list):
        self.event_engine = event_engine  # 事件引擎实例，用于推送行情事件
        self.csv_dir = csv_dir            # CSV 数据文件存放的根目录
        self.symbol_list = symbol_list    # 待回测的股票/品种列表

        self.symbol_data = {}             # 存储每个品种的完整 DataFrame: {symbol: df}
        self.latest_symbol_data = {}      # 存储每个品种已推送过的历史 bar 列表: {symbol: [bar1, bar2, ...]}
        self.continue_backtest = True     # 回测继续标志，当数据耗尽时置为 False
        self.bar_index = 0                # 当前处理的 Bar 索引计数 (可选)

        self._load_local_csv_files()

    def _load_local_csv_files(self):
        """加载 CSV 文件到内存"""
        for symbol in self.symbol_list:
            file_path = os.path.join(self.csv_dir, f"{symbol}.csv")
            # 兼容新旧结构：如果 data/symbol.csv 不存在，尝试 data/daily/symbol.csv
            if not os.path.exists(file_path):
                file_path = os.path.join(self.csv_dir, "daily", f"{symbol}.csv")
            
            if os.path.exists(file_path):
                comb_index = None
                df = pd.read_csv(file_path, index_col='datetime', parse_dates=True).sort_index()
                self.symbol_data[symbol] = df
                self.latest_symbol_data[symbol] = []
            else:
                print(f"警告: 数据文件 {file_path} 未找到")

    def _get_new_bar(self, symbol):
        """返回最新的一个 bar"""
        for b in self.symbol_data[symbol].iterrows():
            yield b

    def get_latest_bar(self, symbol):
        """获取最近的一个 bar"""
        try:
            bars_list = self.latest_symbol_data[symbol]
            return bars_list[-1]
        except (KeyError, IndexError):
            return None

    def update_bars(self):
        """将下一个 bar 推送到事件队列"""
        any_data_left = False
        for symbol in self.symbol_list:
            try:
                # 获取生成器（如果是第一次）
                if not hasattr(self, f"_{symbol}_gen"):
                    setattr(self, f"_{symbol}_gen", self._get_new_bar(symbol))
                
                gen = getattr(self, f"_{symbol}_gen")
                bar = next(gen)
                self.latest_symbol_data[symbol].append(bar)
                # 将最新的行情数据注入事件
                self.event_engine.put(MarketEvent(symbol=symbol, data=bar[1]))
                any_data_left = True
            except StopIteration:
                pass
        
        if not any_data_left:
            self.continue_backtest = False
