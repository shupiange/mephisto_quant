
import pandas as pd
from core.database.load_dataset import load_dataset
from core.backtesting.account import Account
from core.backtesting.strategy_base import Context

class BacktestEngine:
    def __init__(self, strategy_cls, codes, start_date, end_date, initial_cash=100000.0):
        self.strategy_cls = strategy_cls
        self.codes = codes
        self.start_date = start_date
        self.end_date = end_date
        self.account = Account(initial_cash)
        self.context = Context(self.account, self)
        self.data = {} # processed data

    def load_data(self):
        print("Loading data from database...")
        # 批量加载，如果 codes 很多可能需要分批，这里暂一次性加载
        df = load_dataset(self.codes, start_date=self.start_date, end_date=self.end_date)
        
        if df.empty:
            raise ValueError("No data found for the given parameters.")

        # 确保 code 是字符串
        df['code'] = df['code'].astype(str)
        
        # 转换 date 格式方便处理? 或者直接用字符串比较
        # 假设 date 格式是 'YYYYMMDDHHMMSSmmm' 字符串
        # 我们需要按 date 排序
        df = df.sort_values('date')
        
        # 为了快速按时间步访问，可以按 date 分组
        self.grouped_data = df.groupby('date')
        self.unique_dates = sorted(df['date'].unique())
        print(f"Data loaded. Total time steps: {len(self.unique_dates)}")

    def _get_day_from_timestamp(self, ts_str):
        # ts_str: 20251201093000000
        return str(ts_str)[:8]

    def run(self):
        self.load_data()
        
        strategy = self.strategy_cls()
        strategy.initialize(self.context)
        
        last_day = None
        history = []

        for current_ts in self.unique_dates:
            current_day = self._get_day_from_timestamp(current_ts)
            
            # 日期变更处理
            if current_day != last_day:
                if last_day is not None:
                    strategy.on_day_end(self.context, last_day)
                
                # 新的一天，进行结算 (T+1 解锁)
                self.account.settle()
                strategy.on_day_start(self.context, current_day)
                last_day = current_day

            self.context.current_time = current_ts
            
            # 获取当前时间步的所有股票数据
            group = self.grouped_data.get_group(current_ts)
            
            # 构建 bar_dict: {code: row_series}
            # 同时更新 context 的当前价格 (使用 close 或 open，这里假设撮合用 close)
            bar_dict = {}
            current_prices = {}
            
            for _, row in group.iterrows():
                code = row['code']
                bar_dict[code] = row
                current_prices[code] = row['close'] # 默认用收盘价更新市值和作为最新价
            
            self.context.current_prices = current_prices
            
            # 执行策略
            strategy.on_bar(self.context, bar_dict)
            
            # 记录净值
            total_value = self.account.update_market_value(current_prices)
            history.append({
                'date': current_ts,
                'total_value': total_value,
                'cash': self.account.cash
            })

        # 结束时调用最后一次 on_day_end
        if last_day is not None:
            strategy.on_day_end(self.context, last_day)

        return pd.DataFrame(history)
