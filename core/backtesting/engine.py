
import pandas as pd
from datetime import datetime, timedelta
from core.database.load_dataset import load_dataset
from core.backtesting.account import Account
from core.backtesting.strategy_base import Context

class BacktestEngine:
    def __init__(self, strategy_cls, codes, start_date, end_date, initial_cash=100000.0):
        """
        初始化回测引擎
        start_date: 'YYYYMMDD' 格式
        end_date: 'YYYYMMDD' 格式
        """
        self.strategy_cls = strategy_cls
        self.codes = codes
        self.start_date = str(start_date)
        self.end_date = str(end_date)
        self.account = Account(initial_cash)
        self.context = Context(self.account, self)
        
    def _generate_date_range(self):
        """
        生成日期列表 (简单处理，暂不考虑交易日历，通过 load_dataset 是否有数据来过滤)
        如果需要严格的交易日历，可以引入专门的日历模块
        这里简化为生成自然日序列，然后在 run loop 中查询是否有数据
        """
        start = datetime.strptime(self.start_date, "%Y%m%d")
        end = datetime.strptime(self.end_date, "%Y%m%d")
        
        date_list = []
        current = start
        while current <= end:
            date_list.append(current.strftime("%Y%m%d"))
            current += timedelta(days=1)
        return date_list

    def load_daily_data(self, date_str):
        """
        加载指定日期的数据
        date_str: 'YYYYMMDD'
        """
        print(f"Loading data for {date_str}...")
        # 调用 load_dataset 获取当日数据
        # 注意：load_dataset 的参数 start_date 和 end_date 格式可能需要适配
        # 假设 load_dataset 内部 SQL 是按字符串比较，传入 'YYYYMMDD' 即可
        # 如果 load_dataset 需要 'YYYY-MM-DD'，需要转换
        
        # 转换日期格式适配 load_dataset (假设其支持 YYYY-MM-DD 或 YYYYMMDD，这里按原样尝试，如果需要转换再改)
        # 根据 load_dataset.py 的注释，参数格式如 'YYYY-MM-DD'，所以这里做一下转换
        date_obj = datetime.strptime(date_str, "%Y%m%d")
        query_date = date_obj.strftime("%Y-%m-%d") # 转换为带短横线的格式
        
        # 查询当日数据
        # 注意：load_dataset 如果 start_date=end_date，则只查那一天
        df = load_dataset(self.codes, start_date=query_date, end_date=query_date, table_name='stock_data_30_minute', database_name='quant')
        
        if df.empty:
            return None
            
        # 确保 code 是字符串
        df['code'] = df['code'].astype(str)
        
        # 根据用户需求：date格式是YYYYMMDD, time格式是YYYYMMDDHHMMSSmmm
        # 我们需要按 time 排序 (time 包含了日期和时间信息)
        # 假设数据库返回的字段里有 'date' 和 'time'
        # 根据 table_config.py，30分钟线表里有 'date' (str), 'time' (int 比如 930)
        # 但用户描述 "time的格式是YYYYMMDDHHMMSSmmm"，这可能与 table_config 不一致
        # 我们先检查 DataFrame 列名，适配处理
        
        # 假设数据库返回的 'date' 是 'YYYY-MM-DD' 格式 (SQL date类型) 或者 'YYYYMMDD'
        # 如果 'time' 是 930 这种 int，我们需要构造完整的 timestamp 用于排序
        
        # 尝试判断 time 列的格式
        # Case 1: time 是完整的时间戳字符串 (YYYYMMDDHHMMSSmmm)
        # Case 2: time 是 int (HHMM)
                     
        df = df.sort_values('time')
        return df

    def run(self):
        # 1. 生成回测日期序列
        date_range = self._generate_date_range()
        print(f"Backtest range: {date_range[0]} to {date_range[-1]}, total {len(date_range)} days checked.")
        
        strategy = self.strategy_cls()
        strategy.initialize(self.context)
        
        history = []
        last_date = None

        # 2. 按日迭代
        for current_date in date_range:
            # 每日开盘前：T+1 结算
            self.account.settle()
            
            # 调用每日开始回调
            strategy.on_day_start(self.context, current_date)
            
            # 3. 按需加载当日数据
            daily_df = self.load_daily_data(current_date)
            
            if daily_df is None or daily_df.empty:
                # 当日无数据（非交易日或停牌），跳过
                continue
                
            last_date = current_date
            
            # 按时间步（Bar）迭代当日数据
            # 我们可以按 _timestamp 分组，处理同一时刻多个股票的数据
            grouped = daily_df.groupby('time')
            
            for ts, group in grouped:
                self.context.current_time = ts
                
                bar_dict = {}
                current_prices = {}
                
                for _, row in group.iterrows():
                    code = row['code']
                    bar_dict[code] = row
                    current_prices[code] = row['close']
                
                # 更新当前价格快照
                self.context.current_prices.update(current_prices)
                
                # 执行策略 On Bar
                strategy.on_bar(self.context, bar_dict)
                
                # 记录每一个 Bar 结束后的状态 (可选，或者只记录日末)
                # 这里为了性能，也可以选择只在日末记录
                
            # 4. 日终处理
            strategy.on_day_end(self.context, current_date)
            
            # 记录日末净值
            total_value = self.account.update_market_value(self.context.current_prices)
            history.append({
                'date': current_date,
                'total_value': total_value,
                'cash': self.account.cash
            })

        return pd.DataFrame(history)
