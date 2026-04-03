
import pandas as pd
from datetime import datetime, timedelta
from core.database.load_dataset import load_dataset
from core.backtesting.account import Account
from core.backtesting.strategy_base import Context

class BacktestEngine:
    def __init__(self, strategy_cls, codes, start_date, end_date,
                 initial_cash=100000.0, risk_manager=None,
                 table_name='stock_data_30_minute', indicator_table=None):
        """
        初始化回测引擎
        start_date: 'YYYYMMDD' 格式
        end_date: 'YYYYMMDD' 格式
        risk_manager: RiskManager 实例 (可选)
        table_name: 行情数据表 (默认 stock_data_30_minute，日线用 stock_data_1_day_hfq)
        indicator_table: 指标表 (可选，设置后自动 merge 到行情数据)
        """
        self.strategy_cls = strategy_cls
        self.codes = codes
        self.start_date = str(start_date)
        self.end_date = str(end_date)
        self.account = Account(initial_cash)
        self.risk_manager = risk_manager
        self.context = Context(self.account, self, risk_manager=risk_manager)
        self.table_name = table_name
        self.indicator_table = indicator_table
        
    def _generate_date_range(self):
        """
        生成日期列表 (简单处理,暂不考虑交易日历,通过 load_dataset 是否有数据来过滤)
        如果需要严格的交易日历,可以引入专门的日历模块
        这里简化为生成自然日序列,然后在 run loop 中查询是否有数据
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
        # 将 YYYYMMDD 转为 YYYY-MM-DD 以匹配数据库格式
        if len(date_str) == 8 and '-' not in date_str:
            query_date = f"{date_str[:4]}-{date_str[4:6]}-{date_str[6:8]}"
        else:
            query_date = date_str

        df = load_dataset(self.codes, start_date=query_date, end_date=query_date,
                          table_name=self.table_name, database_name='quant')

        if df.empty:
            return None

        df['code'] = df['code'].astype(str)

        # 将 Decimal 类型的数值列转为 float（MySQL 的 DECIMAL 类型返回 Python Decimal）
        numeric_cols = ['open', 'close', 'high', 'low', 'amount', 'turn',
                        'diff', 'dea', 'macd', 'k', 'd', 'j', 'cci', 'mfi',
                        'ma3', 'ma5', 'ma10', 'ma20', 'ma30', 'ma60', 'ma90',
                        'boll_upper', 'boll_middle', 'boll_lower']
        for col in numeric_cols:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce')

        # 如果指定了指标表，merge 指标数据
        if self.indicator_table:
            ind_df = load_dataset(self.codes, start_date=query_date, end_date=query_date,
                                  table_name=self.indicator_table, database_name='quant')
            if not ind_df.empty:
                ind_df['code'] = ind_df['code'].astype(str)
                # 去掉指标表中与行情表重复的列（date, code, id 等），保留指标列
                merge_on = ['date', 'code']
                if 'time' in df.columns and 'time' in ind_df.columns:
                    merge_on.append('time')
                drop_cols = [c for c in ind_df.columns
                             if c in df.columns and c not in merge_on]
                ind_df = ind_df.drop(columns=drop_cols, errors='ignore')
                df = df.merge(ind_df, on=merge_on, how='left')

        if 'time' in df.columns:
            df = df.sort_values('time')
        else:
            df = df.sort_values('code')

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
            self.account.current_date = current_date
            
            # 调用每日开始回调
            strategy.on_day_start(self.context, current_date)

            # 风控每日检查（止损/止盈/回撤熔断等）
            if self.risk_manager:
                actions = self.risk_manager.daily_check(self.context)
                for action in actions:
                    if action['action'] == 'FORCE_SELL':
                        price = self.context.current_prices.get(action['code'])
                        if price:
                            print(f"[风控] {action['reason']}")
                            self.account.sell(action['code'], price, action['volume'])

            # 3. 按需加载当日数据
            daily_df = self.load_daily_data(current_date)
            
            if daily_df is None or daily_df.empty:
                # 当日无数据(非交易日或停牌),跳过
                continue
                
            last_date = current_date
            
            # 按时间步(Bar)迭代当日数据
            # 日线模式：无 time 列，整天作为一个 bar
            # 30分钟模式：按 time 分组，每个时间步一个 bar
            if 'time' in daily_df.columns:
                grouped = daily_df.groupby('time')
            else:
                grouped = [('daily', daily_df)]

            for ts, group in grouped:
                self.context.current_time = ts
                self.account.current_time = ts
                
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
                
                # 记录每一个 Bar 结束后的状态 (可选,或者只记录日末)
                # 这里为了性能,也可以选择只在日末记录
                
            # 4. 日终处理
            strategy.on_day_end(self.context, current_date)
            
            # 记录日末净值
            total_value = self.account.update_market_value(self.context.current_prices)
            history.append({
                'date': current_date,
                'total_value': total_value,
                'cash': self.account.cash
            })

        return pd.DataFrame(history), self.account.trade_logger.to_dataframe()
