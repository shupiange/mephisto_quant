from database.load_dataset import load_dataset

class BacktestEngine:
    def __init__(self, strategy_cls, codes, start_date, end_date, initial_cash=100000.0):
        self.strategy_cls = strategy_cls
        self.codes = codes
        self.start_date = start_date
        self.end_date = end_date
        self.initial_cash = initial_cash
        self.hold = Hold(initial_cash)
        self.context = Context(self.hold)
        self.daily_data = {} # date -> {code -> {open, close}}

    def _parse_custom_date(self, date_str):
        # 格式: 20251201093000000 -> YYYYMMDD
        # 只需要提取日期部分进行分组
        return str(date_str)[:8]

    def load_data(self):
        print("Loading data...")
        all_dfs = []
        for code in self.codes:
            # 假设 load_dataset 返回 DataFrame
            # 列: open, close, max, min, date
            df = load_dataset(code, start_date=self.start_date, end_date=self.end_date, table_name='stock_data_30_minute')
            if df is None or df.empty:
                print(f"Warning: No data for {code}")
                continue
            
            df = df.copy()
            df['code'] = code
            df['day_str'] = df['date'].apply(self._parse_custom_date)
            all_dfs.append(df)
        
        if not all_dfs:
            raise ValueError("No data loaded")

        combined_df = pd.concat(all_dfs)
        
        # 按日期分组处理
        # 我们需要每一天的 Open (09:30 bar's open) 和 Close (15:00 bar's close)
        # 假设数据是按时间排序的
        
        grouped = combined_df.groupby(['day_str', 'code'])
        
        processed_data = {} # day_str -> {code -> {open_price, close_price}}
        
        for (day, code), group in grouped:
            # 按 date 字符串排序确保时间顺序
            group = group.sort_values('date')
            
            # 取第一根K线的Open作为当日Open
            day_open = group.iloc[0]['open']
            # 取最后一根K线的Close作为当日Close
            day_close = group.iloc[-1]['close']
            
            if day not in processed_data:
                processed_data[day] = {}
            
            processed_data[day][code] = {
                'open': day_open,
                'close': day_close,
                'date': day # use the date string as ID
            }
            
        self.daily_data = processed_data
        print(f"Data processed. Total trading days: {len(self.daily_data)}")

    def run(self):
        self.load_data()
        
        strategy = self.strategy_cls()
        strategy.initialize(self.context)
        
        sorted_days = sorted(self.daily_data.keys())
        
        history = []
        
        for day in sorted_days:
            day_quotes = self.daily_data[day]
            
            # 1. 每日结算 (T+1 解锁)
            self.hold.settle()
            
            # 2. 开盘竞价交易
            # 构造 current_prices 用于 context
            current_opens = {code: data['open'] for code, data in day_quotes.items()}
            self.context.update_env(current_opens, f"{day} Open")
            
            strategy.on_open(self.context, day_quotes)
            
            # 3. 收盘竞价交易
            current_closes = {code: data['close'] for code, data in day_quotes.items()}
            self.context.update_env(current_closes, f"{day} Close")
            
            strategy.on_close(self.context, day_quotes)
            
            # 4. 记录当日净值
            total_value = self.hold.get_total_value(current_closes)
            history.append({
                'date': day,
                'total_value': total_value,
                'cash': self.hold.cash
            })
            # print(f"Day {day} end. Value: {total_value:.2f}")

        return pd.DataFrame(history)