import pandas as pd
import numpy as np
<<<<<<< HEAD
import sys
import os
import argparse
from tqdm import tqdm

# Ensure project root is in path
project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
if project_root not in sys.path:
    sys.path.append(project_root)

from core.database.db_manager import MySQLManager
from core.config.database_config import DATABASE_CONFIG
from core.config.table_config import TABLE_FIELDS_CONFIG

class Scholar:
    """
    Scholar 用于计算股票技术指标并存入数据库。
    支持指标: MACD, KDJ, CCI, MFI
    """
    def __init__(self, source_table='stock_data_1_day', target_table='stock_indicators_1_day', start_date=None, end_date=None):
        self.db_config = DATABASE_CONFIG
        self.db_manager = MySQLManager(**self.db_config)
                
        # Physical table names
        self.source_table = source_table
        self.target_table = target_table
        self.start_date = start_date
        self.end_date = end_date

        self.fields_config = TABLE_FIELDS_CONFIG

    def get_stock_codes(self):
        """获取所有股票代码"""
        results = self.db_manager.execute_query(f"SELECT DISTINCT code FROM `{self.source_table}`")
        return [row[0] for row in results]

    def get_stock_data(self, code):
        """获取单个股票的历史数据"""
        db = self.db_manager
        # Get columns to create DataFrame
        config_key = f'quant.{self.source_table}'
        source_fields = self.fields_config.get(f'{config_key}_fields')
        
        # 确定查询的列
        if source_fields:
            cols_str = ", ".join(source_fields)
            select_clause = f"SELECT {cols_str}"
        else:
            print(f"Warning: No fields config found for {config_key}, using SELECT *")
            select_clause = "SELECT *"
            
        # 构造查询条件
        where_clauses = ["code = %s"]
        params = [code]
        
        # 如果指定了 start_date，需要向前回溯一段数据(Lookback)以确保指标计算正确
        # 这里的 buffer_size 设为 300，足以覆盖 MA90, EMA convergence, Bollinger Bands 等需求
        actual_start_date = None
        if self.start_date:
            buffer_size = 300
            # 查找回溯的起始日期
            lookback_query = f"SELECT date FROM `{self.source_table}` WHERE code = %s AND date < %s ORDER BY date DESC LIMIT 1 OFFSET {buffer_size}"
            
            # 尝试找到 cutoff_date
            cutoff_result = db.execute_query(lookback_query, (code, self.start_date))
            if cutoff_result:
                actual_start_date = cutoff_result[0][0]
            else:
                # 如果找不到(说明历史数据不足300条)，则取所有历史数据
                actual_start_date = '1900-01-01'
            
            where_clauses.append("date >= %s")
            params.append(actual_start_date)
        
        if self.end_date:
            where_clauses.append("date <= %s")
            params.append(self.end_date)
        
        query = f"{select_clause} FROM `{self.source_table}` WHERE {' AND '.join(where_clauses)} ORDER BY date ASC"
        
        # 对于30分钟线，增加时间排序
        if '30_minute' in self.source_table:
                query = f"{select_clause} FROM `{self.source_table}` WHERE {' AND '.join(where_clauses)} ORDER BY date ASC, time ASC"

        results = db.execute_query(query, tuple(params))
        if not results:
            return pd.DataFrame()
        
        if source_fields:
            df = pd.DataFrame(results, columns=source_fields)
        else:
            df = pd.DataFrame(results) # Might lack column names
            
        return df

    def calculate_indicators(self, df):
        """计算技术指标"""
        if df.empty:
            return df

        # Ensure numeric types
        numeric_cols = ['open', 'close', 'high', 'low', 'volume']
        for col in numeric_cols:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col])

        # 1. MACD (12, 26, 9)
        # DIF = EMA(close, 12) - EMA(close, 26)
        # DEA = EMA(DIF, 9)
        # MACD = (DIF - DEA) * 2
        ema12 = df['close'].ewm(span=12, adjust=False).mean()
        ema26 = df['close'].ewm(span=26, adjust=False).mean()
        df['diff'] = ema12 - ema26
        df['dea'] = df['diff'].ewm(span=9, adjust=False).mean()
        df['macd'] = (df['diff'] - df['dea']) * 2

        # 2. KDJ (9, 3, 3)
        # RSV = (Close - MinLow) / (MaxHigh - MinLow) * 100
        low_min = df['low'].rolling(window=9).min()
        high_max = df['high'].rolling(window=9).max()
        rsv = (df['close'] - low_min) / (high_max - low_min) * 100
        # Fill NaN with 50 or 0
        rsv = rsv.fillna(50)
        
        # SMA implementation for K and D (com=2 is equivalent to alpha=1/3)
        df['k'] = rsv.ewm(com=2, adjust=False).mean()
        df['d'] = df['k'].ewm(com=2, adjust=False).mean()
        df['j'] = 3 * df['k'] - 2 * df['d']

        # 3. CCI (14)
        # TP = (High + Low + Close) / 3
        # MA = SMA(TP, 14)
        # MD = Mean Deviation(TP, 14)
        # CCI = (TP - MA) / (0.015 * MD)
        tp = (df['high'] + df['low'] + df['close']) / 3
        sma_tp = tp.rolling(window=14).mean()
        
        def rolling_mad(x):
            return np.mean(np.abs(x - np.mean(x)))
            
        md = tp.rolling(window=14).apply(rolling_mad, raw=True)
        # Avoid division by zero
        md = md.replace(0, 1e-9) 
        df['cci'] = (tp - sma_tp) / (0.015 * md)

        # 4. MFI (14)
        # Typical Price = (High + Low + Close) / 3
        # Money Flow = Typical Price * Volume
        # Positive Flow if TP > Prev TP, else 0
        # Negative Flow if TP < Prev TP, else 0
        # MFI = 100 - 100 / (1 + Pos / Neg)
        typical_price = (df['high'] + df['low'] + df['close']) / 3
        money_flow = typical_price * df['volume']
        
        # Shift typical price to compare with previous
        tp_shift = typical_price.shift(1)
        
        pos_flow = pd.Series(np.where(typical_price > tp_shift, money_flow, 0), index=df.index)
        neg_flow = pd.Series(np.where(typical_price < tp_shift, money_flow, 0), index=df.index)
        
        pos_mf_sum = pos_flow.rolling(window=14).sum()
        neg_mf_sum = neg_flow.rolling(window=14).sum()
        
        # Avoid division by zero
        mfi_ratio = pos_mf_sum / neg_mf_sum.replace(0, 1e-9)
        df['mfi'] = 100 - (100 / (1 + mfi_ratio))

        # 5. Moving Averages (3, 5, 10, 20, 30, 60, 90)
        ma_windows = [3, 5, 10, 20, 30, 60, 90]
        for window in ma_windows:
            df[f'ma{window}'] = df['close'].rolling(window=window).mean()

        # 6. Bollinger Bands (20, 2)
        # Middle Band = 20-day SMA
        # Upper Band = Middle Band + (2 * 20-day Std Dev)
        # Lower Band = Middle Band - (2 * 20-day Std Dev)
        # We can reuse ma20 if available, but let's recalculate to be safe and independent
        boll_window = 20
        boll_std = 2
        
        # Calculate MA20 (Middle Band)
        df['boll_middle'] = df['close'].rolling(window=boll_window).mean()
        
        # Calculate Standard Deviation
        rolling_std = df['close'].rolling(window=boll_window).std()
        
        # Calculate Upper and Lower Bands
        df['boll_upper'] = df['boll_middle'] + (boll_std * rolling_std)
        df['boll_lower'] = df['boll_middle'] - (boll_std * rolling_std)

        return df

    def save_indicators(self, df, auto_commit=True):
        """保存计算结果到数据库"""
        if df.empty:
            return
            
        # 如果指定了 start_date，在保存前过滤掉为了计算指标而多读取的历史数据
        if self.start_date:
            df = df[df['date'] >= self.start_date]
            
        if df.empty:
            return

        config_key = f'quant.{self.target_table}'
        target_fields = self.fields_config.get(f'{config_key}_fields')
        if not target_fields:
            print(f"Error: Target fields not found for {config_key}")
            return
        
        # Prepare data for insertion
        # Only keep rows where we have valid indicators (drop NaNs from start)
        # Or keep them as None/NULL. 
        # Usually we drop initial NaN rows for indicators, but we might want to keep the dates.
        # Let's fill NaN with 0 or keep as None. MySQL float accepts NULL.
        
        # Filter columns
        data_to_save = df[target_fields].copy()
        
        # Convert infinite values to NaN first
        data_to_save = data_to_save.replace([np.inf, -np.inf], np.nan)
        
        # Convert to object to allow None values (SQL NULL) instead of NaN
        data_to_save = data_to_save.astype(object)
        data_to_save = data_to_save.where(pd.notnull(data_to_save), None)
        
        values = []
        for _, row in data_to_save.iterrows():
            values.append(tuple(row[field] for field in target_fields))
            
        placeholders = ", ".join(["%s"] * len(target_fields))
        insert_sql = f"REPLACE INTO `{self.target_table}` ({', '.join(target_fields)}) VALUES ({placeholders})"
        
        # Batch insert
        batch_size = 1000
        db = self.db_manager
        for i in range(0, len(values), batch_size):
            batch = values[i:i+batch_size]
            cursor = db.conn.cursor()
            cursor.executemany(insert_sql, batch)
            if auto_commit:
                db.conn.commit()
            cursor.close()

    def run(self):
        print(f"Starting indicator calculation (Source: {self.source_table}, Target: {self.target_table})...")
        
        # Connect to DB once
        self.db_manager.connect()
        
        try:
            codes = self.get_stock_codes()
            print(f"Found {len(codes)} stocks.")
            
            # Counter for batch commit
            processed_count = 0
            
            for code in tqdm(codes, desc="Calculating Indicators"):
                # print(f"Processing {code} ({i+1}/{len(codes)})...")
                df = self.get_stock_data(code)
                if df.empty or len(df) < 2:
                    continue
                
                df_indicators = self.calculate_indicators(df)
                self.save_indicators(df_indicators, auto_commit=False)
                
                processed_count += 1
                if processed_count % 1000 == 0:
                    self.db_manager.conn.commit()
            
            # Final commit for remaining data
            self.db_manager.conn.commit()
            
        finally:
            self.db_manager.disconnect()

        print("Done.")


if __name__ == "__main__":

    parser = argparse.ArgumentParser(description='Calculate stock indicators and save to database.')
    parser.add_argument('--source-table', type=str, default='stock_data_1_day', help='Source table name for stock data: stock_data_30_minute, stock_data_1_day')
    parser.add_argument('--target-table', type=str, default='stock_indicators_1_day', help='Target table name for indicators: stock_indicators_1_day, stock_indicators_30_minute')
    parser.add_argument('--start-date', type=str, default=None, help='Start date for calculation (YYYY-MM-DD)')
    parser.add_argument('--end-date', type=str, default=None, help='End date for calculation (YYYY-MM-DD)')
    
    args = parser.parse_args()
    
    scholar = Scholar(
        source_table=args.source_table, 
        target_table=args.target_table,
        start_date=args.start_date,
        end_date=args.end_date
    )
    scholar.run()
=======
from typing import List, Union
from core.database.load_dataset import load_dataset
from core.database.db_manager import MySQLManager
from core.config.database_config import DATABASE_CONFIG

class Scholar:
    def __init__(self):
        self.db_config = DATABASE_CONFIG
        self.table_name = 'stock_data_1_day_indicators'
        self.source_table = 'stock_data_1_day'

    def _get_db_manager(self):
        return MySQLManager(
            host=self.db_config['host'],
            database=self.db_config['database'],
            user=self.db_config['user'],
            password=self.db_config['password']
        )

    def calculate_cci(self, df: pd.DataFrame, n: int = 14) -> pd.Series:
        """
        计算 CCI (Commodity Channel Index)
        TP = (High + Low + Close) / 3
        CCI = (TP - SMA(TP, N)) / (0.015 * MeanDeviation(TP, N))
        """
        tp = (df['high'] + df['low'] + df['close']) / 3
        sma_tp = tp.rolling(window=n).mean()
        # Mean Deviation = SMA(abs(TP - SMA_TP), N)
        # Pandas rolling().mean() is SMA. 
        # But standard Mean Deviation in CCI is mean(abs(TP - SMA_TP)) over the window?
        # Actually it is mean(abs(x - mean(x)))
        
        def mad(x):
            return np.mean(np.abs(x - np.mean(x)))
            
        # For efficiency, rolling apply is slower. 
        # But standard CCI uses Mean Absolute Deviation.
        md = tp.rolling(window=n).apply(mad, raw=True)
        
        cci = (tp - sma_tp) / (0.015 * md)
        return cci

    def calculate_mfi(self, df: pd.DataFrame, n: int = 14) -> pd.Series:
        """
        计算 MFI (Money Flow Index)
        Typical Price = (High + Low + Close) / 3
        Raw Money Flow = Typical Price * Volume
        Money Ratio = Positive Money Flow / Negative Money Flow
        MFI = 100 - 100 / (1 + Money Ratio)
        """
        tp = (df['high'] + df['low'] + df['close']) / 3
        rmf = tp * df['volume']
        
        # Shift to compare with previous day
        prev_tp = tp.shift(1)
        
        positive_flow = pd.Series(0.0, index=df.index)
        negative_flow = pd.Series(0.0, index=df.index)
        
        positive_flow[tp > prev_tp] = rmf[tp > prev_tp]
        negative_flow[tp < prev_tp] = rmf[tp < prev_tp]
        
        # Rolling sum over N periods
        pos_mf_sum = positive_flow.rolling(window=n).sum()
        neg_mf_sum = negative_flow.rolling(window=n).sum()
        
        # Avoid division by zero
        mfi = 100 - 100 / (1 + pos_mf_sum / neg_mf_sum.replace(0, np.nan))
        mfi = mfi.fillna(0) # Or handle appropriately
        return mfi

    def calculate_macd(self, df: pd.DataFrame, fast: int = 12, slow: int = 26, signal: int = 9) -> pd.DataFrame:
        """
        计算 MACD
        DIF = EMA(Close, fast) - EMA(Close, slow)
        DEA = EMA(DIF, signal)
        MACD = (DIF - DEA) * 2
        """
        ema_fast = df['close'].ewm(span=fast, adjust=False).mean()
        ema_slow = df['close'].ewm(span=slow, adjust=False).mean()
        
        dif = ema_fast - ema_slow
        dea = dif.ewm(span=signal, adjust=False).mean()
        macd_hist = (dif - dea) * 2
        
        return pd.DataFrame({
            'macd': dif,
            'macd_signal': dea,
            'macd_hist': macd_hist
        })

    def calculate_kdj(self, df: pd.DataFrame, n: int = 9, m1: int = 3, m2: int = 3) -> pd.DataFrame:
        """
        计算 KDJ
        RSV = (Close - LowestLow) / (HighestHigh - LowestLow) * 100
        K = (M1-1)/M1 * PrevK + 1/M1 * RSV
        D = (M2-1)/M2 * PrevD + 1/M2 * K
        J = 3K - 2D
        """
        low_min = df['low'].rolling(window=n).min()
        high_max = df['high'].rolling(window=n).max()
        
        rsv = (df['close'] - low_min) / (high_max - low_min) * 100
        # Fill NaN with 50 or 0? Standard usually starts calculation after N days.
        rsv = rsv.fillna(50)
        
        # Using ewm to simulate the recursive formula (M1-1)/M1 * Prev + 1/M1 * Curr
        # alpha = 1/M1. com = 1/alpha - 1 = M1 - 1
        k = rsv.ewm(alpha=1/m1, adjust=False).mean()
        d = k.ewm(alpha=1/m2, adjust=False).mean()
        j = 3 * k - 2 * d
        
        return pd.DataFrame({
            'kdj_k': k,
            'kdj_d': d,
            'kdj_j': j
        })

    def process_one_code(self, df_code: pd.DataFrame) -> pd.DataFrame:
        """
        处理单个股票的数据，计算所有指标
        """
        # Ensure sorted by date
        df_code = df_code.sort_values('date').copy()
        
        # Calculate Indicators
        df_code['cci'] = self.calculate_cci(df_code)
        df_code['mfi'] = self.calculate_mfi(df_code)
        
        macd_df = self.calculate_macd(df_code)
        df_code = pd.concat([df_code, macd_df], axis=1)
        
        kdj_df = self.calculate_kdj(df_code)
        df_code = pd.concat([df_code, kdj_df], axis=1)
        
        # Filter only result columns plus keys
        result_cols = [
            'date', 'code', 'cci', 'mfi', 
            'macd', 'macd_signal', 'macd_hist', 
            'kdj_k', 'kdj_d', 'kdj_j'
        ]
        
        return df_code[result_cols]

    def run(self, codes: Union[str, List[str]], start_date: str = None, end_date: str = None):
        """
        主运行方法：加载数据 -> 计算指标 -> 保存入库
        """
        print(f"正在加载数据: {codes} ({start_date} ~ {end_date})...")
        df = load_dataset(codes, start_date=start_date, end_date=end_date, table_name=self.source_table)
        
        if df.empty:
            print("未获取到数据。")
            return

        print("数据加载完成，开始计算指标...")
        results = []
        grouped = df.groupby('code')
        
        for code, group in grouped:
            try:
                processed_df = self.process_one_code(group)
                # Drop rows with NaN (due to rolling windows)
                processed_df = processed_df.dropna()
                results.append(processed_df)
            except Exception as e:
                print(f"计算股票 {code} 指标时出错: {e}")

        if not results:
            print("没有可保存的指标数据。")
            return

        final_df = pd.concat(results, ignore_index=True)
        
        print(f"计算完成，准备保存 {len(final_df)} 条记录到表 {self.table_name}...")
        
        with self._get_db_manager() as db:
            # Check if table exists, if not create it (Simple check)
            # Or just rely on insert failing if not exists.
            # Ideally we should use the DDL script logic, but here we just insert.
            db.insert_from_dataframe(self.table_name, final_df)
            
        print("所有指标保存完成。")

if __name__ == '__main__':
    # Simple test case
    scholar = Scholar()
    # Example usage: update a specific stock
    # scholar.run('sh.600519', start_date='2023-01-01', end_date='2023-12-31')
    
    import argparse
    parser = argparse.ArgumentParser(description="计算并更新股票技术指标")
    parser.add_argument('--codes', type=str, required=True, help="股票代码，逗号分隔")
    parser.add_argument('--start-date', type=str, help="开始日期 YYYY-MM-DD")
    parser.add_argument('--end-date', type=str, help="结束日期 YYYY-MM-DD")
    
    args = parser.parse_args()
    
    code_list = [c.strip() for c in args.codes.split(',')]
    scholar.run(code_list, start_date=args.start_date, end_date=args.end_date)
>>>>>>> 88f84e4 (fix)
