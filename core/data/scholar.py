import pandas as pd
import numpy as np
import sys
import os

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
    def __init__(self):
        self.db_config = DATABASE_CONFIG
        
        # Config keys
        self.source_config_key = 'quant.stock_data_1_day'
        self.target_config_key = 'quant.stock_indicators_1_day'
        
        # Physical table names (remove 'quant.' prefix if it exists in config but not in DB)
        # Based on SHOW TABLES, existing tables don't have 'quant.' prefix.
        self.source_table = 'stock_data_1_day'
        self.target_table = 'stock_indicators_1_day'
        
        self.fields_config = TABLE_FIELDS_CONFIG

    def _get_db_manager(self):
        return MySQLManager(**self.db_config)

    def create_table(self):
        """创建指标表"""
        fields = self.fields_config.get(f'{self.target_config_key}_fields')
        types = self.fields_config.get(self.target_config_key)
        
        if not fields or not types:
            print(f"Error: Configuration for {self.target_config_key} not found.")
            return

        # Build CREATE TABLE statement
        columns = []
        for field in fields:
            field_type = types.get(field)
            sql_type = "VARCHAR(20)"
            if field_type == float:
                sql_type = "DOUBLE"
            elif field_type == int:
                sql_type = "BIGINT"
            elif field == 'date':
                sql_type = "DATE"
            elif field == 'code':
                sql_type = "VARCHAR(10)"
            
            columns.append(f"`{field}` {sql_type}")
        
        # Add primary key
        columns.append("PRIMARY KEY (`date`, `code`)")
        
        create_sql = f"CREATE TABLE IF NOT EXISTS `{self.target_table}` ({', '.join(columns)}) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;"
        
        with self._get_db_manager() as db:
            print(f"Creating table {self.target_table}...")
            db.execute_non_query(create_sql)
            print("Table created successfully.")

    def get_stock_codes(self):
        """获取所有股票代码"""
        with self._get_db_manager() as db:
            results = db.execute_query(f"SELECT DISTINCT code FROM `{self.source_table}`")
            return [row[0] for row in results]

    def get_stock_data(self, code):
        """获取单个股票的历史数据"""
        query = f"SELECT * FROM `{self.source_table}` WHERE code = %s ORDER BY date ASC"
        with self._get_db_manager() as db:
            # Get columns to create DataFrame
            # Assuming we know the columns from config, but it's safer to rely on query order if we select *
            # Or we can select specific columns.
            source_fields = self.fields_config.get(f'{self.source_config_key}_fields')
            cols_str = ", ".join(source_fields)
            query = f"SELECT {cols_str} FROM `{self.source_table}` WHERE code = %s ORDER BY date ASC"
            
            results = db.execute_query(query, (code,))
            if not results:
                return pd.DataFrame()
            
            df = pd.DataFrame(results, columns=source_fields)
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

        return df

    def save_indicators(self, df):
        """保存计算结果到数据库"""
        if df.empty:
            return

        target_fields = self.fields_config.get(f'{self.target_config_key}_fields')
        
        # Prepare data for insertion
        # Only keep rows where we have valid indicators (drop NaNs from start)
        # Or keep them as None/NULL. 
        # Usually we drop initial NaN rows for indicators, but we might want to keep the dates.
        # Let's fill NaN with 0 or keep as None. MySQL float accepts NULL.
        
        # Filter columns
        data_to_save = df[target_fields].copy()
        
        # Convert NaN to None for SQL insertion
        data_to_save = data_to_save.where(pd.notnull(data_to_save), None)
        
        values = []
        for _, row in data_to_save.iterrows():
            values.append(tuple(row[field] for field in target_fields))
            
        placeholders = ", ".join(["%s"] * len(target_fields))
        insert_sql = f"REPLACE INTO `{self.target_table}` ({', '.join(target_fields)}) VALUES ({placeholders})"
        
        # Batch insert
        batch_size = 1000
        with self._get_db_manager() as db:
            for i in range(0, len(values), batch_size):
                batch = values[i:i+batch_size]
                try:
                    cursor = db.conn.cursor()
                    cursor.executemany(insert_sql, batch)
                    db.conn.commit()
                    cursor.close()
                except Exception as e:
                    print(f"Error inserting batch: {e}")

    def run(self):
        print("Starting indicator calculation...")
        self.create_table()
        
        codes = self.get_stock_codes()
        print(f"Found {len(codes)} stocks.")
        
        for i, code in enumerate(codes):
            try:
                print(f"Processing {code} ({i+1}/{len(codes)})...")
                df = self.get_stock_data(code)
                if df.empty or len(df) < 2:
                    continue
                
                df_indicators = self.calculate_indicators(df)
                self.save_indicators(df_indicators)
                
            except Exception as e:
                print(f"Error processing {code}: {e}")
                import traceback
                traceback.print_exc()

        print("Done.")

if __name__ == "__main__":
    scholar = Scholar()
    scholar.run()
