import pandas as pd
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


class Aggregator:
    """
    Aggregator 将 30 分钟后复权数据聚合为日线后复权数据。

    聚合规则（按 date + code 分组）：
        open   = 当日第一根 Bar 的 open（time_rank 最小）
        high   = MAX(high)
        low    = MIN(low)
        close  = 当日最后一根 Bar 的 close（time_rank 最大）
        volume = SUM(volume)
        amount = SUM(amount)
        turn   = NULL（30 分钟数据无换手率）
    """

    def __init__(self, source_table='stock_data_30_minute', target_table='stock_data_1_day_hfq',
                 start_date=None, end_date=None):
        self.db_config = DATABASE_CONFIG
        self.db_manager = MySQLManager(**self.db_config)
        self.source_table = source_table
        self.target_table = target_table
        self.start_date = start_date
        self.end_date = end_date
        self.fields_config = TABLE_FIELDS_CONFIG

    def get_stock_codes(self):
        """获取源表中所有股票代码"""
        results = self.db_manager.execute_query(f"SELECT DISTINCT code FROM `{self.source_table}`")
        return [row[0] for row in results]

    def get_stock_data(self, code):
        """获取单个股票在日期范围内的 30 分钟数据"""
        where_clauses = ["code = %s"]
        params = [code]

        if self.start_date:
            where_clauses.append("date >= %s")
            params.append(self.start_date)
        if self.end_date:
            where_clauses.append("date <= %s")
            params.append(self.end_date)

        config_key = f'quant.{self.source_table}'
        source_fields = self.fields_config.get(f'{config_key}_fields')

        if source_fields:
            cols_str = ", ".join(source_fields)
            select_clause = f"SELECT {cols_str}"
        else:
            select_clause = "SELECT *"

        query = f"{select_clause} FROM `{self.source_table}` WHERE {' AND '.join(where_clauses)} ORDER BY date ASC, time_rank ASC"
        results = self.db_manager.execute_query(query, tuple(params))

        if not results:
            return pd.DataFrame()

        if source_fields:
            df = pd.DataFrame(results, columns=source_fields)
        else:
            df = pd.DataFrame(results)
        return df

    def aggregate_to_daily(self, df):
        """将 30 分钟数据聚合为日线数据"""
        if df.empty:
            return pd.DataFrame()

        # 确保数值类型
        for col in ['open', 'close', 'high', 'low', 'volume', 'amount']:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce')

        if 'time_rank' in df.columns:
            df['time_rank'] = pd.to_numeric(df['time_rank'], errors='coerce')

        grouped = df.groupby(['date', 'code'])

        daily = grouped.agg(
            open=('open', 'first'),
            high=('high', 'max'),
            low=('low', 'min'),
            close=('close', 'last'),
            volume=('volume', 'sum'),
            amount=('amount', 'sum'),
        ).reset_index()

        # turn 留空
        daily['turn'] = None

        return daily

    def save_daily(self, df, auto_commit=True):
        """保存聚合结果到目标表"""
        if df.empty:
            return

        config_key = f'quant.{self.target_table}'
        target_fields = self.fields_config.get(f'{config_key}_fields')
        if not target_fields:
            print(f"Error: Target fields not found for {config_key}")
            return

        data_to_save = df[target_fields].copy()
        data_to_save = data_to_save.astype(object)
        data_to_save = data_to_save.where(pd.notnull(data_to_save), None)

        values = []
        for _, row in data_to_save.iterrows():
            values.append(tuple(row[field] for field in target_fields))

        placeholders = ", ".join(["%s"] * len(target_fields))
        insert_sql = f"REPLACE INTO `{self.target_table}` ({', '.join(target_fields)}) VALUES ({placeholders})"

        batch_size = 1000
        db = self.db_manager
        for i in range(0, len(values), batch_size):
            batch = values[i:i + batch_size]
            cursor = db.conn.cursor()
            cursor.executemany(insert_sql, batch)
            if auto_commit:
                db.conn.commit()
            cursor.close()

    def run(self):
        print(f"Starting aggregation (Source: {self.source_table} -> Target: {self.target_table})...")

        self.db_manager.connect()

        try:
            codes = self.get_stock_codes()
            print(f"Found {len(codes)} stocks.")

            processed_count = 0

            for code in tqdm(codes, desc="Aggregating 30m -> Daily"):
                df = self.get_stock_data(code)
                if df.empty or len(df) < 1:
                    continue

                daily_df = self.aggregate_to_daily(df)
                self.save_daily(daily_df, auto_commit=False)

                processed_count += 1
                if processed_count % 1000 == 0:
                    self.db_manager.conn.commit()

            # Final commit
            self.db_manager.conn.commit()

        finally:
            self.db_manager.disconnect()

        print("Aggregation done.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Aggregate 30-minute back-adjusted data to daily back-adjusted data.')
    parser.add_argument('--source-table', type=str, default='stock_data_30_minute',
                        help='Source table (30-minute data)')
    parser.add_argument('--target-table', type=str, default='stock_data_1_day_hfq',
                        help='Target table (daily aggregated data)')
    parser.add_argument('--start-date', type=str, default=None, help='Start date (YYYY-MM-DD)')
    parser.add_argument('--end-date', type=str, default=None, help='End date (YYYY-MM-DD)')

    args = parser.parse_args()

    aggregator = Aggregator(
        source_table=args.source_table,
        target_table=args.target_table,
        start_date=args.start_date,
        end_date=args.end_date
    )
    aggregator.run()
