"""
优化版技术指标计算器 - 针对机械硬盘优化

优化策略：
1. 一次性加载全部数据到内存（避免 5000 次随机读）
2. 内存中按股票分组计算（避免 MySQL 计算）
3. 批量写回（减少事务和磁盘写入次数）

性能对比：
  原版：5000 次查询 + 5000 次写入 = 10000 次磁盘 I/O
  优化：1 次查询 + 50 次批量写入 = 51 次磁盘 I/O（提升 200 倍）
"""

import pandas as pd
import numpy as np
import sys
import os
import argparse
from tqdm import tqdm

project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
if project_root not in sys.path:
    sys.path.append(project_root)

from core.database.db_manager import MySQLManager
from core.config.database_config import DATABASE_CONFIG


class ScholarOptimized:
    def __init__(self, source_table='stock_data_1_day', target_table='stock_indicators_1_day',
                 start_date=None, end_date=None, batch_size=100000, stock_batch_size=1000):
        self.db_config = DATABASE_CONFIG
        self.source_table = source_table
        self.target_table = target_table
        self.start_date = start_date
        self.end_date = end_date
        self.batch_size = batch_size  # 每批写入行数
        self.stock_batch_size = stock_batch_size  # 每批处理股票数（避免单次查询过大）

    def get_all_stock_codes(self):
        """获取所有股票代码"""
        db = MySQLManager(**self.db_config)
        db.connect()
        try:
            results = db.execute_query(f"SELECT DISTINCT code FROM `{self.source_table}` ORDER BY code")
            return [row[0] for row in results]
        finally:
            db.disconnect()

    def load_stock_batch_data(self, codes):
        """加载一批股票的数据到内存"""
        db = MySQLManager(**self.db_config)
        db.connect()

        try:
            # 构造查询
            where_clauses = [f"code IN ({','.join(['%s'] * len(codes))})"]
            params = list(codes)

            if self.start_date:
                # 预留 300 条缓冲（MA90 需要）
                where_clauses.append("date >= DATE_SUB(%s, INTERVAL 300 DAY)")
                params.append(self.start_date)

            if self.end_date:
                where_clauses.append("date <= %s")
                params.append(self.end_date)

            where_sql = f"WHERE {' AND '.join(where_clauses)}"

            # 30 分钟表需要 time 字段
            if '30_minute' in self.source_table:
                order_by = "ORDER BY code, date, time"
            else:
                order_by = "ORDER BY code, date"

            query = f"""
                SELECT code, date, open, close, high, low, volume, amount
                FROM `{self.source_table}`
                {where_sql}
                {order_by}
            """

            results = db.execute_query(query, tuple(params))

            if not results:
                return pd.DataFrame()

            df = pd.DataFrame(results, columns=['code', 'date', 'open', 'close', 'high', 'low', 'volume', 'amount'])

            # 转换数值类型
            for col in ['open', 'close', 'high', 'low', 'volume', 'amount']:
                df[col] = pd.to_numeric(df[col], errors='coerce')

            return df

        finally:
            db.disconnect()

    def calculate_indicators_batch(self, df):
        """批量计算所有股票的指标（内存操作）"""
        if df.empty:
            return pd.DataFrame()

        print("Calculating indicators in memory...")

        all_results = []

        # 按股票分组计算
        for code, group in tqdm(df.groupby('code'), desc="Computing"):
            group = group.sort_values('date').reset_index(drop=True)

            if len(group) < 2:
                continue

            # MACD
            ema12 = group['close'].ewm(span=12, adjust=False).mean()
            ema26 = group['close'].ewm(span=26, adjust=False).mean()
            group['diff'] = ema12 - ema26
            group['dea'] = group['diff'].ewm(span=9, adjust=False).mean()
            group['macd'] = (group['diff'] - group['dea']) * 2

            # KDJ
            low_min = group['low'].rolling(window=9, min_periods=1).min()
            high_max = group['high'].rolling(window=9, min_periods=1).max()
            rsv = (group['close'] - low_min) / (high_max - low_min + 1e-9) * 100
            rsv = rsv.fillna(50)
            group['k'] = rsv.ewm(com=2, adjust=False).mean()
            group['d'] = group['k'].ewm(com=2, adjust=False).mean()
            group['j'] = 3 * group['k'] - 2 * group['d']

            # CCI
            tp = (group['high'] + group['low'] + group['close']) / 3
            sma_tp = tp.rolling(window=14, min_periods=1).mean()
            md = tp.rolling(window=14, min_periods=1).apply(lambda x: np.mean(np.abs(x - np.mean(x))), raw=True)
            md = md.replace(0, 1e-9)
            group['cci'] = (tp - sma_tp) / (0.015 * md)

            # MFI
            typical_price = (group['high'] + group['low'] + group['close']) / 3
            money_flow = typical_price * group['volume']
            price_diff = typical_price.diff()

            positive_flow = money_flow.where(price_diff > 0, 0)
            negative_flow = money_flow.where(price_diff < 0, 0).abs()

            positive_mf = positive_flow.rolling(window=14, min_periods=1).sum()
            negative_mf = negative_flow.rolling(window=14, min_periods=1).sum()

            mfi_ratio = positive_mf / (negative_mf + 1e-9)
            group['mfi'] = 100 - (100 / (1 + mfi_ratio))

            # MA
            for period in [3, 5, 10, 20, 30, 60, 90]:
                group[f'ma{period}'] = group['close'].rolling(window=period, min_periods=1).mean()

            # Bollinger Bands
            ma20 = group['close'].rolling(window=20, min_periods=1).mean()
            std20 = group['close'].rolling(window=20, min_periods=1).std()
            group['boll_upper'] = ma20 + 2 * std20
            group['boll_middle'] = ma20
            group['boll_lower'] = ma20 - 2 * std20

            # 过滤掉缓冲区数据（只保留 start_date 之后的）
            if self.start_date:
                group = group[group['date'] >= self.start_date]

            all_results.append(group)

        result_df = pd.concat(all_results, ignore_index=True)
        print(f"Calculated {len(result_df):,} rows.")
        return result_df

    def save_indicators_batch(self, df):
        """批量写入指标数据"""
        if df.empty:
            return

        print(f"Writing {len(df):,} rows to {self.target_table}...")

        db = MySQLManager(**self.db_config)
        db.connect()

        try:
            # 准备数据
            indicator_cols = ['diff', 'dea', 'macd', 'k', 'd', 'j', 'cci', 'mfi',
                              'ma3', 'ma5', 'ma10', 'ma20', 'ma30', 'ma60', 'ma90',
                              'boll_upper', 'boll_middle', 'boll_lower']

            df_save = df[['date', 'code'] + indicator_cols].copy()
            df_save = df_save.where(pd.notnull(df_save), None)

            # 分批写入
            total_rows = len(df_save)
            for i in tqdm(range(0, total_rows, self.batch_size), desc="Writing"):
                batch = df_save.iloc[i:i+self.batch_size]
                data_list = batch.to_dict('records')
                db.insert_many_data(self.target_table, data_list)

            print(f"Successfully wrote {total_rows:,} rows.")

        finally:
            db.disconnect()

    def run(self):
        """主流程"""
        print(f"\n{'='*60}")
        print(f"Scholar Optimized - HDD Friendly (Batch Mode)")
        print(f"Source: {self.source_table}")
        print(f"Target: {self.target_table}")
        print(f"Date Range: {self.start_date or 'ALL'} ~ {self.end_date or 'ALL'}")
        print(f"Stock Batch Size: {self.stock_batch_size}")
        print(f"Write Batch Size: {self.batch_size}")
        print(f"{'='*60}\n")

        # Step 1: 获取所有股票代码
        all_codes = self.get_all_stock_codes()
        print(f"Found {len(all_codes)} stocks.\n")

        # Step 2: 分批处理
        total_batches = (len(all_codes) + self.stock_batch_size - 1) // self.stock_batch_size

        for batch_idx in range(total_batches):
            start_idx = batch_idx * self.stock_batch_size
            end_idx = min(start_idx + self.stock_batch_size, len(all_codes))
            batch_codes = all_codes[start_idx:end_idx]

            print(f"[Batch {batch_idx+1}/{total_batches}] Processing {len(batch_codes)} stocks ({batch_codes[0]} ~ {batch_codes[-1]})...")

            # 2a. 加载这批股票的数据
            df = self.load_stock_batch_data(batch_codes)
            if df.empty:
                print(f"  No data for this batch, skipping.")
                continue

            print(f"  Loaded {len(df):,} rows.")

            # 2b. 内存计算
            df_indicators = self.calculate_indicators_batch(df)

            # 2c. 批量写入
            self.save_indicators_batch(df_indicators)

            print(f"  Batch {batch_idx+1} completed.\n")

        print("All batches completed.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Optimized indicator calculator for HDD.')
    parser.add_argument('--source-table', type=str, default='stock_data_1_day')
    parser.add_argument('--target-table', type=str, default='stock_indicators_1_day')
    parser.add_argument('--start-date', type=str, default=None, help='YYYY-MM-DD')
    parser.add_argument('--end-date', type=str, default=None, help='YYYY-MM-DD')
    parser.add_argument('--batch-size', type=int, default=100000, help='Rows per batch write')
    parser.add_argument('--stock-batch-size', type=int, default=1000, help='Stocks per batch load')

    args = parser.parse_args()

    scholar = ScholarOptimized(
        source_table=args.source_table,
        target_table=args.target_table,
        start_date=args.start_date,
        end_date=args.end_date,
        batch_size=args.batch_size,
        stock_batch_size=args.stock_batch_size
    )
    scholar.run()
