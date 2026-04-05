from decimal import Decimal

from core.config.database_config import DATABASE_CONFIG
from core.database.db_manager import MySQLManager
from core.database.load_dataset import load_dataset
from core.research.models import display_date


class ResearchValidator:
    def __init__(self, config):
        self.config = config
        self.required_data_columns = ['date', 'code', 'open', 'close', 'high', 'low', 'volume', 'amount']
        self.required_indicator_columns = ['date', 'code', 'diff', 'dea', 'macd', 'ma10', 'ma20', 'ma60']

    def validate(self, codes):
        if self.config.start_date > self.config.end_date:
            raise ValueError('开始日期不能晚于结束日期')
        if not codes:
            raise ValueError('股票池为空，无法启动研究')

        sample_codes = codes[:min(len(codes), self.config.validation_sample_size)]
        data_columns = self._fetch_columns(self.config.table_name)
        indicator_columns = self._fetch_columns(self.config.indicator_table)
        missing_data_columns = [col for col in self.required_data_columns if col not in data_columns]
        missing_indicator_columns = [col for col in self.required_indicator_columns if col not in indicator_columns]

        if missing_data_columns:
            raise ValueError(f'行情表缺少字段: {missing_data_columns}')
        if missing_indicator_columns:
            raise ValueError(f'指标表缺少字段: {missing_indicator_columns}')

        data_df = load_dataset(
            sample_codes,
            start_date=display_date(self.config.start_date),
            end_date=display_date(self.config.end_date),
            table_name=self.config.table_name,
            database_name='quant',
        )
        indicator_df = load_dataset(
            sample_codes,
            start_date=display_date(self.config.start_date),
            end_date=display_date(self.config.end_date),
            table_name=self.config.indicator_table,
            database_name='quant',
        )

        if data_df.empty:
            raise ValueError('行情表在验证样本中无数据')
        if indicator_df.empty:
            raise ValueError('指标表在验证样本中无数据')

        overlap_pairs = data_df[['date', 'code']].merge(
            indicator_df[['date', 'code']],
            on=['date', 'code'],
            how='inner'
        )
        if overlap_pairs.empty:
            raise ValueError('行情表与指标表在验证样本中没有重叠记录')

        data_null_ratio = self._null_ratio(data_df, ['open', 'close', 'amount'])
        indicator_null_ratio = self._null_ratio(indicator_df, ['macd', 'ma20', 'ma60'])

        return {
            'validation_sample_size': len(sample_codes),
            'universe_size': len(codes),
            'data_table': self._coverage_stats(data_df, self.config.table_name),
            'indicator_table': self._coverage_stats(indicator_df, self.config.indicator_table),
            'overlap_rows': int(len(overlap_pairs)),
            'missing_data_columns': missing_data_columns,
            'missing_indicator_columns': missing_indicator_columns,
            'data_null_ratio': data_null_ratio,
            'indicator_null_ratio': indicator_null_ratio,
        }

    def _fetch_columns(self, table_name):
        config = DATABASE_CONFIG.copy()
        with MySQLManager(**config) as db:
            db._ensure_connected()
            cursor = db.conn.cursor(buffered=True)
            try:
                cursor.execute(f"SELECT * FROM `{table_name}` LIMIT 0")
                return [desc[0] for desc in cursor.description]
            finally:
                cursor.close()

    def _coverage_stats(self, df, table_name):
        return {
            'table_name': table_name,
            'rows': int(len(df)),
            'codes': int(df['code'].nunique()),
            'dates': int(df['date'].nunique()),
            'start_date': str(df['date'].min()),
            'end_date': str(df['date'].max()),
        }

    def _null_ratio(self, df, columns):
        ratios = {}
        for column in columns:
            if column not in df.columns:
                continue
            value = df[column].isna().mean()
            if isinstance(value, Decimal):
                value = float(value)
            ratios[column] = float(value)
        return ratios
