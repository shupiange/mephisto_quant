from typing import List, Dict, Tuple, Optional
import pandas as pd

from core.database.db_manager import MySQLManager
from core.config.database_config import DATABASE_CONFIG


class FilterCondition:
    """
    单个过滤条件。

    支持字段 vs 数值：FilterCondition('macd', '>', 0)
    支持字段 vs 字段：FilterCondition('close', '>', 'ma20')
    """

    VALID_OPS = ('>', '<', '>=', '<=', '=', '!=')
    # 指标表和行情表中已知的列名（用于判断 value 是列名还是数值）
    KNOWN_COLUMNS = {
        'date', 'code', 'time', 'open', 'close', 'high', 'low',
        'volume', 'amount', 'turn', 'time_rank',
        'diff', 'dea', 'macd', 'k', 'd', 'j', 'cci', 'mfi',
        'ma3', 'ma5', 'ma10', 'ma20', 'ma30', 'ma60', 'ma90',
        'boll_upper', 'boll_middle', 'boll_lower',
    }

    def __init__(self, field: str, operator: str, value):
        if operator not in self.VALID_OPS:
            raise ValueError(f"Invalid operator: {operator}. Must be one of {self.VALID_OPS}")
        self.field = field
        self.operator = operator
        self.value = value

    def _is_column_ref(self) -> bool:
        return isinstance(self.value, str) and self.value in self.KNOWN_COLUMNS

    def to_sql_clause(self) -> Tuple[str, list]:
        if self._is_column_ref():
            return f"`{self.field}` {self.operator} `{self.value}`", []
        else:
            return f"`{self.field}` {self.operator} %s", [self.value]


class CrossCondition:
    """
    金叉/死叉检测。

    CrossCondition('diff', 'dea', 'golden')  # MACD 金叉
    CrossCondition('k', 'd', 'death')         # KDJ 死叉
    """

    def __init__(self, fast_field: str, slow_field: str, cross_type: str):
        if cross_type not in ('golden', 'death'):
            raise ValueError("cross_type must be 'golden' or 'death'")
        self.fast_field = fast_field
        self.slow_field = slow_field
        self.cross_type = cross_type


class StockSelector:
    """
    基于技术指标的条件选股器。

    Usage:
        selector = StockSelector()
        selector.add_filter(FilterCondition('macd', '>', 0))
        selector.add_filter(FilterCondition('close', '>', 'ma20'))
        selector.add_cross_filter(CrossCondition('diff', 'dea', 'golden'))
        codes = selector.select('2026-03-28')
    """

    def __init__(self, table_name='stock_indicators_1_day',
                 data_table_name='stock_data_1_day',
                 database_name='quant'):
        self.table_name = table_name
        self.data_table_name = data_table_name
        self.database_name = database_name
        self._filters: List[FilterCondition] = []
        self._cross_filters: List[CrossCondition] = []

    def add_filter(self, condition: FilterCondition):
        self._filters.append(condition)
        return self

    def add_cross_filter(self, condition: CrossCondition):
        self._cross_filters.append(condition)
        return self

    def clear_filters(self):
        self._filters.clear()
        self._cross_filters.clear()
        return self

    def _build_filter_sql(self) -> Tuple[str, list]:
        """构建 WHERE 子句片段（不含交叉条件）"""
        clauses = []
        params = []
        for f in self._filters:
            clause, p = f.to_sql_clause()
            clauses.append(clause)
            params.extend(p)
        return ' AND '.join(clauses) if clauses else '', params

    def _get_db(self) -> MySQLManager:
        config = DATABASE_CONFIG.copy()
        config['database'] = self.database_name
        return MySQLManager(**config)

    def select(self, date: str, limit: Optional[int] = None) -> List[str]:
        """
        查询指定日期满足所有条件的股票代码。
        date: 'YYYY-MM-DD' 或 'YYYYMMDD'
        """
        db = self._get_db()
        try:
            db.connect()
            codes = self._select_impl(db, date, limit, return_data=False)
            return codes
        finally:
            db.disconnect()

    def select_with_data(self, date: str, limit: Optional[int] = None) -> pd.DataFrame:
        """返回匹配股票的完整指标数据"""
        db = self._get_db()
        try:
            db.connect()
            return self._select_impl(db, date, limit, return_data=True)
        finally:
            db.disconnect()

    def select_range(self, start_date: str, end_date: str) -> Dict[str, List[str]]:
        """批量选股：返回 {date: [codes]}"""
        db = self._get_db()
        try:
            db.connect()
            # 获取日期范围内的所有交易日
            date_sql = f"SELECT DISTINCT `date` FROM `{self.table_name}` WHERE `date` >= %s AND `date` <= %s ORDER BY `date`"
            rows = db.execute_query(date_sql, (date, date))
            # 获取所有日期
            date_sql = f"SELECT DISTINCT `date` FROM `{self.table_name}` WHERE `date` >= %s AND `date` <= %s ORDER BY `date`"
            rows = db.execute_query(date_sql, (start_date, end_date))
            dates = [str(r[0]) for r in rows]

            result = {}
            for d in dates:
                codes = self._select_impl(db, d, limit=None, return_data=False)
                if codes:
                    result[d] = codes
            return result
        finally:
            db.disconnect()

    def _select_impl(self, db: MySQLManager, date: str, limit: Optional[int], return_data: bool):
        """内部选股实现"""
        filter_clause, filter_params = self._build_filter_sql()

        if not self._cross_filters:
            # 简单过滤查询
            where_parts = ["`date` = %s"]
            params = [date]
            if filter_clause:
                where_parts.append(filter_clause)
                params.extend(filter_params)

            where_sql = ' AND '.join(where_parts)

            if return_data:
                sql = f"SELECT * FROM `{self.table_name}` WHERE {where_sql}"
            else:
                sql = f"SELECT `code` FROM `{self.table_name}` WHERE {where_sql}"

            if limit:
                sql += f" LIMIT {int(limit)}"

            rows = db.execute_query(sql, tuple(params))

            if return_data:
                if not rows:
                    return pd.DataFrame()
                # 获取列名
                cursor = db.conn.cursor()
                cursor.execute(f"SELECT * FROM `{self.table_name}` LIMIT 0")
                columns = [desc[0] for desc in cursor.description]
                cursor.close()
                return pd.DataFrame(rows, columns=columns)
            else:
                return [str(r[0]) for r in rows]

        else:
            # 有交叉条件：需要 self-join 比较当日和前一交易日
            return self._select_with_cross(db, date, filter_clause, filter_params, limit, return_data)

    def _select_with_cross(self, db, date, filter_clause, filter_params, limit, return_data):
        """处理含有金叉/死叉条件的查询"""
        # 构建交叉条件
        cross_clauses = []
        for cc in self._cross_filters:
            if cc.cross_type == 'golden':
                # 当日 fast > slow，前日 fast <= slow
                cross_clauses.append(
                    f"curr.`{cc.fast_field}` > curr.`{cc.slow_field}` "
                    f"AND prev.`{cc.fast_field}` <= prev.`{cc.slow_field}`"
                )
            else:  # death
                cross_clauses.append(
                    f"curr.`{cc.fast_field}` < curr.`{cc.slow_field}` "
                    f"AND prev.`{cc.fast_field}` >= prev.`{cc.slow_field}`"
                )

        # 普通过滤条件加 curr. 前缀
        curr_filter = ''
        if filter_clause:
            curr_filter = ' AND ' + filter_clause.replace('`', 'curr.`')

        cross_sql = ' AND '.join(cross_clauses)

        select_cols = "curr.*" if return_data else "curr.`code`"

        sql = f"""
            SELECT {select_cols}
            FROM `{self.table_name}` curr
            JOIN `{self.table_name}` prev
                ON curr.`code` = prev.`code`
                AND prev.`date` = (
                    SELECT MAX(`date`) FROM `{self.table_name}`
                    WHERE `code` = curr.`code` AND `date` < %s
                )
            WHERE curr.`date` = %s
                AND {cross_sql}
                {curr_filter}
        """
        params = [date, date] + filter_params

        if limit:
            sql += f" LIMIT {int(limit)}"

        rows = db.execute_query(sql, tuple(params))

        if return_data:
            if not rows:
                return pd.DataFrame()
            cursor = db.conn.cursor()
            cursor.execute(f"SELECT * FROM `{self.table_name}` LIMIT 0")
            columns = [desc[0] for desc in cursor.description]
            cursor.close()
            return pd.DataFrame(rows, columns=columns)
        else:
            return [str(r[0]) for r in rows]
