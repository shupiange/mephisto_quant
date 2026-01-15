from database.db_manager import MySQLManager
from mysql.connector import Error
import pandas as pd
import os
import shutil
from config.database_config import DATABASE_CONFIG
from config.work_config import WORK_DIR
import argparse


def load_dataset(codes, start_date: str = None, end_date: str = None, table_name: str = 'stock_data_30_minute', database_name: str = 'quant') -> pd.DataFrame:
    """查询一个或多个 `code` 的数据并返回 Pandas DataFrame。

    Args:
        codes (str|int|list): 单个 code(字符串或整数)或多个 code 的可迭代对象。
        start_date (str, optional): 起始日期（包含），格式如 'YYYY-MM-DD'。
        end_date (str, optional): 结束日期（包含），格式如 'YYYY-MM-DD'。
        table_name (str): 表名（不带数据库前缀）。
        database_name (str): 数据库名。

    Returns:
        pd.DataFrame: 查询结果，若出错或无数据返回空的 DataFrame。
    """
    # 规范化 codes 为列表
    if codes is None:
        raise ValueError("参数 'codes' 不能为空")

    if isinstance(codes, (str, int)):
        codes_list = [str(codes)]
    elif isinstance(codes, (list, tuple, set)):
        codes_list = [str(c) for c in codes]
    else:
        raise TypeError("参数 'codes' 必须是 str/int 或 可迭代的 codes 列表")

    if not codes_list:
        return pd.DataFrame()

    # 准备 DB 配置
    DB_CONFIG = {
        'host': DATABASE_CONFIG['host'],
        'database': DATABASE_CONFIG['database'],
        'user': DATABASE_CONFIG['user'],
        'password': DATABASE_CONFIG['password']
    }

    # 构造 SQL
    placeholders = ','.join(['%s'] * len(codes_list))
    sql = f"SELECT * FROM `{database_name}`.`{table_name}` WHERE `code` IN ({placeholders})"
    params = tuple(codes_list)

    if start_date:
        sql += " AND `date` >= %s"
        params = params + (start_date,)
    if end_date:
        sql += " AND `date` <= %s"
        params = params + (end_date,)

    sql += " ORDER BY `date` ASC"

    # 执行查询并构造 DataFrame（带列名）
    try:
        with MySQLManager(**DB_CONFIG) as db:
            db._ensure_connected()
            cursor = db.conn.cursor()
            try:
                cursor.execute(sql, params)
                rows = cursor.fetchall()
                cols = [desc[0] for desc in cursor.description] if cursor.description else []
                df = pd.DataFrame(rows, columns=cols)
                return df
            finally:
                cursor.close()

    except Error as e:
        print(f"查询数据库时发生错误: {e}")
        return pd.DataFrame()


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="按 code 查询数据集并打印结果。支持多个 code（用逗号分隔）。")
    parser.add_argument('--codes', type=str, required=True, help="单个 code 或以逗号分隔的多个 code，例如: 600001 或 600001,600002")
    parser.add_argument('--start-date', type=str, default=None, help="起始日期，格式 YYYY-MM-DD")
    parser.add_argument('--end-date', type=str, default=None, help="结束日期，格式 YYYY-MM-DD")
    parser.add_argument('--table-name', type=str, default='stock_data_30_minute', help="表名（不带库名）")
    parser.add_argument('--database-name', type=str, default='quant', help="数据库名")

    args = parser.parse_args()
    codes = [c.strip() for c in args.codes.split(',') if c.strip()]

    print(f"查询 codes={codes}，日期范围: {args.start_date} - {args.end_date}，表: {args.database_name}.{args.table_name}")
    df = load_dataset(codes, start_date=args.start_date, end_date=args.end_date, table_name=args.table_name, database_name=args.database_name)
    if df.empty:
        print("未查询到数据。")
    else:
        print(df.head(100).to_string(index=False))