from database.db_connector import StockDBManager
from config.table_config import TABLE_FIELDS_CONFIG
import pandas as pd
from typing import List, Dict, Any


def read_dataframe_from_mysql(db_manager: StockDBManager, query: str) -> pd.DataFrame:
    return


def write_dataframe_to_mysql(db_manager: StockDBManager, df: pd.DataFrame, table_fields: List[str]):
    """
    将 Pandas DataFrame 写入 MySQL 表。

    Args:
        db_manager: 已经连接的 StockDBManager 实例。
        df: 要写入的 Pandas DataFrame。
        table_fields: 数据库表中的目标字段名列表，**顺序必须与 DataFrame 列名一致**。
    """
    if df.empty:
        print("警告: DataFrame 为空，没有数据可写入。")
        return

    # 1. 确保 DataFrame 的列名与数据库字段名匹配，并调整顺序
    try:
        # 仅选择与数据库字段匹配的列，并按顺序排列
        df_selected = df[table_fields]
    except KeyError as e:
        print(f"错误:DataFrame 缺少数据库所需的字段: {e}")
        return

    # 2. 将 DataFrame 转换为字典列表
    # 'records' 格式将每一行转换为一个字典
    data_list: List[Dict[str, Any]] = df_selected.to_dict('records')

    # 3. 调用 StockDBManager 的批量插入方法
    # 注意：insert_many_data 默认假设字典的键和顺序就是 SQL 语句的字段。
    # 因为我们已经在 df_selected 中确保了正确的列和顺序，所以这里可以安全调用。
    db_manager.insert_many_data(data_list)
    
    print(f"尝试将 {len(data_list)} 条记录写入数据库。")
    
    return

def write_csv_to_mysql(db_manager: StockDBManager, csv_file_path: str, table_fields_name: str):
    """
    读取本地 CSV 文件并将其内容写入 MySQL 表。

    Args:
        db_manager: 已经连接的 StockDBManager 实例。
        csv_file_path: 本地 CSV 文件路径。
        table_fields: 数据库表中的目标字段名列表，**顺序必须与 CSV 列名一致**。
    """
    try:
        df = pd.read_csv(csv_file_path, dtype=TABLE_FIELDS_CONFIG[table_fields_name])
    except Exception as e:
        print(f"错误: 无法读取 CSV 文件 {csv_file_path}: {e}")
        return
    table_fields = TABLE_FIELDS_CONFIG[f'{table_fields_name}_fields']
    write_dataframe_to_mysql(db_manager, df, table_fields)
    
    return