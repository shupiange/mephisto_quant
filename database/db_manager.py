import mysql.connector
from mysql.connector import Error
from typing import List, Tuple, Dict, Any
import csv

import pandas as pd
from pandas import DataFrame

class MySQLManager:
    """
    一个通用的 MySQL 数据库管理器，支持上下文管理、
    常规查询、批量插入以及从 CSV/Pandas DataFrame 导入数据。
    """
    def __init__(self, host, database, user, password, port=3306):
        self.host = host
        self.database = database
        self.user = user
        self.password = password
        self.port = port
        self.conn = None

    def __enter__(self):
        """上下文管理器：建立数据库连接"""
        self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """上下文管理器：关闭数据库连接"""
        self.disconnect()
        # 返回 False 会重新抛出在 'with' 块内部发生的异常
        return False

    def connect(self):
        """手动建立数据库连接"""
        if self.conn is None or not self.conn.is_connected():
            try:
                self.conn = mysql.connector.connect(
                    host=self.host,
                    database=self.database,
                    user=self.user,
                    password=self.password,
                    port=self.port
                )
                if self.conn.is_connected():
                    print("数据库连接成功。")
            except Error as e:
                print(f"连接数据库时发生错误: {e}")
                self.conn = None # 确保连接失败时 self.conn 为 None
                raise # 抛出异常，阻止后续操作

    def disconnect(self):
        """手动关闭数据库连接"""
        if self.conn and self.conn.is_connected():
            self.conn.close()
            print("数据库连接关闭。")

    def _ensure_connected(self):
        """内部方法：确保数据库已连接"""
        if not self.conn or not self.conn.is_connected():
            print("错误：数据库未连接或连接已断开。")
            raise ConnectionError("数据库未连接")

    def execute_query(self, query: str, params: Tuple = None) -> List[Tuple]:
        """
        【推荐】执行 SELECT 查询并返回所有结果。

        Args:
            query (str): SQL SELECT 语句。
            params (Tuple, optional): 查询参数（用于防止 SQL 注入）。

        Returns:
            List[Tuple]: 查询结果, 列表中的每个元素是一个元组 (row)。
        """
        self._ensure_connected()
        cursor = self.conn.cursor()
        try:
            cursor.execute(query, params or ())
            results = cursor.fetchall()
            return results
        except Error as e:
            print(f"执行查询失败: {e}")
            return []
        finally:
            cursor.close()

    def execute_non_query(self, query: str, params: Tuple = None) -> int:
        """
        【推荐】执行非查询语句 (INSERT, UPDATE, DELETE, CREATE 等)。
        
        Args:
            query (str): SQL DML/DDL 语句。
            params (Tuple, optional): 查询参数。

        Returns:
            int: 受影响的行数。
        """
        self._ensure_connected()
        cursor = self.conn.cursor()
        try:
            cursor.execute(query, params or ())
            self.conn.commit()
            rowcount = cursor.rowcount
            print(f"操作成功：{rowcount} 行受到影响。")
            return rowcount
        except Error as e:
            self.conn.rollback()
            print(f"执行非查询操作失败: {e}")
            return -1
        finally:
            cursor.close()

    # --- 增 (Create) ---
    
    def insert_many_data(self, table_name: str, data_list: List[Dict[str, Any]]):
        """
        【改进】批量插入多条数据记录（来自字典列表）。

        Args:
            table_name (str): 目标表名。
            data_list (List[Dict[str, Any]]): 包含多条记录字典的列表。
        """
        if not data_list:
            print("警告：传入的批量数据列表为空。")
            return

        self._ensure_connected()
        
        # 改进点 1: 确保键的顺序是固定的
        first_record = data_list[0]
        # 改进点 2: 使用反引号 (`) 来保护列名，防止是 SQL 关键字
        keys_list = list(first_record.keys())
        keys_sql = ', '.join([f"`{k}`" for k in keys_list])
        
        # 使用 %s 作为占位符
        placeholders = ', '.join(['%s'] * len(keys_list))
        
        # 构造 SQL 语句
        sql = f"INSERT INTO {table_name} ({keys_sql}) VALUES ({placeholders})"
        
        # 改进点 3: 严格按照 keys_list 的顺序从每个字典中取值
        data_to_insert = [tuple(d[key] for key in keys_list) for d in data_list]

        cursor = self.conn.cursor()
        try:
            # 使用 executemany 进行高效批量写入
            cursor.executemany(sql, data_to_insert)
            self.conn.commit()
            print(f"批量写入 {table_name} 成功：共插入 {cursor.rowcount} 行数据。")
        
        except Error as e:
            self.conn.rollback()
            print(f"执行批量写入 {table_name} 失败: {e}")
            
        finally:
            cursor.close()

    def insert_from_csv(self, table_name: str, file_path: str, encoding='utf-8'):
        """
        【新增】从 CSV 文件读取数据并批量插入数据库。
        假设 CSV 的第一行是表头（列名），且与数据库列名匹配。
        
        Args:
            table_name (str): 目标表名。
            file_path (str): CSV 文件路径。
            encoding (str, optional): 文件编码。
        """
        print(f"开始从 CSV ({file_path}) 导入数据到表 {table_name}...")
        try:
            with open(file_path, mode='r', encoding=encoding) as f:
                # 使用 DictReader 可以自动将每行转为字典
                reader = csv.DictReader(f)
                data_list = [row for row in reader]
                
            if not data_list:
                print(f"警告：CSV 文件 {file_path} 为空或只有表头。")
                return
            
            # 复用 insert_many_data 方法
            self.insert_many_data(table_name, data_list)

        except FileNotFoundError:
            print(f"错误：CSV 文件未找到: {file_path}")
        except Exception as e:
            print(f"读取或插入 CSV 时发生错误: {e}")

    def insert_from_dataframe(self, table_name: str, df: DataFrame):
        """
        【新增】从 Pandas DataFrame 读取数据并批量插入数据库。
        假设 DataFrame 的列名与数据库列名匹配。

        Args:
            table_name (str): 目标表名。
            df (DataFrame): Pandas DataFrame。
        """
        if pd is None:
            print("错误：`insert_from_dataframe` 需要 'pandas' 库，请先安装。")
            return
            
        if not isinstance(df, pd.DataFrame):
            print(f"错误：提供的数据类型不是 Pandas DataFrame (而是 {type(df)})。")
            return

        if df.empty:
            print("警告：传入的 DataFrame 为空。")
            return
            
        print(f"开始从 DataFrame 导入数据到表 {table_name}...")
        # 将 DataFrame 转换为字典列表
        # 'records' 格式: [{'col1': val1, 'col2': val2}, ...]
        data_list = df.to_dict('records')
        
        # 复用 insert_many_data 方法
        self.insert_many_data(table_name, data_list)
        
        
# --- 主程序 ---
if __name__ == "__main__":
    

    # 1. 数据库配置 (请替换为你自己的信息)
    DB_CONFIG = {
        'host': 'localhost',
        'database': 'quant', # 你的数据库名
        'user': 'root',
        'password': ''  # 你的密码
    }
    
    # 假设你有一个表叫 stock_daily_data
    # 你需要提前创建它：
    # CREATE TABLE IF NOT EXISTS stock_daily_data (
    #     id INT AUTO_INCREMENT PRIMARY KEY,
    #     date DATE,
    #     code VARCHAR(10),
    #     open DECIMAL(10, 2),
    #     close DECIMAL(10, 2),
    #     high DECIMAL(10, 2),
    #     low DECIMAL(10, 2),
    #     volume BIGINT,
    #     UNIQUE KEY `idx_date_code` (`date`, `code`) # 建议加唯一索引
    # );

    TABLE_NAME = 'stock_data_5_minute' # 你的表名

    # 2. 使用 'with' 语句自动管理连接
    try:
        with MySQLManager(**DB_CONFIG) as db:
            
            # --- 示例 1：循环写入多个 CSV 文件 (你的核心需求) ---
            print("\n--- 开始批量导入 CSV ---")
            csv_dir = 'csv_data'
            csv_files = ['csv_data/stock_A.csv', 'csv_data/stock_B.csv']
            
            for file_path in csv_files:
                db.insert_from_csv(TABLE_NAME, file_path)
            
            print("--- CSV 导入完成 ---")


            # --- 示例 2：从 Pandas DataFrame 写入 ---
            print("\n--- 开始导入 DataFrame ---")
            # 假设这是你用其他方式获取的数据
            from config.table_config import TABLE_FIELDS_CONFIG
            table_fields_name = 'quant.stock_data_5_minute'
            df_c_data = {
                'date': ['2025-11-15'],
                'code': ['600003'],
                'open': [30.0], 'close': [31.0], 'high': [31.5], 'low': [29.9],
                'volume': [8000]
            }
            df_c = pd.DataFrame(df_c_data, dtype=TABLE_FIELDS_CONFIG[table_fields_name])
            
            db.insert_from_dataframe(TABLE_NAME, df_c)
            print("--- DataFrame 导入完成 ---")


            # --- 示例 3：执行查询 (SELECT) ---
            print("\n--- 开始查询数据 ---")
            query = f"SELECT * FROM {TABLE_NAME} WHERE code = %s"
            results = db.execute_query(query, ('600001',))
            
            print(f"查询 600001 的结果 (共 {len(results)} 条):")
            for row in results:
                print(row)
            
            # --- 示例 4：执行非查询 (UPDATE/DELETE) ---
            print("\n--- 开始执行非查询操作 ---")
            # 比如，更新一条数据
            update_sql = f"UPDATE {TABLE_NAME} SET volume = %s WHERE code = %s AND date = %s"
            rows_affected = db.execute_non_query(update_sql, (12500, '600001', '2025-11-14'))

    except Error as e:
        print(f"数据库操作主流程发生严重错误: {e}")
    except ConnectionError as ce:
        print(f"无法连接到数据库，请检查配置: {ce}")