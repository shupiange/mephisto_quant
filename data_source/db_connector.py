import mysql.connector
from mysql.connector import Error
from typing import List, Tuple, Dict, Any

class StockDBManager:
    """
    用于管理 stock_data 表的 MySQL 数据库连接和 CRUD 操作的类。
    """
    def __init__(self, host, database, user, password):
        self.host = host
        self.database = database
        self.user = user
        self.password = password
        self.conn = None

    def __enter__(self):
        """上下文管理器：建立数据库连接"""
        try:
            self.conn = mysql.connector.connect(
                host=self.host,
                database=self.database,
                user=self.user,
                password=self.password
            )
            if self.conn.is_connected():
                print("数据库连接成功。")
            return self

        except Error as e:
            print(f"连接数据库时发生错误: {e}")
            self.conn = None # 确保连接失败时 self.conn 为 None
            raise # 抛出异常，阻止后续操作

    def __exit__(self, exc_type, exc_val, exc_tb):
        """上下文管理器：关闭数据库连接"""
        if self.conn and self.conn.is_connected():
            self.conn.close()
            print("数据库连接关闭。")
        
        # 如果 __enter__ 中捕获了连接错误，exc_type/exc_val/exc_tb 将为 None
        # 如果是其他错误，返回 False 会重新抛出异常，True 则抑制异常。
        return False 

    def execute_query(self, query: str, params: Tuple = None) -> List[Tuple]:
        """执行 SQL 查询（增/删/改/查）"""
        if not self.conn or not self.conn.is_connected():
            print("错误：数据库未连接或连接已断开。")
            return []

        cursor = self.conn.cursor()
        try:
            cursor.execute(query, params or ())
            
            if query.strip().upper().startswith(('INSERT', 'UPDATE', 'DELETE')):
                self.conn.commit()
                print(f"操作成功：{cursor.rowcount} 行受到影响。")
                return []
            
            elif query.strip().upper().startswith('SELECT'):
                return cursor.fetchall()
            
            else:
                print("警告：未知的 SQL 操作类型。")
                return [] # 非查询语句且不要求返回数据
        
        except Error as e:
            self.conn.rollback()
            print(f"执行 SQL 失败: {e}")
            return []
        
        finally:
            cursor.close()

    # --- 增 (Create) ---
    def insert_many_data(self, data_list: List[Dict[str, Any]]):
        """
        批量插入多条数据记录。

        Args:
            data_list: 包含多条记录字典的列表。
                       每条记录字典应包含: date, code, open, close, high, low, volume, amount。
        """
        if not data_list:
            print("警告：传入的批量数据列表为空。")
            return

        if not self.conn or not self.conn.is_connected():
            print("错误：数据库未连接或连接已断开。")
            return

        # 假设所有字典的键都相同，以第一个字典为准构造 SQL
        first_record = data_list[0]
        keys = ', '.join(first_record.keys())
        # 使用 %s 作为占位符
        placeholders = ', '.join(['%s'] * len(first_record))
        
        # 构造 SQL 语句
        sql = f"INSERT INTO stock_data ({keys}) VALUES ({placeholders})"
        
        # 将字典列表转换为参数元组的列表
        # 注意：这里要求所有字典的 key 顺序必须一致
        data_to_insert = [tuple(d.values()) for d in data_list]

        cursor = self.conn.cursor()
        try:
            # 使用 executemany 进行批量写入
            cursor.executemany(sql, data_to_insert)
            self.conn.commit()
            print(f"批量写入成功：共插入 {cursor.rowcount} 行数据。")
        
        except Error as e:
            self.conn.rollback()
            print(f"执行批量写入失败: {e}")
            
        finally:
            cursor.close()
            
        return
    

# --- 步骤 3: 示例使用代码 ---

if __name__ == '__main__':
    # 替换为您的数据库连接信息
    DB_CONFIG = {
        "host": "localhost",
        "database": "your_database_name",  # <-- 必须替换
        "user": "your_user",               # <-- 必须替换
        "password": "your_password"        # <-- 必须替换
    }
    
    # 确保您的 stock_data 表已经存在 (参考最开始的建表 SQL)
    
    test_code = '600519'
    test_date = '2025-10-23'
    
    try:
        with StockDBManager(**DB_CONFIG) as db:
            if not db:
                print("无法运行示例，数据库连接失败。")
                exit()
            
            print("\n--- 1. 增 (CREATE) ---")
            
            new_data = {
                'date': test_date,
                'code': test_code,
                'open': 160.00,
                'close': 165.50,
                'high': 168.00,
                'low': 159.00,
                'volume': 100000.55,
                'amount': 16550000.00
            }
            db.insert_data(new_data)

            # --- 2. 查 (RETRIEVE) ---
            print("\n--- 2. 查 (RETRIEVE) - 单条 ---")
            result = db.get_data_by_code_and_date(test_code, test_date)
            if result:
                print(f"查询结果: {result[0]}")
            else:
                print("未查到数据。")

            # --- 3. 改 (UPDATE) ---
            print("\n--- 3. 改 (UPDATE) ---")
            new_close_price = 166.00
            db.update_close_price(new_close_price, test_code, test_date)

            # 验证更新
            print("验证更新后的收盘价:")
            result_updated = db.get_data_by_code_and_date(test_code, test_date)
            if result_updated:
                # 假设收盘价在索引 4 (id, date, code, open, *close*)
                print(f"更新后收盘价: {result_updated[0][4]}")
            
            # --- 4. 删 (DELETE) ---
            print("\n--- 4. 删 (DELETE) ---")
            db.delete_data_by_code(test_code)
            
            # 验证删除
            print("验证删除:")
            result_deleted = db.get_data_by_code_and_date(test_code, test_date)
            if not result_deleted:
                print(f"证券代码 {test_code} 的数据已成功删除。")
                
    except Error as e:
        print(f"程序执行过程中发生数据库错误: {e}")
    except Exception as e:
        print(f"发生其他错误: {e}")