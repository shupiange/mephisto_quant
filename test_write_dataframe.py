from database.db_manager import MySQLManager
from mysql.connector import Error
import pandas as pd
import os
from config.database_config import DATABASE_CONFIG
from config.work_config import WORK_DIR




if __name__ == '__main__':

    
    # 1. 数据库配置 (请替换为你自己的信息)
    DB_CONFIG = {
        'host': DATABASE_CONFIG['host'],
        'database': DATABASE_CONFIG['database'],
        'user': DATABASE_CONFIG['user'],
        'password': DATABASE_CONFIG['password']
    }
    
    # 示例: 读取 CSV 
    csv_file_path = f'{WORK_DIR}/dataset/trade_minute_000001_2023-01-01_2023-01-15.csv'
    
    TABLE_NAME = 'stock_data_5_minute' # 数据库表名

    # 2. 使用 'with' 语句自动管理连接
    try:
        with MySQLManager(**DB_CONFIG) as db:
            
            # # --- 示例 1：循环写入多个 CSV 文件 (你的核心需求) ---
            # print("\n--- 开始批量导入 CSV ---")
            # csv_dir = 'csv_data'
            # csv_files = [csv_file_path]
            
            # for file_path in csv_files:
            #     db.insert_from_csv(TABLE_NAME, file_path)
            
            # print("--- CSV 导入完成 ---")


            # --- 示例 2：从 Pandas DataFrame 写入 ---
            print("\n--- 开始导入 DataFrame ---")
            # 假设这是你用其他方式获取的数据
            from config.table_config import TABLE_FIELDS_CONFIG
            table_fields_name = 'quant.stock_data_5_minute'
            df_c = pd.read_csv(csv_file_path, dtype=TABLE_FIELDS_CONFIG[table_fields_name])
            db.insert_from_dataframe(TABLE_NAME, df_c)
            print("--- DataFrame 导入完成 ---")


            # --- 示例 3：执行查询 (SELECT) ---
            # print("\n--- 开始查询数据 ---")
            # query = f"SELECT * FROM {TABLE_NAME} WHERE code = %s"
            # results = db.execute_query(query, ('600001',))
            
            # print(f"查询 600001 的结果 (共 {len(results)} 条):")
            # for row in results:
            #     print(row)
            
            # --- 示例 4：执行非查询 (UPDATE/DELETE) ---
            # 比如，更新一条数据
            # print("\n--- 开始执行非查询操作 ---")
            # update_sql = f"UPDATE {TABLE_NAME} SET volume = %s WHERE code = %s AND date = %s"
            # rows_affected = db.execute_non_query(update_sql, (12500, '600001', '2025-11-14'))

    except Error as e:
        print(f"数据库操作主流程发生严重错误: {e}")
    except ConnectionError as ce:
        print(f"无法连接到数据库，请检查配置: {ce}")