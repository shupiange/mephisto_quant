from database.update_db_dataset import write_dataframe_to_mysql, write_csv_to_mysql
from database.db_connector import StockDBManager
import pandas as pd
from config.database_config import DATABASE_CONFIG
from config.work_config import WORK_DIR




if __name__ == '__main__':

    # 创建数据库管理实例并连接
    db_manager = StockDBManager(
        host=DATABASE_CONFIG['host'],
        database=DATABASE_CONFIG['database'],
        user=DATABASE_CONFIG['user'],
        password=DATABASE_CONFIG['password']
    )
    db_manager.connect()

    # 示例: 读取 CSV 并写入数据库
    csv_file_path = f'{WORK_DIR}/dataset/trade_minute_000001_2023-01-01_2023-01-15.csv'
    if os.path.exists(csv_file_path):
        write_csv_to_mysql(db_manager, csv_file_path, 'quant.stock_data_5_minute')
        print(f"已将 CSV 文件 {csv_file_path} 的数据写入数据库。")    
    else:
        print(f"错误: CSV 文件 {csv_file_path} 不存在。")

    # 断开数据库连接
    db_manager.disconnect()