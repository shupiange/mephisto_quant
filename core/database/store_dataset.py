from database.db_manager import MySQLManager
from mysql.connector import Error
import pandas as pd
import os
import shutil
import time
from tqdm import tqdm
from config.database_config import DATABASE_CONFIG
from config.work_config import DATASET_DIR
import argparse

parser = argparse.ArgumentParser(description="存储数据集到 MySQL 数据库的脚本。")
parser.add_argument('--dataset-path', type=str, default='', help="数据集名称或目录路径")
parser.add_argument('--table-name', type=str, required=True, help="目标表名")
parser.add_argument('--database-name', type=str, default="quant", help="数据库名称")



def store_dataset(dataset_path: str, table_name: str, database_name: dict):
    """    存储数据集到 MySQL 数据库的主函数。"""
    # 1. 数据库配置 (请替换为你自己的信息)
    DB_CONFIG = {
        'host': DATABASE_CONFIG['host'],
        'database': DATABASE_CONFIG['database'],
        'user': DATABASE_CONFIG['user'],
        'password': DATABASE_CONFIG['password']
    }

    csv_file_path = f'{DATASET_DIR}/{dataset_path}/' if dataset_path != '' else f'{DATASET_DIR}/'

    # 2. 使用 'with' 语句自动管理连接
    try:
        from config.table_config import TABLE_FIELDS_CONFIG
        with MySQLManager(**DB_CONFIG) as db:
            
            if csv_file_path.endswith('.csv') and os.dir.exists(csv_file_path):
                print("\n--- 开始导入 DataFrame ---")
                df_c = pd.read_csv(csv_file_path, dtype=TABLE_FIELDS_CONFIG[f'{database_name}.{table_name}'])
                db.insert_from_dataframe(table_name, df_c)
                shutil.move(f'{DATASET_DIR}/{dataset_path}/{csv_file_path}', f'{DATASET_DIR}/{dataset_path}/archived/{csv_file_path}')
                print("--- CSV 导入完成 ---")

            elif os.path.isdir(csv_file_path):
                # --- 循环写入多个 CSV 文件 (你的核心需求) ---
                print("\n--- 开始批量导入 CSV ---")
                csv_files = [os.path.join(csv_file_path, f) for f in os.listdir(csv_file_path) if f.endswith('.csv')]
                
                if not csv_files:
                    print(f"警告：目录 {csv_file_path} 中没有找到 CSV 文件。")
                    return
                
                # for file_path in tqdm(csv_files, desc='写入MySql中:'):
                #     df_c = pd.read_csv(file_path, dtype=TABLE_FIELDS_CONFIG[f'{database_name}.{table_name}'])
                #     db.insert_from_dataframe(table_name, df_c)
                #     # print(f"已写入MySql: {file_path}")
                #     shutil.move(file_path, f'{DATASET_DIR}/{dataset_path}/archived/{os.path.basename(file_path)}')
                #     # print(f"已移动文件: {file_path}")
                #     # time.sleep(0.1)
                BATCH_SIZE = 10000
                pending_dfs = []
                processed_files = []

                for file_path in tqdm(csv_files, desc='处理并聚合文件中:'):
                    # 1. 读取数据并加入缓存列表
                    df_c = pd.read_csv(file_path, dtype=TABLE_FIELDS_CONFIG[f'{database_name}.{table_name}'])
                    pending_dfs.append(df_c)
                    processed_files.append(file_path)

                    # 2. 检查是否达到 10000 个文件
                    if len(pending_dfs) >= BATCH_SIZE:
                        # 聚合并写入
                        full_df = pd.concat(pending_dfs, ignore_index=True)
                        db.insert_from_dataframe(table_name, full_df)
                        
                        # 批量移动文件
                        for f in processed_files:
                            shutil.move(f, f'{DATASET_DIR}/{dataset_path}/archived/{os.path.basename(f)}')
                        
                        # 清空缓存
                        pending_dfs = []
                        processed_files = []

                # 3. 循环结束后,处理剩余不足 10000 个的文件(收尾)
                if len(pending_dfs) > 0:
                    full_df = pd.concat(pending_dfs, ignore_index=True)
                    db.insert_from_dataframe(table_name, full_df)
                    for f in processed_files:
                        shutil.move(f, f'{DATASET_DIR}/{dataset_path}/archived/{os.path.basename(f)}')
            else:
                print(f"错误：提供的路径 {csv_file_path} 不是有效的 CSV 文件或目录。")
                return
            
            print("--- CSV 导入完成 ---")


    except Error as e:
        print(f"数据库操作主流程发生严重错误: {e}")

    except ConnectionError as ce:
        print(f"无法连接到数据库,请检查配置: {ce}")

    except Exception as e:
        print(f"发生未知错误: {e}")



if __name__ == '__main__':
    args = parser.parse_args()
    print(f"正在存储数据集: {DATASET_DIR}/{args.dataset_path}/ 到表 {args.table_name} (数据库: {args.database_name})")
    store_dataset(args.dataset_path, args.table_name, args.database_name)
    print("\n数据集存储流程完成。")
