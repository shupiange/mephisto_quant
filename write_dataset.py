from database.store_dataset import store_dataset

from config.work_config import DATASET_DIR
import argparse

parser = argparse.ArgumentParser(description="存储数据集到 MySQL 数据库的脚本。")
parser.add_argument('--dataset-path', type=str, default='', help="数据集名称或目录路径")
parser.add_argument('--table-name', type=str, required=True, help="目标表名")
parser.add_argument('--database-name', type=str, default="quant", help="数据库名称")


if __name__ == '__main__':
    args = parser.parse_args()
    print(f"正在存储数据集: {DATASET_DIR}/{args.dataset_path} 到表 {args.table_name} (数据库: {args.database_name})")
    store_dataset(args.dataset_path, args.table_name, args.database_name)
    print("\n数据集存储流程完成。")
