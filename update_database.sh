#!/bin/bash

# 获取脚本所在目录的绝对路径，作为项目根目录
PROJECT_ROOT="$( cd "$( dirname "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )"

# 检查是否提供了至少一个参数
if [ -z "$1" ]; then
    echo "Usage: $0 [daily|30m]"
    echo "Example 1: $0 daily   (Update daily stock data to DB)"
    echo "Example 2: $0 30m     (Update 30-minute stock data to DB)"
    exit 1
fi

MODE=$1

# 根据模式设置参数
if [ "$MODE" == "daily" ]; then
    DATASET_PATH="daily_1_data"
    TABLE_NAME="stock_data_1_day"
    DATABASE_NAME="quant"
elif [ "$MODE" == "30m" ]; then
    DATASET_PATH="minutes_30_data"
    TABLE_NAME="stock_data_30_minute"
    DATABASE_NAME="quant"
else
    echo "Error: Invalid mode '$MODE'. Please use 'daily' or '30m'."
    exit 1
fi

# 构建命令
# 假设 write_dataset.py 位于 core/database/ 目录下，或者项目根目录下
# 根据之前的 ls 结果，如果 write_dataset.py 在根目录，则直接调用；如果在 core/database，则调整路径
# 既然原文件写的是 python write_dataset.py，我先假设它在根目录。
# 但为了保险，我先检查根目录是否有该文件，如果没有，则检查 core/database/
SCRIPT_PATH="$PROJECT_ROOT/write_dataset.py"
if [ ! -f "$SCRIPT_PATH" ]; then
    SCRIPT_PATH="$PROJECT_ROOT/core/database/write_dataset.py"
    if [ ! -f "$SCRIPT_PATH" ]; then
        echo "Error: write_dataset.py not found in project root or core/database/"
        exit 1
    fi
fi

CMD="python3 $SCRIPT_PATH --dataset-path $DATASET_PATH --table-name $TABLE_NAME --database-name $DATABASE_NAME"

# 打印并执行命令
echo "---------------------------------------------------"
echo "Starting Database Update Task"
echo "Mode: $MODE"
echo "Dataset: $DATASET_PATH"
echo "Target Table: $TABLE_NAME"
echo "Command: $CMD"
echo "---------------------------------------------------"

$CMD
