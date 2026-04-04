#!/bin/bash

# CSV → MySQL 导入脚本
# 用法: ./update_database.sh [daily|30m]

PROJECT_ROOT="$( cd "$( dirname "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )"

MODE=$1

if [ "$MODE" = "daily" ]; then
    DATASET_PATH="daily_1_data"
    TABLE_NAME="stock_data_1_day"
elif [ "$MODE" = "30m" ]; then
    DATASET_PATH="minutes_30_data"
    TABLE_NAME="stock_data_30_minute"
else
    echo "Error: Invalid mode '$MODE'. Use 'daily' or '30m'."
    exit 1
fi

echo "Importing CSV from $DATASET_PATH to table $TABLE_NAME..."

python3 "$PROJECT_ROOT/write_dataset.py" \
    --dataset-path "$DATASET_PATH" \
    --table-name "$TABLE_NAME" \
    --database-name "quant"

if [ $? -ne 0 ]; then
    echo "Error: Database import failed."
    exit 1
fi

echo "Database import completed successfully."
