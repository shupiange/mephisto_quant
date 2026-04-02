#!/bin/bash

# 获取脚本所在目录的绝对路径，作为项目根目录
PROJECT_ROOT="$( cd "$( dirname "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )"

# 检查是否提供了至少一个参数
if [ -z "$1" ]; then
    echo "Usage: $0 [daily|30m] [start_date] [end_date]"
    echo "Example 1: $0 daily                   (Run daily data update for today)"
    echo "Example 2: $0 daily 2025-01-01        (Run from 2025-01-01 to today)"
    echo "Example 3: $0 daily 2025-01-01 2025-01-15 (Run specific range)"
    echo "Example 4: $0 30m 2025-01-01 2025-01-15"
    exit 1
fi

MODE=$1
START_DATE=$2
END_DATE=$3

# 如果未提供 start_date，默认为今天
if [ -z "$START_DATE" ]; then
    START_DATE=$(date +%Y-%m-%d)
fi

# 如果未提供 end_date，默认为今天
if [ -z "$END_DATE" ]; then
    END_DATE=$(date +%Y-%m-%d)
fi

# 根据模式设置参数
if [ "$MODE" = "daily" ]; then
    # 日线数据：前复权(2)，频率 d，路径 daily_1_data，开启复权检测(pre-adjust)
    ADJUST_FLAG="2"
    FREQUENCY="d"
    PATH_NAME="daily_1_data"
    PRE_ADJUST="--pre-adjust True"
    
    # 指标更新相关参数
    SOURCE_TABLE="stock_data_1_day"
    TARGET_TABLE="stock_indicators_1_day"
    
elif [ "$MODE" = "30m" ]; then
    # 30分钟线数据：后复权(1)，频率 30，路径 minutes_30_data，不检测复权
    ADJUST_FLAG="1"
    FREQUENCY="30"
    PATH_NAME="minutes_30_data"
    PRE_ADJUST=""

    # 指标更新相关参数（30分钟级别）
    SOURCE_TABLE="stock_data_30_minute"
    TARGET_TABLE="stock_indicators_30_minute"

    # 聚合后的日线后复权相关参数
    HFQ_DAILY_SOURCE_TABLE="stock_data_30_minute"
    HFQ_DAILY_TARGET_TABLE="stock_data_1_day_hfq"
    HFQ_DAILY_INDICATOR_SOURCE="stock_data_1_day_hfq"
    HFQ_DAILY_INDICATOR_TARGET="stock_indicators_1_day_hfq"
else
    echo "Error: Invalid mode '$MODE'. Please use 'daily' or '30m'."
    exit 1
fi

# 1. 运行数据下载/更新
echo "==================================================="
echo "Step 1: Updating Market Data ($MODE)"
echo "Date Range: $START_DATE to $END_DATE"
echo "==================================================="

CMD_DATA="python3 $PROJECT_ROOT/core/data/collector.py \
    --start-date $START_DATE \
    --end-date $END_DATE \
    --adjust-flag $ADJUST_FLAG \
    --frequency $FREQUENCY \
    --path $PATH_NAME \
    $PRE_ADJUST"

echo "Executing: $CMD_DATA"
$CMD_DATA

# 检查上一步是否成功
if [ $? -ne 0 ]; then
    echo "Error: Market data update failed. Aborting indicator update."
    exit 1
fi

# 2. 将 CSV 数据导入数据库
echo "==================================================="
echo "Step 2: Importing Data to Database ($MODE)"
echo "Dataset: $PATH_NAME -> Table: $SOURCE_TABLE"
echo "==================================================="

CMD_DB_IMPORT="$PROJECT_ROOT/update_database.sh $MODE"

echo "Executing: $CMD_DB_IMPORT"
$CMD_DB_IMPORT

# 检查上一步是否成功
if [ $? -ne 0 ]; then
    echo "Error: Database import failed. Aborting indicator update."
    exit 1
fi

# 3. (仅30m模式) 聚合30分钟后复权数据为日线后复权数据
if [ "$MODE" = "30m" ]; then
    echo "==================================================="
    echo "Step 3: Aggregating 30m -> Daily Back-Adjusted (hfq)"
    echo "Source: $HFQ_DAILY_SOURCE_TABLE -> Target: $HFQ_DAILY_TARGET_TABLE"
    echo "==================================================="

    CMD_AGGREGATE="python3 $PROJECT_ROOT/core/data/aggregator.py \
        --source-table $HFQ_DAILY_SOURCE_TABLE \
        --target-table $HFQ_DAILY_TARGET_TABLE \
        --start-date $START_DATE \
        --end-date $END_DATE"

    echo "Executing: $CMD_AGGREGATE"
    $CMD_AGGREGATE

    if [ $? -ne 0 ]; then
        echo "Error: Aggregation failed. Aborting."
        exit 1
    fi

    # 4. (仅30m模式) 计算日线后复权指标
    echo "==================================================="
    echo "Step 4: Updating Daily Back-Adjusted Indicators (hfq)"
    echo "Source: $HFQ_DAILY_INDICATOR_SOURCE -> Target: $HFQ_DAILY_INDICATOR_TARGET"
    echo "==================================================="

    CMD_HFQ_INDICATORS="python3 $PROJECT_ROOT/core/data/scholar.py \
        --source-table $HFQ_DAILY_INDICATOR_SOURCE \
        --target-table $HFQ_DAILY_INDICATOR_TARGET \
        --start-date $START_DATE \
        --end-date $END_DATE"

    echo "Executing: $CMD_HFQ_INDICATORS"
    $CMD_HFQ_INDICATORS

    if [ $? -ne 0 ]; then
        echo "Error: Daily hfq indicator calculation failed. Aborting."
        exit 1
    fi
fi

# 5. 运行30分钟/日线指标计算
echo "==================================================="
echo "Step 5: Updating Indicators ($MODE)"
echo "Source: $SOURCE_TABLE -> Target: $TARGET_TABLE"
echo "==================================================="

CMD_INDICATORS="python3 $PROJECT_ROOT/core/data/scholar.py \
    --source-table $SOURCE_TABLE \
    --target-table $TARGET_TABLE \
    --start-date $START_DATE \
    --end-date $END_DATE"

echo "Executing: $CMD_INDICATORS"
$CMD_INDICATORS

echo "==================================================="
echo "All Tasks Completed Successfully!"
echo "==================================================="
