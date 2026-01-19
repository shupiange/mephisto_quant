#!/bin/bash

# 要创建的数据库和表名
DATABASE_NAME="quant"
TABLE_NAME="stock_data_1_day_indicators"

MYSQL_CMD="sudo mysql"

echo "开始创建数据库和表..."

# SQL命令
SQL_COMMAND="
CREATE TABLE IF NOT EXISTS quant.stock_data_1_day_indicators (
    -- 主键
    id BIGINT PRIMARY KEY AUTO_INCREMENT COMMENT '主键ID',

    -- 交易日期
    date CHAR(10) NOT NULL          COMMENT '交易日期 (YYYY-MM-DD)',

    -- 证券代码
    code VARCHAR(15) NOT NULL       COMMENT '证券代码 (例如 sh.600519)',

    -- CCI指标
    cci DECIMAL(16, 4)              COMMENT 'CCI指标',

    -- MFI指标
    mfi DECIMAL(16, 4)              COMMENT 'MFI指标',

    -- MACD指标
    macd DECIMAL(16, 4)             COMMENT 'MACD: DIF',
    macd_signal DECIMAL(16, 4)      COMMENT 'MACD: DEA',
    macd_hist DECIMAL(16, 4)        COMMENT 'MACD: MACD柱',

    -- KDJ指标
    kdj_k DECIMAL(16, 4)            COMMENT 'KDJ: K',
    kdj_d DECIMAL(16, 4)            COMMENT 'KDJ: D',
    kdj_j DECIMAL(16, 4)            COMMENT 'KDJ: J',

    -- 索引定义
    INDEX idx_date (date),
    INDEX idx_code (code),
    UNIQUE KEY idx_code_date (code, date)
);
"

# 执行SQL命令
if echo "$SQL_COMMAND" | $MYSQL_CMD; then
    echo "表 '$TABLE_NAME' 创建成功！"
else
    echo "错误: 执行SQL命令失败。"
    exit 1
fi

echo "脚本执行完成。"
