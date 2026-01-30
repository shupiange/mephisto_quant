#!/bin/bash

# 要创建的数据库和表名
DATABASE_NAME="quant"
TABLE_NAME="stock_indicators_1_day"

MYSQL_CMD="sudo mysql"

echo "开始创建数据库和表..."

# SQL命令
SQL_COMMAND="
CREATE TABLE IF NOT EXISTS quant.stock_indicators_1_day (
    -- 主键
    id BIGINT PRIMARY KEY AUTO_INCREMENT COMMENT '主键ID',

    -- 交易日期
    date CHAR(10) NOT NULL          COMMENT '交易日期 (YYYY-MM-DD)',

    -- 证券代码
    code VARCHAR(15) NOT NULL       COMMENT '证券代码',

    -- MACD指标
    diff DECIMAL(20, 6)             COMMENT 'MACD DIF线',
    dea  DECIMAL(20, 6)             COMMENT 'MACD DEA线',
    macd DECIMAL(20, 6)             COMMENT 'MACD柱',

    -- KDJ指标
    k DECIMAL(20, 6)                COMMENT 'KDJ K值',
    d DECIMAL(20, 6)                COMMENT 'KDJ D值',
    j DECIMAL(20, 6)                COMMENT 'KDJ J值',

    -- CCI指标
    cci DECIMAL(20, 6)              COMMENT 'CCI指标',

    -- MFI指标
    mfi DECIMAL(20, 6)              COMMENT 'MFI指标',

    -- 移动平均线 (MA)
    ma3  DECIMAL(20, 6)             COMMENT '3日均线',
    ma5  DECIMAL(20, 6)             COMMENT '5日均线',
    ma10 DECIMAL(20, 6)             COMMENT '10日均线',
    ma20 DECIMAL(20, 6)             COMMENT '20日均线',
    ma30 DECIMAL(20, 6)             COMMENT '30日均线',
    ma60 DECIMAL(20, 6)             COMMENT '60日均线',
    ma90 DECIMAL(20, 6)             COMMENT '90日均线',

    -- 布林通道 (Bollinger Bands)
    boll_upper  DECIMAL(20, 6)      COMMENT '布林通道上轨',
    boll_middle DECIMAL(20, 6)      COMMENT '布林通道中轨',
    boll_lower  DECIMAL(20, 6)      COMMENT '布林通道下轨',

    -- 索引定义
    UNIQUE KEY uk_date_code (date, code),
    INDEX idx_date (date),
    INDEX idx_code (code)
);
"

# 执行SQL命令
if echo "$SQL_COMMAND" | $MYSQL_CMD; then
    echo "数据库 '$DATABASE_NAME' 和表 '$TABLE_NAME' 创建成功！"
else
    echo "错误: 执行SQL命令失败。"
    exit 1
fi

echo "脚本执行完成。"
