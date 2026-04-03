#!/bin/bash

DATABASE_NAME="quant"
TABLE_NAME="market_environment"

MYSQL_CMD="sudo mysql"

echo "开始创建数据库和表..."

SQL_COMMAND="
CREATE TABLE IF NOT EXISTS quant.market_environment (
    id BIGINT PRIMARY KEY AUTO_INCREMENT COMMENT '主键ID',

    date CHAR(10) NOT NULL          COMMENT '日期 (YYYY-MM-DD)',
    code VARCHAR(20) NOT NULL       COMMENT '标的代码',
    name VARCHAR(50)                COMMENT '标的名称',

    open DECIMAL(20, 4)             COMMENT '开盘价/值',
    close DECIMAL(20, 4)            COMMENT '收盘价/值',
    high DECIMAL(20, 4)             COMMENT '最高价/值',
    low DECIMAL(20, 4)              COMMENT '最低价/值',
    volume BIGINT                   COMMENT '成交量',
    amount DECIMAL(20, 4)           COMMENT '成交额',

    UNIQUE KEY uk_date_code (date, code),
    INDEX idx_date (date),
    INDEX idx_code (code)
) COMMENT='市场环境数据（指数/黄金/利率等）';
"

if echo "$SQL_COMMAND" | $MYSQL_CMD; then
    echo "数据库 '$DATABASE_NAME' 和表 '$TABLE_NAME' 创建成功！"
else
    echo "错误: 执行SQL命令失败。"
    exit 1
fi

echo "脚本执行完成。"
