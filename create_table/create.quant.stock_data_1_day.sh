#!/bin/bash

# 要创建的数据库和表名
DATABASE_NAME="quant"
TABLE_NAME="stock_data_1_day"

MYSQL_CMD="sudo mysql"

echo "开始创建数据库和表..."

# SQL命令
SQL_COMMAND="
CREATE TABLE quant.stock_data_1_day (
    -- 主键
    id BIGINT PRIMARY KEY AUTO_INCREMENT COMMENT '主键ID',

    -- 交易日期,使用 CHAR(10) 或 DATE 类型存储
    date CHAR(10) NOT NULL          COMMENT '交易日期 (YYYY-MM-DD)',

    -- 证券代码
    code VARCHAR(15) NOT NULL       COMMENT '证券代码 (例如 sh.600519)',

    -- 开盘价
    open DECIMAL(16, 4)             COMMENT '开盘价',

    -- 收盘价
    close DECIMAL(16, 4)            COMMENT '收盘价',

    -- 最高价
    high DECIMAL(16, 4)             COMMENT '最高价',

    -- 最低价
    low DECIMAL(16, 4)              COMMENT '最低价',

    -- 成交量(单位：股,取决于数据源)
    volume BIGINT                   COMMENT '成交量',
    
    -- 成交额 (注意：您提供的 amount 类型应为 DECIMAL(18, 2),但金额通常需要更高的精度和位数,这里沿用您的 DECIMAL(18, 2))
    amount DECIMAL(20, 4)           COMMENT '成交额',

    -- 换手率(百分比,0-100之间)
    turn  DECIMAL(10, 4)            COMMENT '换手率',

    -- 涨跌幅(百分比,0-100之间)
    pct_chg DECIMAL(10, 4)          COMMENT '涨跌幅',

    -- 滚动市盈率(pe TTM)
    pe_ttm DECIMAL(16, 4)           COMMENT '滚动市盈率(TTM)',

    -- 市净率(PB)
    pb DECIMAL(16, 4)               COMMENT '市净率(PB)',

    -- 滚动市销率(ps TTM)
    ps_ttm DECIMAL(16, 4)           COMMENT '滚动市销率(TTM)',

    -- 滚动市现率(pcf TTM)
    pcf_ttm DECIMAL(16, 4)          COMMENT '滚动市现率(TTM)',

    -- 交易状态(0未停牌,1停牌)
    trade_status TINYINT            COMMENT '交易状态(0停牌,1正常交易)',

    -- 索引定义
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
