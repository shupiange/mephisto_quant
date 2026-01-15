import math

import pandas as pd


def verify_trades():
    # 1. 加载交易记录
    try:
        df = pd.read_csv("trade_log.csv")
    except FileNotFoundError:
        print("错误: 未找到 trade_log.csv 文件")
        return

    print("开始验算交易记录...\n")

    initial_capital = 500000.0
    print(f"初始资金: {initial_capital:.2f}")
    calculated_cash = initial_capital

    # 错误容忍度 (浮点数精度)
    epsilon = 1e-2

    for index, row in df.iterrows():
        symbol = row["symbol"]
        direction = row["direction"]
        price = row["price"]
        quantity = row["quantity"]
        commission = row["commission"]
        log_cash_before = row["cash_before"]
        log_cash_after = row["cash_after"]

        # 验算 cash_before
        if abs(calculated_cash - log_cash_before) > epsilon:
            print(
                f"行 {index + 2} 错误: 预期 cash_before {calculated_cash:.2f}, 实际 {log_cash_before:.2f}"
            )

        trade_amount = price * quantity

        if direction == "BUY":
            cash_change = -(trade_amount + commission)
            print(
                f"行 {index + 2} BUY {symbol}: 价格 {price:.2f} * 数量 {quantity} = {trade_amount:.2f}"
            )
            print(f"    佣金/费用: {commission:.2f}")
            print(f"    现金变更: {cash_change:.2f}")

        elif direction == "SELL":
            cash_change = trade_amount - commission
            print(
                f"行 {index + 2} SELL {symbol}: 价格 {price:.2f} * 数量 {quantity} = {trade_amount:.2f}"
            )
            print(f"    佣金/费用: {commission:.2f}")
            print(f"    现金变更: {cash_change:.2f}")

        calculated_cash += cash_change

        # 验算 cash_after
        if abs(calculated_cash - log_cash_after) > epsilon:
            print(
                f"行 {index + 2} 错误: 预期 cash_after {calculated_cash:.2f}, 实际 {log_cash_after:.2f}"
            )
            print(f"    差异: {calculated_cash - log_cash_after:.2f}")
        else:
            print(f"    -> 验算通过, 当前现金: {calculated_cash:.2f}\n")

    print("验算结束。")
    if calculated_cash < 0:
        print(f"警告: 最终现金为负数 ({calculated_cash:.2f})")


if __name__ == "__main__":
    verify_trades()
