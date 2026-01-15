import os
import sys

import pandas as pd

# 添加项目根目录到 sys.path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from core import IndicatorHelper


def main():
    """
    演示如何使用 IndicatorHelper 计算各种技术指标。
    """
    # 1. 创建模拟数据
    data = {
        'open': [100, 101, 102, 103, 104, 105, 104, 103, 102, 101] * 5,
        'high': [105, 106, 107, 108, 109, 110, 109, 108, 107, 106] * 5,
        'low': [95, 96, 97, 98, 99, 100, 99, 98, 97, 96] * 5,
        'close': [102, 103, 104, 105, 106, 107, 106, 105, 104, 103] * 5,
        'volume': [1000] * 50
    }
    df = pd.DataFrame(data)

    print("--- 原始数据 ---")
    print(df.head())

    # 2. 计算均线 (SMA)
    print("\n--- 计算 5日 和 10日 均线 ---")
    IndicatorHelper.add_sma(df, length=5)
    IndicatorHelper.add_sma(df, length=10)
    print(df[['close', 'SMA_5', 'SMA_10']].tail())

    # 3. 计算 RSI
    print("\n--- 计算 RSI ---")
    IndicatorHelper.add_rsi(df, length=14)
    print(df[['close', 'RSI_14']].tail())

    # 4. 计算 MACD
    print("\n--- 计算 MACD ---")
    IndicatorHelper.add_macd(df)
    print(df.filter(like='MACD').tail())

    # 5. 计算布林带 (BBands)
    print("\n--- 计算布林带 ---")
    IndicatorHelper.add_bbands(df, length=20)
    print(df.filter(like='BBL').tail()) # 打印下轨作为示例

if __name__ == "__main__":
    main()
