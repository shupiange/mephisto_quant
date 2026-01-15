import os
import sys

# 添加项目根目录到 sys.path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from core import DataProvider


def main():
    """
    演示如何使用 DataProvider 下载 A 股数据。
    """
    # 初始化数据提供者
    provider = DataProvider(base_data_path="data")

    # 1. 下载 A 股全市场股票列表
    # 这会生成 data/symbols.csv
    # print("--- 步骤 1: 下载股票列表 ---")
    # provider.get_all_symbols()

    # 2. 下载特定股票的历史日线数据
    # 这会生成 data/daily/600036.csv (招商银行)
    print("\n--- 步骤 2: 下载历史日线数据 ---")
    provider.download_stock_data(
        symbol="000001", start_date="20230101", end_date="20260114", period="daily"
    )

    # 3. 下载周线数据
    # print("\n--- 步骤 3: 下载历史周线数据 ---")
    # provider.download_stock_data(
    #     symbol="000001", start_date="20230101", end_date="20231231", period="weekly"
    # )

if __name__ == "__main__":
    main()
