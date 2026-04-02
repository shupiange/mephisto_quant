
import sys
import os
import pandas as pd
from unittest.mock import patch

# 将项目根目录添加到 sys.path,确保能导入 core 模块
current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.dirname(current_dir)
sys.path.insert(0, project_root)

from core.backtesting.engine import BacktestEngine
from core.analysis.performance import PerformanceAnalyzer
from examples.strategies.demo_strategy import DemoStrategy

def run_demo():
    # 1. 定义 Mock 的数据加载函数
    def mock_load_dataset(codes, start_date=None, end_date=None, **kwargs):
        """
        Mock 函数,替代真实的 load_dataset。
        从 examples/data/mock_data.csv 读取数据,并根据日期筛选。
        """
        csv_path = os.path.join(current_dir, 'data', 'mock_data.csv')
        df = pd.read_csv(csv_path)
        
        # 转换 date 列为 YYYYMMDD 格式字符串,与 engine 的日期格式一致
        df['date'] = pd.to_datetime(df['date']).dt.strftime('%Y%m%d')
        # 转换 time 列为字符串,防止被识别为数字
        df['time'] = df['time'].astype(str)
        # 转换 code 列为字符串,并补齐6位
        df['code'] = df['code'].astype(str).str.zfill(6)
        
        # 简单的日期筛选逻辑
        if start_date:
            df = df[df['date'] >= start_date]
        if end_date:
            df = df[df['date'] <= end_date]
            
        return df

    # 2. Patch 掉 core.database.load_dataset.load_dataset
    # 注意：engine.py 中是 from core.database.load_dataset import load_dataset
    # 但是 engine.py 内部调用时使用的是 self.load_daily_data -> load_dataset
    # 我们需要 patch engine.py 中导入的那个 load_dataset 引用
    
    # 也可以直接 patch core.database.load_dataset.load_dataset,这样更通用
    with patch('core.backtesting.engine.load_dataset', side_effect=mock_load_dataset):
        print(">>> 开始运行 Demo 回测 (使用 CSV Mock 数据)...")
        
        # 3. 初始化回测引擎
        engine = BacktestEngine(
            strategy_cls=DemoStrategy,
            start_date='20250101',
            end_date='20250103',
            codes=['000001'],
            initial_cash=100000.0
        )
        
        # 4. 运行回测
        history_df, trades_df = engine.run()

        print("\n>>> 回测结束")
        print("\n=== 每日净值 ===")
        print(history_df.to_string(index=False))
        print("\n=== 交易记录 ===")
        print(trades_df.to_string(index=False))

        # 绩效分析
        analyzer = PerformanceAnalyzer(history_df, trades_df, initial_cash=100000.0)
        analyzer.print_report()

if __name__ == "__main__":
    run_demo()
