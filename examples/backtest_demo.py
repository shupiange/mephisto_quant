import os
import sys
import threading
import time

# 添加项目根目录到 sys.path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from core import (
    BacktestDataHandler,
    ChinaStockExecutionHandler,
    EventEngine,
    EventType,
    NaivePortfolio,
)


def run_simple_backtest():
    """
    演示一个最简单的回测流程。
    """
    # 1. 创建事件引擎
    engine = EventEngine()

    # 2. 准备数据源
    # 假设我们已经下载了 600036 的数据
    csv_dir = os.path.join(os.getcwd(), "data")
    symbol_list = ["600036"]

    # 如果数据不存在，先下载或者手动创建一个假数据（这里为了演示简单处理）
    data_file = os.path.join(csv_dir, "daily", "600036.csv")
    if not os.path.exists(data_file):
        print(f"提示: 请先运行 examples/data_download.py 下载数据，或者确保 {data_file} 存在。")
        return

    # 3. 初始化核心组件
    data_handler = BacktestDataHandler(engine, csv_dir, symbol_list)
    portfolio = NaivePortfolio(engine)
    execution = ChinaStockExecutionHandler(engine)

    # 4. 注册一个简单的策略逻辑 (这里直接在主脚本中定义)
    def simple_strategy(event):
        """
        简单的买入并持有策略逻辑
        """
        latest_bar = data_handler.get_latest_bar("600036")
        if latest_bar is not None:
            # 仅仅演示：每收到一个行情就打印一下
            print(f"收到行情: {latest_bar[0]}, 收盘价: {latest_bar[1]["close"]}")

    engine.register(EventType.MARKET, simple_strategy)
    engine.register(EventType.SIGNAL, portfolio.update_signal)
    engine.register(EventType.ORDER, execution.execute_order)
    engine.register(EventType.FILL, portfolio.update_fill)

    # 5. 启动引擎线程
    engine_thread = threading.Thread(target=engine.run)
    engine_thread.daemon = True
    engine_thread.start()

    print("--- 回测启动 ---")

    # 6. 行情驱动循环
    while data_handler.continue_backtest:
        data_handler.update_bars()
        time.sleep(0.1) # 模拟回测速度

    # 7. 停止回测
    print("--- 回测结束 ---")
    engine.stop()
    engine_thread.join()

if __name__ == "__main__":
    run_simple_backtest()
