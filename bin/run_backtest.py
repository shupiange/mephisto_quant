import os
import sys
import threading
import time

import pandas as pd

# 将项目根目录添加到 sys.path
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(project_root)


import datetime

from core import (
    BacktestDataHandler,
    ChinaStockExecutionHandler,
    CompositeExecutionHandler,
    DataProvider,
    EventEngine,
    EventType,
    HongKongStockConnectExecutionHandler,
    NaivePortfolio,
    Visualizer,
)
from strategies.mean_reversion import MeanReversionStrategy
from strategies.moving_average_cross import TrendFollowingStrategy


def run_backtest():
    """
    标准回测入口脚本。
    """
    # 1. 配置参数
    # 支持多支股票回测
    # 01810: 小米集团-W (港股)
    # 00700: 腾讯控股 (港股)
    # 03690: 美团-W (港股)
    symbols = [
        # --- 权重股 (Top 10) ---
        "00700",  # 腾讯控股
        "01810",  # 小米集团-W
        "03690",  # 美团-W
        "09988",  # 阿里巴巴-SW
        "09618",  # 京东集团-SW
        "01024",  # 快手-W
        "09888",  # 百度集团-SW
        "02015",  # 理想汽车-W
        "00999",  # 网易-S
        "00981",  # 中芯国际
        # --- 其他主要成分股 ---
        "01211",  # 比亚迪股份
        "06690",  # 海尔智家
        "09868",  # 小鹏汽车-W
        "09866",  # 蔚来-SW
        "09626",  # 哔哩哔哩-SW
        "00285",  # 比亚迪电子
        "06618",  # 京东健康
        "00241",  # 阿里健康
        "02382",  # 舜宇光学科技
        "00268",  # 金蝶国际
        "00020",  # 商汤-W
        "01347",  # 华虹半导体
        "03888",  # 金山软件
        "09961",  # 携程集团-S
        "00780",  # 同程旅行
        "09992",  # 联想集团
        "00522",  # ASMPT
        "09660",  # 地平线机器人-W
        "01698",  # 腾讯音乐-SW
        "02318",  # 中国平安 (注: 非恒生科技成分股，但常被关联观察，此处保留作为对比)
    ]

    csv_dir = os.path.join(project_root, "data")

    # 动态设定日期范围
    start_date = "20230101"  # 根据用户图片，回测大概是从2023年开始的

    # 结束时间为昨天
    yesterday = datetime.datetime.now() - datetime.timedelta(days=1)
    end_date = yesterday.strftime("%Y%m%d")

    print(f"回测配置: 股票列表={symbols}, 开始时间={start_date}, 结束时间={end_date}")

    # 检查数据是否存在并下载
    provider = DataProvider(base_data_path="data")
    for symbol in symbols:
        # 强制更新数据以确保预测准确性
        print(f"正在检查/更新 {symbol} 数据 ({start_date}-{end_date})...")
        try:
            if os.path.exists(os.path.join(csv_dir + "/daily", f"{symbol}.csv")):
                print(f"数据已存在，跳过下载 {symbol}")
                continue
            provider.download_stock_data(
                symbol=symbol, start_date=start_date, end_date=end_date, period="daily"
            )
        except Exception as e:
            print(f"下载 {symbol} 失败: {e}")

    # 2. 初始化引擎与组件
    engine = EventEngine()
    data_handler = BacktestDataHandler(engine, csv_dir, symbols)
    initial_capital = 500000.0  # 增加初始资金以确保能买入茅台等高价股 (1手约15万)
    portfolio = NaivePortfolio(engine, initial_capital)
    # 使用组合式执行器 (自动路由 A 股和港股)
    execution = CompositeExecutionHandler(engine)

    # 3. 初始化策略
    # 选项 A: 趋势跟踪策略 (TrendFollowingStrategy) - 适合大趋势
    strategy = TrendFollowingStrategy(atr_period=14, atr_multiplier=2.5)

    # 选项 B: 均值回归策略 (MeanReversionStrategy) - 适合震荡市 (如小米近期)
    # 调整参数: bb_std=1.5 以增加交易频率
    # strategy = MeanReversionStrategy(bb_length=20, bb_std=1.5, rsi_length=14)

    strategy.set_engine(engine)

    # 4. 调整风控参数
    # 配合 ATR 动态止损，我们可以适当放宽固定止损，或者完全依赖策略的 ATR 信号
    portfolio.max_single_pos_pct = 0.3  # 单只股票最大仓位 30% (防止单吊)
    portfolio.allow_short = False  # 禁止做空 (A股默认)
    portfolio.buy_pct = 1.0  # 每次买入100%可用资金 (分散投资，确保多只股票有机会)
    portfolio.sell_pct = 1.0  # 卖出信号时清仓 (100%)

    # 将固定止损放宽作为最后的保险，主要依赖 ATR 移动止损
    portfolio.stop_loss_pct = 0.10  # 10% 硬止损
    portfolio.take_profit_pct = 0.3  # 30% 固定止盈 (均值回归通常有目标价)
    portfolio.trailing_stop_pct = 0.05  # 5% 回撤止盈

    # 5. 运行回测引擎注册事件处理
    # 行情事件由数据处理器触发，传给策略
    engine.register(EventType.MARKET, strategy.calculate_signals)
    # 同时也传给组合管理器更新市值（用于风控和权益记录）
    engine.register(EventType.MARKET, portfolio.update_market_value)
    # 传给执行器记录最新价（用于模拟成交）
    engine.register(EventType.MARKET, execution.update_price)

    # 信号事件由策略触发，传给组合管理器
    engine.register(EventType.SIGNAL, portfolio.update_signal)

    # 订单事件由组合管理器触发，传给执行器
    engine.register(EventType.ORDER, execution.execute_order)

    # 成交事件由执行器触发，传给组合管理器更新持仓
    engine.register(EventType.FILL, portfolio.update_fill)

    # 5. 启动引擎 (移除多线程模式，改为单线程顺序执行)
    # engine_thread = threading.Thread(target=engine.run)
    # engine_thread.daemon = True
    # engine_thread.start()

    print(f"--- AlphaMint 回测启动 | 代码: {symbols} ---")

    # 6. 回测循环
    while data_handler.continue_backtest:
        data_handler.update_bars()
        # 立即处理当前队列中的所有事件，确保顺序一致性
        engine.process_all_events()

    # 7. 等待队列处理完成 (单线程模式下不再需要等待)
    print("\n回测结束，正在生成统计图表...")
    # engine.stop()
    # engine_thread.join(timeout=2)

    # 可视化结果
    equity_df = portfolio.get_equity_curve()
    if not equity_df.empty:
        # 保存或显示结果 (这里演示显示)
        Visualizer.plot_equity_curve(equity_df)

        # 计算持仓总市值
        holdings_value = sum(
            portfolio.current_positions.get(s, 0) * portfolio.latest_prices.get(s, 0)
            for s in portfolio.current_positions
        )
        final_equity = portfolio.current_cash + holdings_value

        # 绘制资本对比图
        Visualizer.plot_capital_comparison(initial_capital, final_equity)

        print(f"最终资产净值: {final_equity:.2f}")
        print(f"共记录 {len(equity_df)} 条权益快照。")

        # --- 深度诊断分析 ---
        print("\n" + "=" * 50)
        print("【策略深度诊断报告】")
        print("=" * 50)

        # --- 修改：所有标的盈亏统计 ---
        print("\n=== 所有标的盈亏统计 (All Symbols Performance) ===")

        # 1. 准备数据：计算已实现盈亏
        trade_log = portfolio.get_trade_log()
        realized_pnl_map = {}
        trade_count_map = {}

        if not trade_log.empty:
            realized_pnl_map = trade_log.groupby("symbol")["realized_pnl"].sum().to_dict()
            # 统计卖出次数作为交易轮次
            trade_count_map = (
                trade_log[trade_log["direction"] == "SELL"].groupby("symbol").size().to_dict()
            )

        summary_data = []
        total_realized_all = 0.0
        total_unrealized_all = 0.0
        total_market_value_all = 0.0

        for symbol in symbols:
            # 获取当前状态
            qty = portfolio.current_positions.get(symbol, 0)
            price = portfolio.latest_prices.get(symbol, 0.0)
            cost = portfolio.position_costs.get(symbol, 0.0)

            # 计算浮动盈亏
            market_val = 0.0
            unrealized_pnl = 0.0
            pnl_pct_str = "-"

            if qty > 0:
                market_val = qty * price
                unrealized_pnl = (price - cost) * qty
                pnl_pct = (price - cost) / cost if cost > 0 else 0.0
                pnl_pct_str = f"{pnl_pct:.2%}"

            # 获取已实现盈亏
            realized_pnl = realized_pnl_map.get(symbol, 0.0)
            trade_count = trade_count_map.get(symbol, 0)

            # 总盈亏
            total_pnl = unrealized_pnl + realized_pnl

            # 累加总计
            total_realized_all += realized_pnl
            total_unrealized_all += unrealized_pnl
            total_market_value_all += market_val

            summary_data.append(
                {
                    "代码": symbol,
                    "持仓": qty,
                    "现价": f"{price:.2f}",
                    "成本": f"{cost:.2f}" if qty > 0 else "-",
                    "浮动盈亏": f"{unrealized_pnl:.2f}",
                    "已实现盈亏": f"{realized_pnl:.2f}",
                    "总盈亏": total_pnl,  # 保持数值以便排序
                    "总盈亏(显)": f"{total_pnl:.2f}",
                    "交易次数": trade_count,
                }
            )

        if summary_data:
            # 按总盈亏降序排列
            summary_data.sort(key=lambda x: x["总盈亏"], reverse=True)

            # 格式化输出数据（去掉用于排序的数值列）
            display_data = []
            for item in summary_data:
                display_data.append(
                    {
                        "代码": item["代码"],
                        "持仓": item["持仓"],
                        "现价": item["现价"],
                        "成本": item["成本"],
                        "浮动盈亏": item["浮动盈亏"],
                        "已实现盈亏": item["已实现盈亏"],
                        "总盈亏": item["总盈亏(显)"],
                        "交易次数": item["交易次数"],
                    }
                )

            df_summary = pd.DataFrame(display_data)

            # 设置显示选项
            pd.set_option("display.max_rows", None)
            pd.set_option("display.width", 1000)
            try:
                pd.set_option("display.unicode.east_asian_width", True)
            except:
                pass

            print(df_summary.to_string(index=False))

        print("-" * 100)
        print(f"当前总持仓市值: {total_market_value_all:.2f}")
        print(f"当前浮动盈亏:   {total_unrealized_all:.2f}")
        print(f"累计已实现盈亏: {total_realized_all:.2f}")

        final_equity = portfolio.current_cash + total_market_value_all
        print(f"当前可用现金:   {portfolio.current_cash:.2f}")
        print(f"账户总权益:     {final_equity:.2f}")
        print("=" * 50 + "\n")

        trade_log = portfolio.get_trade_log()
        if not trade_log.empty:
            # 1. 保存交易记录
            trade_log.to_csv("trade_log.csv", index=False)
            print("交易记录已保存至 trade_log.csv")

            # 2. 计算胜率 (仅统计卖出/平仓交易)
            sell_trades = trade_log[trade_log["direction"] == "SELL"]
            if not sell_trades.empty:
                total_trades = len(sell_trades)
                winning_trades = len(sell_trades[sell_trades["realized_pnl"] > 0])
                win_rate = winning_trades / total_trades

                avg_profit = (
                    sell_trades[sell_trades["realized_pnl"] > 0]["realized_pnl"].mean()
                    if winning_trades > 0
                    else 0
                )
                avg_loss = (
                    sell_trades[sell_trades["realized_pnl"] <= 0]["realized_pnl"].mean()
                    if (total_trades - winning_trades) > 0
                    else 0
                )
                pl_ratio = abs(avg_profit / avg_loss) if avg_loss != 0 else float("inf")

                print(f"交易总次数: {total_trades}")
                print(f"胜率 (Win Rate): {win_rate:.2%}")
                print(f"盈亏比 (P/L Ratio): {pl_ratio:.2f}")
                print(f"平均盈利: {avg_profit:.2f}, 平均亏损: {avg_loss:.2f}")
                print(f"总手续费: {trade_log['commission'].sum():.2f}")
            else:
                print("无卖出交易，无法计算胜率。")

            # 3. 计算最大回撤
            equity_df["total"] = pd.to_numeric(equity_df["total"])
            equity_df["peak"] = equity_df["total"].cummax()
            equity_df["drawdown"] = (equity_df["total"] - equity_df["peak"]) / equity_df["peak"]
            max_drawdown = equity_df["drawdown"].min()
            print(f"最大回撤 (Max Drawdown): {max_drawdown:.2%}")

            # 4. 可视化复盘 (保存图表)
            print("\n正在生成交易复盘图表...")
            for symbol in symbols:
                symbol_trades = trade_log[trade_log["symbol"] == symbol]
                if not symbol_trades.empty:
                    # 读取行情数据
                    data_path = os.path.join(csv_dir, "daily", f"{symbol}.csv")
                    if os.path.exists(data_path):
                        df = pd.read_csv(data_path, index_col="datetime", parse_dates=True)
                        # 截取回测时间段
                        df = df[
                            (df.index >= pd.Timestamp(start_date))
                            & (df.index <= pd.Timestamp(end_date))
                        ]

                        try:
                            # Visualizer.plot_trades(df, symbol_trades, title=f"Trade Analysis: {symbol}")
                            print(f"  -> {symbol}: 交易点已准备就绪 (请在本地运行以查看图表)")
                        except Exception as e:
                            print(f"  -> {symbol} 绘图失败: {e}")

        else:
            print("无交易记录。")

        print("\n" + "=" * 50)
        print(f"【明日策略预测】(基于截至 {end_date} 的数据)")
        print("=" * 50)
        for symbol in symbols:
            current_pos = portfolio.current_positions.get(symbol, 0)
            pred = strategy.predict_next(symbol, current_position=current_pos)
            print(f"\n股票代码: {symbol}")
            print(pred)
        print("\n" + "=" * 50)


if __name__ == "__main__":
    run_backtest()
