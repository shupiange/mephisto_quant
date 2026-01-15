import platform

import matplotlib.pyplot as plt
import pandas as pd

# 配置中文字体
system_name = platform.system()
if system_name == "Darwin":  # macOS
    plt.rcParams["font.sans-serif"] = ["Arial Unicode MS", "PingFang SC", "Heiti TC"]
elif system_name == "Windows":
    plt.rcParams["font.sans-serif"] = ["SimHei", "Microsoft YaHei"]
else:
    plt.rcParams["font.sans-serif"] = ["WenQuanYi Micro Hei", "SimHei"]
plt.rcParams["axes.unicode_minus"] = False

try:
    import mplfinance as mpf
except ImportError:
    mpf = None

class Visualizer:
    """
    Visualizer 负责将回测结果、行情数据及指标可视化。
    """

    @staticmethod
    def plot_candlestick(df: pd.DataFrame, title="股价走势", indicators=None):
        """
        绘制 K 线图及技术指标。

        :param df: 包含 OHLCV 数据的 DataFrame，索引需为 DatetimeIndex
        :param title: 图表标题
        :param indicators: 需要同时绘制的指标列名列表
        """
        if mpf is None:
            print("未安装 mplfinance，无法绘制 K 线图。请运行 pip install mplfinance")
            # 退而求其次绘制简单的折线图
            df["close"].plot(title=title)
            plt.show()
            return

        # 准备 mplfinance 所需的数据格式
        # 确保列名正确 (case-insensitive in mplfinance but good practice)
        plot_df = df.copy()

        add_plots = []
        if indicators:
            for col in indicators:
                if col in plot_df.columns:
                    add_plots.append(mpf.make_addplot(plot_df[col]))

        # 使用自定义 style 以支持中文标题 (如果 mplfinance 内部处理了字体)
        # 但通常外部设置 plt.rcParams 已经足够
        mpf.plot(
            plot_df,
            type="candle",
            style="charles",
            title=title,
            ylabel="价格",
            ylabel_lower="成交量",
            volume=True,
            addplot=add_plots if add_plots else None,
            figsize=(12, 8),
        )

    @staticmethod
    def plot_trades(df: pd.DataFrame, trades: pd.DataFrame, title="交易分析"):
        """
        在 K 线图上标记交易点。

        :param df: 股票历史数据 (OHLCV)
        :param trades: 交易记录 DataFrame (必须包含 'datetime', 'direction', 'price')
        :param title: 标题
        """
        if mpf is None:
            print("未安装 mplfinance，无法绘制交易图。")
            return

        plot_df = df.copy()
        if not isinstance(plot_df.index, pd.DatetimeIndex):
            plot_df.index = pd.to_datetime(plot_df.index)

        # 准备买卖点标记
        buy_signals = [float("nan")] * len(plot_df)
        sell_signals = [float("nan")] * len(plot_df)

        # 确保 trades 的 datetime 格式与 plot_df 索引一致
        trades["datetime"] = pd.to_datetime(trades["datetime"])

        for _, trade in trades.iterrows():
            date = trade["datetime"]
            # 找到最近的交易日索引 (防止时间戳不完全匹配)
            # 使用 get_loc 获取位置，如果日期不存在会抛出 KeyError，这里使用 asof 查找最近日期
            try:
                # 截断时间部分，只比较日期
                date_only = date.normalize()
                # 查找匹配的日期
                if date_only in plot_df.index:
                    idx = plot_df.index.get_loc(date_only)
                    if trade["direction"] == "BUY":
                        buy_signals[idx] = trade["price"] * 0.95  # 标记在 K 线下方更明显位置
                    elif trade["direction"] == "SELL":
                        sell_signals[idx] = trade["price"] * 1.05  # 标记在 K 线上方更明显位置
            except Exception:
                pass

        add_plots = [
            mpf.make_addplot(
                buy_signals, type="scatter", markersize=100, marker="^", color="r", label="买入"
            ),
            mpf.make_addplot(
                sell_signals, type="scatter", markersize=100, marker="v", color="g", label="卖出"
            ),
        ]

        # 保存图表到文件而不是显示
        filename = f"trade_analysis_{title.split(': ')[-1]}.png"
        mpf.plot(
            plot_df,
            type="candle",
            style="charles",
            title=title,
            ylabel="价格",
            volume=True,
            addplot=add_plots,
            figsize=(12, 8),
            savefig=filename,
        )
        print(f"交易分析图表已保存至: {filename}")

    @staticmethod
    def plot_equity_curve(equity_df: pd.DataFrame, title="账户权益曲线"):
        """
        绘制账户权益曲线 (增强版，包含多子图)。

        Subplot 1: 权益曲线 (Equity Curve)
        Subplot 2: 回撤曲线 (Drawdown)
        Subplot 3: 每日收益 (Daily Returns)
        Subplot 4: 现金 vs 持仓 (Cash vs Market Value)

        :param equity_df: 包含 'total', 'cash', 'market_value', 'datetime' 列的 DataFrame
        :param title: 图表标题
        """
        # 数据预处理
        plot_df = equity_df.copy()
        if "datetime" in plot_df.columns:
            plot_df["datetime"] = pd.to_datetime(plot_df["datetime"])
            plot_df.set_index("datetime", inplace=True)
        elif not isinstance(plot_df.index, pd.DatetimeIndex):
            try:
                plot_df.index = pd.to_datetime(plot_df.index)
            except:
                pass

        # 确定权益列
        equity_col = "total" if "total" in plot_df.columns else plot_df.columns[0]

        # 计算回撤
        equity = plot_df[equity_col]
        running_max = equity.cummax()
        drawdown = (equity - running_max) / running_max

        # 计算每日收益 (百分比)
        daily_returns = equity.pct_change().fillna(0)

        # 创建 2x2 的子图布局
        fig, axes = plt.subplots(
            4, 1, figsize=(12, 16), sharex=True, gridspec_kw={"height_ratios": [3, 1, 1, 1]}
        )
        plt.subplots_adjust(hspace=0.1)

        # --- Subplot 1: Equity Curve ---
        ax1 = axes[0]
        ax1.plot(plot_df.index, equity, label="总权益", color="blue", linewidth=1.5)
        ax1.set_title(title)
        ax1.set_ylabel("权益净值")
        ax1.legend(loc="upper left")
        ax1.grid(True)

        # --- Subplot 2: Drawdown ---
        ax2 = axes[1]
        ax2.fill_between(plot_df.index, drawdown, 0, color="red", alpha=0.3, label="回撤")
        ax2.plot(plot_df.index, drawdown, color="red", linewidth=0.5)
        ax2.set_ylabel("回撤幅度")
        ax2.set_ylim(bottom=drawdown.min() * 1.1, top=0.05)  # 稍微留点空间
        ax2.legend(loc="lower left")
        ax2.grid(True)

        # --- Subplot 3: Daily Returns ---
        ax3 = axes[2]
        # 使用 fill_between 替代 bar，避免数据量大时显示过细
        ax3.fill_between(
            plot_df.index,
            daily_returns,
            0,
            where=(daily_returns >= 0),
            facecolor="red",
            alpha=0.6,
            label="日盈利",
        )
        ax3.fill_between(
            plot_df.index,
            daily_returns,
            0,
            where=(daily_returns < 0),
            facecolor="green",
            alpha=0.6,
            label="日亏损",
        )
        # 添加一条细线轮廓，增强对比度
        ax3.plot(plot_df.index, daily_returns, color="gray", linewidth=0.3, alpha=0.5)

        ax3.set_ylabel("日收益率")
        ax3.legend(loc="upper left")
        ax3.grid(True, alpha=0.3)

        # --- Subplot 4: Asset Composition (Stacked) ---
        ax4 = axes[3]

        # 准备堆叠数据
        stack_labels = ["现金"]
        # 确保现金不为负
        safe_cash = plot_df["cash"].clip(lower=0)
        stack_data = [safe_cash]
        stack_colors = ["#2ca02c"]  # Green for cash

        # 查找所有以 '_val' 结尾的列 (代表各股票市值)
        val_cols = [c for c in plot_df.columns if c.endswith("_val")]

        if val_cols:
            # 填充 NaN 为 0
            for col in val_cols:
                plot_df[col] = plot_df[col].fillna(0)

            # 如果有详细市值记录
            for col in val_cols:
                symbol = col.replace("_val", "")
                stack_labels.append(symbol)
                stack_data.append(plot_df[col])
                # 为不同股票分配不同颜色
                stack_colors.append(None)
        elif "market_value" in plot_df.columns:
            # 如果只有总市值
            stack_labels.append("股票持仓 (总计)")
            stack_data.append(plot_df["market_value"].fillna(0))
            stack_colors.append("#ff7f0e")  # Orange

        try:
            ax4.stackplot(
                plot_df.index,
                *stack_data,
                labels=stack_labels,
                colors=stack_colors if len(stack_colors) == len(stack_labels) else None,
                alpha=0.6,
            )
        except Exception as e:
            print(f"绘制堆叠图失败: {e}")
            # 回退到简单线条图
            ax4.plot(plot_df.index, plot_df["total"], label="总资产")

        ax4.set_ylabel("资产配置")
        ax4.legend(loc="upper left")
        ax4.set_xlabel("日期")
        ax4.grid(True)

        # 优化日期显示
        fig.autofmt_xdate()

        # 保存图表
        filename = "equity_dashboard.png"
        plt.savefig(filename, dpi=150, bbox_inches="tight")
        print(f"综合回测报表已保存至: {filename}")

        # 仍然保留旧的单图保存逻辑以便兼容，或者直接替换
        # 这里我们生成一个新的 dashboard 文件，不覆盖原来的单图（如果需要）
        # plt.show()

    @staticmethod
    def plot_drawdown(equity_df: pd.DataFrame):
        """
        绘制回撤曲线。
        """
        if 'total' in equity_df.columns:
            equity = equity_df['total']
        else:
            equity = equity_df.iloc[:, 0]

        running_max = equity.cummax()
        drawdown = (equity - running_max) / running_max

        plt.figure(figsize=(10, 4))
        plt.fill_between(drawdown.index, drawdown, 0, color='red', alpha=0.3)
        plt.title("回撤分析")
        plt.grid(True)
        plt.show()

    @staticmethod
    def plot_capital_comparison(initial_capital: float, final_capital: float):
        """
        绘制初始资本与最终资本的对比图。
        """
        plt.figure(figsize=(8, 6))

        values = [initial_capital, final_capital]
        labels = ["初始资金", "最终资金"]
        colors = ["#1f77b4", "#d62728"]  # Blue and Red

        bars = plt.bar(labels, values, color=colors, width=0.5)

        # 计算收益率
        roi = (final_capital - initial_capital) / initial_capital

        # 在柱状图上方添加数值标签
        for bar, value in zip(bars, values):
            height = bar.get_height()
            plt.text(
                bar.get_x() + bar.get_width() / 2.0,
                height,
                f"{value:,.2f}",
                ha="center",
                va="bottom",
                fontsize=12,
                fontweight="bold",
            )

        # 在图表中间显示 ROI
        plt.title(f"资金对比 (投资回报率: {roi:+.2%})", fontsize=14)
        plt.ylabel("金额")
        plt.grid(axis="y", linestyle="--", alpha=0.7)

        # 保存图片
        filename = "capital_comparison.png"
        plt.savefig(filename, dpi=100, bbox_inches="tight")
        print(f"资本对比图已保存至: {filename}")
