class ReportFormatter:

    @staticmethod
    def text_report(summary: dict) -> str:
        lines = []
        lines.append('=' * 50)
        lines.append('        回测绩效报告 (Backtest Report)')
        lines.append('=' * 50)
        lines.append('')

        # 收益
        lines.append('【收益指标】')
        lines.append(f'  总收益率         : {summary["total_return"]:>10.2%}')
        lines.append(f'  年化收益率       : {summary["annualized_return"]:>10.2%}')
        lines.append(f'  年化波动率       : {summary["volatility"]:>10.2%}')
        lines.append('')

        # 风险
        lines.append('【风险指标】')
        lines.append(f'  最大回撤         : {summary["max_drawdown"]:>10.2%}')
        lines.append(f'  最大回撤持续天数 : {summary["max_drawdown_duration"]:>10d}')
        lines.append('')

        # 风险调整收益
        lines.append('【风险调整收益】')
        lines.append(f'  夏普比率         : {_fmt_ratio(summary["sharpe_ratio"]):>10s}')
        lines.append(f'  索提诺比率       : {_fmt_ratio(summary["sortino_ratio"]):>10s}')
        lines.append(f'  卡尔玛比率       : {_fmt_ratio(summary["calmar_ratio"]):>10s}')
        lines.append('')

        # 交易统计
        lines.append('【交易统计】')
        lines.append(f'  总交易次数       : {summary["total_trades"]:>10d}')
        lines.append(f'  胜率             : {summary["win_rate"]:>10.2%}')
        lines.append(f'  盈亏比           : {_fmt_ratio(summary["profit_factor"]):>10s}')
        lines.append(f'  平均盈利         : {summary["avg_win"]:>10.2f}')
        lines.append(f'  平均亏损         : {summary["avg_loss"]:>10.2f}')
        lines.append(f'  平均持仓天数     : {summary["avg_holding_days"]:>10.1f}')
        lines.append(f'  最大连续盈利     : {summary["max_consecutive_wins"]:>10d}')
        lines.append(f'  最大连续亏损     : {summary["max_consecutive_losses"]:>10d}')
        lines.append('')
        lines.append('=' * 50)

        return '\n'.join(lines)

    @staticmethod
    def trades_summary(trades_df) -> str:
        if trades_df.empty:
            return '无交易记录'
        cols = ['trade_id', 'date', 'code', 'direction', 'price', 'volume', 'amount', 'commission']
        display_cols = [c for c in cols if c in trades_df.columns]
        return trades_df[display_cols].to_string(index=False)


def _fmt_ratio(value) -> str:
    if value == float('inf'):
        return 'inf'
    if value == float('-inf'):
        return '-inf'
    return f'{value:.2f}'
