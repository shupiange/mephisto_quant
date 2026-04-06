import json
from pathlib import Path

from core.analysis.report import ReportFormatter


def to_json_safe(value):
    if isinstance(value, dict):
        return {key: to_json_safe(item) for key, item in value.items()}
    if isinstance(value, list):
        return [to_json_safe(item) for item in value]
    if isinstance(value, tuple):
        return [to_json_safe(item) for item in value]
    if hasattr(value, 'item'):
        try:
            return value.item()
        except Exception:
            return str(value)
    return value


class ResearchArtifactExporter:
    def __init__(self, config):
        self.config = config
        self.output_dir = Path(config.output_dir)

    def export(self, history_df, trades_df, summary, validation_summary, metadata):
        self.output_dir.mkdir(parents=True, exist_ok=True)
        history_path = self.output_dir / 'history.csv'
        trades_path = self.output_dir / 'trades.csv'
        summary_path = self.output_dir / 'summary.json'
        validation_path = self.output_dir / 'validation.json'
        config_path = self.output_dir / 'config.json'
        report_path = self.output_dir / 'report.txt'
        research_report_path = self.output_dir / 'research_report.md'

        history_df.to_csv(history_path, index=False)
        trades_df.to_csv(trades_path, index=False)
        summary_path.write_text(json.dumps(to_json_safe(summary), ensure_ascii=False, indent=2), encoding='utf-8')
        validation_path.write_text(json.dumps(to_json_safe(validation_summary), ensure_ascii=False, indent=2), encoding='utf-8')
        config_path.write_text(json.dumps(to_json_safe(self.config.to_dict()), ensure_ascii=False, indent=2), encoding='utf-8')
        report_path.write_text(ReportFormatter.text_report(summary), encoding='utf-8')
        research_report_path.write_text(
            self._build_markdown_report(summary, validation_summary, history_df, trades_df, metadata),
            encoding='utf-8',
        )

        return {
            'output_dir': str(self.output_dir),
            'history': str(history_path),
            'trades': str(trades_path),
            'summary': str(summary_path),
            'validation': str(validation_path),
            'config': str(config_path),
            'report': str(report_path),
            'research_report': str(research_report_path),
        }

    def _build_markdown_report(self, summary, validation_summary, history_df, trades_df, metadata):
        lines = []
        lines.append(f"# 研究报告：{self.config.strategy_name}")
        lines.append('')
        lines.append('## 研究配置')
        lines.append('')
        lines.append(f"- 回测区间：{self.config.start_date_display} 至 {self.config.end_date_display}")
        lines.append(f"- 初始资金：{self.config.initial_cash:.2f}")
        lines.append(f"- 股票池规模：{metadata['universe_size']}")
        lines.append(f"- 最大持仓数：{self.config.max_holdings}")
        lines.append(f"- 单票仓位上限：{self.config.position_pct:.2%}")
        lines.append(f"- 止损阈值：{self.config.stop_loss_pct:.2%}")
        if self.config.strategy_name in {'pullback_breakout_daily', 'relative_strength_pullback_daily'}:
            lines.append(f"- 移动止盈回撤阈值：{self.config.trail_stop_pct:.2%}")
            lines.append(f"- 启动移动止盈浮盈阈值：{self.config.min_gain_to_trail_pct:.2%}")
            lines.append(f"- 回调前强势观察窗口：{self.config.prior_strength_lookback} 天")
            lines.append(f"- 历史强势最小涨幅：{self.config.min_prior_runup_pct:.2%}")
            if self.config.strategy_name == 'relative_strength_pullback_daily':
                lines.append(f"- 短期强势观察窗口：{self.config.short_strength_lookback} 天")
                lines.append(f"- 短期最小涨幅：{self.config.min_short_return_pct:.2%}")
                lines.append(f"- 候选保留比例：{self.config.top_rank_pct:.2%}")
        else:
            lines.append(f"- 止盈阈值：{self.config.take_profit_pct:.2%}")
        lines.append(f"- 最大回撤限制：{self.config.max_drawdown_pct:.2%}")
        lines.append('')
        lines.append('## 数据校验')
        lines.append('')
        lines.append(f"- 验证样本股票数：{validation_summary['validation_sample_size']}")
        lines.append(f"- 行情覆盖：{validation_summary['data_table']['rows']} 行，{validation_summary['data_table']['dates']} 个交易日")
        lines.append(f"- 指标覆盖：{validation_summary['indicator_table']['rows']} 行，{validation_summary['indicator_table']['dates']} 个交易日")
        lines.append(f"- 重叠记录数：{validation_summary['overlap_rows']}")
        lines.append(f"- 行情空值率：{json.dumps(validation_summary['data_null_ratio'], ensure_ascii=False)}")
        lines.append(f"- 指标空值率：{json.dumps(validation_summary['indicator_null_ratio'], ensure_ascii=False)}")
        lines.append('')
        lines.append('## 绩效摘要')
        lines.append('')
        lines.append('| 指标 | 数值 |')
        lines.append('| --- | --- |')
        lines.append(f"| 总收益率 | {summary['total_return']:.2%} |")
        lines.append(f"| 年化收益率 | {summary['annualized_return']:.2%} |")
        lines.append(f"| 最大回撤 | {summary['max_drawdown']:.2%} |")
        lines.append(f"| 夏普比率 | {summary['sharpe_ratio']:.4f} |")
        lines.append(f"| 胜率 | {summary['win_rate']:.2%} |")
        lines.append(f"| 盈亏比 | {summary['profit_factor']:.4f} |")
        lines.append(f"| 交易次数 | {summary['total_trades']} |")
        lines.append('')
        lines.append('## 净值尾部样本')
        lines.append('')
        if history_df.empty:
            lines.append('无净值结果')
        else:
            lines.append('```')
            lines.append(history_df.tail(10).to_string(index=False))
            lines.append('```')
        lines.append('')
        lines.append('## 交易尾部样本')
        lines.append('')
        if trades_df.empty:
            lines.append('无交易记录')
        else:
            display_cols = [col for col in ['trade_id', 'date', 'code', 'direction', 'price', 'volume', 'amount'] if col in trades_df.columns]
            lines.append('```')
            lines.append(trades_df[display_cols].tail(10).to_string(index=False))
            lines.append('```')
        lines.append('')
        lines.append('## 结论')
        lines.append('')
        lines.append(f"- 最终权益：{metadata['final_value']:.2f}")
        lines.append(f"- 期末现金：{metadata['final_cash']:.2f}")
        lines.append(f"- 期末持仓数：{metadata['final_positions']}")
        return '\n'.join(lines)
