import argparse
import json

from core.analysis.performance import PerformanceAnalyzer
from core.config.work_config import PARAMS_DIR
from core.research import ResearchArtifactExporter, ResearchConfig, ResearchValidator
from core.research.output import to_json_safe
from core.risk.risk_manager import (
    DrawdownLimitRule,
    MaxHoldingsRule,
    PositionSizeRule,
    RiskManager,
    StopLossRule,
    TakeProfitRule,
)
from core.strategy import PullbackBreakoutDailyStrategy, RelativeStrengthPullbackDailyStrategy, TrendMacdDailyStrategy
from core.strategy.pullback_breakout_daily import PullbackBreakoutDailyParams
from core.strategy.relative_strength_pullback_daily import RelativeStrengthPullbackDailyParams
from core.strategy.trend_macd_daily import TrendMacdDailyParams
from core.utils.utils import json_load
from core.backtesting.engine import BacktestEngine


# 统一策略注册表：
# 外部只需要传 strategy 名称，研究入口就能自动实例化对应策略。
STRATEGY_REGISTRY = {
    'pullback_breakout_daily': PullbackBreakoutDailyStrategy,
    'relative_strength_pullback_daily': RelativeStrengthPullbackDailyStrategy,
    'trend_macd_daily': TrendMacdDailyStrategy,
}


def build_parser():
    """
    构造命令行参数。

    这个入口的目标是把“策略名 + 数据范围 + 风控参数 + 输出目录”
    统一收敛到一个脚本里，避免每个研究都自己写临时脚本。
    """
    parser = argparse.ArgumentParser(description='统一策略研究入口')
    # 基础研究配置
    parser.add_argument('--strategy', type=str, default='trend_macd_daily')
    parser.add_argument('--start-date', type=str, required=True)
    parser.add_argument('--end-date', type=str, required=True)
    parser.add_argument('--initial-cash', type=float, default=1000000.0)
    # 数据表配置
    parser.add_argument('--table-name', type=str, default='stock_data_1_day_hfq')
    parser.add_argument('--indicator-table', type=str, default='stock_indicators_1_day_hfq')
    # 股票池控制：可以传显式代码，也可以让系统自动按股票基础信息生成全市场股票池
    parser.add_argument('--codes', type=str, default='')
    parser.add_argument('--code-limit', type=int, default=None)
    parser.add_argument('--validation-sample-size', type=int, default=50)
    # 输出目录配置
    parser.add_argument('--output-root', type=str, default='research_outputs')
    parser.add_argument('--run-name', type=str, default=None)
    # 通用交易/风控参数
    parser.add_argument('--min-amount', type=float, default=5000000.0)
    parser.add_argument('--max-holdings', type=int, default=10)
    parser.add_argument('--position-pct', type=float, default=0.15)
    parser.add_argument('--stop-loss-pct', type=float, default=0.08)
    parser.add_argument('--take-profit-pct', type=float, default=0.20)
    # 适用于第二版、第三版策略的移动止盈参数
    parser.add_argument('--trail-stop-pct', type=float, default=0.06)
    parser.add_argument('--min-gain-to-trail-pct', type=float, default=0.10)
    # 适用于第二版、第三版策略的“前强 + 回调”参数
    parser.add_argument('--prior-strength-lookback', type=int, default=20)
    parser.add_argument('--min-prior-runup-pct', type=float, default=0.15)
    parser.add_argument('--pullback-from-high-min-pct', type=float, default=0.03)
    parser.add_argument('--pullback-from-high-max-pct', type=float, default=0.18)
    parser.add_argument('--ma20-proximity-pct', type=float, default=0.03)
    parser.add_argument('--rebound-volume-ratio', type=float, default=1.05)
    # 第三版策略新增：短期强势与横截面筛选参数
    parser.add_argument('--short-strength-lookback', type=int, default=10)
    parser.add_argument('--min-short-return-pct', type=float, default=0.03)
    parser.add_argument('--top-rank-pct', type=float, default=0.15)
    # 组合风险控制
    parser.add_argument('--max-drawdown-pct', type=float, default=0.15)
    parser.add_argument('--liquidate-on-drawdown', action='store_true')
    return parser


def load_stock_info():
    # 股票基础信息来自参数目录，用于生成全市场股票池并过滤未上市/已退市股票
    return json_load(f'{PARAMS_DIR}/stock_info_detail_list.json')


def normalize_code(raw_code, stock_info):
    """
    统一股票代码格式。

    支持以下几种输入：
    - 完整代码：sh.600519
    - 纯数字：600519
    - stock_info 里的 key
    """
    candidate = str(raw_code).strip()
    if not candidate:
        return ''
    if candidate in stock_info:
        return stock_info[candidate]['code']
    if '.' in candidate:
        return candidate
    digits = ''.join(ch for ch in candidate if ch.isdigit())
    if digits in stock_info:
        return stock_info[digits]['code']
    return candidate


def deduplicate_codes(codes):
    # 保持顺序的去重，避免同一股票被重复加入研究股票池
    seen = set()
    result = []
    for code in codes:
        if code and code not in seen:
            result.append(code)
            seen.add(code)
    return result


def resolve_codes(args, config):
    """
    解析最终股票池。

    两种模式：
    1. 用户显式传入 --codes，则只研究这些股票
    2. 否则从 stock_info_detail_list.json 里自动生成全市场有效股票池
    """
    stock_info = load_stock_info()
    if args.codes:
        explicit_codes = [normalize_code(item, stock_info) for item in args.codes.split(',')]
        explicit_codes = deduplicate_codes(explicit_codes)
        if config.code_limit:
            explicit_codes = explicit_codes[:config.code_limit]
        return explicit_codes

    # 自动股票池模式：
    # 过滤逻辑只保留在回测区间内已上市且未退市、状态正常的股票
    selected = []
    start_display = config.start_date_display
    end_display = config.end_date_display
    for _, info in sorted(stock_info.items()):
        ipo_date = str(info.get('ipoDate', '') or '')
        out_date = str(info.get('outDate', '') or '')
        status = str(info.get('status', '1'))
        if status != '1':
            continue
        if ipo_date and ipo_date > end_display:
            continue
        if out_date and out_date < start_display:
            continue
        selected.append(info['code'])

    if config.code_limit:
        selected = selected[:config.code_limit]
    return deduplicate_codes(selected)


def build_risk_manager(config):
    """
    构造组合级风控管理器。

    注意：
    - 第一版策略仍使用固定止损/止盈
    - 第二版、第三版策略已经把主要退出逻辑写进策略内部，
      所以这里不再重复挂固定止盈止损，避免双重卖出信号互相干扰
    """
    manager = RiskManager()
    manager.add_rule(PositionSizeRule(max_position_pct=config.position_pct))
    manager.add_rule(MaxHoldingsRule(max_holdings=config.max_holdings))
    if config.strategy_name != 'pullback_breakout_daily':
        manager.add_rule(StopLossRule(stop_loss_pct=config.stop_loss_pct))
        manager.add_rule(TakeProfitRule(take_profit_pct=config.take_profit_pct))
    manager.add_rule(
        DrawdownLimitRule(
            max_drawdown_pct=config.max_drawdown_pct,
            liquidate_on_breach=config.liquidate_on_drawdown,
        )
    )
    return manager


def build_strategy_factory(config):
    """
    按 strategy_name 构造策略实例工厂。

    这里不直接返回对象，而是返回 lambda，
    目的是让 BacktestEngine 每次运行时都能拿到一个全新策略实例。
    """
    strategy_cls = STRATEGY_REGISTRY[config.strategy_name]
    if config.strategy_name == 'pullback_breakout_daily':
        # 第二版：前强 + 回调 + 启动
        params = PullbackBreakoutDailyParams(
            min_amount=config.min_amount,
            max_holdings=config.max_holdings,
            position_pct=config.position_pct,
            stop_loss_pct=config.stop_loss_pct,
            trail_stop_pct=config.trail_stop_pct,
            min_gain_to_trail_pct=config.min_gain_to_trail_pct,
            prior_strength_lookback=config.prior_strength_lookback,
            min_prior_runup_pct=config.min_prior_runup_pct,
            pullback_from_high_min_pct=config.pullback_from_high_min_pct,
            pullback_from_high_max_pct=config.pullback_from_high_max_pct,
            ma20_proximity_pct=config.ma20_proximity_pct,
            rebound_volume_ratio=config.rebound_volume_ratio,
        )
    elif config.strategy_name == 'relative_strength_pullback_daily':
        # 第三版：在第二版基础上进一步强调“短期相对强势”
        params = RelativeStrengthPullbackDailyParams(
            min_amount=config.min_amount,
            max_holdings=config.max_holdings,
            position_pct=config.position_pct,
            stop_loss_pct=config.stop_loss_pct,
            trail_stop_pct=config.trail_stop_pct,
            min_gain_to_trail_pct=config.min_gain_to_trail_pct,
            prior_strength_lookback=config.prior_strength_lookback,
            short_strength_lookback=config.short_strength_lookback,
            min_prior_runup_pct=config.min_prior_runup_pct,
            min_short_return_pct=config.min_short_return_pct,
            pullback_from_high_min_pct=config.pullback_from_high_min_pct,
            pullback_from_high_max_pct=config.pullback_from_high_max_pct,
            ma20_proximity_pct=config.ma20_proximity_pct,
            rebound_volume_ratio=config.rebound_volume_ratio,
            top_rank_pct=config.top_rank_pct,
        )
    else:
        # 第一版：趋势 + MACD
        params = TrendMacdDailyParams(
            min_amount=config.min_amount,
            max_holdings=config.max_holdings,
            position_pct=config.position_pct,
            stop_loss_pct=config.stop_loss_pct,
            take_profit_pct=config.take_profit_pct,
        )
    return lambda: strategy_cls(params=params)


def run_research(args):
    """
    统一研究主流程：

    1. 解析配置
    2. 生成股票池
    3. 做前置数据校验
    4. 构造回测引擎并执行
    5. 分析绩效
    6. 导出报告与产物
    """
    if args.strategy not in STRATEGY_REGISTRY:
        raise ValueError(f'未知策略: {args.strategy}')

    # 把命令行参数标准化为 ResearchConfig，方便统一传递和落盘
    config = ResearchConfig(
        strategy_name=args.strategy,
        start_date=args.start_date,
        end_date=args.end_date,
        initial_cash=args.initial_cash,
        table_name=args.table_name,
        indicator_table=args.indicator_table,
        output_root=args.output_root,
        run_name=args.run_name,
        code_limit=args.code_limit,
        validation_sample_size=args.validation_sample_size,
        min_amount=args.min_amount,
        max_holdings=args.max_holdings,
        position_pct=args.position_pct,
        stop_loss_pct=args.stop_loss_pct,
        take_profit_pct=args.take_profit_pct,
        trail_stop_pct=args.trail_stop_pct,
        min_gain_to_trail_pct=args.min_gain_to_trail_pct,
        prior_strength_lookback=args.prior_strength_lookback,
        min_prior_runup_pct=args.min_prior_runup_pct,
        pullback_from_high_min_pct=args.pullback_from_high_min_pct,
        pullback_from_high_max_pct=args.pullback_from_high_max_pct,
        ma20_proximity_pct=args.ma20_proximity_pct,
        rebound_volume_ratio=args.rebound_volume_ratio,
        short_strength_lookback=args.short_strength_lookback,
        min_short_return_pct=args.min_short_return_pct,
        top_rank_pct=args.top_rank_pct,
        max_drawdown_pct=args.max_drawdown_pct,
        liquidate_on_drawdown=args.liquidate_on_drawdown,
    )

    # 先确定研究股票池，再做数据校验
    codes = resolve_codes(args, config)
    validator = ResearchValidator(config)
    validation_summary = validator.validate(codes)

    # 构造回测引擎
    engine = BacktestEngine(
        strategy_cls=build_strategy_factory(config),
        codes=codes,
        start_date=config.start_date,
        end_date=config.end_date,
        initial_cash=config.initial_cash,
        risk_manager=build_risk_manager(config),
        table_name=config.table_name,
        indicator_table=config.indicator_table,
    )
    history_df, trades_df = engine.run()
    if history_df.empty:
        raise ValueError('回测结果为空，请检查数据表或股票池')

    # 用统一分析器计算绩效指标
    analyzer = PerformanceAnalyzer(history_df, trades_df, initial_cash=config.initial_cash)
    summary = analyzer.summary()
    summary['final_value'] = float(history_df.iloc[-1]['total_value'])
    summary['final_cash'] = float(history_df.iloc[-1]['cash'])
    summary['universe_size'] = len(codes)
    summary['history_rows'] = int(len(history_df))
    summary['trade_rows'] = int(len(trades_df))

    # metadata 是辅助导出信息，供报告展示使用
    metadata = {
        'universe_size': len(codes),
        'final_value': summary['final_value'],
        'final_cash': summary['final_cash'],
        'final_positions': len(engine.account.positions),
    }
    exporter = ResearchArtifactExporter(config)
    exported_paths = exporter.export(history_df, trades_df, summary, validation_summary, metadata)
    return {
        'config': config,
        'codes': codes,
        'validation_summary': validation_summary,
        'summary': summary,
        'exported_paths': exported_paths,
    }


def main():
    # 入口函数只做三件事：解析参数、执行研究、把摘要打印到终端
    args = build_parser().parse_args()
    result = run_research(args)
    print('研究执行完成')
    print(json.dumps(to_json_safe({
        'strategy': result['config'].strategy_name,
        'start_date': result['config'].start_date_display,
        'end_date': result['config'].end_date_display,
        'universe_size': len(result['codes']),
        'output_dir': result['exported_paths']['output_dir'],
        'total_return': result['summary']['total_return'],
        'max_drawdown': result['summary']['max_drawdown'],
        'sharpe_ratio': result['summary']['sharpe_ratio'],
        'win_rate': result['summary']['win_rate'],
        'profit_factor': result['summary']['profit_factor'],
    }), ensure_ascii=False, indent=2))
    print(result['exported_paths']['report'])


if __name__ == '__main__':
    main()
