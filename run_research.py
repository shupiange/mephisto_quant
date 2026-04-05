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
from core.strategy import TrendMacdDailyStrategy
from core.strategy.trend_macd_daily import TrendMacdDailyParams
from core.utils.utils import json_load
from core.backtesting.engine import BacktestEngine


STRATEGY_REGISTRY = {
    'trend_macd_daily': TrendMacdDailyStrategy,
}


def build_parser():
    parser = argparse.ArgumentParser(description='统一策略研究入口')
    parser.add_argument('--strategy', type=str, default='trend_macd_daily')
    parser.add_argument('--start-date', type=str, required=True)
    parser.add_argument('--end-date', type=str, required=True)
    parser.add_argument('--initial-cash', type=float, default=1000000.0)
    parser.add_argument('--table-name', type=str, default='stock_data_1_day_hfq')
    parser.add_argument('--indicator-table', type=str, default='stock_indicators_1_day_hfq')
    parser.add_argument('--codes', type=str, default='')
    parser.add_argument('--code-limit', type=int, default=None)
    parser.add_argument('--validation-sample-size', type=int, default=50)
    parser.add_argument('--output-root', type=str, default='research_outputs')
    parser.add_argument('--run-name', type=str, default=None)
    parser.add_argument('--min-amount', type=float, default=5000000.0)
    parser.add_argument('--max-holdings', type=int, default=10)
    parser.add_argument('--position-pct', type=float, default=0.15)
    parser.add_argument('--stop-loss-pct', type=float, default=0.08)
    parser.add_argument('--take-profit-pct', type=float, default=0.20)
    parser.add_argument('--max-drawdown-pct', type=float, default=0.15)
    parser.add_argument('--liquidate-on-drawdown', action='store_true')
    return parser


def load_stock_info():
    return json_load(f'{PARAMS_DIR}/stock_info_detail_list.json')


def normalize_code(raw_code, stock_info):
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
    seen = set()
    result = []
    for code in codes:
        if code and code not in seen:
            result.append(code)
            seen.add(code)
    return result


def resolve_codes(args, config):
    stock_info = load_stock_info()
    if args.codes:
        explicit_codes = [normalize_code(item, stock_info) for item in args.codes.split(',')]
        explicit_codes = deduplicate_codes(explicit_codes)
        if config.code_limit:
            explicit_codes = explicit_codes[:config.code_limit]
        return explicit_codes

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
    manager = RiskManager()
    manager.add_rule(PositionSizeRule(max_position_pct=config.position_pct))
    manager.add_rule(MaxHoldingsRule(max_holdings=config.max_holdings))
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
    params = TrendMacdDailyParams(
        min_amount=config.min_amount,
        max_holdings=config.max_holdings,
        position_pct=config.position_pct,
        stop_loss_pct=config.stop_loss_pct,
        take_profit_pct=config.take_profit_pct,
    )
    strategy_cls = STRATEGY_REGISTRY[config.strategy_name]
    return lambda: strategy_cls(params=params)


def run_research(args):
    if args.strategy not in STRATEGY_REGISTRY:
        raise ValueError(f'未知策略: {args.strategy}')

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
        max_drawdown_pct=args.max_drawdown_pct,
        liquidate_on_drawdown=args.liquidate_on_drawdown,
    )

    codes = resolve_codes(args, config)
    validator = ResearchValidator(config)
    validation_summary = validator.validate(codes)

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

    analyzer = PerformanceAnalyzer(history_df, trades_df, initial_cash=config.initial_cash)
    summary = analyzer.summary()
    summary['final_value'] = float(history_df.iloc[-1]['total_value'])
    summary['final_cash'] = float(history_df.iloc[-1]['cash'])
    summary['universe_size'] = len(codes)
    summary['history_rows'] = int(len(history_df))
    summary['trade_rows'] = int(len(trades_df))

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
