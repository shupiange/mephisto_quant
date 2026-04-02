"""预置选股策略配置"""

from core.stock_selector.selector import StockSelector, FilterCondition, CrossCondition


def macd_golden_cross(table_name='stock_indicators_1_day') -> StockSelector:
    """MACD 金叉：DIF 上穿 DEA，MACD 柱转正"""
    selector = StockSelector(table_name=table_name)
    selector.add_cross_filter(CrossCondition('diff', 'dea', 'golden'))
    selector.add_filter(FilterCondition('macd', '>', 0))
    return selector


def oversold_kdj(table_name='stock_indicators_1_day') -> StockSelector:
    """KDJ 超卖：K < 20，J < 0"""
    selector = StockSelector(table_name=table_name)
    selector.add_filter(FilterCondition('k', '<', 20))
    selector.add_filter(FilterCondition('j', '<', 0))
    return selector


def bollinger_squeeze(table_name='stock_indicators_1_day') -> StockSelector:
    """布林带下轨突破：价格触及或跌破下轨"""
    selector = StockSelector(table_name=table_name)
    selector.add_filter(FilterCondition('close', '<=', 'boll_lower'))
    return selector


def volume_breakout(table_name='stock_indicators_1_day') -> StockSelector:
    """放量突破：价格站上 MA20 和 MA5"""
    selector = StockSelector(table_name=table_name)
    selector.add_filter(FilterCondition('close', '>', 'ma20'))
    selector.add_filter(FilterCondition('close', '>', 'ma5'))
    return selector
