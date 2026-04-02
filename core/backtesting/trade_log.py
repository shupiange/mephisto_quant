from dataclasses import dataclass, field, asdict
from typing import List, Optional
import pandas as pd


@dataclass
class TradeRecord:
    trade_id: int
    timestamp: str          # context.current_time
    date: str               # 交易日期
    code: str               # 股票代码
    direction: str          # 'BUY' or 'SELL'
    price: float            # 成交价
    volume: int             # 成交量
    amount: float           # price * volume
    commission: float       # 佣金
    cash_before: float      # 成交前现金
    cash_after: float       # 成交后现金
    position_volume_after: int   # 成交后持仓量
    avg_cost_after: float        # 成交后持仓成本


class TradeLogger:
    def __init__(self):
        self._trades: List[TradeRecord] = []
        self._next_id: int = 1

    def log_trade(self, timestamp, date, code, direction, price, volume,
                  commission, cash_before, cash_after,
                  position_volume_after, avg_cost_after) -> TradeRecord:
        record = TradeRecord(
            trade_id=self._next_id,
            timestamp=str(timestamp),
            date=str(date),
            code=str(code),
            direction=direction,
            price=float(price),
            volume=int(volume),
            amount=float(price) * int(volume),
            commission=float(commission),
            cash_before=float(cash_before),
            cash_after=float(cash_after),
            position_volume_after=int(position_volume_after),
            avg_cost_after=float(avg_cost_after),
        )
        self._trades.append(record)
        self._next_id += 1
        return record

    def get_trades(self) -> List[TradeRecord]:
        return list(self._trades)

    def to_dataframe(self) -> pd.DataFrame:
        if not self._trades:
            return pd.DataFrame(columns=[
                'trade_id', 'timestamp', 'date', 'code', 'direction',
                'price', 'volume', 'amount', 'commission',
                'cash_before', 'cash_after',
                'position_volume_after', 'avg_cost_after'
            ])
        return pd.DataFrame([asdict(t) for t in self._trades])

    def get_trades_by_code(self, code: str) -> List[TradeRecord]:
        return [t for t in self._trades if t.code == code]

    def get_trades_by_date(self, date: str) -> List[TradeRecord]:
        return [t for t in self._trades if t.date == date]

    def clear(self):
        self._trades.clear()
        self._next_id = 1
