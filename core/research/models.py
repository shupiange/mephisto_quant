from dataclasses import asdict, dataclass
from datetime import datetime
from pathlib import Path


def normalize_date(value: str) -> str:
    raw = str(value).strip()
    if len(raw) == 8 and '-' not in raw:
        return raw
    return datetime.strptime(raw, "%Y-%m-%d").strftime("%Y%m%d")


def display_date(value: str) -> str:
    compact = normalize_date(value)
    return f"{compact[:4]}-{compact[4:6]}-{compact[6:8]}"


@dataclass
class ResearchConfig:
    strategy_name: str
    start_date: str
    end_date: str
    initial_cash: float = 1000000.0
    table_name: str = 'stock_data_1_day_hfq'
    indicator_table: str = 'stock_indicators_1_day_hfq'
    output_root: str = 'research_outputs'
    run_name: str | None = None
    code_limit: int | None = None
    validation_sample_size: int = 50
    min_amount: float = 5000000.0
    max_holdings: int = 10
    position_pct: float = 0.15
    stop_loss_pct: float = 0.08
    take_profit_pct: float = 0.20
    max_drawdown_pct: float = 0.15
    liquidate_on_drawdown: bool = False

    def __post_init__(self):
        self.start_date = normalize_date(self.start_date)
        self.end_date = normalize_date(self.end_date)
        if not self.run_name:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            self.run_name = f"{self.strategy_name}_{self.start_date}_{self.end_date}_{timestamp}"

    @property
    def start_date_display(self) -> str:
        return display_date(self.start_date)

    @property
    def end_date_display(self) -> str:
        return display_date(self.end_date)

    @property
    def effective_run_name(self) -> str:
        return self.run_name

    @property
    def output_dir(self) -> Path:
        return Path(self.output_root).resolve() / self.effective_run_name

    def to_dict(self) -> dict:
        payload = asdict(self)
        payload['start_date_display'] = self.start_date_display
        payload['end_date_display'] = self.end_date_display
        payload['effective_run_name'] = self.effective_run_name
        payload['output_dir'] = str(self.output_dir)
        return payload
