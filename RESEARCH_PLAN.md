# 量化策略研究管线

## 概述

这是一套**可复用的标准化研究流程**。当你有一个策略想法时，按照以下管线执行：

```
想法 → Step 1 定义 → Step 2 开发 → Step 3 调试 → Step 4 回测 → Step 5 评估 → Step 6 优化
```

### 交易模型

- **交易节奏**：日级别，利用开盘和收盘的集合竞价时间交易
- **决策频率**：每日一次（收盘后决策，次日开盘执行 或 盘中决策收盘执行）
- **数据来源**：
  - **日线后复权数据**（`stock_data_1_day_hfq` + `stock_indicators_1_day_hfq`）：核心决策依据
  - **30 分钟数据**（`stock_data_30_minute`）：用于提取日内特征（波动率、量价分布、日内动量等），辅助日级决策

---

## 前置条件

### 数据

| 表 | 用途 | 时间范围 |
|----|------|---------|
| `stock_data_1_day_hfq` | 日线后复权行情（OHLCV） | 2023-01 ~ 至今 |
| `stock_indicators_1_day_hfq` | 日线后复权指标（MACD/KDJ/CCI/MFI/MA/布林带） | 同上 |

### 引擎现状（已具备）

当前 `core/backtesting/engine.py` 已具备日线研究所需的基础能力：

- `BacktestEngine.__init__` 已支持 `table_name` 和 `indicator_table`
- `load_daily_data` 已可按表名加载数据，并在需要时 merge 指标表
- `run()` 已支持按 `time` 分组的 30 分钟模式，以及无 `time` 列的日线模式
- 日线模式下，`on_bar()` 每天只调用一次，bar 可包含当日 OHLCV + 指标字段
- 策略可以显式使用 `bar['open']` 或 `bar['close']` 模拟开盘/收盘成交

当前可直接这样启动日线回测：

```python
engine = BacktestEngine(
    strategy_cls=MyStrategy,
    codes=selected_codes,
    start_date='20240101',
    end_date='20251231',
    initial_cash=1000000.0,
    table_name='stock_data_1_day_hfq',
    indicator_table='stock_indicators_1_day_hfq',
    risk_manager=rm,
)
history_df, trades_df = engine.run()
```

---

## 可执行策略设计计划

### 1. 数据依赖

先把策略运行所需的数据拆成四层，避免后续开发阶段反复补字段：

| 层级 | 依赖对象 | 当前来源 | 用途 | 是否首个策略必需 |
|------|----------|----------|------|------------------|
| 行情层 | `stock_data_1_day_hfq` | MySQL 日线后复权表 | 提供 open/high/low/close/volume/amount | 是 |
| 指标层 | `stock_indicators_1_day_hfq` | MySQL 日线指标表 | 提供 MACD、KDJ、CCI、MFI、MA、布林带 | 是 |
| 日内补充层 | `stock_data_30_minute` | MySQL 30 分钟表 | 提取日内波动率、量价分布、收盘前强弱等特征 | 否，二期接入 |
| 参数层 | `trade_date.json`、股票基础信息 | 参数 JSON | 交易日过滤、上市时间过滤、股票状态过滤 | 建议接入 |

首个策略的最小字段集建议固定为：

- 行情字段：`date`、`code`、`open`、`close`、`high`、`low`、`volume`、`amount`
- 指标字段：`macd`、`diff`、`dea`、`ma10`、`ma20`、`ma60`
- 风控状态：持仓成本、可卖数量、账户净值、当前回撤

数据准备按下面顺序检查：

1. 确认 `stock_data_1_day_hfq` 与 `stock_indicators_1_day_hfq` 日期区间一致
2. 确认候选股票在回测窗口内已上市且未退市
3. 确认指标字段不存在大面积空值，尤其是 `ma20`、`ma60`、`macd`
4. 在首个版本中先不强依赖 30 分钟表，只把它定义为后续增强特征输入

### 2. 阶段流程

把“研究想法”变成“可回测、可比较、可复现”的阶段流程如下：

| 阶段 | 目标 | 主要动作 | 产出 |
|------|------|----------|------|
| Phase 0 研究定义 | 固化策略假设 | 明确市场假设、交易频率、买卖时点、股票池范围 | 一页策略定义 |
| Phase 1 数据确认 | 锁定输入字段 | 校验表、字段、日期、缺失值、样本覆盖率 | 数据依赖清单 |
| Phase 2 引擎接线 | 打通运行链路 | 配置表名、指标 merge、统一研究入口、参数注入 | 可执行回测脚本 |
| Phase 3 策略开发 | 实现首版逻辑 | 编写选股、入场、出场、仓位、风控 | 首版策略代码 |
| Phase 4 小样本调试 | 验证正确性 | 2~3 只股票、1~2 个月窗口验证交易行为 | 调试记录 |
| Phase 5 全量回测 | 获取绩效 | 扩展到完整股票池和完整时间区间 | 净值曲线与交易明细 |
| Phase 6 评估优化 | 判断是否继续 | 走前测试、参数扫描、失败归因 | 保留/淘汰结论 |

建议执行节奏：

- 第一周完成 Phase 0~2，目标是“研究入口稳定可跑”
- 第二周完成首个策略开发与小样本调试
- 第三周完成全量回测、报告输出与参数回看

### 3. 引擎改造计划

虽然日线模式已经可用，但要把它变成稳定的研究底座，还需要补齐下面四项：

| 改造项 | 当前状态 | 下一步动作 | 目标 |
|--------|----------|------------|------|
| 研究入口统一 | 仅有 `examples/run_demo.py` | 新增 `run_research.py` 统一解析策略名、日期、股票池、风控参数 | 降低重复脚本成本 |
| 配置对象化 | 参数分散在引擎初始化处 | 增加研究配置结构，集中管理表名、时间区间、资金、风控 | 方便复现与批量实验 |
| 数据校验前置 | 依赖运行期发现异常 | 在回测启动前校验空数据、字段缺失、股票池为空 | 尽早失败 |
| 结果输出标准化 | 主要依赖 DataFrame 返回值 | 统一保存净值、交易、参数、摘要报告 | 便于横向比较 |

引擎改造的建议落地顺序：

1. 先做 `run_research.py`，让首个策略有统一入口
2. 再补数据校验与配置对象，减少试错成本
3. 最后补结果归档、参数扫描与走前测试支持

如果进入二期增强，可继续增加：

- 基于 `trade_date.json` 的严格交易日历，而不是自然日遍历
- 滑点、涨跌停限制、基准收益对比
- 30 分钟特征聚合接口，让日内信息可以在日级策略中复用

### 4. 首个策略落地

首个策略建议选择“日线趋势过滤 + MACD 确认”的轻量版本，原因是：

- 完全使用现有日线行情表和指标表即可落地
- 买卖规则直观，便于先验证引擎与风控链路是否正确
- 和现有 `StockSelector`、`RiskManager` 能力匹配度高

#### 策略名称

`trend_macd_daily`

#### 策略假设

只做中短期向上趋势明确、且动量继续加强的股票，避免在震荡和弱趋势环境中频繁交易。

#### 股票池

- 全 A 股中剔除未上市样本和无 `ma60` 数据样本
- 当日成交额 `amount > 5000000`
- `close > ma60`，保留中期趋势向上的股票

#### 入场规则

同时满足以下条件时，于下一交易日开盘买入：

1. `close > ma20`
2. `ma20 > ma60`
3. `macd > 0`
4. `diff > dea`

#### 出场规则

满足任一条件时，于当日收盘卖出：

1. `close < ma10`
2. `macd < 0`
3. 触发 8% 止损
4. 触发 20% 止盈

#### 仓位与风控

- 最大持仓数：10
- 单票仓位上限：15%
- 组合回撤限制：15%
- 保留 T+1 交收约束，不做日内回转

#### 首个版本交付物

| 文件 | 内容 |
|------|------|
| `core/strategy/trend_macd_daily.py` | 首个可回测日线策略 |
| `run_research.py` | 统一研究入口 |
| `examples/` 或测试脚本 | 小样本调试脚本 |
| 回测输出 | `history_df`、`trades_df`、绩效摘要 |

#### 验收标准

- 小样本调试可正常产生交易，且买卖方向符合规则
- 全量回测可稳定跑完，无字段缺失或空股票池报错
- 输出收益、回撤、夏普、胜率、盈亏比五项核心指标
- 训练期与验证期结果可横向比较

---

## Step 1：定义策略

把想法转化为明确的规则。需要回答：

| 问题 | 示例 |
|------|------|
| **选股**：从 5000 只股票中选哪些？ | close > MA60 且日均成交额 > 500 万 |
| **入场信号**：什么条件触发买入？ | MACD 金叉且 close > MA20 |
| **出场信号**：什么条件触发卖出？ | MACD 死叉 或 close < MA10 |
| **仓位**：每只买多少？最多持几只？ | 等权 10 只，单只上限 15% |
| **风控**：止损/止盈/回撤限制？ | 止损 8%，组合回撤 15% 熔断 |

**产出**：一段文字描述，足够我写代码。

---

## Step 2：开发

### 2a. 选股

使用 `StockSelector` 预先筛选股票池：

```python
from core.stock_selector.selector import StockSelector, FilterCondition

selector = StockSelector(table_name='stock_indicators_1_day_hfq')
selector.add_filter(FilterCondition('ma60', '>', 0))      # 有 MA60 数据
selector.add_filter(FilterCondition('close', '>', 'ma60')) # 长期上升趋势
codes = selector.select('2024-01-02')  # 返回股票代码列表
```

或直接用 SQL 预选：

```python
from core.database.load_dataset import load_dataset
df = load_dataset(codes, start_date='2024-01-02', end_date='2024-01-02',
                  table_name='stock_data_1_day_hfq')
df = df[df['amount'] > 5000000]  # 成交额过滤
```

### 2b. 策略类

在 `core/strategy/` 下新建文件，继承 `Strategy`：

```python
# core/strategy/my_strategy.py
from core.backtesting.strategy_base import Strategy

class MyStrategy(Strategy):
    def initialize(self, context):
        """一次性初始化：设置参数、状态变量"""
        self.holding_days = {}  # 跟踪持仓天数等状态

    def on_day_start(self, context, date):
        """每日开盘前：可做日级别判断"""
        pass

    def on_bar(self, context, bar_dict):
        """核心逻辑：每个 bar 调用一次
        bar_dict: {code: pandas.Series}，包含 OHLCV + 所有指标字段
        
        可用字段：open, close, high, low, volume, amount,
                  diff, dea, macd, k, d, j, cci, mfi,
                  ma3, ma5, ma10, ma20, ma30, ma60, ma90,
                  boll_upper, boll_middle, boll_lower
        """
        for code, bar in bar_dict.items():
            # bar 包含：open, close, high, low, volume, amount,
            #           diff, dea, macd, k, d, j, cci, mfi,
            #           ma3~ma90, boll_upper/middle/lower
            
            # 入场逻辑（以开盘价买入 = 模拟次日集合竞价）
            if self._should_buy(bar, context):
                context.buy(code, volume, bar['open'])
            
            # 出场逻辑（以收盘价卖出 = 模拟尾盘集合竞价）
            if self._should_sell(bar, context):
                pos = context.positions.get(code)
                if pos and pos.available_volume > 0:
                    context.sell(code, pos.available_volume, bar['close'])

    def on_day_end(self, context, date):
        """每日收盘后：记录状态、日志"""
        pass
```

### 2c. 风控配置

```python
from core.risk.risk_manager import RiskManager, PositionSizeRule, StopLossRule, MaxHoldingsRule, DrawdownLimitRule

rm = RiskManager()
rm.add_rule(PositionSizeRule(max_position_pct=0.15))
rm.add_rule(StopLossRule(stop_loss_pct=0.08))
rm.add_rule(MaxHoldingsRule(max_holdings=10))
rm.add_rule(DrawdownLimitRule(max_drawdown_pct=0.15))
```

---

## Step 3：调试

用小数据集快速验证策略不报错：

```python
# 选 2-3 只股票、1-2 个月
engine = BacktestEngine(
    strategy_cls=MyStrategy,
    codes=['sh.600519', 'sz.000001'],
    start_date='20250101',
    end_date='20250228',
    initial_cash=100000.0,
    table_name='stock_data_1_day_hfq',
    indicator_table='stock_indicators_1_day_hfq',
)
history_df, trades_df = engine.run()

# 检查：
print(trades_df)           # 是否产生了预期的交易
print(history_df.tail())   # 净值是否合理
```

**检查要点**：
- 是否产生了交易（trades_df 非空）
- 交易方向是否正确（BUY/SELL 配对）
- 净值变化是否合理（没有跳到 0 或负数）
- 信号触发频率是否正常（不是每天都买/每天都卖）

---

## Step 4：回测

用完整数据集运行：

```python
# 完整回测
engine = BacktestEngine(
    strategy_cls=MyStrategy,
    codes=selected_codes,       # Step 2a 选出的股票池
    start_date='20240101',
    end_date='20251231',
    initial_cash=1000000.0,
    table_name='stock_data_1_day_hfq',
    indicator_table='stock_indicators_1_day_hfq',
    risk_manager=rm,
)
history_df, trades_df = engine.run()
```

---

## Step 5：评估

```python
from core.analysis.performance import PerformanceAnalyzer

analyzer = PerformanceAnalyzer(history_df, trades_df, initial_cash=1000000.0)
analyzer.print_report()
```

输出示例：

```
==================================================
        回测绩效报告 (Backtest Report)
==================================================

【收益指标】
  总收益率         :     32.50%
  年化收益率       :     15.80%
  年化波动率       :     12.30%

【风险指标】
  最大回撤         :     12.30%
  最大回撤持续天数 :         45

【风险调整收益】
  夏普比率         :       1.23
  索提诺比率       :       1.67
  卡尔玛比率       :       1.28

【交易统计】
  总交易次数       :         87
  胜率             :     45.20%
  盈亏比           :       2.10
  ...
==================================================
```

### 达标标准

| 指标 | 门槛 |
|------|------|
| 夏普比率 | > 1.0 |
| 最大回撤 | < 20% |
| 胜率 | > 40% |
| 盈亏比 | > 1.5 |
| 验证期表现 | 与训练期无显著衰减（夏普下降 < 30%） |
| 年交易次数 | < 200（避免过度交易） |

---

## Step 6：优化（可选）

### 6a. 走前测试

```python
# 训练期
engine_train = BacktestEngine(..., start_date='20240101', end_date='20241231')
hist_train, trades_train = engine_train.run()
analyzer_train = PerformanceAnalyzer(hist_train, trades_train, 1000000.0)

# 验证期
engine_test = BacktestEngine(..., start_date='20250101', end_date='20251231')
hist_test, trades_test = engine_test.run()
analyzer_test = PerformanceAnalyzer(hist_test, trades_test, 1000000.0)

# 对比
print("训练期夏普:", analyzer_train.sharpe_ratio())
print("验证期夏普:", analyzer_test.sharpe_ratio())
```

### 6b. 参数敏感性

```python
for stop_loss in [0.05, 0.08, 0.10, 0.12]:
    rm = RiskManager()
    rm.add_rule(StopLossRule(stop_loss_pct=stop_loss))
    engine = BacktestEngine(..., risk_manager=rm)
    hist, trades = engine.run()
    analyzer = PerformanceAnalyzer(hist, trades, 1000000.0)
    print(f"止损{stop_loss:.0%}: 夏普={analyzer.sharpe_ratio():.2f}, 回撤={analyzer.max_drawdown():.2%}")
```

---

## 文件结构

```
core/strategy/                 # 所有策略代码
├── __init__.py
├── trend_ma.py               # 示例：趋势跟踪
├── mean_reversion.py         # 示例：均值回归
└── multi_factor.py           # 示例：多因子轮动

run_research.py                # 研究运行脚本（统一入口）
```

### run_research.py 设计

```bash
# 运行某个策略
python3 run_research.py --strategy trend_ma --start 20240101 --end 20251231

# 走前测试
python3 run_research.py --strategy trend_ma --walk-forward --train-end 20241231

# 参数扫描
python3 run_research.py --strategy trend_ma --sweep stop_loss=0.05,0.08,0.10
```

---

## 管线总结

```
┌─────────────────────────────────────────────────────────────┐
│ 你说："我想试试 xxx 策略"                                     │
└─────────────────────┬───────────────────────────────────────┘
                      ▼
        Step 1: 定义（选股/入场/出场/仓位/风控）
                      ▼
        Step 2: 开发（选股代码 + 策略类 + 风控配置）
                      ▼
        Step 3: 调试（小数据集验证无报错、有交易）
                      ▼
        Step 4: 回测（完整数据集运行）
                      ▼
        Step 5: 评估（PerformanceAnalyzer 输出报告）
                      │
            ┌─────────┴──────────┐
            │ 达标？             │
            │ 夏普>1 回撤<20%   │
            └─────────┬──────────┘
              是 ↙         ↘ 否
         Step 6: 优化      回到 Step 1
         走前测试           调整参数或逻辑
         参数敏感性
```
