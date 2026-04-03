# Mephisto Quant 项目概览

**Python 环境**: `conda activate quant`

## 项目简介

Mephisto Quant 是一个面向中国 A 股市场的量化交易系统，覆盖约 5,000 只 A 股（沪市 60xxxx/68xxxx、深市 00xxxx/30xxxx），提供从原始行情采集、数据库持久化、技术指标计算、策略回测到绩效分析的完整流水线。

---

## 系统架构

```
mephisto_quant/
├── update_dataset.sh              # 主流水线入口（串行调度）
├── update_database.sh             # CSV → MySQL 导入脚本
├── write_dataset.py               # 数据导入 Python 入口
├── RESEARCH_PLAN.md               # 量化策略研究计划
├── core/
│   ├── data/
│   │   ├── collector.py           # 阶段一：行情数据下载器 (Baostock)
│   │   ├── aggregator.py          # 30分钟后复权 → 日线后复权聚合
│   │   └── scholar.py             # 技术指标计算引擎
│   ├── database/
│   │   ├── db_manager.py          # MySQL 连接与操作管理
│   │   ├── store_dataset.py       # 阶段二：CSV 批量导入
│   │   └── load_dataset.py        # 数据查询接口（供回测使用）
│   ├── config/
│   │   ├── database_config.py     # 数据库连接配置
│   │   ├── work_config.py         # 目录路径配置
│   │   └── table_config.py        # 数据库表结构与字段类型定义
│   ├── params/
│   │   ├── get_params.py          # 参数 JSON 读取接口
│   │   ├── update_params.py       # 参数更新（从 Akshare/Baostock 同步）
│   │   ├── stock_code_list.json   # ~5,000 只 A 股代码与名称
│   │   ├── trade_date.json        # 1990 年至今所有交易日
│   │   ├── stock_info_detail_list.json  # IPO/退市日期、上市状态
│   │   ├── adjust_factor.json     # 除权除息复权因子
│   │   └── *_bak.json             # 各参数文件的备份版本
│   ├── utils/
│   │   ├── utils.py               # JSON I/O、字典差集、交易状态判断
│   │   ├── datetime_utils.py      # 日期范围分块
│   │   └── name_utils.py          # 6 位代码 → Baostock 格式转换
│   ├── backtesting/
│   │   ├── engine.py              # 回测引擎（逐 Bar 驱动，支持日线/30分钟）
│   │   ├── strategy_base.py       # Strategy 基类 + Context 交易接口（含风控集成）
│   │   ├── account.py             # 账户、持仓、T+1 交收（含交易日志）
│   │   └── trade_log.py           # 交易记录数据类 TradeRecord + TradeLogger
│   ├── analysis/
│   │   ├── performance.py         # 绩效分析（16项指标：夏普/回撤/胜率等）
│   │   └── report.py              # 格式化中文绩效报告
│   ├── risk/
│   │   └── risk_manager.py        # 风控规则（仓位/止损/止盈/回撤熔断/持股上限）
│   ├── stock_selector/
│   │   ├── selector.py            # 条件选股器（FilterCondition/CrossCondition）
│   │   └── presets.py             # 预置选股策略（MACD金叉/KDJ超卖/布林下轨等）
│   └── message/                   # 下载失败记录（JSON）
├── ddl/                           # 数据库建表 Shell 脚本
└── examples/                      # 示例策略和回测演示
    ├── run_demo.py
    ├── data/mock_data.csv
    └── strategies/demo_strategy.py
```

---

## 核心数据流

### daily 模式（3 步）

```
Step 1: collector.py       → 下载日线前复权 CSV
Step 2: update_database.sh → CSV 导入 stock_data_1_day
Step 3: scholar.py         → 计算 stock_indicators_1_day
```

### 30m 模式（5 步）

```
Step 1: collector.py       → 下载 30 分钟后复权 CSV
Step 2: update_database.sh → CSV 导入 stock_data_30_minute
Step 3: aggregator.py      → 聚合 stock_data_30_minute → stock_data_1_day_hfq
Step 4: scholar.py         → 计算 stock_indicators_1_day_hfq
Step 5: scholar.py         → 计算 stock_indicators_30_minute
```

### 完整数据流图

```
┌──────────────────────────────────────────────────────────────────────────┐
│                         update_dataset.sh                               │
│                   (daily / 30m 两种模式，串行执行)                        │
└──────────────────────────────────────────────────────────────────────────┘
         │                         │                          │
    ┌────▼─────┐           ┌───────▼──────┐           ┌──────▼───────┐
    │ 阶段一    │           │ 阶段二       │           │ 指标计算      │
    │ 数据采集  │ ─CSV──▶  │ 数据入库     │ ─SQL──▶  │ scholar      │
    │collector │           │store_dataset │           └──────────────┘
    └──────────┘           └──────────────┘
         │                         │                          │
    Baostock API            CSV → MySQL               MySQL → MySQL
    Akshare API          (REPLACE INTO)            (读行情，写指标)
         │                         │
         ▼                         ▼
    /home/mephisto/          stock_data_1_day          stock_indicators_1_day
     dataset/quant/          stock_data_30_minute      stock_indicators_30_minute
     ├─daily_1_data/               │
     └─minutes_30_data/            │ (仅 30m 模式)
                                   ▼
                          ┌─────────────────┐
                          │ 30 分钟 → 日线   │
                          │ aggregator.py   │
                          └────────┬────────┘
                                   │
                                   ▼
                          stock_data_1_day_hfq ──► stock_indicators_1_day_hfq
                          (日线后复权行情)          (日线后复权指标)
                                   │
                                   ▼
                          ┌─────────────────┐
                          │ 回测框架         │
                          │ backtesting/     │
                          │ (读取历史数据,   │
                          │  驱动策略运行)   │
                          └─────────────────┘
```

每一步失败时立即中止，不进入下一步。

---

## 阶段一：数据采集（collector.py）

**入口**：`update_dataset.sh` → `python3 core/data/collector.py`

### 类 DataCollector

| 属性 | 说明 |
|------|------|
| `start_date` / `end_date` | 下载日期区间 |
| `adjust_flag` | 复权类型：`1` 后复权、`2` 前复权、`3` 不复权 |
| `frequency` | 数据频率：`d` 日线、`30` 30 分钟线 |
| `path` | CSV 输出目录 |

### 执行流程

```
__main__
  │
  ├─ 1. update_stock_code_list()      # Akshare → 更新股票代码列表
  ├─ 2. update_trade_date()           # Akshare → 更新交易日历
  ├─ 3. update_stock_info_detail_list() # Baostock → 更新 IPO/退市信息
  │
  ├─ 4. [仅日线 + pre_adjust] update_adjust_factor_params()
  │     └─ 检测 start_date~end_date 期间有除权除息的股票
  │     └─ 若有，从 2023-01-01 ~ end_date 重新下载这些股票（前复权数据修正）
  │
  └─ 5. run_download(remaining_codes)
        └─ split_date_range(chunk_size=15)  # 拆为 15 天一块
            └─ 逐块 bs.login → fetch_data → save_data → bs.logout
                ├─ 跳过未上市/已退市股票 (is_stock_on_trade)
                ├─ transform_code_name: 60/68→sh, 00/30→sz
                └─ 失败 → failed_list_{start}_{end}.json
```

### CSV 输出格式

- **路径**：`/home/mephisto/dataset/quant/{daily_1_data|minutes_30_data}/`
- **文件名**：`trade_minute_{code}_{start_date}_{end_date}.csv`
- **日线字段**：`date, code, open, close, high, low, volume, amount, turn`
- **30分钟字段**：`date, time, code, open, high, low, close, volume, amount, time_rank`

### 失败重试

1. 下载失败 → 写入 `core/message/failed_list_{start}_{end}.json`
2. 下次运行时 `_find_failed_files_in_range()` 检测到匹配的失败文件
3. 调用 `run_fix()` 以 0.3s 间隔重试（正常间隔 0.1s）
4. 重试成功 → 删除失败文件；仍有失败 → 更新文件

---

## 阶段二：数据入库（store_dataset.py）

**入口**：`update_dataset.sh` → `update_database.sh` → `write_dataset.py`

### 执行流程

```
store_dataset(dataset_path, table_name, database_name)
  │
  ├─ 扫描 /home/mephisto/dataset/quant/{dataset_path}/ 下所有 .csv
  │
  ├─ 循环读取，每 10,000 个文件为一批：
  │     ├─ pd.read_csv → dropna → astype(TABLE_FIELDS_CONFIG)
  │     ├─ pd.concat 合并
  │     └─ REPLACE INTO {table_name} 批量写入 MySQL
  │
  └─ 入库成功后 shutil.move → archived/ 子目录
```

### 关键设计

- **REPLACE INTO**：主键冲突时覆盖旧数据，实现幂等写入（复权数据修正时直接覆盖）
- **批量聚合**：10,000 个文件合并后一次写入，控制内存与 I/O 开销
- **归档机制**：已处理 CSV 移入 `archived/` 防止重复导入

### 目标表

| 模式 | 表名 | 主键 |
|------|------|------|
| 日线 | `stock_data_1_day` | `(date, code)` |
| 30分钟 | `stock_data_30_minute` | `(date, code, time)` |

---

## 30 分钟 → 日线聚合（aggregator.py，仅 30m 模式）

**入口**：`update_dataset.sh`（30m 模式 Step 3）→ `python3 core/data/aggregator.py`

### 类 Aggregator

将 `stock_data_30_minute`（后复权）按日聚合为 `stock_data_1_day_hfq`（日线后复权）。

### 聚合规则（按 date + code 分组）

| 字段 | 聚合方式 |
|------|---------|
| `open` | 当日第一根 Bar 的 open（按 time_rank ASC 排序） |
| `high` | MAX(所有 Bar 的 high) |
| `low` | MIN(所有 Bar 的 low) |
| `close` | 当日最后一根 Bar 的 close |
| `volume` | SUM(volume) |
| `amount` | SUM(amount) |
| `turn` | NULL（30 分钟数据无换手率，预留字段） |

### 执行流程

```
run()
  │
  ├─ db_manager.connect()
  ├─ get_stock_codes() → SELECT DISTINCT code FROM stock_data_30_minute
  │
  ├─ 逐股票循环（tqdm 进度条）：
  │     ├─ get_stock_data(code)     → 读取日期范围内的 30 分钟数据
  │     ├─ aggregate_to_daily(df)   → GroupBy(date,code) 聚合
  │     └─ save_daily(df)           → REPLACE INTO stock_data_1_day_hfq
  │
  ├─ 每 1,000 只股票 → conn.commit()
  └─ 最终 commit + disconnect
```

聚合完成后，`update_dataset.sh` 接着调用 `scholar.py` 为 `stock_data_1_day_hfq` 计算技术指标，写入 `stock_indicators_1_day_hfq`。

---

## 技术指标计算（scholar.py）

**入口**：`update_dataset.sh` → `python3 core/data/scholar.py`

### 类 Scholar

### 执行流程

```
run()
  │
  ├─ db_manager.connect()（全程复用单连接）
  │
  ├─ get_stock_codes() → SELECT DISTINCT code FROM source_table
  │
  ├─ 逐股票循环（tqdm 进度条）：
  │     ├─ get_stock_data(code)
  │     │     └─ 向前回溯 300 Bar 的预热缓冲（确保 MA90、EMA 收敛等精度）
  │     ├─ calculate_indicators(df) → 向量化计算
  │     └─ save_indicators(df, auto_commit=False)
  │           └─ 过滤掉预热区数据（date >= start_date）
  │
  ├─ 每 1,000 只股票 → conn.commit()（批量提交优化机械硬盘 I/O）
  │
  └─ 最终 commit + disconnect
```

### 计算的技术指标

| 指标 | 字段 | 算法 |
|------|------|------|
| **MACD** | `diff, dea, macd` | EMA(12) - EMA(26)；DEA = EMA(DIF, 9)；MACD = 2×(DIF-DEA) |
| **KDJ** | `k, d, j` | RSV = (C-L9)/(H9-L9)×100；K = EMA(RSV, α=1/3)；D = EMA(K)；J = 3K-2D |
| **CCI** | `cci` | TP=(H+L+C)/3；CCI = (TP-SMA14)/(0.015×MD14) |
| **MFI** | `mfi` | 资金流量比 = ΣPosFlow14/ΣNegFlow14；MFI = 100-100/(1+ratio) |
| **均线** | `ma3/5/10/20/30/60/90` | 简单移动平均 |
| **布林带** | `boll_upper/middle/lower` | Middle=MA20；Upper/Lower = ±2×Std(20) |

### 目标表

| 模式 | 表名 | 主键 |
|------|------|------|
| 日线前复权 | `stock_indicators_1_day` | `(date, code)` |
| 日线后复权 | `stock_indicators_1_day_hfq` | `(date, code)` |
| 30分钟 | `stock_indicators_30_minute` | `(date, code, time)` |

---

## 主入口脚本用法

```bash
# 更新今日日线数据（默认 start=today, end=today）
./update_dataset.sh daily

# 更新指定区间日线数据
./update_dataset.sh daily 2026-01-01 2026-03-17

# 更新今日 30 分钟数据
./update_dataset.sh 30m

# 更新指定区间 30 分钟数据
./update_dataset.sh 30m 2026-03-10 2026-03-17
```

### 模式参数差异

| 参数 | daily | 30m |
|------|-------|-----|
| 复权方式 | 前复权 (qfq, adjust=2) | 后复权 (hfq, adjust=1) |
| 除权检测 | 启用 (--pre-adjust) | 禁用 |
| 频率 | `d` | `30` |
| CSV 目录 | `daily_1_data` | `minutes_30_data` |
| 行情表 | `stock_data_1_day` | `stock_data_30_minute` |
| 指标表 | `stock_indicators_1_day` | `stock_indicators_30_minute` |
| 日线聚合 | — | `stock_data_30_minute` → `stock_data_1_day_hfq` |
| 聚合指标 | — | `stock_indicators_1_day_hfq` |

### 执行步骤

**daily 模式（3 步）**：
```
update_dataset.sh daily
  ├── Step 1: collector.py          → 下载日线前复权 CSV
  ├── Step 2: update_database.sh    → CSV 导入 stock_data_1_day
  └── Step 5: scholar.py            → 计算 stock_indicators_1_day
```

**30m 模式（5 步）**：
```
update_dataset.sh 30m
  ├── Step 1: collector.py          → 下载 30 分钟后复权 CSV
  ├── Step 2: update_database.sh    → CSV 导入 stock_data_30_minute
  ├── Step 3: aggregator.py         → 聚合为 stock_data_1_day_hfq
  ├── Step 4: scholar.py            → 计算 stock_indicators_1_day_hfq
  └── Step 5: scholar.py            → 计算 stock_indicators_30_minute
```

---

## 回测框架（backtesting/）

### 架构

```
BacktestEngine (engine.py)
  ├── 持有 Account 实例
  ├── 持有 Context 实例（暴露给策略）
  ├── 生成日期序列 → 逐日加载 30 分钟数据
  └── 驱动 Strategy 回调

Context (strategy_base.py)
  ├── buy(code, volume, price=None)  → 委托买入
  ├── sell(code, volume, price=None) → 委托卖出
  ├── cash（属性）                    → 可用现金
  ├── positions（属性）               → 当前持仓字典
  ├── current_time                   → 当前 Bar 时间戳
  └── current_prices                 → {code: 最新价}

Strategy (strategy_base.py)  ← 子类实现
  ├── initialize(context)            → 策略初始化（一次）
  ├── on_bar(context, bar_dict)      → 每 Bar 回调
  ├── on_day_start(context, date)    → 每日开盘前
  └── on_day_end(context, date)      → 每日收盘后

Account (account.py)
  ├── cash / initial_cash
  ├── positions: {code → Position}
  ├── buy(code, price, volume)       → 扣费，更新持仓
  ├── sell(code, price, volume)      → 加收益，更新持仓
  ├── settle()                       → T+1 交收
  └── update_market_value(prices)    → 重算总资产

Position (account.py)
  ├── total_volume                   → 总持仓
  ├── available_volume               → 可卖持仓（T+1）
  ├── avg_cost                       → 持仓成本
  ├── on_buy / on_sell               → 成交更新
  └── settle()                       → 交收：available = total
```

### 回测执行流程

```
engine.run()
  │
  ├─ generate_date_range()：生成自然日序列
  ├─ strategy.initialize(context)
  │
  └─ for current_date in date_range:
        ├─ account.settle()                   # T+1 交收
        ├─ strategy.on_day_start(context)
        ├─ load_daily_data(date)              # 查 stock_data_30_minute
        │    └─ 无数据则跳过（非交易日/停牌）
        ├─ grouped = df.groupby('time')
        │    └─ for ts, group in grouped:
        │          ├─ context.current_time = ts
        │          ├─ bar_dict = {code: row}
        │          ├─ context.current_prices.update(...)
        │          └─ strategy.on_bar(context, bar_dict)
        ├─ strategy.on_day_end(context)
        └─ 记录日末 total_value / cash → history
```

### 交易规则

| 规则 | 实现 |
|------|------|
| T+1 | 买入当日 `available_volume=0`，`settle()` 后次日可卖 |
| 手续费 | 双向收取 `commission_rate`（默认 0.02%） |
| 最小交易单位 | 100 股整数倍（`on_buy` 中截断） |
| 现金约束 | `cost + commission > cash` 时买入失败 |
| 卖出约束 | `volume > available_volume` 时按可卖量成交 |

### 运行示例

```bash
python3 examples/run_demo.py
```

---

## 数据库说明

### 连接配置（core/config/database_config.py）

```python
DATABASE_CONFIG = {
    "host": "localhost",
    "database": "quant",
    "user": "mephisto",
    "password": "",
}
```

### 六张核心表

| 表名 | 用途 | 主键 | 关键字段 |
|------|------|------|---------|
| `stock_data_1_day` | 日线行情（前复权） | `(date, code)` | open, close, high, low, volume, amount, turn |
| `stock_data_1_day_hfq` | 日线行情（后复权，从 30 分钟聚合） | `(date, code)` | open, close, high, low, volume, amount, turn(NULL) |
| `stock_data_30_minute` | 30分钟行情（后复权） | `(date, code, time)` | open, close, high, low, volume, amount, time_rank |
| `stock_indicators_1_day` | 日线指标（前复权） | `(date, code)` | diff, dea, macd, k, d, j, cci, mfi, ma3~90, boll_* |
| `stock_indicators_1_day_hfq` | 日线指标（后复权） | `(date, code)` | 同上 |
| `stock_indicators_30_minute` | 30分钟指标 | `(date, code, time)` | 同上 + time |

### 建表脚本

```bash
bash ddl/create.quant.stock_data_1_day.sh
bash ddl/create.quant.stock_data_1_day_hfq.sh
bash ddl/create.quant.stock_data_30_minute.sh
bash ddl/create.quant.stock_indicators_1_day.sh
bash ddl/create.quant.stock_indicators_1_day_hfq.sh
bash ddl/create.quant.stock_indicators_30_minute.sh
```

### MySQLManager（core/database/db_manager.py）

| 方法 | 说明 |
|------|------|
| `execute_query(sql, params)` | SELECT → List[Tuple] |
| `execute_non_query(sql, params)` | INSERT/UPDATE/DELETE → rowcount |
| `insert_many_data(table, data_list)` | REPLACE INTO 批量写入（字典列表） |
| `insert_from_csv(table, path)` | 读 CSV → insert_many_data |
| `insert_from_dataframe(table, df)` | DataFrame → insert_many_data |

支持上下文管理器（`with MySQLManager(...) as db`）自动管理连接。

---

## 路径配置（core/config/work_config.py）

```python
WORK_DIR            = "/home/mephisto/projects/mephisto_quant"
DATASET_DIR         = "/home/mephisto/dataset/quant"      # 独立于项目目录
PARAMS_DIR          = f"{WORK_DIR}/core/params"
FAILURE_MESSAGE_DIR = f"{WORK_DIR}/core/message"
```

---

## 参数文件（core/params/）

| 文件 | 大小 | 内容 | 更新来源 |
|------|------|------|---------|
| `stock_code_list.json` | ~148 KB | ~5,000 只 A 股 `{code: name}` | Akshare `stock_info_a_code_name()` |
| `trade_date.json` | ~189 KB | 1990 至今交易日 `{date: true}` | Akshare `tool_trade_date_hist_sina()` |
| `stock_info_detail_list.json` | ~809 KB | IPO/退市/类型/状态 | Baostock `query_stock_basic()` |
| `adjust_factor.json` | ~574 KB | 复权因子（前/后/综合） | Baostock `query_adjust_factor()` |

每个文件均有 `*_bak.json` 备份版本（更新前自动保留上一版）。

---

## 工具模块（core/utils/）

| 文件 | 函数 | 说明 |
|------|------|------|
| `utils.py` | `json_load(path)` / `json_save(path, data)` | JSON 文件读写 |
| | `dict_key_diff(d1, d2)` | 返回两字典的差异键集合 |
| | `is_stock_on_trade(info, code, start, end)` | 判断股票在区间内是否未上市或已退市 |
| `datetime_utils.py` | `split_date_range(start, end, chunk=30)` | 将大日期区间切分为多个小区间 |
| `name_utils.py` | `transform_code_name(code)` | `60/68xxxx→sh.xxxxxx`，`00/30xxxx→sz.xxxxxx` |

---

## 模块依赖关系

```
update_dataset.sh
 ├─► core/data/collector.py
 │    ├── core/params/update_params.py
 │    │    ├── akshare (外部)
 │    │    ├── baostock (外部)
 │    │    ├── core/params/get_params.py
 │    │    └── core/utils/{utils, name_utils}
 │    ├── core/utils/{utils, name_utils, datetime_utils}
 │    ├── core/params/get_params.py
 │    └── core/config/work_config.py
 │
 ├─► update_database.sh → write_dataset.py
 │    └── core/database/store_dataset.py
 │         ├── core/database/db_manager.py
 │         └── core/config/{database_config, work_config, table_config}
 │
 ├─► core/data/aggregator.py  (仅 30m 模式)
 │    ├── core/database/db_manager.py
 │    └── core/config/{database_config, table_config}
 │
 └─► core/data/scholar.py
      ├── core/database/db_manager.py
      └── core/config/{database_config, table_config}

回测:
examples/run_demo.py
 ├── core/backtesting/engine.py
 │    ├── core/database/load_dataset.py
 │    ├── core/backtesting/account.py
 │    │    └── core/backtesting/trade_log.py
 │    └── core/backtesting/strategy_base.py
 │         └── core/risk/risk_manager.py (可选)
 ├── core/analysis/performance.py
 ├── core/stock_selector/selector.py
 └── examples/strategies/demo_strategy.py
```

---

## 技术栈

| 层次 | 技术 |
|------|------|
| 数据源 | Baostock（行情、复权因子、基本信息）、Akshare（股票列表、交易日历） |
| 数据存储 | MySQL（本地，`quant` 库） |
| 数据处理 | Pandas、NumPy |
| 编程语言 | Python 3、Bash |
| 进度展示 | tqdm |
| 数据库驱动 | mysql-connector-python |

---

## 关键设计决策

| 决策 | 原因 |
|------|------|
| REPLACE INTO 代替 INSERT | 复权数据修正时需覆盖历史记录，确保幂等性 |
| 15 天分块下载 | Baostock API 长连接易超时，分块保持连接活跃 |
| 300 Bar 预热缓冲 | MA90 需要 90 根 K 线，EMA 收敛需要更长窗口 |
| 每 1,000 股提交一次 | 批量提交减少磁盘随机写入，优化机械硬盘性能 |
| 每 10,000 个 CSV 合并入库 | 控制内存峰值，避免单次加载全部文件 |
| 日线前复权 + 30分钟后复权 | 日线前复权便于趋势分析；30 分钟后复权保留历史真实价格 |
| 30 分钟聚合日线后复权 | 避免重复从 API 下载，复用已有 30 分钟后复权数据；turn 字段预留为 NULL |
| T+1 交收 | 符合中国 A 股交易规则 |
| 数据集目录独立于项目 | `/home/mephisto/dataset/quant/` 便于扩容和备份 |

---

## 交易日志（core/backtesting/trade_log.py）

每笔成功的买入/卖出自动记录，无需策略代码干预。

### TradeRecord 字段

| 字段 | 说明 |
|------|------|
| `trade_id` | 自增编号 |
| `timestamp` | 成交时的 Bar 时间戳 |
| `date` | 交易日期 |
| `code` | 股票代码 |
| `direction` | `BUY` / `SELL` |
| `price` / `volume` / `amount` | 成交价/量/额 |
| `commission` | 佣金 |
| `cash_before` / `cash_after` | 成交前后现金 |
| `position_volume_after` | 成交后持仓量 |
| `avg_cost_after` | 成交后持仓成本 |

### 使用

```python
history_df, trades_df = engine.run()  # run() 返回 (净值, 交易记录) 元组
```

---

## 绩效分析（core/analysis/）

### PerformanceAnalyzer

输入回测产出的 `equity_df` 和 `trades_df`，计算 16 项指标：

| 类别 | 指标 |
|------|------|
| 收益 | total_return, annualized_return, daily_returns |
| 风险 | max_drawdown, max_drawdown_duration, volatility |
| 风险调整 | sharpe_ratio, sortino_ratio, calmar_ratio |
| 交易统计 | total_trades, win_rate, profit_factor, avg_win, avg_loss, avg_holding_days, max_consecutive_wins/losses |

```python
from core.analysis.performance import PerformanceAnalyzer

analyzer = PerformanceAnalyzer(equity_df, trades_df, initial_cash=100000.0,
                                risk_free_rate=0.03, trading_days_per_year=242)
analyzer.print_report()   # 格式化中文报告
summary = analyzer.summary()  # 全部指标字典
```

A 股使用 242 个年交易日（非美股 252）。胜率通过 FIFO 配对 BUY/SELL 计算。

---

## 风控模块（core/risk/）

风控作为中间件拦截在策略下单和账户执行之间：

```
Strategy.buy() → Context.buy() → RiskManager.check_order() → Account.buy()
```

### 5 个风控规则

| 规则 | 说明 | 默认值 |
|------|------|--------|
| `PositionSizeRule` | 单票仓位上限 | 20% |
| `StopLossRule` | 止损（按持仓成本） | 8% |
| `TakeProfitRule` | 止盈（按持仓成本） | 20% |
| `DrawdownLimitRule` | 组合回撤熔断（超限禁止买入） | 15% |
| `MaxHoldingsRule` | 最大持仓股票数 | 10 |

### 使用

```python
from core.risk.risk_manager import RiskManager, PositionSizeRule, StopLossRule

rm = RiskManager()
rm.add_rule(PositionSizeRule(max_position_pct=0.15))
rm.add_rule(StopLossRule(stop_loss_pct=0.08))

engine = BacktestEngine(strategy_cls=MyStrategy, codes=[...],
                         start_date='20240101', end_date='20251231',
                         risk_manager=rm)
```

- `risk_manager=None` 时行为与不加风控完全一致（向后兼容）
- 规则 AND 组合：任一规则拒绝即拒绝
- 调整后确保 100 股整数倍
- `on_day_check()` 每日开盘前检查止损/止盈/熔断

---

## 选股模块（core/stock_selector/）

### StockSelector

基于技术指标的条件选股器，直接查询 MySQL 指标表。

```python
from core.stock_selector.selector import StockSelector, FilterCondition, CrossCondition

selector = StockSelector(table_name='stock_indicators_1_day_hfq')
selector.add_filter(FilterCondition('macd', '>', 0))
selector.add_filter(FilterCondition('close', '>', 'ma20'))  # 字段间比较
selector.add_cross_filter(CrossCondition('diff', 'dea', 'golden'))  # MACD 金叉

codes = selector.select('2025-12-29')          # 返回股票代码列表
df = selector.select_with_data('2025-12-29')   # 返回完整指标 DataFrame
result = selector.select_range('2025-12-25', '2025-12-31')  # 批量：{date: [codes]}
```

### 预置选股策略（core/stock_selector/presets.py）

| 函数 | 条件 |
|------|------|
| `macd_golden_cross()` | DIF 上穿 DEA 且 MACD > 0 |
| `oversold_kdj()` | K < 20 且 J < 0 |
| `bollinger_squeeze()` | close ≤ 布林下轨 |
| `volume_breakout()` | close > MA20 且 close > MA5 |
