# Mephisto Quant 项目概览

**Python 环境**: `conda activate quant`

## 项目简介

Mephisto Quant 是一个面向中国 A 股市场的量化交易数据系统，提供自动化数据采集、技术指标计算以及策略回测三大核心功能。

---

## 系统架构

```
mephisto_quant/
├── update_dataset.sh          # 主流水线入口脚本
├── update_database.sh         # CSV 导入数据库脚本
├── write_dataset.py           # 数据导入 Python 入口
├── core/
│   ├── data/
│   │   ├── collector.py       # 行情数据下载器 (Baostock)
│   │   └── scholar.py         # 技术指标计算引擎
│   ├── database/
│   │   ├── db_manager.py      # MySQL 连接与操作管理
│   │   ├── store_dataset.py   # 数据集批量导入
│   │   └── load_dataset.py    # 数据查询接口（供回测使用）
│   ├── config/
│   │   ├── database_config.py # 数据库连接配置
│   │   ├── work_config.py     # 目录路径配置
│   │   └── table_config.py    # 数据库表结构定义
│   ├── params/
│   │   ├── get_params.py      # 参数读取接口
│   │   ├── update_params.py   # 参数更新（从外部 API 同步）
│   │   └── *.json             # 股票代码、交易日历、复权因子等
│   ├── utils/
│   │   ├── utils.py           # 通用工具函数
│   │   ├── datetime_utils.py  # 日期范围分割
│   │   └── name_utils.py      # 股票代码格式转换
│   ├── backtesting/
│   │   ├── engine.py          # 回测引擎
│   │   ├── strategy_base.py   # 策略基类
│   │   └── account.py         # 账户与持仓管理
│   └── message/               # 下载失败记录（JSON 文件）
├── ddl/                       # 数据库建表 SQL/Shell 脚本
└── examples/                  # 示例策略和演示数据
```

---

## 数据流程

整个系统按以下三个阶段串行执行：

```
[外部 API]
    │
    ▼
Stage 1: 数据采集 (collector.py)
    │  从 Baostock 下载行情 CSV
    │  失败记录到 core/message/
    ▼
Stage 2: 数据入库 (store_dataset.py + db_manager.py)
    │  将 CSV 批量导入 MySQL
    │  已处理文件归档到 archived/
    ▼
Stage 3: 指标计算 (scholar.py)
    │  从 MySQL 读取行情
    │  计算技术指标后写回 MySQL
    ▼
[MySQL 数据库: quant]
    │
    ▼
回测框架 (backtesting/)
    读取历史数据，驱动策略逐 Bar 运行
```

---

## 阶段一：数据采集（collector.py）

**触发方式**：`update_dataset.sh` 调用

**主要逻辑**：

1. 调用 akshare API 更新元数据（股票代码列表、交易日历、复权因子）
2. 对日线模式：检测除权除息事件，确定需要重新下载的历史区间
3. 将大日期区间分割为 **15 天一块** 发送给 Baostock API，降低连接超时风险
4. 下载原始 OHLCV 数据保存为 CSV 文件
5. 失败的股票代码写入 `core/message/failed_list_[start]_[end].json`
6. 支持 `run_fix()` 模式重试失败记录

**CSV 输出路径**：
```
/home/mephisto/dataset/quant/
├── daily_1_data/        # 日线数据
└── minutes_30_data/     # 30 分钟数据
```

**文件命名**：`trade_minute_[code]_[start_date]_[end_date].csv`

**股票代码转换规则**：
- `60xxxx` / `68xxxx` → `sh.xxxxxx`（上海）
- `00xxxx` / `30xxxx` → `sz.xxxxxx`（深圳）

---

## 阶段二：数据入库（store_dataset.py + db_manager.py）

**触发方式**：`update_database.sh` 调用 `write_dataset.py`

**主要逻辑**：

1. 扫描目标目录下所有 CSV 文件
2. 以 **10,000 个文件为一批** 读取，控制内存占用
3. 使用 `REPLACE INTO` 实现幂等写入（覆盖已存在的主键数据）
4. 成功入库后将 CSV 移动到 `archived/` 子目录

**目标 MySQL 表**：

| 数据频率 | 表名 |
|---------|------|
| 日线 | `quant.stock_data_1_day` |
| 30 分钟 | `quant.stock_data_30_minute` |

**表字段**（日线）：`date, code, open, close, high, low, volume, amount, turn`

---

## 阶段三：技术指标计算（scholar.py）

**触发方式**：`update_dataset.sh` 最后一步调用

**主要逻辑**：

1. 从 MySQL 读取股票行情，携带 **300 天预热缓冲** 确保指标计算精度
2. 使用 Pandas 向量化运算批量计算指标
3. 每处理 **1000 只股票** 批量提交一次，优化机械硬盘 I/O
4. 将指标结果写入对应指标表

**计算的技术指标**：

| 指标 | 字段 | 说明 |
|------|------|------|
| MACD | diff, dea, macd | 12/26/9 EMA |
| KDJ | k, d, j | RSV + EMA 平滑 |
| CCI | cci | 顺势指标 |
| MFI | mfi | 资金流量指标 |
| 均线 | ma3/5/10/20/30/60/90 | 简单移动平均 |
| 布林带 | upper/middle/lower | 20 周期，2 倍标准差 |

**目标 MySQL 表**：

| 数据频率 | 表名 |
|---------|------|
| 日线 | `quant.stock_indicators_1_day` |
| 30 分钟 | `quant.stock_indicators_30_minute` |

---

## 主入口脚本（update_dataset.sh）

```bash
# 更新今日日线数据
./update_dataset.sh daily

# 更新指定区间日线数据（含历史补录）
./update_dataset.sh daily 2026-01-01 2026-03-17

# 更新今日 30 分钟数据
./update_dataset.sh 30m

# 更新指定区间 30 分钟数据
./update_dataset.sh 30m 2026-03-10 2026-03-17
```

**模式差异**：

| 参数 | 复权方式 | 除权检测 |
|------|---------|---------|
| `daily` | 前复权 (qfq, adjust=2) | 启用 |
| `30m` | 后复权 (hfq, adjust=1) | 禁用 |

**执行顺序**：

```
update_dataset.sh
    ├── 1. python3 collector.py   → 下载行情 CSV
    ├── 2. ./update_database.sh   → 导入 MySQL
    └── 3. python3 scholar.py     → 计算技术指标
```

每一步失败时立即中止，不进入下一步。

---

## 回测框架（backtesting/）

### 核心类

**`BacktestEngine`**（engine.py）
- 加载指定日期区间的历史行情
- 逐 Bar 驱动策略回调
- 管理日期序列和数据加载

**`Strategy`**（strategy_base.py）
- 策略基类，子类实现以下回调：
  - `initialize()`: 策略初始化
  - `on_bar(context, bar_data)`: 每 Bar 触发
  - `on_day_start(context)` / `on_day_end(context)`: 每日开收盘触发

**`Account`**（account.py）
- 现金与持仓管理
- T+1 交收规则
- 手续费计算（默认 0.02%）
- 字段：cash、total_volume、available_volume、avg_cost

### 运行示例

```bash
python3 examples/run_demo.py
```

---

## 配置说明

### 数据库连接（core/config/database_config.py）
```python
HOST     = "localhost"
DATABASE = "quant"
USER     = "mephisto"
PASSWORD = ""
```

### 路径配置（core/config/work_config.py）
```python
WORK_DIR            = "/home/mephisto/projects/mephisto_quant"
DATASET_DIR         = "/home/mephisto/dataset/quant"
PARAMS_DIR          = "core/params"
FAILURE_MESSAGE_DIR = "core/message"
```

---

## 参数文件（core/params/）

| 文件 | 大小 | 内容 |
|------|------|------|
| `stock_code_list.json` | 148 KB | ~5000 只 A 股代码与名称 |
| `trade_date.json` | 189 KB | 1990 年至今所有交易日 |
| `stock_info_detail_list.json` | 809 KB | 上市/退市日期、流通市值 |
| `adjust_factor.json` | 574 KB | 除权除息复权因子 |

每个文件均有对应 `*_bak.json` 备份版本。

---

## 失败重试机制

1. 下载失败时，将股票代码写入 `core/message/failed_list_[start]_[end].json`
2. 下次运行 `collector.py` 时，检测到对应失败文件后自动调用 `run_fix()` 重试
3. 重试间隔：正常 0.1s / 失败后 0.3s
4. 重试成功后更新或删除失败记录文件

---

## 技术栈

| 层次 | 技术 |
|------|------|
| 数据源 | Baostock（行情）、Akshare（元数据） |
| 数据存储 | MySQL（本地） |
| 数据处理 | Pandas、NumPy |
| 编程语言 | Python 3、Bash |
| 进度展示 | tqdm |

---

## 数据库建表

建表脚本位于 `ddl/` 目录，直接执行对应 Shell 脚本即可：

```bash
bash ddl/create.quant.stock_data_1_day.sh
bash ddl/create.quant.stock_data_30_minute.sh
bash ddl/create.quant.stock_indicators_1_day.sh
```
