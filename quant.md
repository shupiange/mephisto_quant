# Mephisto Quant 项目架构与流程说明

本文档描述了 Mephisto Quant 量化项目的整体架构、数据处理流程以及核心代码模块。

## 1. 项目架构概览

本项目主要功能是**自动化获取股票行情数据、处理除权除息、导入数据库并计算技术指标**。系统设计为模块化，通过 Shell 脚本串联各个 Python 核心组件。

### 核心模块

*   **数据采集 (Collector)**: 负责从外部数据源 (Baostock) 下载股票行情数据（日线、分钟线）。
*   **参数更新 (Params)**: 负责维护股票列表、交易日历、复权因子等基础元数据。
*   **数据库管理 (Database)**: 负责将 CSV 数据文件高效导入 MySQL 数据库，支持去重和覆盖更新。
*   **指标计算 (Scholar)**: 基于数据库中的基础行情，计算 MACD, KDJ, MA, Bollinger Bands 等技术指标并存回数据库。
*   **自动化脚本 (Shell)**: 提供一键式操作入口，串联上述所有步骤。

---

## 2. 数据处理流程 (Pipeline)

整个数据更新流程由 `update_dataset.sh` 脚本统一调度，分为三个主要步骤：

### Step 1: 数据采集与复权处理

**入口脚本**: [update_dataset.sh](file:///home/mephisto/projects/mephisto_quant/update_dataset.sh)
**核心代码**: [collector.py](file:///home/mephisto/projects/mephisto_quant/core/data/collector.py)

1.  **基础更新**: 更新股票代码列表、交易日历等元数据。
2.  **复权检测**: (仅日线模式) 检查指定日期范围内是否有股票发生除权除息。
3.  **历史重跑**: 如果发现复权股，自动下载该股票从 2023-01-01 至今的完整复权后数据。
4.  **增量下载**: 下载其余股票在指定日期范围内的最新行情。
5.  **数据落地**: 数据保存为 CSV 文件，存储在 `dataset/quant/` 目录下。

### Step 2: 数据库导入

**入口脚本**: [update_database.sh](file:///home/mephisto/projects/mephisto_quant/update_database.sh)
**核心代码**: [db_manager.py](file:///home/mephisto/projects/mephisto_quant/core/database/db_manager.py) & [write_dataset.py](file:///home/mephisto/projects/mephisto_quant/core/database/write_dataset.py)

1.  **读取 CSV**: 扫描数据目录下的 CSV 文件。
2.  **批量写入**: 使用 `REPLACE INTO` 语句将数据批量写入 MySQL。
    *   **优势**: 自动处理主键冲突，确保新下载的复权数据能覆盖旧数据。
3.  **目标表**:
    *   日线 -> `quant.stock_data_1_day`
    *   30分钟线 -> `quant.stock_data_30_minute`

### Step 3: 指标计算

**核心代码**: [scholar.py](file:///home/mephisto/projects/mephisto_quant/core/data/scholar.py)

1.  **读取数据**: 从 MySQL 读取股票的历史行情数据。
2.  **内存计算**: 使用 Pandas 进行向量化指标计算 (MACD, KDJ, CCI, MFI, MA, BOLL)。
3.  **批量保存**: 将计算结果写回 MySQL 指标表。
    *   **优化**: 使用持久化数据库连接，大幅减少连接开销。
    *   **HDD优化**: 针对机械硬盘采用批量 Commit 策略 (每 100 只股票提交一次)，显著提升写入速度。
4.  **目标表**:
    *   日线指标 -> `quant.stock_indicators_1_day`

---

## 3. 核心代码文件说明

### 3.1 自动化脚本

*   **[update_dataset.sh](file:///home/mephisto/projects/mephisto_quant/update_dataset.sh)**
    *   **作用**: 全流程主控脚本。
    *   **用法**: `./update_dataset.sh daily [开始日期] [结束日期]`
    *   **逻辑**: 依次调用 `collector.py` -> `update_database.sh` -> `scholar.py`。

*   **[update_database.sh](file:///home/mephisto/projects/mephisto_quant/update_database.sh)**
    *   **作用**: 数据库导入脚本。
    *   **用法**: `./update_database.sh daily`
    *   **逻辑**: 根据模式选择对应的 CSV 目录和数据库表名，调用 Python 导入脚本。

### 3.2 Python 核心组件

*   **[core/data/collector.py](file:///home/mephisto/projects/mephisto_quant/core/data/collector.py)**
    *   **类**: `DataCollector`
    *   **功能**: Baostock 数据下载器。支持断点续传、失败重试、复权检测逻辑。
    *   **关键逻辑**: 在 `run` 方法中实现了“先处理复权股历史，再处理普通股增量”的顺序逻辑。

*   **[core/data/scholar.py](file:///home/mephisto/projects/mephisto_quant/core/data/scholar.py)**
    *   **类**: `Scholar`
    *   **功能**: 技术指标计算器。
    *   **关键优化**:
        *   **Single Connection**: 全程复用一个 MySQL 连接。
        *   **Batch Commit**: 减少磁盘 I/O，适配机械硬盘。
        *   **tqdm**: 提供进度条显示。

*   **[core/database/db_manager.py](file:///home/mephisto/projects/mephisto_quant/core/database/db_manager.py)**
    *   **类**: `MySQLManager`
    *   **功能**: 数据库操作封装。
    *   **关键方法**: `insert_many_data` 使用 `REPLACE INTO` 确保数据幂等性（Idempotency）。

*   **[core/params/update_params.py](file:///home/mephisto/projects/mephisto_quant/core/params/update_params.py)**
    *   **功能**: 更新基础元数据（股票代码、交易日、复权因子）。

---

## 4. 数据库设计

### 4.1 行情表 (stock_data_*)
存储原始行情数据（OHLCV）。
*   `stock_data_1_day`: 日线数据 (前复权)
*   `stock_data_30_minute`: 30分钟线数据

### 4.2 指标表 (stock_indicators_*)
存储计算后的技术指标。
*   `stock_indicators_1_day`: 日线指标 (MACD, KDJ, CCI, MFI, MA3/5/10/20/30/60/90, BOLL)

---

## 5. 常用操作命令

**日常更新 (更新今天的数据)**
```bash
./update_dataset.sh daily
```

**补录历史数据 (指定日期范围)**
```bash
./update_dataset.sh daily 2026-01-16 2026-03-06
```

**仅更新 30 分钟线**
```bash
./update_dataset.sh 30m
```
