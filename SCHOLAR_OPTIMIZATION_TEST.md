# Scholar 优化版测试计划

## 优化内容

### 原版问题（机械硬盘性能瓶颈）
- **每只股票一次查询**：5000 只股票 = 5000 次随机读
- **每只股票一次写入**：5000 次随机写
- **总计 10,000 次磁盘 I/O**，机械硬盘 IOPS ~100，理论耗时 100 秒，实际更慢

### 优化方案
1. **分批加载**：每次加载 1000 只股票的数据（避免单次查询过大和内存爆炸）
2. **内存计算**：所有指标计算在 Pandas DataFrame 中完成（避免 MySQL 计算）
3. **批量写入**：每 100,000 行一次 REPLACE INTO（减少事务次数）

### 性能提升预期
- **原版**：10,000 次磁盘 I/O
- **优化版**：5 次查询（5000/1000）+ 50 次批量写入 = 55 次磁盘 I/O
- **提升**：约 180 倍

---

## 测试步骤

### 前置条件
- MySQL 已恢复正常（无回滚进程）
- 确认没有其他大规模读写任务

### 测试 1：小数据集验证正确性

**目的**：验证计算逻辑正确，结果与原版一致

```bash
# 1. 用原版计算 2025-12 月的指标（作为基准）
conda activate quant
python3 core/data/scholar.py \
    --source-table stock_data_1_day_hfq \
    --target-table stock_indicators_1_day_hfq_baseline \
    --start-date 2025-12-01 \
    --end-date 2025-12-31

# 2. 用优化版计算同样的数据
python3 core/data/scholar_optimized.py \
    --source-table stock_data_1_day_hfq \
    --target-table stock_indicators_1_day_hfq_optimized \
    --start-date 2025-12-01 \
    --end-date 2025-12-31 \
    --stock-batch-size 1000 \
    --batch-size 50000

# 3. 对比结果（抽样验证）
sudo mysql -e "
SELECT 
    a.code, a.date, 
    a.macd AS macd_baseline, b.macd AS macd_optimized,
    a.k AS k_baseline, b.k AS k_optimized,
    a.ma20 AS ma20_baseline, b.ma20 AS ma20_optimized
FROM quant.stock_indicators_1_day_hfq_baseline a
JOIN quant.stock_indicators_1_day_hfq_optimized b
    ON a.code = b.code AND a.date = b.date
WHERE a.code = 'sh.600519' AND a.date >= '2025-12-25'
ORDER BY a.date;
"
```

**预期结果**：两个表的指标值应该完全一致（或误差 < 0.01%）

---

### 测试 2：性能对比（日线全量）

**目的**：验证性能提升

```bash
# 清空目标表
sudo mysql -e "TRUNCATE TABLE quant.stock_indicators_1_day_hfq;"

# 计时运行优化版（全量 2023-01 ~ 2025-12）
time python3 core/data/scholar_optimized.py \
    --source-table stock_data_1_day_hfq \
    --target-table stock_indicators_1_day_hfq \
    --start-date 2023-01-01 \
    --end-date 2025-12-31 \
    --stock-batch-size 1000 \
    --batch-size 100000
```

**预期结果**：
- 5000 只股票，每只约 700 条日线 = 350 万行
- 分 5 批加载（每批 1000 只股票）
- 分 35 批写入（每批 10 万行）
- 总耗时：< 10 分钟（机械硬盘）

---

### 测试 3：30 分钟线（大数据量）

**目的**：验证大数据量场景

```bash
# 先测试小范围（1 个月）
python3 core/data/scholar_optimized.py \
    --source-table stock_data_30_minute \
    --target-table stock_indicators_30_minute \
    --start-date 2025-12-01 \
    --end-date 2025-12-31 \
    --stock-batch-size 500 \
    --batch-size 50000

# 如果成功，再跑全量（2023-01 ~ 2025-12）
# 注意：30 分钟线约 1.45 亿条，建议分段跑或调小 stock-batch-size
```

**预期结果**：
- 1 个月：约 500 万行，耗时 < 5 分钟
- 全量：约 1.45 亿行，耗时 < 2 小时（机械硬盘）

---

## 监控指标

### 运行时监控

```bash
# 监控 MySQL 连接数
watch -n 5 "sudo mysql -e 'SHOW PROCESSLIST;' | grep quant"

# 监控磁盘 I/O
iostat -x 5

# 监控内存使用
watch -n 5 "free -h"
```

### 完成后验证

```bash
# 检查写入行数
sudo mysql -e "SELECT COUNT(*) FROM quant.stock_indicators_1_day_hfq;"

# 检查数据完整性（每只股票的记录数）
sudo mysql -e "
SELECT code, COUNT(*) as cnt 
FROM quant.stock_indicators_1_day_hfq 
GROUP BY code 
ORDER BY cnt DESC LIMIT 10;
"

# 检查是否有 NULL 值（MACD/KDJ 前几条可能为 NULL，正常）
sudo mysql -e "
SELECT 
    SUM(CASE WHEN macd IS NULL THEN 1 ELSE 0 END) as null_macd,
    SUM(CASE WHEN k IS NULL THEN 1 ELSE 0 END) as null_k,
    SUM(CASE WHEN ma20 IS NULL THEN 1 ELSE 0 END) as null_ma20
FROM quant.stock_indicators_1_day_hfq;
"
```

---

## 故障排查

### 如果内存不足
- 减小 `--stock-batch-size`（如改为 500 或 300）
- 减小 `--batch-size`（如改为 50000）

### 如果 MySQL 连接超时
- 检查 `wait_timeout` 配置：`sudo mysql -e "SHOW VARIABLES LIKE 'wait_timeout';"`
- 增加超时时间：`sudo mysql -e "SET GLOBAL wait_timeout=28800;"`

### 如果磁盘 I/O 过高导致系统卡顿
- 暂停其他磁盘密集型任务
- 使用 `ionice` 降低优先级：`ionice -c3 python3 core/data/scholar_optimized.py ...`

---

## 清理测试数据

```bash
# 删除测试用的 baseline 表
sudo mysql -e "DROP TABLE IF EXISTS quant.stock_indicators_1_day_hfq_baseline;"
sudo mysql -e "DROP TABLE IF EXISTS quant.stock_indicators_1_day_hfq_optimized;"
```

---

## 后续优化方向

如果性能仍不满意，可以考虑：

1. **并行计算**：多进程处理不同批次（需要注意 MySQL 连接池）
2. **增量更新**：只计算新增日期的指标（需要修改逻辑）
3. **SSD 缓存**：将 MySQL 数据目录迁移到 SSD（硬件升级）
4. **预聚合**：将常用指标预计算并缓存到 Redis（架构调整）
