# TODO: 日线后复权数据验证清单

## 前置：建表

```bash
bash ddl/create.quant.stock_data_1_day_hfq.sh
bash ddl/create.quant.stock_indicators_1_day_hfq.sh
```

- [ ] 确认两张表创建成功：`SHOW TABLES FROM quant LIKE '%hfq%';`

## 验证一：30 分钟 → 日线聚合

```bash
# 先确保 30 分钟数据已入库（选一个有数据的日期范围）
./update_dataset.sh 30m 2026-03-01 2026-03-05
```

- [ ] 检查 `stock_data_1_day_hfq` 有数据：
  ```sql
  SELECT COUNT(*) FROM quant.stock_data_1_day_hfq;
  SELECT * FROM quant.stock_data_1_day_hfq LIMIT 10;
  ```

- [ ] 抽查聚合正确性（选一只股票，手动对比 30 分钟数据）：
  ```sql
  -- 查看某日 30 分钟原始数据
  SELECT * FROM quant.stock_data_30_minute
  WHERE code = '000001' AND date = '2026-03-03'
  ORDER BY time_rank;

  -- 查看聚合后的日线数据
  SELECT * FROM quant.stock_data_1_day_hfq
  WHERE code = '000001' AND date = '2026-03-03';
  ```
  验证：
  - open = 第一根 Bar 的 open
  - close = 最后一根 Bar 的 close
  - high = 所有 Bar 中 MAX(high)
  - low = 所有 Bar 中 MIN(low)
  - volume = SUM(volume)
  - amount = SUM(amount)
  - turn = NULL（预期为空）

## 验证二：日线后复权指标计算

- [ ] 检查 `stock_indicators_1_day_hfq` 有数据：
  ```sql
  SELECT COUNT(*) FROM quant.stock_indicators_1_day_hfq;
  SELECT * FROM quant.stock_indicators_1_day_hfq
  WHERE code = '000001' LIMIT 10;
  ```

- [ ] 确认指标列非全 NULL（除了开头几行因预热期可能为 NULL）

## 验证三：幂等性

- [ ] 重复执行一次 `./update_dataset.sh 30m 2026-03-01 2026-03-05`
- [ ] 确认数据量不变（REPLACE INTO 覆盖，不产生重复行）

## 验证四：daily 模式不受影响

- [ ] 执行 `./update_dataset.sh daily 2026-03-01 2026-03-05`
- [ ] 确认流程正常，不执行聚合步骤
