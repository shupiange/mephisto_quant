方法一：直接调用 Python 脚本
Step 1 — 聚合 30m → 日线后复权行情：


conda activate quant
python3 core/data/aggregator.py \
    --start-date 2023-01-01 \
    --end-date 2026-04-02
Step 2 — 计算日线后复权指标：


python3 core/data/scholar.py \
    --source-table stock_data_1_day_hfq \
    --target-table stock_indicators_1_day_hfq \
    --start-date 2023-01-01 \
    --end-date 2026-04-02
方法二：不传日期参数 = 全量转换
aggregator.py:46-51 中 start_date 和 end_date 默认为 None，此时会处理 stock_data_30_minute 表中所有数据：


# 全量聚合（不限日期）
python3 core/data/aggregator.py

# 全量计算指标
python3 core/data/scholar.py \
    --source-table stock_data_1_day_hfq \
    --target-table stock_indicators_1_day_hfq