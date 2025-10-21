import baostock as bs
import pandas as pd

# 1. 登录系统
lg = bs.login()

# 2. 查询复权因子
stock_code = "sh.600519"  # 贵州茅台
start_date = "2023-01-01"
end_date = "2024-10-20"

stock_code = "sh.688981"  # 示例：中芯国际

rs = bs.query_history_k_data_plus(
    code=stock_code,
    fields="date,time,code,open,high,low,close,volume,amount,adjustflag", # 注意这里新增了 time 字段
    start_date="2024-01-01",  # 分钟线数据量大，建议缩短查询时间范围
    end_date="2024-01-09",
    frequency="5",   # !!! 设置为 "5" 来获取 5 分钟线数据
    adjustflag="2"   # 前复权
)

# 4. 显示和保存
print(f"--- {stock_code} 在 {start_date} 到 {end_date} 间的复权因子 ---")
print(rs.get_data().head())
# adj_factor_df.to_csv(f"{stock_code}_adj_factor.csv", index=False)

# 5. 登出系统
bs.logout()

# 重要的字段说明：
# * 'date'：交易日期
# * 'code'：证券代码
# * 'factor'：当日的累积复权因子。
#   这个 'factor' 通常指的是**后复权因子**，
#   即：后复权价格 = 原始价格 * factor
#   或：原始价格 / factor = 前复权价格