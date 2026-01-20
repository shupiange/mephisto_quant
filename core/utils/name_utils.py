from core.utils.utils import json_load

import re

def transform_code_name(stock_code: str) -> tuple[str, bool]:
    """
    根据中国 A 股的 6 位数字代码,返回其在 BaoStock 中使用的标准代码格式 (例如: sh.600519)。

    Args:
        stock_code: 6 位数字的股票代码字符串 (例如: "600519", "000001", "300750", "688981")。

    Returns:
        BaoStock 标准代码字符串 (例如: "sh.600519"),如果代码格式不正确则返回原始代码。
    """
    # 确保输入是字符串,并移除可能的空格
    code = str(stock_code).strip()
    
    # 检查代码长度和是否全为数字
    if not (len(code) == 6 and code.isdigit()):
        print(f"警告: 股票代码 '{stock_code}' 格式不正确,应为 6 位数字。")
        return code

    # 根据代码开头数字判断交易所
    if code.startswith(('60', '68')):
        # 以 60 开头: 上海主板 (如 600519)
        # 以 68 开头: 上海科创板 (如 688981)
        prefix = "sh."
    elif code.startswith(('00', '30')):
        # 以 00 开头: 深圳主板 (如 000001)
        # 以 30 开头: 深圳创业板 (如 300750)
        prefix = "sz."
    else:
        # 其他不常见或不识别的起始代码
        # print(f"警告: 无法识别代码 '{code}' 对应的 BaoStock 前缀。")
        return code, False

    # 返回带前缀的 BaoStock 代码
    return prefix + code, True




# # 1. 登录系统
# lg = bs.login()

# # 2. 查询复权因子
# stock_code = "sh.600519"  # 贵州茅台
# start_date = "2023-01-01"
# end_date = "2024-10-20"

# rs = bs.query_adjust_factor(
#     code=stock_code, 
#     start_date=start_date, 
#     end_date=end_date
# )

# # 3. 整理数据
# data_list = []
# while (rs.error_code == '0') & rs.next():
#     data_list.append(rs.get_row_data())

# # 转换为 pandas DataFrame
# adj_factor_df = pd.DataFrame(data_list, columns=rs.fields)

# # 4. 显示和保存
# print(f"--- {stock_code} 在 {start_date} 到 {end_date} 间的复权因子 ---")
# print(adj_factor_df.head())
# # adj_factor_df.to_csv(f"{stock_code}_adj_factor.csv", index=False)

# # 5. 登出系统
# bs.logout()