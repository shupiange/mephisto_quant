import pandas as pd
import numpy as np
import baostock as bs
import datetime
import time
import argparse
import pytz
import os

from tqdm import tqdm

from utils.utils import json_save, json_load
from utils.trade_utils import transform_code_name
from params.get_params import get_stock_code_list, get_trade_date

import argparse
parser = argparse.ArgumentParser()

parser.add_argument('--date', type=str, default='', required=False)

TRADE_DATE = get_trade_date()
FAILURE_MSG_PATH = './message'
os.makedirs(FAILURE_MSG_PATH, exist_ok=True)


def concat_trade_data(all_minute_data, date_info, path='./dataset'):
    """
    合并所有股票的分钟数据并保存。
    date_info 可以是单个日期字符串，或者 [start_date, end_date] 列表。
    """
    os.makedirs(path, exist_ok=True)
    df_list = []
    
    if isinstance(date_info, str):
        # 单日数据，合并所有股票到一个文件
        date = date_info
        for code, code_df in all_minute_data.items():
            # 确保 time_rank 是基于该交易日的排名
            code_df.loc[:, 'time_rank'] = code_df['time'].rank()
            df_list.append(code_df)
            
        total_df = pd.concat(df_list, axis=0, ignore_index=True)
        total_df.to_csv(f'{path}/trade_minute_{date}.csv', index=False)
        
    elif isinstance(date_info, list) and len(date_info) == 2:
        # 日期范围数据，每个股票保存一个文件
        start_date, end_date = date_info[0], date_info[1]
        
        for code, code_df in all_minute_data.items():
            # 确保 time_rank 是在 'code' 和 'date' 分组下的排名
            code_df.loc[:, 'time_rank'] = code_df.groupby(['code', 'date'])['time'].rank()
            
            # 为每个股票保存一个文件
            code_df.to_csv(f'{path}/trade_minute_{code}_{start_date}-{end_date}.csv', index=False)

    return


def update_failed_list(failed_list, start_date, end_date):
    """加载已存在的失败列表，合并新的失败代码，并保存。"""
    filename = f'failed_list_{start_date}_{end_date}.json'
    filepath = os.path.join(FAILURE_MSG_PATH, filename)
    
    # 1. 加载旧列表
    exist_failed_list = json_load(filepath)
    
    # 2. 合并并去重
    new_total_list = list(set(failed_list + exist_failed_list))
    
    # 3. 保存新列表
    json_save(filepath, new_total_list)
    print(f'\n更新失败代码列表: {start_date}_{end_date} 共有 {len(new_total_list)} 个代码失败。')
    return new_total_list


def get_trade_minutes_data(bs, start_date, end_date, request_interval=0.5, code_list=None):
    """获取指定代码列表在指定日期范围内的分钟线数据"""
    
    if start_date == end_date and TRADE_DATE.get(start_date) is None:
        print(f'{start_date} Is Not Trade Day !')
        return None

    # 确定要处理的代码列表
    if code_list is None or len(code_list) == 0:
        stock_code_list = get_stock_code_list()
        total_code_list = sorted(list(stock_code_list.keys()))
    else:
        # 如果是修复模式，使用传入的 code_list
        total_code_list = code_list 
    
    all_minute_data = dict()
    failed_list = []
    
    for code in tqdm(total_code_list, desc=f"获取 {start_date}-{end_date} 数据"):
        try:
            # 转换代码为 Baostock 格式 (如 sh.600519)
            bs_code = transform_code_name(code)
            
            # 获取该股票的分钟数据
            df_minute = bs.query_history_k_data_plus(
                code=bs_code,
                fields="date,time,code,open,high,low,close,volume,amount",
                start_date=start_date,
                end_date=end_date,
                frequency="5", 
                adjustflag="2"  # 前复权
            ).get_data()
            # print(df_minute.head()) # 频繁打印会影响速度和日志清晰度，建议注释或移除
            
            # 检查数据是否为空
            if not df_minute.empty:
                df_minute['代码'] = code  # 添加原始股票代码列
                all_minute_data[code] = df_minute
            else:
                # 即使 Baostock 返回空 DataFrame，也可能被视为失败
                raise ValueError('Empty Query Result')
                
            time.sleep(request_interval)
            
        except Exception as e:
            # 记录获取失败的股票
            failed_list.append(code)
            print(f"\n获取股票 {code} 的分钟数据失败: {e}")
            time.sleep(request_interval * 2 + 1) # 失败时暂停更长时间
            
    # 更新失败列表文件
    if len(failed_list) > 0:
        update_failed_list(failed_list, start_date, end_date)
        
    return all_minute_data


# --- 主功能函数 ---

def get_daily_trade_data(bs, date, path='./dataset'):
    """获取单个交易日的分钟数据"""
    if TRADE_DATE.get(date) is not None:
        print(f'Start Get Trade Data By Day: {date} ')
        all_minute_df = get_trade_minutes_data(bs, date, date, request_interval=0.5)
        if all_minute_df is not None:
             concat_trade_data(all_minute_df, date, path=path)
    else:
        print(f'{date} Is Not Trade Day !')
    return 

def get_range_trade_data(bs, start_date, end_date, path='./dataset'):
    """获取日期范围内的分钟数据"""
    print(f'Start Get Trade Data By Range: {start_date} - {end_date} ')
    # 示例代码只取 600519, 000001。如果要获取全部，请将 code_list=... 移除。
    all_minute_df = get_trade_minutes_data(bs, start_date, end_date, request_interval=1, code_list=None)
    
    if all_minute_df is not None:
        concat_trade_data(all_minute_df, [start_date, end_date], path=path)
    return

def fix_daily_trade_data(bs, start_date, end_date, path='./dataset'):
    """
    修复指定日期范围内的失败代码数据。
    注意：这里的逻辑是针对日期范围存储失败文件的格式。
    """
    
    filename = f'failed_list_{start_date}.json' if start_date == end_date else f'failed_list_{start_date}_{end_date}.json'
    filepath = os.path.join(FAILURE_MSG_PATH, filename)
    
    failed_codes = json_load(filepath)
    
    if not failed_codes:
        print(f"日期范围 {start_date}-{end_date} 没有失败代码需要修复。")
        return
    
    print(f"--- 启动修复模式：正在尝试重新获取 {len(failed_codes)} 个失败代码 ---")
    
    # 获取失败的代码数据
    newly_fetched_data = get_trade_minutes_data(
        bs, 
        start_date, 
        end_date, 
        request_interval=1.5, # 失败修复时可以把间隔调长一点
        code_list=failed_codes
    )
    
    if newly_fetched_data:
        # 将新获取的数据保存到对应文件
        concat_trade_data(newly_fetched_data, [start_date, end_date], path=path)
        
        # 移除已成功获取的代码，更新失败列表
        successful_codes = set(newly_fetched_data.keys())
        remaining_failed_codes = [code for code in failed_codes if code not in successful_codes]
        
        if len(remaining_failed_codes) < len(failed_codes):
            print(f"成功修复了 {len(successful_codes)} 个代码。")
            json_save(filepath, remaining_failed_codes)
            print(f"剩余 {len(remaining_failed_codes)} 个代码未修复，已更新失败列表。")
        else:
            print("本次修复尝试未能成功获取更多数据。")
            
    return


# if __name__ == '__main__':
    
#     args = parser.parse_args()

#     date = datetime.datetime.now(pytz.timezone('Asia/Shanghai')).strftime('%Y-%m-%d')
#     if args.date != '':
#         date = args.date
    
#     all_minute_df = get_trade_data(bs, date, date)
#     concat_trade_data(all_minute_df, date)
    
    