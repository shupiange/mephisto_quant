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
from utils.name_utils import transform_code_name
from utils.datetime_utils import split_date_range
from params.get_params import get_stock_code_list, get_trade_date


import argparse

parser = argparse.ArgumentParser(description="批量下载和修复股票分钟线数据 (Baostock)")
parser.add_argument('--start_date', type=str, required=True, help="开始日期 (YYYY-MM-DD)")
parser.add_argument('--end_date', type=str, required=True, help="结束日期 (YYYY-MM-DD)")
parser.add_argument('--fix', action='store_true', help="是否运行失败代码的修复模式")
parser.add_argument('--path', type=str, default='./dataset', help="数据保存目录")

TRADE_DATE = get_trade_date()
FAILURE_MSG_PATH = './message'
os.makedirs(FAILURE_MSG_PATH, exist_ok=True)


def get_failed_filepath(start_date, end_date):
    """生成失败列表文件的完整路径"""
    filename = f'failed_list_{start_date}_{end_date}.json'
    return os.path.join(FAILURE_MSG_PATH, filename)


def update_failed_list(newly_failed_codes, start_date, end_date):
    """加载旧列表，合并新失败代码，并保存"""
    filepath = get_failed_filepath(start_date, end_date)
    
    # 1. 加载旧列表
    exist_failed_list = json_load(filepath)
    
    # 2. 合并并去重
    new_total_list = list(set(newly_failed_codes + exist_failed_list))
    
    # 3. 保存新列表
    json_save(filepath, new_total_list)
    return new_total_list


# --- 核心函数 1: 数据下载 ---

def get_trade_minutes_data(bs_session, start_date, end_date, code_list=None, request_interval=0.5):
    """
    获取指定代码列表在指定日期范围内的分钟线数据，并处理失败情况。
    """
    
    # 确定要处理的代码列表
    if code_list is None or len(code_list) == 0:
        stock_codes = get_stock_code_list().keys()
        total_code_list = sorted(list(stock_codes))
    else:
        # 如果是修复模式，使用传入的 code_list
        total_code_list = code_list 
    
    all_minute_data = dict()
    newly_failed_codes = []
    
    desc = f"下载数据 ({start_date} ~ {end_date})"
    if code_list is not None:
        desc = f"修复代码 ({start_date} ~ {end_date})"

    for code in tqdm(total_code_list, desc=desc):
        try:
            bs_code = transform_code_name(code)
            
            df_minute = bs_session.query_history_k_data_plus(
                code=bs_code,
                fields="date,time,code,open,high,low,close,volume,amount",
                start_date=start_date,
                end_date=end_date,
                frequency="5", 
                adjustflag="2"  # 前复权
            ).get_data()
            
            if not df_minute.empty:
                # Baostock 返回的 'code' 字段是带 sh./sz. 的，我们用原始 6 位代码作为 key
                all_minute_data[code] = df_minute
            else:
                raise ValueError('Empty Query Result')
                
            time.sleep(request_interval)
            
        except Exception as e:
            newly_failed_codes.append(code)
            print(f"\n获取股票 {code} 的分钟数据失败: {e}")
            time.sleep(request_interval * 2 + 1)
            
    # 更新失败列表
    if newly_failed_codes:
        update_failed_list(newly_failed_codes, start_date, end_date)
        
    return all_minute_data


# --- 核心函数 2: 数据合并与保存 (统一的范围模式) ---

def concat_trade_data(all_minute_data, start_date, end_date, path='./dataset'):
    """
    将下载的数据追加/合并到各自的 CSV 文件中 (每个代码一个文件)。
    此函数适用于初始下载和修复。
    """
    os.makedirs(path, exist_ok=True)
        
    for code, code_df in all_minute_data.items():
        
        # 1. 构造文件名
        filename = f'trade_minute_{code}_{start_date}_{end_date}.csv'
        filepath = os.path.join(path, filename)
        
        # 2. 计算 time_rank (用于去重或分析)
        code_df.loc[:, 'time_rank'] = code_df.groupby(['code', 'date'])['time'].rank()
        
        # 3. 读取、合并、去重、覆盖
        if os.path.exists(filepath):
            # 文件已存在，执行合并追加逻辑
            try:
                existing_df = pd.read_csv(filepath)
                combined_df = pd.concat([existing_df, code_df], ignore_index=True)
                
                # 去重：确保没有重复的 (date, time) 记录
                combined_df.drop_duplicates(subset=['date', 'time', 'code'], keep='last', inplace=True)
                
                # 覆盖保存
                combined_df.to_csv(filepath, index=False)
                print(f"成功更新/追加 {code} 的数据到 {filename}。")
            except Exception as e:
                print(f"警告: 合并文件 {filepath} 失败 ({e})，将覆盖保存新获取的数据。")
                code_df.to_csv(filepath, index=False)
        else:
            # 文件不存在，直接创建
            code_df.to_csv(filepath, index=False)
            print(f"成功保存 {code} 的数据到 {filename}。")

    return


# --- 主模式函数 ---

def run_download_mode(bs_session, start_date, end_date, path):
    """运行初始下载模式"""
    print(f'--- 开始下载数据范围: {start_date} 至 {end_date} ---')
    
    all_minute_data = get_trade_minutes_data(
        bs_session, 
        start_date, 
        end_date, 
        request_interval=1.5
    )
    
    if all_minute_data:
        concat_trade_data(all_minute_data, start_date, end_date, path=path)
        print("--- 初始下载模式完成 ---")

def run_fix_mode(bs_session, start_date, end_date, path):
    """运行失败代码修复模式"""
    filepath = get_failed_filepath(start_date, end_date)
    failed_codes = json_load(filepath)
    
    if not failed_codes:
        print(f"--- 修复模式: {start_date} ~ {end_date} 没有失败代码需要修复 ---")
        return
    
    print(f"--- 启动修复模式：正在尝试重新获取 {len(failed_codes)} 个失败代码 ---")
    
    # 1. 获取失败的代码数据
    newly_fetched_data = get_trade_minutes_data(
        bs_session, 
        start_date, 
        end_date, 
        request_interval=1.5,
        code_list=failed_codes
    )
    
    if newly_fetched_data:
        # 2. 将新获取的数据合并到各自的文件
        concat_trade_data(newly_fetched_data, start_date, end_date, path=path)
        
        # 3. 移除已成功获取的代码，更新失败列表
        successful_codes = set(newly_fetched_data.keys())
        remaining_failed_codes = [code for code in failed_codes if code not in successful_codes]
        
        if len(remaining_failed_codes) < len(failed_codes):
            print(f"成功修复了 {len(successful_codes)} 个代码。")
            json_save(filepath, remaining_failed_codes)
            print(f"剩余 {len(remaining_failed_codes)} 个代码未修复，失败列表已更新。")
        else:
            print("本次修复尝试未能成功获取更多数据。")
            
    print("--- 修复模式完成 ---")


def main_get_trade_data(start_date: str, end_date: str, is_fix: bool, path: str):
    """
    主数据处理函数，可供外部调用。
    """
    
    # 登录 Baostock
    lg = bs.login()
    print('login respond error_code:' + lg.error_code)
    print('login respond error_msg:' + lg.error_msg)

    try:
        if is_fix:
            # 使用修复模式函数
            run_fix_mode(bs, start_date, end_date, path)
        else:
            # 使用初始下载模式函数
            date_chunks = split_date_range(start_date, end_date, chunk_size_days=15)
            for i, (chunk_start, chunk_end) in enumerate(date_chunks, 1):
                print(f"\n[Chunk {i}/{len(date_chunks)}] 正在处理: {chunk_start} ~ {chunk_end}")            
                run_download_mode(bs, chunk_start, chunk_end, path)

    finally:
        # 退出 Baostock
        bs.logout()
        
# ... (run_download_mode, run_fix_mode 等函数保持不变) ...

if __name__ == '__main__':
    args = parser.parse_args()
    
    # 确保主脚本可以直接运行
    main_get_trade_data(args.start_date, args.end_date, args.fix, args.path)