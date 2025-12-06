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
from params.get_params import get_stock_code_list, get_trade_date, get_stock_info_detail_list, get_adjust_factor_params


import argparse

parser = argparse.ArgumentParser(description="批量下载和修复股票分钟线数据 (Baostock)")
parser.add_argument('--start_date', type=str, required=True, help="开始日期 (YYYY-MM-DD)")
parser.add_argument('--end_date', type=str, required=True, help="结束日期 (YYYY-MM-DD)")
parser.add_argument('--adjust_flag', type=str, default="2", help="1: hfq  2: qfq  3: 不复权")
parser.add_argument('--frequency', type=str, default="5", help="5: 5min  d: day")
parser.add_argument('--fix', action='store_true', default=False, help="是否运行失败代码的修复模式")
parser.add_argument('--path', type=str, default='./dataset', help="数据保存目录")

from config.work_config import FAILURE_MESSAGE_DIR, PARAMS_DIR

TRADE_DATE = get_trade_date()
STOCK_INFO = get_stock_info_detail_list(PARAMS_DIR)


def get_failed_filepath(start_date, end_date):
    
    """生成失败列表文件的完整路径"""
    
    filename = f'failed_list_{start_date}_{end_date}.json'
    
    return os.path.join(FAILURE_MESSAGE_DIR, filename)


def read_failed_list(start_date, end_date):
    """Read failed stock list JSON file and return the list of codes (or empty list if not found)."""
    filepath = get_failed_filepath(start_date, end_date)
    if not os.path.exists(filepath):
        return []
    try:
        failed = json_load(filepath)
        if isinstance(failed, list):
            return failed
        return []
    except Exception:
        return []


def find_failed_files_in_range(start_date, end_date):
    """Find all failed_list files under FAILURE_MSG_PATH whose date ranges are fully contained in the given range.

    Returns a list of filepaths.
    """
    files = []
    try:
        for fname in os.listdir(FAILURE_MESSAGE_DIR):
            if not fname.startswith('failed_list_') or not fname.endswith('.json'):
                continue
            # expected format: failed_list_{start}_{end}.json
            parts = fname[len('failed_list_'):-len('.json')].split('_')
            if len(parts) < 2:
                continue
            file_start, file_end = parts[0], parts[1]
            try:
                f_start = datetime.datetime.strptime(file_start, '%Y-%m-%d').date()
                f_end = datetime.datetime.strptime(file_end, '%Y-%m-%d').date()
                s = datetime.datetime.strptime(start_date, '%Y-%m-%d').date()
                e = datetime.datetime.strptime(end_date, '%Y-%m-%d').date()
                # include only if the file's date range is fully within provided date range
                if f_start >= s and f_end <= e:
                    files.append(os.path.join(FAILURE_MESSAGE_DIR, fname))
            except Exception:
                continue
    except Exception:
        pass
    return files


def update_failed_list(newly_failed_codes, start_date, end_date):
    
    """加载旧列表，合并新失败代码，并保存"""
    
    filepath = get_failed_filepath(start_date, end_date)
    
    # 1. 加载旧列表
    if os.path.exists(filepath):
        exist_failed_list = json_load(filepath)
        
        # 2. 合并并去重
        new_total_list = list(set(newly_failed_codes + exist_failed_list))
    
    else:
        new_total_list = list(set(newly_failed_codes))
    
    # 3. 保存新列表
    json_save(filepath, new_total_list)
    return new_total_list


# --- 核心函数 1: 数据下载 ---

def get_trade_minutes_data(bs_session, start_date, end_date, adjust_flag="2", frequency="5", code_list=None, request_interval=0.5):
    
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
        
        if STOCK_INFO.get(code) is None or STOCK_INFO[code]['ipoDate'] > start_date or (STOCK_INFO[code]['outDate'] != '' and STOCK_INFO[code]['outDate'] < start_date):
            print(f"跳过未上市或已退市股票: {code}")
            continue
        try:
            bs_code, ok = transform_code_name(code)
            
            if ok:
                df_minute = bs_session.query_history_k_data_plus(
                    code=bs_code,
                    fields="date,time,code,open,high,low,close,volume,amount",
                    start_date=start_date,
                    end_date=end_date,
                    frequency="5", 
                    adjustflag=adjust_flag  # 前复权  1: 后复权  2: 前复权  3: 不复权
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

def run_download_mode(bs_session, start_date, end_date, adjust_flag, frequency, path, code_list=None):
    
    """运行初始下载模式"""
    
    print(f'--- 开始下载数据范围: {start_date} 至 {end_date} ---')
    
    all_minute_data = get_trade_minutes_data(
        bs_session, 
        start_date, 
        end_date, 
        adjust_flag,
        frequency=frequency,
        request_interval=0.7,
        code_list=code_list
    )
    
    if all_minute_data:
        concat_trade_data(all_minute_data, start_date, end_date, path=path)
        print("--- 初始下载模式完成 ---")

    return


def run_fix_mode(bs_session, start_date, end_date, adjust_flag, frequency, path):
    
    """运行失败代码修复模式"""
    
    # Find matching failed_list files within provided range and aggregate codes
    matching_files = find_failed_files_in_range(start_date, end_date)
    if not matching_files:
        print(f"--- 修复模式: {start_date} ~ {end_date} 找不到失败记录或列表为空 ---")
        return

    # For each matching file, retry fetching only for that file's start/end range
    total_success = 0
    total_tried = 0
    for fpath in matching_files:
        try:
            # parse start/end from filename
            fname = os.path.basename(fpath)
            parts = fname[len('failed_list_'):-len('.json')].split('_')
            if len(parts) < 2:
                print(f"警告：无法从文件名解析日期范围：{fname}，跳过。")
                continue
            file_start, file_end = parts[0], parts[1]
            codes = json_load(fpath)
            if not isinstance(codes, list) or len(codes) == 0:
                print(f"文件 {fname} 没有需要修复的代码，跳过。")
                continue

            print(f"--- 修复文件 {fname}（范围 {file_start} ~ {file_end}）: 尝试重新获取 {len(codes)} 个代码 ---")
            total_tried += len(codes)

            # call per-file fetch
            newly_fetched_data = get_trade_minutes_data(
                bs_session,
                file_start,
                file_end,
                adjust_flag,
                frequency,
                request_interval=1,
                code_list=codes,
            )

            if newly_fetched_data:
                concat_trade_data(newly_fetched_data, file_start, file_end, path=path)
                successful_codes = set(newly_fetched_data.keys())
                total_success += len(successful_codes)
                remaining = [c for c in codes if c not in successful_codes]
                if set(remaining) != set(codes):
                    json_save(fpath, remaining)
                    print(f"更新失败列表文件 {fname}，剩余 {len(remaining)} 个未修复代码。")
                else:
                    print(f"未能修复文件 {fname} 中任何代码。")
            else:
                print(f"本次尝试未能修复 {fname} 中的任何代码（无新数据）。")

        except Exception as e:
            print(f"错误：处理失败文件 {fpath} 时出现异常：{e}")
            continue

    if total_tried > 0:
        print(f"修复完成：成功修复 {total_success} / {total_tried} 个代码。")
    else:
        print("修复完成：没有要尝试的失败代码文件。")
            
    print("--- 修复模式完成 ---")
    
    return


def run_pre_adjust_mode(date, path):
    
    """运行前复权调整模式"""
    
    start_date = '2023-01-01'
    
    print(f'--- 启动后前复权调整模式: {start_date} 至 {date} ---')
    
    print('获取股票代码列表...')
    
    adjust_factor_params = get_adjust_factor_params(path='./params')
    code_list = []
    for code, factors in adjust_factor_params.items():
        if factors.get(date):
            print(f'股票 {code} 在 {date} 有复权因子更新，需重新下载数据。')
            code_list.append(code)
            
    if len(code_list) > 0:
        
        # 登录 Baostock
        lg = bs.login() 
        print('login respond error_code:' + lg.error_code)
        print('login respond error_msg:' + lg.error_msg)

        print(f'共 {len(code_list)} 支股票需要重新下载数据进行复权调整。')
        
        date_chunks = split_date_range(start_date, date, chunk_size_days=15)
        for i, (chunk_start, chunk_end) in enumerate(date_chunks, 1):
            print(f"\n[Chunk {i}/{len(date_chunks)}] 正在处理: {chunk_start} ~ {chunk_end}")            
            run_download_mode(bs, chunk_start, chunk_end, "2", "d", path, code_list=code_list)
    
    return


def main_get_trade_data(start_date: str, end_date: str, adjust_flag: str, is_fix: bool, frequency: str, path: str):
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
            run_fix_mode(bs, start_date, end_date, adjust_flag, frequency, path)
        else:
            # 使用初始下载模式函数
            date_chunks = split_date_range(start_date, end_date, chunk_size_days=15)
            for i, (chunk_start, chunk_end) in enumerate(date_chunks, 1):
                print(f"\n[Chunk {i}/{len(date_chunks)}] 正在处理: {chunk_start} ~ {chunk_end}")            
                run_download_mode(bs, chunk_start, chunk_end, adjust_flag, frequency, path)

    finally:
        # 退出 Baostock
        bs.logout()
        
# ... (run_download_mode, run_fix_mode 等函数保持不变) ...

if __name__ == '__main__':
    args = parser.parse_args()
    
    # 确保主脚本可以直接运行，传入 adjust_flag/frequency/is_fix/path
    main_get_trade_data(args.start_date, args.end_date, args.adjust_flag, args.fix, args.frequency, args.path)