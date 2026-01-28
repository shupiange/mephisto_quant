import pandas as pd
import numpy as np
import baostock as bs
import datetime
import time
import argparse
import pytz
import os
import sys
from tqdm import tqdm

# Add project root to path to allow imports from core
current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.dirname(os.path.dirname(current_dir))
core_dir = os.path.join(project_root, 'core')

if project_root not in sys.path:
    sys.path.insert(0, project_root)
if core_dir not in sys.path:
    sys.path.insert(0, core_dir)

from core.utils.utils import json_save, json_load, is_stock_on_trade
from core.utils.name_utils import transform_code_name
from core.utils.datetime_utils import split_date_range
from core.params.get_params import get_stock_code_list, get_trade_date, get_stock_info_detail_list, get_adjust_factor_params
from core.config.work_config import FAILURE_MESSAGE_DIR, PARAMS_DIR, DATASET_DIR

class DataCollector:
    def __init__(self, start_date, end_date, adjust_flag="1", frequency="30", path=DATASET_DIR, failure_dir=FAILURE_MESSAGE_DIR):
        """
        初始化 DataCollector
        :param start_date: 开始日期 (YYYY-MM-DD)
        :param end_date: 结束日期 (YYYY-MM-DD)
        :param adjust_flag: 复权类型 1: hfq, 2: qfq, 3: 不复权
        :param frequency: 频率 5: 5min, d: day
        :param path: 数据保存路径
        :param failure_dir: 失败记录保存路径
        """
        self.start_date = start_date
        self.end_date = end_date
        self.adjust_flag = adjust_flag
        self.frequency = frequency
        self.path = path
        self.failure_dir = failure_dir
        self.fields = "date,code,open,close,high,low,volume,amount,turn" if self.frequency == "d" else "date,time,code,open,high,low,close,volume,amount"
        
        # 加载参数
        self.stock_info = get_stock_info_detail_list(PARAMS_DIR)
        self.stock_codes = sorted(list(get_stock_code_list().keys()))
        
        # 确保目录存在
        os.makedirs(self.path, exist_ok=True)
        os.makedirs(self.failure_dir, exist_ok=True)

    def _get_failed_filepath(self, start_date, end_date):
        """生成失败列表文件的完整路径"""
        filename = f'failed_list_{start_date}_{end_date}.json'
        return os.path.join(self.failure_dir, filename)

    def _update_failed_list(self, newly_failed_codes, start_date, end_date):
        """更新失败列表"""
        filepath = self._get_failed_filepath(start_date, end_date)
        if len(newly_failed_codes) == 0:
            if os.path.exists(filepath):
                os.remove(filepath)
        else:
            json_save(filepath, newly_failed_codes)

    def _find_failed_files_in_range(self):
        """查找在当前日期范围内的所有失败记录文件"""
        files = []
        try:
            for fname in os.listdir(self.failure_dir):
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
                    s = datetime.datetime.strptime(self.start_date, '%Y-%m-%d').date()
                    e = datetime.datetime.strptime(self.end_date, '%Y-%m-%d').date()
                    # include only if the file's date range is fully within provided date range
                    if f_start >= s and f_end <= e:
                        files.append(os.path.join(self.failure_dir, fname))
                except Exception:
                    continue
        except Exception:
            pass
        return files

    def fetch_data(self, start_date, end_date, code_list=None, request_interval=0.1):
        """
        获取指定代码列表在指定日期范围内的分钟线数据
        """
        # 确定要处理的代码列表
        target_codes = code_list if code_list is not None else self.stock_codes
        
        all_minute_data = dict()
        newly_failed_codes = []
        
        desc = f"下载数据 ({start_date} ~ {end_date})"
        if code_list is not None:
            desc = f"修复代码 ({start_date} ~ {end_date})"

        for code in tqdm(target_codes, desc=desc):
            if is_stock_on_trade(self.stock_info, code, start_date, end_date):
                print(f"跳过未上市或已退市股票: {code}")
                continue

            try:
                bs_code, ok = transform_code_name(code)
                if ok:
                    # Baostock query
                    df_minute = bs.query_history_k_data_plus(
                        code=bs_code,
                        fields=self.fields,
                        start_date=start_date,
                        end_date=end_date,
                        frequency=self.frequency,
                        adjustflag=self.adjust_flag
                    ).get_data()
                    
                    if not df_minute.empty:
                        all_minute_data[code] = df_minute
                    else:
                        raise ValueError('Empty Query Result')
                        
                    time.sleep(request_interval)
            except Exception as e:
                newly_failed_codes.append(code)
                print(f"\n获取股票 {code} 的数据失败: {e}")
                time.sleep(request_interval * 2 + 1)
                        
        return all_minute_data, newly_failed_codes

    def save_data(self, all_minute_data, start_date, end_date):
        """保存数据"""
        for code, code_df in all_minute_data.items():
            filename = f'trade_minute_{code}_{start_date}_{end_date}.csv'
            filepath = os.path.join(self.path, filename)
            
            # 计算 time_rank
            if self.frequency != "d":
                code_df.loc[:, 'time_rank'] = code_df.groupby(['code', 'date'])['time'].rank()
                code_df.loc[:, 'time'] = code_df['time'].map(lambda x: x[:12])

            if os.path.exists(filepath):
                try:
                    existing_df = pd.read_csv(filepath)
                    combined_df = pd.concat([existing_df, code_df], ignore_index=True)
                    combined_df.drop_duplicates(subset=['date'] if self.frequency == "d" else ['date', 'time', 'code'], keep='last', inplace=True)
                    combined_df.to_csv(filepath, index=False)
                    print(f"成功更新/追加 {code} 的数据到 {filename}。")
                except Exception as e:
                    print(f"警告: 合并文件 {filepath} 失败 ({e}),将覆盖保存新获取的数据。")
                    code_df.to_csv(filepath, index=False)
            else:
                code_df.to_csv(filepath, index=False)
                print(f"成功保存 {code} 的数据到 {filename}。")

    def run_download(self, code_list=None, chunk_size_days=15):
        """运行分块下载模式"""
        date_chunks = split_date_range(self.start_date, self.end_date, chunk_size_days=chunk_size_days)
        
        for i, (chunk_start, chunk_end) in enumerate(date_chunks, 1):
            print(f"\n[Chunk {i}/{len(date_chunks)}] 正在处理: {chunk_start} ~ {chunk_end}")
            
            # 每次 chunk 重新登录,保持连接活跃
            lg = bs.login()
            if lg.error_code != '0':
                print(f"Baostock login failed: {lg.error_msg}")
                continue

            try:
                all_data, failed_list = self.fetch_data(chunk_start, chunk_end, code_list, request_interval=0.3)
                self._update_failed_list(failed_list, chunk_start, chunk_end)
                if all_data:
                    self.save_data(all_data, chunk_start, chunk_end)
            finally:
                bs.logout()

    def run_fix(self):
        """运行修复模式"""
        matching_files = self._find_failed_files_in_range()
        if not matching_files:
            print(f"--- 修复模式: {self.start_date} ~ {self.end_date} 找不到失败记录或列表为空 ---")
            return

        lg = bs.login()
        if lg.error_code != '0':
             print(f"Baostock login failed: {lg.error_msg}")
             return

        try:
            for fpath in matching_files:
                fname = os.path.basename(fpath)
                parts = fname[len('failed_list_'):-len('.json')].split('_')
                if len(parts) < 2:
                    continue
                
                file_start, file_end = parts[0], parts[1]
                codes = json_load(fpath)
                
                if not isinstance(codes, list) or not codes:
                    print(f"文件 {fname} 没有需要修复的代码,跳过。")
                    continue

                print(f"--- 修复文件 {fname}(范围 {file_start} ~ {file_end}): 尝试重新获取 {len(codes)} 个代码 ---")
                
                newly_fetched_data, failed_list = self.fetch_data(file_start, file_end, code_list=codes, request_interval=0.3)
                
                self._update_failed_list(failed_list, file_start, file_end)
                
                if newly_fetched_data:
                    self.save_data(newly_fetched_data, file_start, file_end)
                    successful_codes = set(newly_fetched_data.keys())
                    remaining = [c for c in codes if c not in successful_codes]
                    print(f"更新后剩余 {len(remaining)} 个未修复代码。")
                else:
                    print(f"本次尝试未能修复 {fname} 中的任何代码。")
        finally:
            bs.logout()

    def run(self, is_fix=False, code_list=None):
        if is_fix:
            self.run_fix()
        else:
            self.run_download(code_list=code_list)



if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="批量下载和修复股票分钟线数据 (Baostock)")
    parser.add_argument('--start-date', type=str, required=True, help="开始日期 (YYYY-MM-DD)")
    parser.add_argument('--end-date', type=str, required=True, help="结束日期 (YYYY-MM-DD)")
    parser.add_argument('--adjust-flag', type=str, default="2", help="1: hfq  2: qfq  3: 不复权")
    parser.add_argument('--frequency', type=str, default="5", help="30: 30min  d: day")
    parser.add_argument('--fix', action='store_true', default=False, help="是否运行失败代码的修复模式")
    parser.add_argument('--path', type=str, default='', help="数据保存目录")
    
    args = parser.parse_args()
    
    path = f'{DATASET_DIR}/{args.path}'

    from core.params.update_params import update_stock_code_list, update_trade_date, update_stock_info_detail_list, update_adjust_factor_params
    update_stock_code_list()
    update_trade_date()
    update_stock_info_detail_list()

    collector = DataCollector(
        start_date=args.start_date,
        end_date=args.end_date,
        adjust_flag=args.adjust_flag,
        frequency=args.frequency,
        path=path
    )
    collector.run(is_fix=args.fix)

    # if args.adjust_flag == '2' and args.frequency == 'd':
    #     adjusted_codes = update_adjust_factor_params(args.start_date, args.end_date)
    #     if len(adjusted_codes) > 0:
    #         print(f"有 {len(adjusted_codes)} 个股票需要更新复权因子参数。")
    #         collector = DataCollector(
    #             start_date='2023-01-01',
    #             end_date=args.start_date,
    #             adjust_flag=args.adjust_flag,
    #             frequency=args.frequency,
    #             path=path
    #         )
    #         collector.run(code_list=adjusted_codes, is_fix=args.fix)


    
    