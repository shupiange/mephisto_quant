import pandas as pd
import numpy as np
import akshare as ak
import datetime
import time
import argparse

from tqdm import tqdm

from utils.utils import json_load, json_save
from utils.trade_utils import get_trade_date

from params.get_params import get_stock_code_list

import argparse
parser = argparse.ArgumentParser()

parser.add_argument('--date', type=str, default='', required=False)

TRADE_DATE = get_trade_date()


def concat_trade_data(all_minute_data, date, path='./dataset', source_path='./params'):
    
    dictionary = json_load(f'{source_path}/dictionary.json')
    
    df_list = []
    for code, code_df in all_minute_data.items():
        code_df.loc[:, 'time_rank'] = code_df['时间'].rank()
        df_list.append(code_df.rename(columns=dictionary))
        
    total_df = pd.concat(df_list, axis=0)
    total_df.to_csv(f'{path}/trade_minute_{date}.csv', index=False)
    
    return


def get_trade_data_by_day(ak, date='', code_list=[]):

    # date = '2025-09-30'

    if date == '':
        date = datetime.date.today().strftime('%Y-%m-%d')
    
    if TRADE_DATE.get(date) is None:
        print(f'{date} Is Not Trade Day !')
        return 
        
    pattern_date_start = f'{date} 09:30:00'
    pattern_date_end = f'{date} 15:00:00'

    stock_code_list = get_stock_code_list()
    
    all_minute_data = dict()

    failed_list = []
    total_code_list = sorted(list(stock_code_list.keys())) if len(code_list) == 0 else code_list
    total_code_num = len(total_code_list)
    
    for i, code in tqdm(enumerate(total_code_list, start=1)):
        try:
            # 获取该股票的分钟数据
            df_minute = ak.stock_zh_a_hist_min_em(
                symbol=code, 
                period='1', 
                start_date=pattern_date_start,
                end_date=pattern_date_end,
                adjust="hfq" # 可以选择 "qfq" (前复权), "hfq" (后复权) 或 "" (不复权)
            )
            
            # 检查数据是否为空
            if not df_minute.empty:
                df_minute['代码'] = code  # 添加股票代码列
                all_minute_data[code] = df_minute
            else:
                raise(ValueError, 'Empty Query Result')
            # print(f'Step: {total_code_num} / {i}')
            time.sleep(0.5) # 建议暂停 0.5 到 2 秒，根据你的网络和数据源情况调整
    
        except Exception as e:
            # 记录获取失败的股票
            failed_list.append(code)
            print(f"\n获取股票 {code} 的分钟数据失败: {e}")
            time.sleep(1) # 失败时可以暂停更长时间
            
    if len(failed_list) > 0:
        json_save(f'./message/failed_list_{date}.json', failed_list)
        print(f'{date} failed code counts: ', len(failed_list))
        
    return all_minute_data


def get_daily_trade_data(ak, date, path='./dataset'):
    if TRADE_DATE.get(date) is not None:
        print(f'Start Get Trade Data By Day: {date} ')
        all_minute_df = get_trade_data_by_day(ak, date)
        concat_trade_data(all_minute_df, date, path=path)
    else:
        print(f'{date} Is Not Trade Day !')
    return 


if __name__ == '__main__':
    
    args = parser.parse_args()

    date = datetime.date.today().strftime('%Y-%m-%d')
    if args.date != '':
        date = args.date
    
    all_minute_df = get_trade_data_by_day(ak, date)
    concat_trade_data(all_minute_df, date)
    
    