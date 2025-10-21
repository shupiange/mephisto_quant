import pandas as pd
import numpy as np
import baostock as bs
import datetime
import time
import argparse
import pytz

from tqdm import tqdm

from utils.utils import json_save
from utils.trade_utils import transform_code_name, transform_code
from params.get_params import get_stock_code_list, get_trade_date

import argparse
parser = argparse.ArgumentParser()

parser.add_argument('--date', type=str, default='', required=False)

TRADE_DATE = get_trade_date()


def concat_trade_data(all_minute_data, date, path='./dataset', source_path='./params'):
        
    df_list = []
    for code, code_df in all_minute_data.items():
        code_df.loc[:, 'time_rank'] = code_df['time'].rank()
        df_list.append(code_df)
        
    total_df = pd.concat(df_list, axis=0)
    total_df.to_csv(f'{path}/trade_minute_{date}.csv', index=False)
    
    return


def get_trade_data_daily(bs, start_date, end_date, code_list=[]):
    
    if start_date == end_date and TRADE_DATE.get(start_date) is None:
        print(f'{start_date} Is Not Trade Day !')
        return 

    stock_code_list = get_stock_code_list()
    
    all_minute_data = dict()

    failed_list = []
    total_code_list = sorted(list(stock_code_list.keys())) if len(code_list) == 0 else code_list
    
    for i, code in tqdm(enumerate(total_code_list, start=1)):
        try:
            # 获取该股票的分钟数据
            df_minute = bs.query_history_k_data_plus(
                code=transform_code_name(code),
                fields="date,time,code,open,high,low,close,volume,amount", # 注意这里新增了 time 字段
                start_date=start_date,  # 分钟线数据量大，建议缩短查询时间范围
                end_date=end_date,
                frequency="5",   # !!! 设置为 "5" 来获取 5 分钟线数据
                adjustflag="2"   # 前复权
            ).get_data()
            
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
            time.sleep(5) # 失败时可以暂停更长时间
            
    if len(failed_list) > 0:
        json_save(f'./message/failed_list_{start_date}_{end_date}.json', failed_list)
        print(f'{start_date}_{end_date} failed code counts: ', len(failed_list))
        
    return all_minute_data


def get_daily_trade_data(bs, date, path='./dataset'):
    if TRADE_DATE.get(date) is not None:
        print(f'Start Get Trade Data By Day: {date} ')
        all_minute_df = get_trade_data_daily(bs, date, date)
        concat_trade_data(all_minute_df, date, path=path)
    else:
        print(f'{date} Is Not Trade Day !')
    return 


if __name__ == '__main__':
    
    args = parser.parse_args()

    date = datetime.datetime.now(pytz.timezone('Asia/Shanghai')).strftime('%Y-%m-%d')
    if args.date != '':
        date = args.date
    
    all_minute_df = get_trade_data_daily(bs, date, date)
    concat_trade_data(all_minute_df, date)
    
    