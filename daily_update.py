from params.update_params import update_all_params
from data_source.get_trade_data import get_daily_trade_data, get_range_trade_data

import akshare as ak
import baostock as bs


import datetime
import pytz
import argparse

parser = argparse.ArgumentParser()


parser.add_argument('--date', type=str, default='', required=False)
parser.add_argument('--start-date', type=str, default='', required=False)
parser.add_argument('--end-date', type=str, default='', required=False)
parser.add_argument('--data-path', type=str, default='./dataset', required=False)



if __name__ == '__main__':

    args = parser.parse_args()
    
    date = args.date
    start_date = args.start_date
    end_date = args.end_date
    data_path = args.data_path
    
    if date == datetime.datetime.now(pytz.timezone('Asia/Shanghai')).strftime('%Y-%m-%d'):
        update_all_params(ak, path='./params')

    lg = bs.login()
    print('login respond error_code:'+lg.error_code)
    print('login respond error_msg:'+lg.error_msg)
    
    if date == '' and start_date == '' and end_date == '':
        current_date = datetime.date.today().strftime('%Y-%m-%d')
        get_daily_trade_data(bs, current_date, data_path)
        
    elif date != '':
        get_daily_trade_data(bs, date, data_path)
        
    elif start_date != '' and end_date != '':
        get_range_trade_data(bs, start_date, end_date, data_path)
            
    bs.logout()