from params.update_params import update_all_params
from data_source.get_daily_trade_data import get_daily_trade_data, get_trade_data_by_day, concat_trade_data

import akshare as ak


import datetime
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
    
    update_all_params(ak, path='./params')

    if date == '' and start_date == '' and end_date == '':
        current_date = datetime.date.today().strftime('%Y-%m-%d')
        get_daily_trade_data(ak, current_date, data_path)
    elif date != '':
        get_daily_trade_data(ak, date, data_path)
    elif start_date != '' and end_date != '':
        start_date = datetime.datetime.strptime(start_date, '%Y-%m-%d').date()
        end_date = datetime.datetime.strptime(end_date, '%Y-%m-%d').date()
        delta = datetime.timedelta(days=1)
        current_date = start_date
        while current_date <= end_date:
            current_date_str = current_dt.strftime('%Y-%m-%d')
            get_daily_trade_data(ak, current_date_str, data_path)
            current_dt += delta