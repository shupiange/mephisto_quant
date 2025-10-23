from utils.utils import json_load




def get_stock_code_list(path='./params'):
    return json_load(f'{path}/stock_code_list.json')


def get_trade_date(path='./params'):
    return json_load(f'{path}/trade_date.json')

def get_adjust_factor_params(path='./params'):
    return json_load(f'{path}/adjust_factor.json')
