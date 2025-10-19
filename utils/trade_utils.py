from utils.utils import json_load



def get_trade_date(path='./params'):
    return json_load(f'{path}/trade_date.json')