from utils.utils import json_load




def get_stock_code_list(path='./params'):
    return json_load(f'{path}/stock_code_list.json')