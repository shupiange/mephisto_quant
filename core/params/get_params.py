from utils.utils import json_load
from core.config.work_config import PARAMS_DIR



def get_stock_code_list(path=PARAMS_DIR):
    return json_load(f'{path}/stock_code_list.json')


def get_trade_date(path=PARAMS_DIR):
    return json_load(f'{path}/trade_date.json')


def get_stock_info_detail_list(path=PARAMS_DIR):
    return json_load(f'{path}/stock_info_detail_list.json')


def get_adjust_factor_params(path=PARAMS_DIR):
    return json_load(f'{path}/adjust_factor.json')
