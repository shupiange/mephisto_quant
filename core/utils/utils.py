import json
import numpy as np
import datetime

def json_load(path):
    """
    从指定路径加载JSON文件并返回其内容
    
    Args:
        path (str): JSON文件的路径
        
    Returns:
        dict or list: JSON文件的内容
    """
    with open(path, 'r', encoding='utf-8') as f:
        return json.load(f)

def json_save(path, d):
    """
    将数据保存到指定路径的JSON文件中
    
    Args:
        path (str): 保存JSON文件的路径
        d (dict or list): 要保存的数据
    """
    with open(path, 'w', encoding='utf-8') as f:
        json.dump(d, f, ensure_ascii=False, indent=2)
    return



def dict_key_diff(dict1, dict2):
    keys1 = set(dict1.keys())
    keys2 = set(dict2.keys())
    
    only_in_dict1 = keys1 - keys2      # 只在 dict1 中的键
    only_in_dict2 = keys2 - keys1      # 只在 dict2 中的键
    common_keys   = keys1 & keys2      # 两个字典都有的键（可选）
    
    return {
        'only_in_first': only_in_dict1,
        'only_in_second': only_in_dict2,
        'common': common_keys
    }


def generate_dates(start_date_str, end_date_str):

    start_date = datetime.datetime.strptime(start_date_str, '%Y-%m-%d').date()
    end_date = datetime.datetime.strptime(end_date_str, '%Y-%m-%d').date()

    date_list = []
    current_date = start_date
    while current_date <= end_date:
        date_list.append(current_date.strftime('%Y-%m-%d'))
        current_date += datetime.timedelta(days=1)
    return date_list



def is_stock_on_trade(stock_codes, code, start_date, end_date):
    return stock_codes.get(code) is None or stock_codes[code]['ipoDate'] > start_date or (stock_codes[code]['outDate'] != '' and stock_codes[code]['outDate'] < start_date)