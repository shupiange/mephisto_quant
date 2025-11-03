from utils.utils import json_load, json_save, dict_key_diff
from utils.name_utils import transform_code_name
from params.get_params import get_stock_code_list
import datetime
import pytz
import os
import akshare as ak
import baostock as bs


def update_stock_code_list(path='./params'):
    
    """更新股票代码参数文件"""
    
    try:
        
        total_df = ak.stock_info_a_code_name()
        if total_df.empty:
            raise(ValueError, '')
            
        print('Success Download Stock Code List !')
        
    except Exception as e:
        
        print('Failed Update A Code List:', e)
        
        return 


    stock_code_list = total_df.set_index('code')['name'].to_dict()


    if not os.path.exists(f'{path}/stock_code_list.json'):

        json_save(f'{path}/stock_code_list.json', stock_code_list)

        print('Success Create Stock Code List !')

    else:

        code_list = json_load(f'{path}/stock_code_list.json')

        update_num = 0

        
        gap = dict_key_diff(code_list, stock_code_list)

        print('Delete Codes:', gap['only_in_first'])
        print('Update Codes:', gap['only_in_second'])
        
        json_save(f'{path}/stock_code_list.json', stock_code_list)
        json_save(f'{path}/stock_code_list_bak.json', code_list)
        
        print('Success Update Stock Code List !')
    
    return


def update_trade_date(path='./params'):
    
    """更新交易日期参数文件"""

    try:
        
        date_df = ak.tool_trade_date_hist_sina()
        if date_df.empty:
            raise(ValueError, '')
            
        print('Success Download Trade Date !')

    except Exception as e:

        print('Failed Update A Trade Date:', e)

        return

    new_trade_date = {x: True for x in sorted(date_df['trade_date'].map(lambda x: x.strftime('%Y-%m-%d')).tolist())}


    if not os.path.exists(f'{path}/trade_date.json'):
        
        json_save(f'{path}/trade_date.json', new_trade_date)

        print('Success Create Trade Days !')

    else:

        trade_date = json_load(f'{path}/trade_date.json')

        change = False
        
        if len(trade_date) > len(new_trade_date):
            print('Delete Some Trade Days ...')
            change = True

        if len(trade_date) < len(new_trade_date):
            print('Update Trade Days ...')
            change = True

        if change:
            json_save(f'{path}/trade_date.json', new_trade_date)
            json_save(f'{path}/trade_date_bak.json', trade_date)
            print('Success Update Trade Days !')
            
    return


def update_adjust_factor_params(start_date, end_date, path='./params'):
    
    """更新复权因子参数文件"""
    
    current_date = datetime.datetime.now(pytz.timezone('Asia/Shanghai')).strftime('%Y-%m-%d')
    stock_codes = sorted(get_stock_code_list().keys())
    
    
    adjust_factorys = dict()
    if os.path.exists(f'{path}/adjust_factor.json'):
        adjust_factorys = json_load(f'{path}/adjust_factor.json')
    
    bs.login()
    change = False
    for code in stock_codes:
        code_name, valid = transform_code_name(code)
        if not valid:
            continue
        rs = bs.query_adjust_factor(
            code=code_name, 
            start_date=start_date, 
            end_date=end_date
        )
        
        if (rs.error_code == '0') & rs.next():
            change = True
            adj_factor = {k: v for k, v in zip(rs.fields, rs.get_row_data())}
            adjust_factorys[code_name] = adjust_factorys.get(code_name, dict())[adj_factor['dividOperateDate']] = {
                'foreAdjustFactor': adj_factor['foreAdjustFactor'], 
                'backAdjustFactor': adj_factor['backAdjustFactor'], 
                'adjustFactor': adj_factor['adjustFactor']
            }
    
    bs.logout()

    if change:
        if os.path.exists(f'{path}/adjust_factor.json'):
            json_save(f'{path}/adjust_factor_bak.json', json_load(f'{path}/adjust_factor.json'))
        
        json_save(f'{path}/adjust_factor.json', adjust_factorys)
        print('Success Update Adjust Factor Params !')
    
    return change

















        