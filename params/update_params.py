from utils.utils import json_load, json_save, dict_key_diff

import os

def update_all_params(ak, path='./params'):
    update_stock_code_list(ak, path)
    update_trade_date(ak, path)
    return


def update_stock_code_list(ak, path='./params'):

    
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


def update_trade_date(ak, path='./params'):


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




















        