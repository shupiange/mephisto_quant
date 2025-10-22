# run.py (位于项目根目录)

import argparse
import sys
import os
import datetime
import pytz

from params.update_params import update_all_params
from data_source.get_trade_data import main_get_trade_data


def parse_and_run():
    """
    解析命令行参数，并调用 data_source/get_trade_data.py 中的主处理函数。
    """
    
    # 定义参数解析器，与子脚本的参数保持一致
    parser = argparse.ArgumentParser(
        description="启动股票分钟线数据下载和修复脚本 (通过导入调用)。"
    )
    
    parser.add_argument('--start-date', type=str, required=True, help="开始日期 (YYYY-MM-DD)")
    parser.add_argument('--end-date', type=str, required=True, help="结束日期 (YYYY-MM-DD)")
    parser.add_argument('--fix', type=bool, default=False, help="是否运行失败代码的修复模式")
    parser.add_argument('--path', type=str, default='./dataset', help="数据保存目录")

    args = parser.parse_args()

    print(f"正在启动数据处理: {args.start_date} 到 {args.end_date} (修复模式: {args.fix})")

    if args.end_date == datetime.datetime.now(pytz.timezone('Asia/Shanghai')).strftime('%Y-%m-%d'):
        update_all_params(path='./params')


    # 调用导入的主处理函数
    main_get_trade_data(
        start_date=args.start_date,
        end_date=args.end_date,
        is_fix=args.fix,
        path=args.path
    )
    
    print("\n数据处理流程完成。")


if __name__ == '__main__':
    parse_and_run()