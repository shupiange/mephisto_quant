"""
市场环境数据采集器

采集指数、黄金、利率等宏观环境数据，写入 quant.market_environment 表。

数据源：
  - A股指数（上证/深成/中证500/创业板/科创50/沪深300）: Baostock
  - 恒生指数: Akshare (东方财富)
  - 黄金基准价: Akshare (上海金交所)
  - 国债收益率（中国10年/美国10年）: Akshare

用法：
  # 更新指定日期范围
  python3 core/data/market_env_collector.py --start-date 2025-12-01 --end-date 2025-12-31

  # 更新今天
  python3 core/data/market_env_collector.py
"""

import argparse
import sys
import os
import time
from datetime import datetime, date

import pandas as pd
import baostock as bs

project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

from core.database.db_manager import MySQLManager
from core.config.database_config import DATABASE_CONFIG

TABLE_NAME = 'market_environment'

# ── A 股指数（Baostock） ──

BAOSTOCK_INDICES = {
    'sh.000001': '上证指数',
    'sz.399001': '深证成指',
    'sh.000905': '中证500',
    'sz.399006': '创业板指',
    'sh.000300': '沪深300',
}


def fetch_baostock_indices(start_date: str, end_date: str) -> pd.DataFrame:
    """从 Baostock 获取 A 股指数日线数据"""
    lg = bs.login()
    if lg.error_code != '0':
        print(f"Baostock login failed: {lg.error_msg}")
        return pd.DataFrame()

    all_rows = []
    try:
        for code, name in BAOSTOCK_INDICES.items():
            rs = bs.query_history_k_data_plus(
                code, 'date,code,open,high,low,close,volume,amount',
                start_date=start_date, end_date=end_date, frequency='d'
            )
            df = rs.get_data()
            if df.empty:
                print(f"  {name} ({code}): 无数据")
                continue
            df['name'] = name
            all_rows.append(df)
            print(f"  {name} ({code}): {len(df)} 行")
            time.sleep(0.1)
    finally:
        bs.logout()

    if not all_rows:
        return pd.DataFrame()

    result = pd.concat(all_rows, ignore_index=True)
    for col in ['open', 'high', 'low', 'close', 'amount']:
        result[col] = pd.to_numeric(result[col], errors='coerce')
    result['volume'] = pd.to_numeric(result['volume'], errors='coerce').astype('Int64')
    return result[['date', 'code', 'name', 'open', 'close', 'high', 'low', 'volume', 'amount']]


# ── 恒生指数 + 科创50（Akshare / 东方财富，可能限流） ──

AKSHARE_INDICES = {
    'HSI': ('hk.HSI', '恒生指数', 'hk'),
    '000688': ('sh.000688', '科创50', 'a'),
}


def fetch_akshare_indices(start_date: str, end_date: str) -> pd.DataFrame:
    """从 Akshare 获取恒生指数和科创50"""
    all_rows = []
    try:
        import akshare as ak
        for symbol, (code, name, market) in AKSHARE_INDICES.items():
            try:
                if market == 'hk':
                    df = ak.stock_hk_index_daily_em(symbol=symbol)
                    df = df.rename(columns={'日期': 'date', '开盘': 'open', '收盘': 'close',
                                            '最高': 'high', '最低': 'low', '成交量': 'volume',
                                            '成交额': 'amount'})
                else:
                    df = ak.index_zh_a_hist(symbol=symbol, period='daily',
                                            start_date=start_date.replace('-', ''),
                                            end_date=end_date.replace('-', ''))
                    df = df.rename(columns={'日期': 'date', '开盘': 'open', '收盘': 'close',
                                            '最高': 'high', '最低': 'low', '成交量': 'volume',
                                            '成交额': 'amount'})
                df['date'] = pd.to_datetime(df['date']).dt.strftime('%Y-%m-%d')
                df = df[(df['date'] >= start_date) & (df['date'] <= end_date)]
                df['code'] = code
                df['name'] = name
                for col in ['open', 'close', 'high', 'low', 'amount']:
                    if col in df.columns:
                        df[col] = pd.to_numeric(df[col], errors='coerce')
                if 'volume' in df.columns:
                    df['volume'] = pd.to_numeric(df['volume'], errors='coerce').astype('Int64')
                all_rows.append(df[['date', 'code', 'name', 'open', 'close', 'high', 'low', 'volume', 'amount']])
                print(f"  {name} ({code}): {len(df)} 行")
                time.sleep(1)
            except Exception as e:
                print(f"  {name} ({code}) 获取失败（可能被限流）: {e}")
    except ImportError:
        print("  akshare 未安装，跳过")

    if not all_rows:
        return pd.DataFrame()
    return pd.concat(all_rows, ignore_index=True)


# ── 黄金基准价（Akshare / 上海金交所） ──

def fetch_gold(start_date: str, end_date: str) -> pd.DataFrame:
    """从 Akshare 获取上海金交所黄金基准价"""
    try:
        import akshare as ak
        df = ak.spot_golden_benchmark_sge()
        df = df.rename(columns={'交易时间': 'date', '晚盘价': 'close', '早盘价': 'open'})
        df['date'] = pd.to_datetime(df['date']).dt.strftime('%Y-%m-%d')
        df = df[(df['date'] >= start_date) & (df['date'] <= end_date)]
        df['code'] = 'AU.SGE'
        df['name'] = '黄金基准价'
        for col in ['open', 'close']:
            df[col] = pd.to_numeric(df[col], errors='coerce')
        df['high'] = df[['open', 'close']].max(axis=1)
        df['low'] = df[['open', 'close']].min(axis=1)
        df['volume'] = None
        df['amount'] = None
        print(f"  黄金基准价 (AU.SGE): {len(df)} 行")
        return df[['date', 'code', 'name', 'open', 'close', 'high', 'low', 'volume', 'amount']]
    except Exception as e:
        print(f"  黄金基准价获取失败: {e}")
        return pd.DataFrame()


# ── 国债收益率（Akshare） ──

def fetch_bond_rates(start_date: str, end_date: str) -> pd.DataFrame:
    """从 Akshare 获取中美国债收益率"""
    try:
        import akshare as ak
        start_fmt = start_date.replace('-', '')
        df = ak.bond_zh_us_rate(start_date=start_fmt)
        df = df.rename(columns={'日期': 'date'})
        df['date'] = pd.to_datetime(df['date']).dt.strftime('%Y-%m-%d')
        df = df[(df['date'] >= start_date) & (df['date'] <= end_date)]

        rows = []
        # 中国10年期国债
        if '中国国债收益率10年' in df.columns:
            cn = df[['date', '中国国债收益率10年']].dropna().copy()
            cn['code'] = 'CN10Y'
            cn['name'] = '中国10年期国债收益率'
            cn['close'] = pd.to_numeric(cn['中国国债收益率10年'], errors='coerce')
            cn['open'] = cn['close']
            cn['high'] = cn['close']
            cn['low'] = cn['close']
            cn['volume'] = None
            cn['amount'] = None
            rows.append(cn[['date', 'code', 'name', 'open', 'close', 'high', 'low', 'volume', 'amount']])

        # 美国10年期国债
        if '美国国债收益率10年' in df.columns:
            us = df[['date', '美国国债收益率10年']].dropna().copy()
            us['code'] = 'US10Y'
            us['name'] = '美国10年期国债收益率'
            us['close'] = pd.to_numeric(us['美国国债收益率10年'], errors='coerce')
            us['open'] = us['close']
            us['high'] = us['close']
            us['low'] = us['close']
            us['volume'] = None
            us['amount'] = None
            rows.append(us[['date', 'code', 'name', 'open', 'close', 'high', 'low', 'volume', 'amount']])

        if rows:
            result = pd.concat(rows, ignore_index=True)
            print(f"  国债收益率: {len(result)} 行")
            return result
        return pd.DataFrame()
    except Exception as e:
        print(f"  国债收益率获取失败: {e}")
        return pd.DataFrame()


# ── 入库 ──

def save_to_db(df: pd.DataFrame):
    """将数据写入 market_environment 表"""
    if df.empty:
        print("无数据需要写入。")
        return

    db = MySQLManager(**DATABASE_CONFIG)
    db.connect()
    try:
        # 转为字典列表
        df_save = df.copy()
        df_save = df_save.astype(object)
        df_save = df_save.where(pd.notnull(df_save), None)
        data_list = df_save.to_dict('records')

        db.insert_many_data(TABLE_NAME, data_list)
        print(f"成功写入 {len(data_list)} 行到 {TABLE_NAME}。")
    finally:
        db.disconnect()


# ── 主流程 ──

def run(start_date: str, end_date: str):
    print(f"\n=== 采集市场环境数据: {start_date} ~ {end_date} ===\n")

    all_data = []

    # 1. A股指数 (Baostock, 最稳定)
    print("[1/4] A股指数 (Baostock)...")
    df = fetch_baostock_indices(start_date, end_date)
    if not df.empty:
        all_data.append(df)

    # 2. 恒生指数 + 科创50 (Akshare/东方财富，可能限流)
    print("[2/4] 恒生指数 + 科创50 (Akshare)...")
    df = fetch_akshare_indices(start_date, end_date)
    if not df.empty:
        all_data.append(df)

    # 3. 黄金 (Akshare/上海金交所)
    print("[3/4] 黄金基准价 (Akshare)...")
    df = fetch_gold(start_date, end_date)
    if not df.empty:
        all_data.append(df)

    # 4. 国债收益率 (Akshare)
    print("[4/4] 国债收益率 (Akshare)...")
    df = fetch_bond_rates(start_date, end_date)
    if not df.empty:
        all_data.append(df)

    if all_data:
        combined = pd.concat(all_data, ignore_index=True)
        print(f"\n合计 {len(combined)} 行数据，写入数据库...")
        save_to_db(combined)
    else:
        print("\n未获取到任何数据。")

    print("\n=== 完成 ===")


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='采集市场环境数据（指数/黄金/利率）')
    parser.add_argument('--start-date', type=str, default=None, help='开始日期 (YYYY-MM-DD)')
    parser.add_argument('--end-date', type=str, default=None, help='结束日期 (YYYY-MM-DD)')

    args = parser.parse_args()

    today = date.today().strftime('%Y-%m-%d')
    start = args.start_date or today
    end = args.end_date or today

    run(start, end)
