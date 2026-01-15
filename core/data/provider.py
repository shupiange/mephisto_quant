from pathlib import Path
from typing import List

from core.database.load_dataset import load_dataset
import akshare as ak
import pandas as pd


class DataProvider:
    """
    DataProvider 负责从互联网获取 A 股数据并按规范组织在本地目录。
    """
    def __init__(self, base_data_path: str = "data"):
        self.base_path = Path(base_data_path)
        self.daily_path = self.base_path / "daily"
        self.symbols_file = self.base_path / "symbols.csv"

        # 自动创建目录
        self.daily_path.mkdir(parents=True, exist_ok=True)

    def get_all_symbols(self) -> pd.DataFrame:
        """获取 A 股全市场股票列表并保存为 symbols.csv"""
        print("正在获取全市场股票列表...")
        try:
            stock_info_df = ak.stock_zh_a_spot_em()
            stock_info_df = stock_info_df[['代码', '名称']]
            stock_info_df.columns = ['symbol', 'name']
            stock_info_df.to_csv(self.symbols_file, index=False)
            print(f"股票列表已保存至: {self.symbols_file}")
            return stock_info_df
        except Exception as e:
            print(f"获取股票列表失败: {e}")
            return pd.DataFrame()

    def download_stock_data(self, symbol: str, start_date: str, end_date: str, period: str = "daily"):
        """下载个股历史数据并按周期分类存储 (支持 A股和港股)"""
        print(f"正在下载 {symbol} ({period}) 数据...")
        try:
            # 判断是 A股还是港股
            if len(symbol) == 5:
                # 港股 (例如 00700)
                # start_date 格式需转换为 YYYYMMDD? AkShare 港股接口通常需要 YYYYMMDD
                # stock_hk_hist 参数: symbol="00700", start_date="20200101", end_date="20210101", adjust="qfq"
                df = ak.stock_hk_hist(symbol=symbol, period=period, start_date=start_date, end_date=end_date, adjust="qfq")
                
                if df.empty:
                    print(f"未找到 {symbol} 在该时间段的数据")
                    return
                
                # 港股返回列名: 日期, 开盘, 收盘, 最高, 最低, 成交量, 成交额...
                # 需要重命名以匹配系统标准
                df.rename(columns={
                    '日期': 'datetime',
                    '开盘': 'open', 
                    '最高': 'high', 
                    '最低': 'low', 
                    '收盘': 'close', 
                    '成交量': 'volume'
                }, inplace=True)
                
            else:
                # A股 (6位代码)
                # AkShare 接口: stock_zh_a_hist
                df = ak.stock_zh_a_hist(symbol=symbol, period=period, start_date=start_date, end_date=end_date, adjust="qfq")
                if df.empty:
                    print(f"未找到 {symbol} 在该时间段的数据")
                    return

                # 标准化列名
                df = df[['日期', '开盘', '最高', '最低', '收盘', '成交量']]
                df.columns = ['datetime', 'open', 'high', 'low', 'close', 'volume']
            
            # 统一后续处理
            # 确保 datetime 列是 datetime 类型 (港股接口返回的可能是 YYYY-MM-DD 字符串)
            df['datetime'] = pd.to_datetime(df['datetime'])
            df.set_index('datetime', inplace=True)
            # 按日期升序排序
            df.sort_index(inplace=True)

            save_dir = self.base_path / period
            save_dir.mkdir(exist_ok=True)

            file_path = save_dir / f"{symbol}.csv"
            df.to_csv(file_path)
            print(f"成功: {symbol} 数据已保存至 {file_path}")
        except Exception as e:
            print(f"失败: 下载 {symbol} 错误: {e}")

    def read_stock_data(self, symbol: str, start_date: str, end_date: str, table_name: str = "daily") -> pd.DataFrame:
        return load_dataset(symbol, start_date, end_date, table_name)

if __name__ == "__main__":
    # 演示代码
    provider = DataProvider(base_data_path="./data")
    # provider.get_all_symbols()
    # provider.download_stock_data("600036", "20230101", "20231231")
