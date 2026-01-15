import datetime


def split_date_range(start_date_str: str, end_date_str: str, chunk_size_days: int = 30):
    """
    将一个大的日期范围分割成小的、连续的日期块（默认为 30 天）。

    Args:
        start_date_str: 起始日期字符串 (YYYY-MM-DD)。
        end_date_str: 结束日期字符串 (YYYY-MM-DD)。
        chunk_size_days: 每块的最大天数。

    Returns:
        一个包含 (chunk_start_date_str, chunk_end_date_str) 元组的列表。
    """
    start_date = datetime.datetime.strptime(start_date_str, '%Y-%m-%d').date()
    end_date = datetime.datetime.strptime(end_date_str, '%Y-%m-%d').date()
    
    date_ranges = []
    current_start = start_date

    while current_start <= end_date:
        # 计算当前块的结束日期
        current_end = current_start + datetime.timedelta(days=chunk_size_days - 1)
        
        # 确保块结束日期不超过总的结束日期
        if current_end > end_date:
            current_end = end_date
        
        # 将日期格式化回字符串
        date_ranges.append((
            current_start.strftime('%Y-%m-%d'),
            current_end.strftime('%Y-%m-%d')
        ))
        
        # 更新下一个块的起始日期
        current_start = current_end + datetime.timedelta(days=1)
        
    return date_ranges