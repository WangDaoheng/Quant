import pandas as pd
from sqlalchemy import create_engine, text
from datetime import datetime

# 创建数据库连接
engine = create_engine('mysql+pymysql://username:password@host:port/database')

# 新数据示例
new_data = pd.DataFrame({
    'ymd': ['20240801', '20240802', '20240803', '20240804', '20240805', '20240806', '20240807'],
    'stock_code': ['AAPL', 'GOOGL', 'AMZN', 'MSFT', 'TSLA', 'NFLX', 'FB'],
    'open': [value1, value2, value3, value4, value5, value6, value7],
    'close': [value8, value9, value10, value11, value12, value13, value14],
    'high': [value15, value16, value17, value18, value19, value20, value21],
    'low': [value22, value23, value24, value25, value26, value27, value28]
})

# 将日期列转换为日期类型
new_data['ymd'] = pd.to_datetime(new_data['ymd'], format='%Y%m%d')

# 获取当前年份和月份
current_date = new_data['ymd'].dt.to_period('M')[0]
current_year = current_date.year
current_month = current_date.month

# 构建分区名
partition_name = f"p{current_year}{str(current_month).zfill(2)}"

# 检查并创建分区
create_partition_if_not_exists(engine, partition_name, current_year, current_month)

# 批量插入或更新数据
# 将新数据写入临时表
temp_table_name = 'temp_table'
new_data.to_sql(temp_table_name, engine, if_exists='replace', index=False)

# 使用 ON DUPLICATE KEY UPDATE 从临时表插入或更新数据
with engine.connect() as conn:
    insert_query = f"""
    INSERT INTO your_table (ymd, stock_code, open, close, high, low)
    SELECT ymd, stock_code, open, close, high, low FROM {temp_table_name}
    ON DUPLICATE KEY UPDATE
        open = VALUES(open),
        close = VALUES(close),
        high = VALUES(high),
        low = VALUES(low)
    """
    conn.execute(text(insert_query))

# 删除临时表
with engine.connect() as conn:
    conn.execute(text(f"DROP TABLE {temp_table_name}"))

print("Data inserted/updated successfully.")
