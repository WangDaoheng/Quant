import pandas as pd
import sqlalchemy
import backtrader as bt

import CommonProperties.Base_Properties as base_properties
from CommonProperties.Mysql_Utils import data_from_mysql_to_dataframe_latest, data_from_mysql_to_dataframe


# 连接到 MySQL 数据库
user = base_properties.origin_mysql_user
password = base_properties.origin_mysql_password
database = base_properties.origin_mysql_database
host = base_properties.origin_mysql_host
port = '3306'

# 连接到 MySQL 数据库

# 从数据库中读取筹码数据，取最新日期的
chouma_df = data_from_mysql_to_dataframe_latest(user=user, password=password, host=host, database=database,
                                                table_name='ods_stock_chouma_insight', cols=['htsc_code','ymd','winner_rate','diversity'])


# 抽取概念板块数据



industry_df = pd.read_sql('SELECT * FROM quant.ods_astock_industry_detail', engine)

# 筛选条件
filtered_stocks = chouma_df[
    (chouma_df['winner_rate'] > 60) & (chouma_df['diversity'] > 60)
    ]

# 获取股票代码和名称
filtered_stock_codes = filtered_stocks[['htsc_code', 'name']]

# 合并行业信息
result = pd.merge(filtered_stock_codes, industry_df, on='htsc_code', how='left')

# 输出结果
result_list = result[['htsc_code', 'name', 'industry_name']].values.tolist()
print("筛选结果:")
for item in result_list:
    print(item)


# 定义获取数据的函数
def get_data(htsc_code):
    query = f'SELECT ymd, open, close, high, low, volume FROM quant.ods_stock_kline_daily_insight WHERE htsc_code = "{htsc_code}" ORDER BY ymd'
    df = pd.read_sql(query, engine)
    df['ymd'] = pd.to_datetime(df['ymd'])
    df.set_index('ymd', inplace=True)
    return df


# 定义策略类
class MyStrategy(bt.Strategy):
    def __init__(self):
        self.sma = bt.indicators.SimpleMovingAverage(self.data.close, period=15)

    def next(self):
        if self.data.close[0] > self.sma[0]:
            self.buy()
        elif self.data.close[0] < self.sma[0]:
            self.sell()


# 主程序
if __name__ == '__main__':
    cerebro = bt.Cerebro()

    # 使用筛选出的股票进行回测
    for stock in result['htsc_code']:
        data = get_data(stock)
        data_feed = bt.feeds.PandasData(dataname=data)
        cerebro.adddata(data_feed, name=stock)

    cerebro.addstrategy(MyStrategy)

    # 设置初始资金
    cerebro.broker.setcash(100000.0)

    # 运行回测
    cerebro.run()

    # 输出最终资产
    print('最终资产: %.2f' % cerebro.broker.getvalue())
