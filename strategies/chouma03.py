import pandas as pd
import sqlalchemy
import backtrader as bt

import CommonProperties.Base_Properties as base_properties
from CommonProperties.Mysql_Utils import data_from_mysql_to_dataframe_latest, data_from_mysql_to_dataframe


# 连接到 MySQL 数据库
user = base_properties.local_mysql_user
password = base_properties.local_mysql_password
database = base_properties.local_mysql_database
host = base_properties.local_mysql_host
port = '3306'

# 连接到 MySQL 数据库
# 从数据库中读取筹码数据，取最新日期的
chouma_df = data_from_mysql_to_dataframe_latest(user=user, password=password, host=host, database=database,
                                                table_name='ods_stock_chouma_insight', cols=['htsc_code', 'ymd', 'winner_rate', 'diversity'])

# 抽取概念板块数据
stock_concept_df = data_from_mysql_to_dataframe_latest(user=user, password=password, host=host, database=database,
                                                table_name='ods_tdx_stock_concept_plate', cols=['ymd', 'concept_name', 'stock_code', 'stock_name'])

# 筛选条件
filtered_stocks = chouma_df[
    (chouma_df['winner_rate'] > 60) & (chouma_df['diversity'] > 60)]

# 获取stock代码和名称
filtered_stock_codes = filtered_stocks[['htsc_code', 'winner_rate', 'diversity']]

# 去掉 htsc_code 中的 ".SH"
# filtered_stock_codes['htsc_code'] = filtered_stock_codes['htsc_code'].str.replace('.SH', '', regex=False)
filtered_stock_codes['htsc_code'] = filtered_stock_codes['htsc_code'].str.replace(r'\.\w{2}$', '', regex=True)

# 合并行业信息
result = pd.merge(filtered_stock_codes, stock_concept_df, left_on='htsc_code', right_on='stock_code', how='left')

# 输出结果
result_list = result[['htsc_code', 'stock_name', 'concept_name', 'winner_rate', 'diversity']].values.tolist()
result.to_csv(r'F:\QDatas\stock_res.csv')


# 定义获取数据的函数
def get_data():

    df = data_from_mysql_to_dataframe(user=user, password=password, host=host, database=database,
                                 table_name='ods_stock_kline_daily_insight',
                                 start_date='2024-01-01', end_date='2024-11-04')

    df['ymd'] = pd.to_datetime(df['ymd'])
    df.set_index('ymd', inplace=True)
    return df


# 定义策略类
class MyStrategy(bt.Strategy):
    def __init__(self):
        self.sma = bt.indicators.MovingAverageSimple(self.data.close, period=15)

    def next(self):
        if self.data.close[0] > self.sma[0]:
            self.buy()
        elif self.data.close[0] < self.sma[0]:
            self.sell()


# 主程序
if __name__ == '__main__':
    cerebro = bt.Cerebro()

    # 使用筛选出的股票进行回测
    res = get_data()



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
