
import backtrader as bt
import pandas as pd
import sqlalchemy



# 连接到 MySQL 数据库
username =

engine = sqlalchemy.create_engine(f"mysql+pymysql://{username}:{password}@{host}:{port}/{database}")

# 从数据库中读取历史数据
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

    # 筛选出需要复盘的股票代码
    stocks_to_backtest = ['股票代码1', '股票代码2']  # 替换为你的股票代码

    for stock in stocks_to_backtest:
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





































