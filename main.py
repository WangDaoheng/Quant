import backtrader as bt

import pandas as pd

from insight_python.com.insight import common
from insight_python.com.insight.query import *
from insight_python.com.insight.market_service import market_service

from datetime import datetime

def login():
    # user = "USER019331L1"
    # password = "F_Y+.3mtc4tU"
    print("---------------- 执行main 里面的login -------------------")
    user = "USER019331L1"
    password = "F_Y+.3mtc4tU"
    result = common.login(market_service, user, password)

    print("---------------- 执行main.login.result:{}".format(result))


class Mystrategy(bt.Strategy):
    def __init__(self):

        self.bt_sma = bt.indicators.MovingAverageSimple(self.data, period=3)
        ## 上穿 1   不穿 0    下穿 -1
        self.buy_or_sell = bt.indicators.CrossOver(self.data, self.bt_sma)

        self.addminperiod(21)

    def start(self):
        print("start")

    def prenext(self):
        print("prenext")

    def next(self):
        print("next")

        ##  方法一
        # ma_value = sum(self.data.close[-data] for data in range(3))/3
        # pre_ma_value = sum(self.data.close[-data-1] for data in range(3))/3
        # if self.data.close[0] > ma_value and self.data.close[-1] < pre_ma_value:
        #     self.order = self.buy()
        # if self.data.close[0] < ma_value and self.data.close[-1] > pre_ma_value:
        #     self.order = self.sell()


        ## 方法二
        ma_value = self.bt_sma[0]
        pre_ma_value = self.bt_sma[-1]

        ## 上穿买   下穿卖
        if self.buy_or_sell[0] == 1:
            self.order = self.buy()
        if self.buy_or_sell[0] == -1:
            self.order = self.sell()





if __name__ == "__main__":
    ## 方法一
    login()
    df = get_kline(htsc_code=["601688.SH"], time=[datetime(2021, 5, 10), datetime(2022, 5, 10)],
                   frequency="daily", fq="none")
    # df.to_csv('./ticks.csv')


    ## 方法二
    data = bt.feeds.PandasData(
       dataname=df,
        fromdate=datetime(2021,5,10),
        todate=datetime(2022,5,10),
        datetime='time',
        openinterest=-1
    )
    cerebro = bt.Cerebro()
    cerebro.adddata(data, name='daily_kline')
    cerebro.addstrategy(Mystrategy)
    result = cerebro.run()

    cerebro.plot()













