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


class three_bars(bt.Indicator):

    lines = ('up', 'down')

    def __init__(self):
        self.addminperiod(4)
        self.plotinfo.plotmaster = self.data

    def next(self):
        self.up[0] = max(max(self.data.close.get(ago=-1, size=3)), max(self.data.open.get(ago=-1, size=3)))
        self.down[0] = min(min(self.data.close.get(ago=-1, size=3)), min(self.data.open.get(ago=-1, size=3)))


class Mystrategy(bt.Strategy):
    def __init__(self):
        self.up_down = three_bars(self.data)

        self.buy_signal = bt.indicators.CrossOver(self.data.close, self.up_down.up)
        self.sell_signal = bt.indicators.CrossOver(self.data.close, self.up_down.down)

    def start(self):
        print("start")

    def prenext(self):
        print("prenext")

    def next(self):
        if self.getposition().size >= 0 and self.buy_signal[0] == 1:
            self.order = self.buy()
        if self.getposition().size < 0 and self.buy_signal[0] == 1:
            self.order = self.close()
            self.order = self.buy()
        if self.getposition().size <= 0 and self.sell_signal[0] == -1:
            self.order = self.sell()
        if self.getposition().size > 0 and self.buy_signal[0] == -1:
            self.order = self.close()
            self.order = self.sell()


if __name__ == "__main__":
    ## 方法一
    login()
    df = get_kline(htsc_code=["000001.SZ"], time=[datetime(2021, 5, 10), datetime(2022, 5, 10)],
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













