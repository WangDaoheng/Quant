# 突破类策略
# DualThrust
import backtrader as bt
from insight_python.com.insight import common
from insight_python.com.insight.query import *
from insight_python.com.insight.market_service import market_service
from datetime import datetime, time


# user 用户名
# password 密码
def login():
    # 登陆前 初始化
    user = "USER019331L1"
    password = "F_Y+.3mtc4tU"
    common.login(market_service, user, password)


class DT_Line(bt.Indicator):
    lines = ('U', 'D')
    params = (('period', 2), ('k_u', 0.7), ('k_d', 0.7))

    def __init__(self):
        print('self.p.period  是 {}'.format(self.p.period))
        print('self.params.period  是 {}'.format(self.params.period))

        self.addminperiod(self.p.period + 1)

    def next(self):
        HH = max(self.data.high.get(ago=-1, size=self.p.period))
        LC = min(self.data.close.get(ago=-1, size=self.p.period))
        HC = max(self.data.close.get(ago=-1, size=self.p.period))
        LL = min(self.data.low.get(ago=-1, size=self.p.period))
        R = max(HH - LC, HC - LL)
        self.lines.U[0] = self.data.open[0] + self.p.k_u * R
        self.lines.D[0] = self.data.open[0] - self.p.k_d * R


class DualThrust(bt.Strategy):
    def __init__(self):
        self.dataclose = self.data0.close
        self.D_Line = DT_Line(self.data1)
        # 将Dline放到主图上，同时消除daily和min的区别，需要做映射
        self.D_Line = self.D_Line()
        # self.D_Line.plotinfo.plot = False
        self.D_Line.plotinfo.plotmaster = self.data0

        self.buy_signal = bt.indicators.CrossOver(self.D_Line.U, self.dataclose)
        self.sell_signal = bt.indicators.CrossOver(self.D_Line.D, self.dataclose)

    def next(self):

        print("next")

        if self.data.datetime.time() > time(9, 5) and self.data.datetime.time() < time(15, 30):

            if self.getposition().size >= 0 and self.buy_signal[0] == 1:
                self.order = self.buy()

            if  self.getposition().size < 0 and self.buy_signal[0] == 1:
                self.order = self.close()
                self.order = self.buy()

            if self.getposition().size <= 0 and self.sell_signal[0] == -1:
                self.order = self.sell()

            if self.getposition().size > 0 and self.sell_signal[0] == -1:
                self.order = self.close()
                self.order = self.sell()

        if self.data.datetime.time() >= time(15, 30) and self.position:
            self.order = self.close()


if __name__ == '__main__':
    login()
    cerebro = bt.Cerebro()
    df = get_kline(htsc_code=["601688.SH"], time=[datetime(2021, 5, 10), datetime(2021, 8, 10)],
                   frequency="1min", fq="none")
    data = bt.feeds.PandasData(
        dataname=df,
        fromdate=datetime(2021, 5, 10),
        todate=datetime(2021, 8, 10),
        timeframe=bt.TimeFrame.Minutes,
        datetime='time',
        openinterest=-1
    )
    cerebro.adddata(data, name="daily_kline")
    cerebro.resampledata(data, timeframe=bt.TimeFrame.Days)

    cerebro.addstrategy(DualThrust)
    result = cerebro.run()

    cerebro.plot()

