import pandas as pd
import talib
import backtrader as bt
from insight_python.com.insight import common
from insight_python.com.insight.query import *
from insight_python.com.insight.market_service import market_service
from datetime import datetime
import calendar
import numpy as np
# ************************************用户登录************************************
# 登陆
# user 用户名
# password 密码
def login():
    # 登陆前 初始化，没有密码可以访问进行自动化注册
    #https://findata-insight.htsc.com:9151/terminalWeb/#/signup
    user = "USER019331L1"
    password = "F_Y+.3mtc4tU"
    common.login(market_service, user, password)

def data_loader():
    login()
    stock_list = get_all_stocks_info(listing_state="上市交易")['htsc_code']

    time_start_date = "2019-05-02 15:10:11"
    time_end_date = "2020-07-29 11:20:50"
    time_start_date = datetime.strptime(time_start_date, '%Y-%m-%d %H:%M:%S')
    time_end_date = datetime.strptime(time_end_date, '%Y-%m-%d %H:%M:%S')

    result = get_kline(htsc_code=stock_list.values.tolist(), time=[time_start_date, time_end_date],
                       frequency="daily", fq="pre")
    result.to_csv("all_stock_daily_kline_long.csv")

    # 获取沪深300的一年收益率
    benchmark = get_kline(htsc_code="399300.SZ", time=[time_start_date, time_end_date],
                       frequency="daily", fq="pre")
    benchmark.to_csv("benchmark.csv")

def trend_analyzer(close_all, high_all, low_all, volume_all, benchmark_ann_ret):
    """实现MM选股模型的逻辑，评估单只股票是否满足筛选条件
    Args:
        close(pd.Series): 股票收盘价，默认时间序列索引
        high_all:最高价
        low_all：最低价
        volume_all：成交量
        benchmark_ann_ret(float): 基准指数1年收益率，用于计算相对强弱
    """

    out_list = []
    for close_index, high_index, low_index, volume_index in zip(close_all.columns, high_all.columns, low_all.columns, volume_all.columns):
        close = close_all[close_index]
        high = high_all[high_index]
        low = low_all[low_index]
        volume = volume_all[volume_index]

        # 计算50，150，200日均线
        ema_50 = talib.EMA(close, 50).iloc[-1]
        ema_150 = talib.EMA(close, 150).iloc[-1]
        ema_200 = talib.EMA(close, 200).iloc[-1]

        # 收盘价的52周高点和52周低点
        high_52week = close.rolling(52 * 5).max().iloc[-1]
        low_52week = close.rolling(52 * 5).min().iloc[-1]

        # 短期指标，ROC 价格震荡百分比指数，小于0适合建仓
        roc = talib.ROC(close, 10).iloc[-1]

        # RSI - Relative Strength Index 相对强弱指数，50-80为推荐买入
        rsi = talib.RSI(close, timeperiod=14).iloc[-1]

        # MFI - Money Flow Index 资金流量指标，小于40适合建仓
        mfi = talib.MFI(high, low, close, volume, timeperiod=14)[-1]

        # 最新收盘价
        cl = close.iloc[-1]

        # 筛选条件1：收盘价高于150日均线和200日均线
        if cl > ema_150 and cl > ema_200:
            condition_1 = True
        else:
            condition_1 = False

        # 筛选条件2：150日均线高于200日均线
        if ema_150 > ema_200:
            condition_2 = True
        else:
            condition_2 = False

        # 筛选条件3：roc和rsi
        if roc <= 0 and 50 <= rsi <= 80 and mfi < 40:
            condition_3 = True
        else:
            condition_3 = False

        # 筛选条件4：50日均线高于150日均线和200日均线
        if ema_50 > ema_150 and ema_50 > ema_200:
            condition_4 = True
        else:
            condition_4 = False

        # 筛选条件5：收盘价高于50日均线
        if cl > ema_50:
            condition_5 = True
        else:
            condition_5 = False

        # 筛选条件6：收盘价比52周低点高30%
        if cl >= low_52week * 1.3:
            condition_6 = True
        else:
            condition_6 = False

        # 筛选条件7：收盘价在52周高点的25%以内
        if cl >= high_52week * 0.75 and cl <= high_52week * 1.25:
            condition_7 = True
        else:
            condition_7 = False

        # 筛选条件8：相对强弱指数大于等于70
        ## benchmark_ann_ret 是沪深300 为基准的
        rs = close.pct_change(252).iloc[-1] / benchmark_ann_ret * 100
        if rs >= 70:
            condition_8 = True
        else:
            condition_8 = False

        # 判断股票是否符合标准
        if (condition_1 and condition_2 and condition_3 and
                condition_4 and condition_5 and condition_6 and
                condition_7 and condition_8):
            meet_criterion = True
        else:
            meet_criterion = False

        out = {
            "htsc_code": close.name[1],
            "rs": round(rs, 2),
            "close": cl,
            "ema_50": ema_50,
            "ema_150": ema_150,
            "ema_200": ema_200,
            "high_52week": high_52week,
            "low_52week": low_52week,
            "roc": roc,
            "rsi":rsi,
            "meet_criterion": meet_criterion
        }
        out_list.append(out)
        # result[close.name[1]] = pd.Series(out)
    result = pd.DataFrame(out_list)
    print(result)
    return result

def risk_min(RandomPortfolios, stock_returns):
    # 找到标准差最小数据的索引值
    min_index = RandomPortfolios.Volatility.idxmin()
    # 在收益-风险散点图中突出风险最小的点
    RandomPortfolios.plot('Volatility', 'Returns', kind='scatter', alpha=0.3)
    x = RandomPortfolios.loc[min_index, 'Volatility']
    y = RandomPortfolios.loc[min_index, 'Returns']
    # 将该点坐标显示在图中并保留四位小数
    # 提取最小波动组合对应的权重, 并转换成Numpy数组
    GMV_weights = np.array(RandomPortfolios.iloc[min_index, 0: numstocks])
    # 计算GMV投资组合收益
    stock_returns['Portfolio_GMV'] = stock_returns.mul(GMV_weights, axis=1).sum(axis=1)
    return GMV_weights

def sharp_max(RandomPortfolios, stock_returns):
    # 设置无风险回报率为0
    risk_free = 0
    # 计算每项资产的夏普比率
    RandomPortfolios['Sharpe'] = (RandomPortfolios.Returns - risk_free) / RandomPortfolios.Volatility

    # 找到夏普比率最大数据对应的索引值
    max_index = RandomPortfolios.Sharpe.idxmax()

    # 提取最大夏普比率组合对应的权重，并转化为numpy数组
    MSR_weights = np.array(RandomPortfolios.iloc[max_index, 0:numstocks])
    # 计算MSR组合的收益
    stock_returns['Portfolio_MSR'] = stock_returns.mul(MSR_weights, axis=1).sum(axis=1)
    #输出夏普比率最大的投资组合的权重
    print(MSR_weights)
    return MSR_weights

def Markowitz(total_codes, stock_returns):
    # method1:探索投资组合的最有方案，使用蒙特卡洛模拟Markowitz模型

    # 设置模拟的次数
    number = 1000
    # 设置空的numpy数组，用于存储每次模拟得到的权重、收益率和标准差
    random_p = np.empty((number, numstocks+2))
    # 设置随机数种子，这里是为了结果可重复
    np.random.seed(numstocks+2)

    # 循环模拟1000次随机的投资组合
    for i in range(number):
        # 生成n个随机数，并归一化，得到一组随机的权重数据
        random_n = np.random.random(numstocks)
        random_weight = random_n / np.sum(random_n)

        # 计算年平均收益率
        mean_return = stock_returns.mul(random_weight, axis=1).sum(axis=1).mean()
        annual_return = (1 + mean_return) ** 252 - 1

        # 计算年化标准差，也成为波动率
        # 计算协方差矩阵
        cov_mat = stock_returns.cov()
        # 年化协方差矩阵
        cov_mat_annual = cov_mat * 252
        # 输出协方差矩阵
        print(cov_mat_annual)
        random_volatility = np.sqrt(np.dot(random_weight.T, np.dot(cov_mat_annual, random_weight)))

        # 将上面生成的权重，和计算得到的收益率、标准差存入数组random_p中
        random_p[i][:numstocks] = random_weight
        random_p[i][numstocks] = annual_return
        random_p[i][numstocks+1] = random_volatility

    # 将Numpy数组转化为DataF数据框
    RandomPortfolios = pd.DataFrame(random_p)
    # 设置数据框RandomPortfolios每一列的名称
    RandomPortfolios.columns = [code + '_weight' for code in total_codes] + ['Returns', 'Volatility']


    # weights = risk_min(RandomPortfolios, stock_returns)
    weights = sharp_max(RandomPortfolios, stock_returns)

    return weights

def weight_cal(total_codes, stock_returns):
    stock_returns['time'] = pd.to_datetime(stock_returns['time']).dt.date
    stock_returns = pd.pivot(stock_returns, index="time", columns="htsc_code", values="close")
    stock_returns.columns = [col + "_daily_return" for col in stock_returns.columns]
    stock_returns = stock_returns.pct_change().dropna()

    GMV_weights = Markowitz(total_codes, stock_returns)

    return GMV_weights

def last_day_of_month(any_day):
    """
    获取获得一个月中的最后一天
    :param any_day: 任意日期
    :return: string
    """
    next_month = any_day.replace(day=28) + datetime.timedelta(days=4)  # this will never fail
    return next_month - datetime.timedelta(days=next_month.day)

class Select_Strategy(bt.Strategy):
    def __init__(self):
        self.codes = total_codes
    def next(self):
        today = self.data.datetime.date()
        year, month = today.year, today.month
        d, month_length = calendar.monthrange(year, month)
        if today.day == month_length:
            for i in range(len(self.codes)):
                self.order_target_percent(target=final_weight[i], data=self.codes[i])


if __name__ == '__main__':
    #数据准备，爬取沪深所有股票的日k数据用于选股，第一次执行，后续保存到csv以后就可以从本地读取，不用每次从服务器爬取，会很慢
    #执行过一次后可以注释掉这行代码
    # data_loader()

    #从文件从读取刚下载好的沪深所有标的的日K数据
    df = pd.read_csv("all_stock_daily_kline_long.csv", index_col=0)
    df['datetime'] = pd.to_datetime(df['time']).dt.date

    # 获取benchmark数据，benchmark为沪深300
    benchmark_df = pd.read_csv("benchmark.csv", index_col=0)
    benchmark_df['time'] = pd.to_datetime(benchmark_df['time']).dt.date

    # 计算benchmark的1年累计收益率
    benchmark_df = pd.pivot(benchmark_df, index="time", columns="htsc_code", values="close")
    ##  转换为增长率 并求和
    benchmark_df = benchmark_df.pct_change().dropna().sum(axis=0).reset_index()
    benchmark_ann_ret = benchmark_df.loc[[0], [0]].values.tolist()[0][0]
    print(benchmark_ann_ret)

    # 仅仅筛选有足够历史数据的股票
    symbols_to_screen = list(df.htsc_code.unique())
    print(symbols_to_screen)

    # 将数据框的格式从long-format转化为wide-format
    close_all = pd.pivot(df, index="datetime", columns="htsc_code", values=["close"])
    high_all = pd.pivot(df, index="datetime", columns="htsc_code", values=["high"])
    low_all = pd.pivot(df, index="datetime", columns="htsc_code", values=["low"])
    volume_all = pd.pivot(df, index="datetime", columns="htsc_code", values=["volume"])

    #获取选股结果
    results = trend_analyzer(close_all, high_all, low_all, volume_all, benchmark_ann_ret)

    # 获取满足条件的股票列表
    total_codes = results[results['meet_criterion'] == True]["htsc_code"].tolist()
    print("股票列表 备注：如果为空则无结果", total_codes)


    #以下均为投资组合权重计算模块
    login()
    numstocks = len(total_codes)
    #重新获取选股列表的日k数据，时间范围至少包含回测时间，同时最好向前选一段时间，用于最佳权重的计算
    start_time = datetime(2020, 5, 1)
    end_time = datetime(2020, 8, 31)
    df = get_kline(htsc_code=total_codes, time=[start_time, end_time],
                   frequency="daily", fq="pre")

    # 回测时间,在本例中为2020.7.30-2020.8.30
    backtest_start_time = datetime(2020, 7, 30)
    backtest_end_time = datetime(2020, 8, 4)

    stock_returns = df.copy()
    # 计算最佳权重,在本例中为2020.5.1-2020.7.30
    stock_returns = stock_returns[['time', 'htsc_code', 'close']][stock_returns['time'] <= datetime(2020, 7, 30)]
    final_weight = weight_cal(total_codes, stock_returns)

    #回测模块,参考前一讲的最佳权重策略
    cerebro = bt.Cerebro()
    for code in total_codes:
        data = df[df["htsc_code"] == code]
        date_feed = bt.feeds.PandasData(dataname=data, datetime="time", fromdate=backtest_start_time, todate=backtest_end_time)
        cerebro.adddata(date_feed, name=code)
        print('添加股票数据：code: %s' % code)
    cerebro.addstrategy(Select_Strategy)
    cerebro.broker.setcash(20000.0)

    result = cerebro.run()
    print(result)
    print("value: ", cerebro.broker.get_value())
    print("cash: ", cerebro.broker.getcash())
    cerebro.plot()
