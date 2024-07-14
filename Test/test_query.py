
import os
from insight_python.com.insight import common
from insight_python.com.insight.query import *
from insight_python.com.insight.market_service import market_service
from datetime import datetime
import time
import requests


import yfinance as yf
from yahoo_fin.stock_info import *
import requests_html

import CommonProperties.Base_Properties as dataprepare_properties
import datas_prepare.dataprepare_utils as dataprepare_utils



def login():
    # 登陆前 初始化，没有密码可以访问进行自动化注册
    # https://findata-insight.htsc.com:9151/terminalWeb/#/signup
    user = dataprepare_properties.user
    password = dataprepare_properties.password
    common.login(market_service, user, password)


def get_kline_demo():
    """
    :param htsc_code: 华泰证券代码，支持多个code查询，列表类型
    :param time: 时间范围，list类型，开始结束时间为datetime
    :param frequency: 频率，分钟K（‘1min’，’5min’，’15min’，’60min’），日K（‘daily’），周K（‘weekly’），月K（‘monthly’）
    :param fq: 复权，默认前复权”pre”，后复权为”post”，不复权“none”
    :return:pandas.DataFrame
    """

    # time_start_date = "2022-12-31 15:10:11"
    # time_end_date = "2024-01-02 15:20:50"
    # time_start_date = datetime.strptime(time_start_date, '%Y-%m-%d %H:%M:%S')
    # time_end_date = datetime.strptime(time_end_date, '%Y-%m-%d %H:%M:%S')

    time_start_date = "2024-01-14"
    time_end_date = "2024-04-03"
    time_start_date = datetime.strptime(time_start_date, '%Y-%m-%d')
    time_end_date = datetime.strptime(time_end_date, '%Y-%m-%d')

    result = get_kline(htsc_code=["DINIW.CFE"], time=[time_start_date, time_end_date],
                       frequency="daily", fq="none")
    result.to_csv('./DINIW.txt', sep=',')
    print(result)


def get_dollar_index_yahoo():
    ticker = 'DX-Y.NYB'
    data = yf.download(ticker, period='1d')
    return data


def get_yahoo():
    df = yf.download('TSLA', interval='1d', start='2022-04-17', end='2022-04-24', threads=True, proxy='127.0.0.1:9981')
    print(type(df))
    print(df)



def use_vantage():

    # 替换为你的 Alpha Vantage API 密钥
    api_key = 'ICTN 9 P9 ES 00 EADUF'

    # 构建 API 请求 URL
    base_url = 'https://www.alphavantage.co/query'
    function = 'TIME_SERIES_DAILY'
    symbol = 'TSLA'  # 特斯拉的股票代码
    start_date = '2023-01-01'  # 开始日期
    end_date = '2024-01-01'  # 结束日期
    url = f'{base_url}?function={function}&symbol={symbol}&apikey={api_key}&outputsize=full&datatype=csv'

    # 发送 GET 请求
    response = requests.get(url)

    # 处理响应数据
    if response.status_code == 200:
        data = response.json()  # 将 JSON 响应转换为 Python 字典或列表
        # 在这里可以进一步处理数据，比如解析并显示每日收盘价等信息
        print("TSLA 数据:")
        print(data)
    else:
        print(f'Error fetching TSLA data: {response.status_code} - {response.text}')


# def get_yahoo_data():
#     import yfinance as yf
#
#     msft = yf.Ticker("MSFT")
#
#     # get all stock info
#     msft.info
#
#     # get historical market data
#     hist = msft.history(period="1mo")
#
#     # show meta information about the history (requires history() to be called first)
#     msft.history_metadata
#
#     # show actions (dividends, splits, capital gains)
#     msft.actions
#     msft.dividends
#     msft.splits
#     msft.capital_gains  # only for mutual funds & etfs
#
#     # show share count
#     msft.get_shares_full(start="2022-01-01", end=None)
#
#     # show financials:
#     # - income statement
#     msft.income_stmt
#     msft.quarterly_income_stmt
#     # - balance sheet
#     msft.balance_sheet
#     msft.quarterly_balance_sheet
#     # - cash flow statement
#     msft.cashflow
#     msft.quarterly_cashflow
#     # see `Ticker.get_income_stmt()` for more options
#
#     # show holders
#     msft.major_holders
#     msft.institutional_holders
#     msft.mutualfund_holders
#     msft.insider_transactions
#     msft.insider_purchases
#     msft.insider_roster_holders
#
#     # show recommendations
#     msft.recommendations
#     msft.recommendations_summary
#     msft.upgrades_downgrades
#
#     # Show future and historic earnings dates, returns at most next 4 quarters and last 8 quarters by default.
#     # Note: If more are needed use msft.get_earnings_dates(limit=XX) with increased limit argument.
#     msft.earnings_dates
#
#     # show ISIN code - *experimental*
#     # ISIN = International Securities Identification Number
#     msft.isin
#
#     # show options expirations
#     msft.options
#
#     # show news
#     msft.news
#
#     # get option chain for specific expiration
#     opt = msft.option_chain('YYYY-MM-DD')
#     # data available via: opt.calls, opt.puts


if __name__ =="__main__":

    # login()
    # get_dollar_index_yahoo()
    # get_yahoo()
    # get_yahoo_data()
    # get_kline_demo()
    use_vantage()


    pass
