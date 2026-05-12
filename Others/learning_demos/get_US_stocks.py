
import numpy as np
import pandas as pd
import io
import os
import requests
import datetime
from alpha_vantage.foreignexchange import ForeignExchange

from CommonProperties import Base_utils

# Alpha Vantage API 密钥
Vantage_key = 'ICTN 9 P9 ES 00 EADUF'


def get_US_stocks_by_Vantage_csv(symbol='TSLA', function='TIME_SERIES_DAILY', start_date='', end_date=''):
    # 构建 API 请求 URL
    base_url = 'https://www.alphavantage.co/query'

    outputsize = 'full'  # 获取完整历史数据
    url = f'{base_url}?function={function}&symbol={symbol}&apikey={Vantage_key}&outputsize={outputsize}&datatype=csv'

    # 发送 GET 请求
    response = requests.get(url)
    ## 把返回的csv字符串做IO处理，转换为csv文件
    csv_file = io.StringIO(response.text)

    return csv_file


def get_US_stocks_by_Vantage_json(symbol = 'TSLA', function = 'TIME_SERIES_DAILY'):
    # 构建 API 请求 URL
    base_url = 'https://www.alphavantage.co/query'

    url = f'{base_url}?function={function}&symbol={symbol}&apikey={Vantage_key}&outputsize=full&datatype=json'

    # 发送 GET 请求
    response = requests.get(url)

    # 处理响应数据
    if response.status_code == 200:
        data = response.json()  # 将 JSON 响应转换为 Python 字典或列表
        # 在这里可以进一步处理数据，比如解析并显示每日收盘价等信息

        # 提取每日数据并创建 DataFrame
        daily_data = data['Time Series (Daily)']
        df = pd.DataFrame(daily_data).T  # 转置使日期作为行索引

        # 保留所需的列：日期、开盘价、最高价、最低价、收盘价和成交量
        df = df[['1. open', '2. high', '3. low', '4. close', '5. volume']]

        # 重命名列名
        df.columns = ['Open', 'High', 'Low', 'Close', 'Volume']

        # 将索引列（日期）转换为日期时间类型
        df.index = pd.to_datetime(df.index)

    else:
        print(f'Error fetching  data: {response.status_code} - {response.text}')

    return df


def get_US_Forex_by_Vantage_csv(function='FX_DAILY', from_symbol='EUR', to_symbol='CNY', start_date='', end_date=''):
    """
    Args:
        function:
        from_symbol:
        to_symbol:
        start_date: YYYYmmdd
        end_date:   YYYYmmdd
    Returns:
    """

    ## 时间格式处理
    start_date = datetime.datetime.strptime(start_date, '%Y%m%d').strftime('%Y-%m-%d %H:%M:%S')
    end_date = datetime.datetime.strptime(end_date, '%Y%m%d').strftime('%Y-%m-%d %H:%M:%S')

    base_url = 'https://www.alphavantage.co/query'

    outputsize = 'full'  # 获取完整历史数据
    url = f'{base_url}?function={function}&from_symbol={from_symbol}&to_symbol={to_symbol}&apikey={Vantage_key}&outputsize={outputsize}&datatype=json'

    # 发送 GET 请求
    response = requests.get(url)

    # 处理响应数据
    if response.status_code == 200:
        data = response.json()  # 将 JSON 响应转换为 Python 字典或列表

        # 提取每日数据并创建 DataFrame
        daily_data = data['Time Series FX (Daily)']
        df = pd.DataFrame(daily_data).T  # 转置使日期作为行索引

        # 保留所需的列：开盘价、最高价、最低价、收盘价
        df = df[['1. open', '2. high', '3. low', '4. close']]

        # 重命名列名
        df.columns = ['Open', 'High', 'Low', 'Close']

        # 将索引列（日期）转换为日期时间类型
        df.index = pd.to_datetime(df.index)
        df_filtered = df.loc[start_date:end_date]

        return df_filtered
    else:
        print(f'Error fetching USD Index data: {response.status_code} - {response.text}')


def get_vantage_DXY(outputsize='compact'):
    """
    Args:
        outputsize: full      全量历史数据
                    compact   近100个数据点
    Returns:
    """

    api_key = Vantage_key
    cc = ForeignExchange(key=api_key)

    # 获取美元对一篮子货币的汇率数据（欧元、日元、英镑、加拿大元、瑞典克朗、瑞士法郎）
    currency_pairs = ['EUR', 'JPY', 'GBP', 'CAD', 'SEK', 'CHF']
    exchange_rates = {}

    # 获取每个货币对美元的汇率数据
    for currency in currency_pairs:
        data, _ = cc.get_currency_exchange_daily(from_symbol='USD', to_symbol=currency, outputsize='full')
        df = pd.DataFrame.from_dict(data, orient='index')
        df.index = pd.to_datetime(df.index)
        df = df.astype(float)
        exchange_rates[currency] = df['4. close']

    # 确定每个货币的权重
    weights = {
        'EUR': 0.576,
        'JPY': 0.136,
        'GBP': 0.119,
        'CAD': 0.091,
        'SEK': 0.042,
        'CHF': 0.036
    }

    # 将所有汇率数据合并到一个DataFrame中
    df_combined = pd.DataFrame(exchange_rates)

    # 确保所有货币的数据都对齐
    df_combined = df_combined.dropna()

    # 计算加权平均值，即美元指数
    df_combined['DXY'] = 5 * sum(df_combined[currency] * weights[currency] for currency in currency_pairs)

    # 获取近一年的数据
    start_date = datetime.datetime.now() - datetime.timedelta(days=365)
    end_date = datetime.datetime.now()
    df_filtered = df_combined.loc[start_date:end_date]
    return df_filtered


if __name__ == "__main__":
    # get_US_stocks_by_Vantage_csv(symbol='DX-Y.NYB')
    # get_US_stocks_by_Vantage_json()
    # df = get_US_Forex_by_Vantage_csv(start_date='20230710', end_date='20240701')
    # print(df)
    # vantage_test2()
    get_vantage_DXY()

