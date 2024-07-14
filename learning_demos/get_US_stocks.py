

import numpy as np
import pandas as pd
import io
import requests
import time

# Alpha Vantage API 密钥
Vantage_key = 'ICTN 9 P9 ES 00 EADUF'



def get_US_stocks_by_Vantage_csv(symbol='TSLA', function = 'TIME_SERIES_DAILY'):
    # 构建 API 请求 URL
    base_url = 'https://www.alphavantage.co/query'


    outputsize = 'full'  # 获取完整历史数据
    url = f'{base_url}?function={function}&symbol={symbol}&apikey={Vantage_key}&outputsize={outputsize}&datatype=csv'


    # 发送 GET 请求
    response = requests.get(url)

    ## 把返回的csv字符串做IO处理，转换为csv文件
    csv_file = io.StringIO(response.text)

    # 将文件对象读取为 DataFrame
    df = pd.read_csv(csv_file)
    print('---------- 查询 {} 数据, 共计 {} 条'.format(symbol, df.shape[0]))
    print(df)
    return df





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

    print(df)
    return df



def get_US_Forex_by_Vantage_csv(symbol='', function = 'FX_DAILY'):
    base_url = 'https://www.alphavantage.co/query'
    function = 'FX_DAILY'
    from_symbol = 'EUR'  # 美元指数的代码
    to_symbol = 'CNY'
    outputsize = 'full'  # 获取完整历史数据
    url = f'{base_url}?function={function}&from_symbol={from_symbol}&to_symbol={to_symbol}&apikey={Vantage_key}&outputsize={outputsize}&datatype=json'


    # 发送 GET 请求
    response = requests.get(url)

    # 处理响应数据
    if response.status_code == 200:
        data = response.json()  # 将 JSON 响应转换为 Python 字典或列表
        print(data)

        # 提取每日数据并创建 DataFrame
        daily_data = data['Time Series FX (Daily)']
        df = pd.DataFrame(daily_data).T  # 转置使日期作为行索引

        # 保留所需的列：开盘价、最高价、最低价、收盘价
        df = df[['1. open', '2. high', '3. low', '4. close']]

        # 重命名列名
        df.columns = ['Open', 'High', 'Low', 'Close']

        # 将索引列（日期）转换为日期时间类型
        df.index = pd.to_datetime(df.index)

        print(df)
        return df
    else:
        print(f'Error fetching USD Index data: {response.status_code} - {response.text}')






if __name__ == "__main__":
    # get_US_stocks_by_Vantage_csv(symbol='DX-Y.NYB')
    # get_US_stocks_by_Vantage_json()
    get_US_Forex_by_Vantage_csv()






