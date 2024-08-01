import pandas as pd
from yahoo_fin.stock_info import *
from io import StringIO
import os

from CommonProperties.DateUtility import DateUtility
import CommonProperties.Base_Properties as base_properties
import CommonProperties.Base_utils as base_utils


#  vantage  测试环境文件保存目录
vantage_test_dir = os.path.join(base_properties.dir_vantage_base, 'test')


api_key = 'ICTN 9 P9 ES 00 EADUF'
key_US_stock = ['TSLA', 'AAPL', 'NVDA', 'MSFT', 'META']

# 构建 API 请求 URL
base_url = 'https://www.alphavantage.co/query'


def use_vantage_for_US_stock(function='TIME_SERIES_DAILY', symbol_list=key_US_stock):
    """
    Args:
        function:      TIME_SERIES_DAILY
        symbol_list:

    Returns:
        [name, timestamp  open  high  low   close   volume]
        YYYY-MM-DD
    """

    # 开始日期   取24个月之前     结束日期为今天
    start_date = DateUtility.first_day_of_month_after_n_months(-36)
    end_date = DateUtility.today()

    res_df = pd.DataFrame()


    for symbol in symbol_list:
        url = f'{base_url}?function={function}&symbol={symbol}&apikey={api_key}&outputsize=full&datatype=csv'

        # 发送 GET 请求
        response = requests.get(url)

        # 处理响应数据
        if response.status_code == 200:
            # 返回csv字符串
            csv_string = response.text
            csv_file = StringIO(csv_string)
            vantage_df = pd.read_csv(csv_file)
            vantage_df.insert(0, 'name', symbol)

            res_df = pd.concat([res_df, vantage_df], ignore_index=True)
        else:
            print(f'Error fetching {symbol} data: {response.status_code} - {response.text}')


    #  文件输出模块
    US_stock_filename = base_utils.save_out_filename(filehead='US_stock', file_type='csv')
    index_filedir = os.path.join(vantage_test_dir, US_stock_filename)
    res_df.to_csv(index_filedir, index=False)

    print("------------- get_kline_future_demo 完成测试文件输出 ---------------------")




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
        data = response.text  # 将 JSON 响应转换为 Python 字典或列表
        # 在这里可以进一步处理数据，比如解析并显示每日收盘价等信息
        print("TSLA 数据:")
        print(data)
    else:
        print(f'Error fetching TSLA data: {response.status_code} - {response.text}')




if __name__ == "__main__":
    use_vantage_for_US_stock()
