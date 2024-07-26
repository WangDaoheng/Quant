
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


class SaveVantageData:

    def __init__(self):

        self.init_dirs()

        self.init_variant()



    def init_dirs(self):
        """
        关键路径初始化
        """
        #  文件路径_____vantage 文件基础路径
        self.dir_vantage_base = base_properties.dir_vantage_base

        #  文件路径_____US 的 stock
        self.dir_US_stock_base = os.path.join(self.dir_vantage_base, 'US_stock')



    def init_variant(self):
        """
        结果变量初始化
        """

        #  关键的stock_code
        self.key_US_stock = ['TSLA', 'AAPL', 'NVDA', 'MSFT', 'META']

        #  获得US核心stock  [name, timestamp  open  high  low   close   volume]
        self.vantage_US_stock = pd.DataFrame()


    def get_US_stock_from_vantage(self):
        """
        关键 US
        Returns:
            [name, timestamp  open  high  low   close   volume]
        """

        function = 'TIME_SERIES_DAILY'
        res_df = pd.DataFrame()

        for symbol in self.key_US_stock:
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
        US_stock_filedir = os.path.join(self.dir_US_stock_base, US_stock_filename)
        res_df.to_csv(US_stock_filedir)

        print("------------- get_US_stock_from_vantage 完成测试文件输出 ---------------------")




    def setup(self):

        #  获取 US 主要stock 的全部数据
        self.get_US_stock_from_vantage()





if __name__ == '__main__':
    save_vantage_data = SaveVantageData()
    save_vantage_data.setup()















