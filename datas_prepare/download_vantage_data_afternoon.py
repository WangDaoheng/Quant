
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

        #  文件路径_____USD 的 汇率明细
        self.dir_USD_FX_detail_base = os.path.join(self.dir_vantage_base, 'USD_FX_detail')

        #  文件路径_____USD 的 美元指数
        self.dir_USD_FX_base = os.path.join(self.dir_vantage_base, 'USD_FX')



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
        关键 US stcok
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
        res_df.to_csv(US_stock_filedir, index=False)

        print("------------- get_US_stock_from_vantage 完成测试文件输出 ---------------------")


    def get_USD_FX_core(self, url, flag):
        """
        Args:
            url:
            flag:
        Returns:
            该函数是 get_USD_FX_from_vantage  的核心调用模块
        """

        #  存放汇率结果数据
        res_df = pd.DataFrame()

        # 发送 GET 请求
        response = requests.get(url)

        # 处理响应数据
        if response.status_code == 200:
            # 返回csv字符串
            csv_string = response.text
            csv_file = StringIO(csv_string)
            vantage_df = pd.read_csv(csv_file)
            vantage_df.insert(0, 'name', flag)

            res_df = pd.concat([res_df, vantage_df], ignore_index=True)
        else:
            print(f'Error fetching {flag} data: {response.status_code} - {response.text}')

        print(f"------------- get_USD_FX_core  完成 {flag} 汇率查询 ---------------------")

        return res_df




    def get_USD_FX_from_vantage(self):
        """
        计算美元指数, 从主流货币去计算美元指数
        Returns:
            [name, timestamp  open  high  low   close   volume]
        """
        function = 'FX_DAILY'

        #  存放汇率数据
        res_df = pd.DataFrame()

        # 定义权重
        weights = {
            'EUR_USD': -0.576,
            'USD_JPY': 0.136,
            'GBP_USD': -0.119,
            'USD_CAD': 0.091,
            'USD_SEK': 0.042,
            'USD_CHF': 0.036
        }

        # 定义初始常数
        constant = 50.14348112


        #  欧元兑美元
        url_EUR_USD = f'{base_url}?function={function}&from_symbol=EUR&to_symbol=USD&apikey={api_key}&datatype=csv'
        df_EUR_USD = self.get_USD_FX_core(url=url_EUR_USD, flag='EUR_USD')

        #  美元兑日元
        url_USD_JPY = f'{base_url}?function={function}&from_symbol=USD&to_symbol=JPY&apikey={api_key}&datatype=csv'
        df_USD_JPY = self.get_USD_FX_core(url=url_USD_JPY, flag='USD_JPY')

        #  英镑兑美元
        url_GBP_USD = f'{base_url}?function={function}&from_symbol=GBP&to_symbol=USD&apikey={api_key}&datatype=csv'
        df_GBP_USD = self.get_USD_FX_core(url=url_GBP_USD, flag='GBP_USD')

        #  美元兑加拿大元
        url_USD_CAD = f'{base_url}?function={function}&from_symbol=USD&to_symbol=CAD&apikey={api_key}&datatype=csv'
        df_USD_CAD = self.get_USD_FX_core(url=url_USD_CAD, flag='USD_CAD')

        #  美元兑瑞典克朗
        url_USD_SEK = f'{base_url}?function={function}&from_symbol=USD&to_symbol=SEK&apikey={api_key}&datatype=csv'
        df_USD_SEK = self.get_USD_FX_core(url=url_USD_SEK, flag='USD_SEK')

        #  美元兑瑞士法郎
        url_USD_CHF = f'{base_url}?function={function}&from_symbol=USD&to_symbol=CHF&apikey={api_key}&datatype=csv'
        df_USD_CHF = self.get_USD_FX_core(url=url_USD_CHF, flag='USD_CHF')

        #  汇总得到美元指数的主要成分
        res_df = pd.concat([res_df, df_EUR_USD, df_USD_JPY, df_GBP_USD, df_USD_CAD, df_USD_SEK, df_USD_CHF], ignore_index=True)

        #  --------------------------  开始计算美元指数  ------------------------------
        # 获取唯一的时间戳
        timestamps = res_df['timestamp'].unique()

        # 创建一个空列表来存储结果
        results = []

        for timestamp in timestamps:
            # 获取当前时间戳的所有汇率数据
            current_data = res_df[res_df['timestamp'] == timestamp]
            # 初始化DXY值
            dxy = constant
            # 计算DXY
            for name, weight in weights.items():
                rate = current_data[current_data['name'] == name]['close'].values[0]
                dxy *= rate ** weight
            # 将结果添加到列表中
            results.append([timestamp, dxy])

        # 将结果转换为DataFrame
        dxy_df = pd.DataFrame(results, columns=['timestamp', 'DXY'])

        # #  文件输出模块     输出汇率明细
        USD_FX_detail_filename = base_utils.save_out_filename(filehead='USD_FX', file_type='csv')
        USD_FX_detail_filedir = os.path.join(self.dir_USD_FX_detail_base, USD_FX_detail_filename)
        res_df.to_csv(USD_FX_detail_filedir, index=False)

        # #  文件输出模块     输出美元指数
        USD_FX_filename = base_utils.save_out_filename(filehead='USD_FX', file_type='csv')
        USD_FX_filedir = os.path.join(self.dir_USD_FX_base, USD_FX_filename)
        dxy_df.to_csv(USD_FX_filedir, index=False)


        print("------------- get_USD_FX_from_vantage 完成测试文件输出 ---------------------")






    def setup(self):

        #  获取 US 主要stock 的全部数据
        # self.get_US_stock_from_vantage()
        self.get_USD_FX_from_vantage()





if __name__ == '__main__':
    save_vantage_data = SaveVantageData()
    save_vantage_data.setup()















