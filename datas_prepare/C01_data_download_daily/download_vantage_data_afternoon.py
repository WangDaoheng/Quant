# -*- coding: utf-8 -*-

import pandas as pd
import requests
import platform
# from yahoo_fin.stock_info import *
from io import StringIO
import os
import logging


from CommonProperties.DateUtility import DateUtility
import CommonProperties.Base_Properties as base_properties
import CommonProperties.Base_utils as base_utils
import CommonProperties.Mysql_Utils as mysql_utils
from CommonProperties.Base_utils import timing_decorator
from CommonProperties.set_config import setup_logging_config


# 配置日志处理器
# 调用日志配置
setup_logging_config()

#  vantage  测试环境文件保存目录
vantage_test_dir = os.path.join(base_properties.dir_vantage_base, 'test')


api_key = 'ICTN 9 P9 ES 00 EADUF'
# api_key = 'BI8JFEOOP3C563PO'
key_US_stock = ['TSLA', 'AAPL', 'NVDA', 'MSFT', 'META']

# 构建 API 请求 URL
base_url = 'https://www.alphavantage.co/query'
headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3'
}



######################  mysql 配置信息  本地和远端服务器  ####################
local_user = base_properties.local_mysql_user
local_password = base_properties.local_mysql_password
local_database = base_properties.local_mysql_database
local_host = base_properties.local_mysql_host

origin_user = base_properties.origin_mysql_user
origin_password = base_properties.origin_mysql_password
origin_database = base_properties.origin_mysql_database
origin_host = base_properties.origin_mysql_host



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


    @timing_decorator
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
            response = requests.get(url, headers=headers, timeout=10)

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

        #  8.日期格式转换
        res_df['timestamp'] = pd.to_datetime(res_df['timestamp']).dt.strftime('%Y%m%d')
        res_df.rename(columns={'timestamp': 'ymd', 'name':'stock_name'}, inplace=True)

        ############################   文件输出模块     ############################
        # Windows下先保存到本地数据库
        # if platform.system() == "Windows":
        #     mysql_utils.data_from_dataframe_to_mysql(
        #         user=local_user,
        #         password=local_password,
        #         host=local_host,
        #         database=local_database,
        #         df=res_df,
        #         table_name="ods_us_stock_daily_vantage",
        #         merge_on=['ymd', 'stock_name']
        #     )

        # 总是保存到远端数据库
        mysql_utils.data_from_dataframe_to_mysql(
            user=origin_user,
            password=origin_password,
            host=origin_host,
            database=origin_database,
            df=res_df,
            table_name="ods_us_stock_daily_vantage",
            merge_on=['ymd', 'stock_name']
        )


    def get_USD_FX_core(self, url, flag):
        """
        Args:
            url: 请求的URL地址
            flag: 数据标识符
        Returns:
            返回包含汇率数据的DataFrame
        """
        # 存放汇率结果数据
        res_df = pd.DataFrame()

        # 使用带重试功能的请求
        response = base_utils.get_with_retries(url, headers=headers, timeout=10)

        # 处理响应数据
        if response is not None and response.status_code == 200:
            # 返回csv字符串
            csv_string = response.text
            csv_file = StringIO(csv_string)
            vantage_df = pd.read_csv(csv_file)
            vantage_df.insert(0, 'name', flag)

            res_df = pd.concat([res_df, vantage_df], ignore_index=True)
        else:
            logging.error(f'Error fetching {flag} data: 请求失败或无效的响应')

        logging.info(f"get_USD_FX_core 完成 {flag} 汇率查询")

        return res_df


    @timing_decorator
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

        #  --------------------------  开始计算美元指数  ------------------------------
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

        #  日期格式转换
        res_df['timestamp'] = pd.to_datetime(res_df['timestamp']).dt.strftime('%Y%m%d')
        res_df.rename(columns={'timestamp': 'ymd'}, inplace=True)

        # Windows下先保存到本地数据库
        # if platform.system() == "Windows":
        #     mysql_utils.data_from_dataframe_to_mysql(
        #         user=local_user,
        #         password=local_password,
        #         host=local_host,
        #         database=local_database,
        #         df=res_df,
        #         table_name="ods_exchange_rate_vantage_detail",
        #         merge_on=["ymd", "name"]
        #     )

        # 总是保存到远端数据库
        mysql_utils.data_from_dataframe_to_mysql(
            user=origin_user,
            password=origin_password,
            host=origin_host,
            database=origin_database,
            df=res_df,
            table_name="ods_exchange_rate_vantage_detail",
            merge_on=["ymd", "name"]
        )

        #  --------------------------  开始计算美元指数  ------------------------------
        # 获取唯一的时间戳
        timestamps = res_df['ymd'].unique()

        # 创建一个空列表来存储结果
        results = []

        for timestamp in timestamps:
            # 获取当前时间戳的所有汇率数据
            current_data = res_df[res_df['ymd'] == timestamp]
            if current_data.shape[0] != 6:
                break

            # 初始化DXY值
            dxy = constant
            # 计算DXY
            for name, weight in weights.items():
                rate = current_data[current_data['name'] == name]['close'].values[0]
                dxy *= rate ** weight
            # 将结果添加到列表中
            results.append([timestamp, dxy])

        # 将结果转换为DataFrame
        dxy_df = pd.DataFrame(results, columns=['ymd', 'DXY'])
        # Windows下先保存到本地数据库
        # if platform.system() == "Windows":
        #     mysql_utils.data_from_dataframe_to_mysql(
        #         user=local_user,
        #         password=local_password,
        #         host=local_host,
        #         database=local_database,
        #         df=dxy_df,
        #         table_name="ods_exchange_dxy_vantage",
        #         merge_on=["ymd", "name"]
        #     )

        # 总是保存到远端数据库
        mysql_utils.data_from_dataframe_to_mysql(
            user=origin_user,
            password=origin_password,
            host=origin_host,
            database=origin_database,
            df=dxy_df,
            table_name="ods_exchange_dxy_vantage",
            merge_on=["ymd", "name"]
        )


    @timing_decorator
    def setup(self):

        #  获取 US 主要stock 的全部数据
        # self.get_US_stock_from_vantage()
        self.get_USD_FX_from_vantage()


if __name__ == '__main__':
    save_vantage_data = SaveVantageData()
    save_vantage_data.setup()


