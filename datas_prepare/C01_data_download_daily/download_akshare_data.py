
# -*- coding: utf-8 -*-

import os
import sys
import io
import numpy as np
from insight_python.com.insight import common
from insight_python.com.insight.query import *
from insight_python.com.insight.market_service import market_service
from datetime import datetime
import contextlib

import time
import logging
import platform
import akshare as ak
import pandas as pd


import CommonProperties.Base_Properties as base_properties
import CommonProperties.Base_utils as base_utils
import CommonProperties.Mysql_Utils as mysql_utils
from CommonProperties.DateUtility import DateUtility
from CommonProperties.Base_utils import timing_decorator
from CommonProperties import set_config

# ************************************************************************
# 本代码的作用是下午收盘后下载 insight 行情源数据, 本地保存,用于后续分析
# 需要下载的数据:
# 1.上市股票代码   get_all_stocks()
# 2.筹码分布数据   get_chouma_datas()


# ************************************************************************


# ************************************************************************
#  调用日志配置
set_config.setup_logging_config()
#  调用mysql日志配置
local_user = base_properties.local_mysql_user
local_password = base_properties.local_mysql_password
local_database = base_properties.local_mysql_database
local_host = base_properties.local_mysql_host

origin_user = base_properties.origin_mysql_user
origin_password = base_properties.origin_mysql_password
origin_database = base_properties.origin_mysql_database
origin_host = base_properties.origin_mysql_host
# ************************************************************************


class SaveInsightData:

    def __init__(self):
        pass

    def get_stock_kline_data(self):
        """
        获取k线数据: ymd,open,high,low,close,volumn,TurnoverRate 等
        Returns:

        """


        # 1.确定起止日期
        time_start_date = DateUtility.next_day(-7)
        time_end_date = DateUtility.next_day(0)

        # 2.获取起止日期范围内的日K线数据
        df = mysql_utils.data_from_mysql_to_dataframe(user=origin_user, password=origin_password, host=origin_host,
                                                      database=origin_database,
                                                      table_name='ods_stock_kline_daily_insight',
                                                      start_date=time_start_date, end_date=time_end_date)


        # 获取K线数据
        df = ak.stock_zh_a_hist(symbol=stock_code, period="daily", start_date=start_date, end_date=end_date)

        return df








# 获取股票K线数据





if __name__ == '__main__':
    # 示例：获取股票603065的K线数据
    stock_code = '603065'
    start_date = '20250301'
    end_date = '20250306'

    df_kline = get_stock_kline_data(stock_code, start_date, end_date)
    df_kline.to_csv('./df_kline.csv')
    # 打印结果
    print(df_kline.head())











