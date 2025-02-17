# -*- coding: utf-8 -*-

import os
from datetime import datetime

import pandas as pd
from sqlalchemy import create_engine, text
import time
import platform
import logging


# import dataprepare_properties
# import dataprepare_utils
from CommonProperties import Base_Properties
import CommonProperties.Base_utils as base_utils
from CommonProperties.DateUtility import DateUtility
from CommonProperties.Base_utils import timing_decorator

import CommonProperties.Mysql_Utils as mysql_utils

from CommonProperties import set_config

# ************************************************************************
# 本代码的作用是下午收盘后针对 insight 行情源数据的本地保存部分开展merge
# 需要下载的数据:
# 1.上市股票代码
# 2.筹码分布数据   get_chouma_datas()


# ************************************************************************
#  调用日志配置
set_config.setup_logging_config()

######################  mysql 配置信息  本地和远端服务器  ####################
local_user = Base_Properties.local_mysql_user
local_password = Base_Properties.local_mysql_password
local_database = Base_Properties.local_mysql_database
local_host = Base_Properties.local_mysql_host

origin_user = Base_Properties.origin_mysql_user
origin_password = Base_Properties.origin_mysql_password
origin_database = Base_Properties.origin_mysql_database
origin_host = Base_Properties.origin_mysql_host


class CalDMART:

    def __init__(self):

        pass

    @timing_decorator
    def cal_zt_details(self):
        """
        涨停股票的明细
        Returns:
        """

        #  1.获取日期
        ymd = DateUtility.today()
        time_start_date = DateUtility.next_day(-7)
        # time_start_date = '2024'
        time_end_date = DateUtility.next_day(0)
        # ymd = "20241004"

        # 2.定义 SQL 模板
        sql_statements_template = [
            """
            DELETE FROM quant.mart_stock_zt_details WHERE ymd='{ymd}';
            """,
            """
            INSERT IGNORE INTO quant.mart_stock_zt_details
            SELECT 
                 t1.ymd
                ,t1.stock_code
                ,t1.stock_name
                ,t2.concept_plate                 --概念板块
                ,t2.index_plate                   --指数板块
                ,t2.industry_plate                --行业板块
                ,t2.style_plate                   --风格板块
                ,t2.out_plate                     --外部数据板块
            FROM quant.dwd_stock_zt_list                   t1
            left join  quant.dwd_ashare_stock_base_info    t2
                   on t1.stock_code = t2.stock_code
            where t1.ymd >= '{time_start_date}' and  t1.ymd <= '{time_end_date}';
            """
        ]

        # 3.主程序替换 {ymd} 占位符
        sql_statements = [stmt.format(ymd=ymd, time_start_date=time_start_date, time_end_date=time_end_date) for stmt in sql_statements_template]

        # 4.执行 SQL
        if platform.system() == "Windows":

            # mysql_utils.execute_sql_statements(
            #     user=local_user,
            #     password=local_password,
            #     host=local_host,
            #     database=local_database,
            #     sql_statements=sql_statements)

            mysql_utils.execute_sql_statements(
                user=origin_user,
                password=origin_password,
                host=origin_host,
                database=origin_database,
                sql_statements=sql_statements)
        else:
            mysql_utils.execute_sql_statements(
                user=origin_user,
                password=origin_password,
                host=origin_host,
                database=origin_database,
                sql_statements=sql_statements)




    def setup(self):

        # 涨停股票的明细
        self.cal_zt_details()






if __name__ == '__main__':
    cal_dmart_data = CalDMART()
    cal_dmart_data.setup()