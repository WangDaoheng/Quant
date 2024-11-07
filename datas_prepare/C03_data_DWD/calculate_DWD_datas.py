# -*- coding: utf-8 -*-

import os
from datetime import datetime

import pandas as pd
from sqlalchemy import create_engine, text
import time
import platform

# import dataprepare_properties
# import dataprepare_utils
from CommonProperties import Base_Properties
import CommonProperties.Base_utils as base_utils
from CommonProperties.DateUtility import DateUtility
from CommonProperties.Base_utils import timing_decorator
import CommonProperties.Mysql_Utils as mysql_utils


# ************************************************************************
# 本代码的作用是下午收盘后针对 insight 行情源数据的本地保存部分开展merge
# 需要下载的数据:
# 1.上市股票代码
# 2.筹码分布数据   get_chouma_datas()


# ************************************************************************

######################  mysql 配置信息  本地和远端服务器  ####################
local_user = Base_Properties.local_mysql_user
local_password = Base_Properties.local_mysql_password
local_database = Base_Properties.local_mysql_database
local_host = Base_Properties.local_mysql_host

origin_user = Base_Properties.origin_mysql_user
origin_password = Base_Properties.origin_mysql_password
origin_database = Base_Properties.origin_mysql_database
origin_host = Base_Properties.origin_mysql_host


class CalDWD:

    def __init__(self):
        pass

    @timing_decorator
    def cal_ashare_plate(self):

        #  1.获取日期
        # ymd = DateUtility.today()
        ymd = "20241001"

        # 2.定义 SQL 模板
        sql_statements_template = [
            """
            DELETE FROM quant.ods_stock_a_total_plate WHERE ymd='{ymd}';
            """,
            """
            INSERT INTO quant.ods_stock_a_total_plate
            SELECT 
                ymd, 
                concept_name AS plate_name,
                stock_code,
                stock_name,
                'ods_tdx_stock_concept_plate' AS source_table,
                '' AS remark
            FROM quant.ods_tdx_stock_concept_plate
            WHERE ymd='{ymd}'
            UNION ALL
            SELECT 
                ymd,
                style_name AS plate_name,
                stock_code,
                stock_name,
                'ods_tdx_stock_style_plate' AS source_table,
                '' AS remark
            FROM quant.ods_tdx_stock_style_plate
            WHERE ymd='{ymd}'
            UNION ALL
            SELECT 
                ymd,
                industry_name AS plate_name,
                stock_code,
                stock_name,
                'ods_tdx_stock_industry_plate' AS source_table,
                '' AS remark
            FROM quant.ods_tdx_stock_industry_plate
            WHERE ymd='{ymd}'
            UNION ALL
            SELECT 
                ymd,
                region_name AS plate_name,
                stock_code,
                stock_name,
                'ods_tdx_stock_region_plate' AS source_table,
                '' AS remark
            FROM quant.ods_tdx_stock_region_plate
            WHERE ymd='{ymd}'
            UNION ALL
            SELECT 
                ymd,
                index_name AS plate_name,
                stock_code,
                stock_name,
                'ods_tdx_stock_index_plate' AS source_table,
                '' AS remark
            FROM quant.ods_tdx_stock_index_plate
            WHERE ymd='{ymd}'
            UNION ALL
            SELECT 
                ymd,
                plate_name,
                stock_code,
                stock_name,
                'ods_stock_plate_redbook' AS source_table,
                remark
            FROM quant.ods_stock_plate_redbook
            WHERE ymd='{ymd}';
            """
        ]

        # 3.主程序替换 {ymd} 占位符
        sql_statements = [stmt.format(ymd=ymd) for stmt in sql_statements_template]

        # 4.执行 SQL
        if platform.system() == "Windows":

            mysql_utils.execute_sql_statements(
                user=local_user,
                password=local_password,
                host=local_host,
                database=local_database,
                sql_statements=sql_statements)

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
        #
        self.cal_ashare_plate()



if __name__ == '__main__':
    save_insight_data = CalDWD()
    save_insight_data.setup()















