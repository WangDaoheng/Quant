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
        ymd = "20241004"

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

    # @timing_decorator
    def cal_ZT_DT(self):

        # 1.确定起止日期
        time_start_date = DateUtility.next_day(-15)
        time_end_date = DateUtility.next_day(0)

        # 2.获取起止日期范围内的日K线数据
        df = mysql_utils.data_from_mysql_to_dataframe(user=origin_user, password=origin_password, host=origin_host,
                                                      database=origin_database,
                                                      table_name='ods_stock_kline_daily_insight',
                                                      start_date=time_start_date, end_date=time_end_date)
        df['ymd'] = pd.to_datetime(df['ymd'], format='%Y-%m-%d')

        # 按照 ymd 排序，确保数据是按日期排列的
        latest_15_days = df.sort_values(by=['htsc_code', 'ymd'])

        # 按股票代码分组，然后对每个分组进行 shift(1) 操作, 计算昨日close
        latest_15_days['last_close'] = latest_15_days.groupby('htsc_code')['close'].shift(1)

        # 过滤掉没有昨日数据的行
        latest_15_days = latest_15_days.dropna(subset=['last_close'])

        # 计算昨日的涨停价和跌停价
        latest_15_days['昨日ZT价'] = (latest_15_days['last_close'] * 1.10).round(4)
        latest_15_days['昨日DT价'] = (latest_15_days['last_close'] * 0.90).round(4)

        def ZT_DT_orz(price, target_price):
            # 如果 price 和 target_price 之间的差距小于等于0.01，才进一步计算
            if abs(target_price - price) <= 0.01:
                # 计算 price 周围 0.01 范围内的最接近的2个价格
                left_price = price - 0.01
                right_price = price + 0.01

                # 算价差
                left_delta = abs(left_price - target_price)
                mid_delta = abs(price - target_price)
                right_delta = abs(right_price - target_price)
                min_delta = min(left_delta, mid_delta, right_delta)

                # 判断为ZT or DT
                if mid_delta == min_delta:
                    return True

            # 不可能 ZT or DT
            return False

        # 3. 判断每日的涨停或跌停
        latest_15_days['是否涨停'] = latest_15_days.apply(
            lambda row: ZT_DT_orz(row['close'], row['昨日ZT价']), axis=1)
        latest_15_days['是否跌停'] = latest_15_days.apply(
            lambda row: ZT_DT_orz(row['close'], row['昨日DT价']), axis=1)

        # 4. 筛选出涨停和跌停的记录，分别存入两个 DataFrame
        zt_records = latest_15_days[latest_15_days['是否涨停'] == True].copy()
        zt_records['rate'] = ((zt_records['close'] - zt_records['last_close']) / zt_records['last_close'] * 100).round(2)
        zt_df = zt_records[['ymd', 'htsc_code', 'last_close', 'open', 'high', 'low', 'close', 'rate']]

        dt_records = latest_15_days[latest_15_days['是否跌停'] == True].copy()
        dt_records['rate'] = ((dt_records['close'] - dt_records['last_close']) / dt_records['last_close'] * 100).round(2)
        dt_df = dt_records[['ymd', 'htsc_code', 'last_close', 'open', 'high', 'low', 'close', 'rate']]

        ############################   文件输出模块     ############################
        if platform.system() == "Windows":
            #  涨停数据保存到 本地 mysql中
            mysql_utils.data_from_dataframe_to_mysql(user=local_user,
                                                     password=local_password,
                                                     host=local_host,
                                                     database=local_database,
                                                     df=zt_df,
                                                     table_name="dwd_stock_ZT_list",
                                                     merge_on=['ymd', 'htsc_code'])

            #  涨停数据保存到 远端 mysql中
            mysql_utils.data_from_dataframe_to_mysql(user=origin_user,
                                                     password=origin_password,
                                                     host=origin_host,
                                                     database=origin_database,
                                                     df=zt_df,
                                                     table_name="dwd_stock_ZT_list",
                                                     merge_on=['ymd', 'htsc_code'])

            #  跌停数据保存到 本地 mysql中
            mysql_utils.data_from_dataframe_to_mysql(user=local_user,
                                                     password=local_password,
                                                     host=local_host,
                                                     database=local_database,
                                                     df=dt_df,
                                                     table_name="dwd_stock_DT_list",
                                                     merge_on=['ymd', 'htsc_code'])

            #  跌停数据保存到 远端 mysql中
            mysql_utils.data_from_dataframe_to_mysql(user=origin_user,
                                                     password=origin_password,
                                                     host=origin_host,
                                                     database=origin_database,
                                                     df=dt_df,
                                                     table_name="dwd_stock_DT_list",
                                                     merge_on=['ymd', 'htsc_code'])
        else:
            #  涨停数据保存到 远端 mysql中
            mysql_utils.data_from_dataframe_to_mysql(user=origin_user,
                                                     password=origin_password,
                                                     host=origin_host,
                                                     database=origin_database,
                                                     df=zt_df,
                                                     table_name="dwd_stock_ZT_list",
                                                     merge_on=['ymd', 'htsc_code'])

            #  跌停数据保存到 远端 mysql中
            mysql_utils.data_from_dataframe_to_mysql(user=origin_user,
                                                     password=origin_password,
                                                     host=origin_host,
                                                     database=origin_database,
                                                     df=dt_df,
                                                     table_name="dwd_stock_DT_list",
                                                     merge_on=['ymd', 'htsc_code'])




    def setup(self):
        #
        # self.cal_ashare_plate()

        self.cal_ZT_DT()


if __name__ == '__main__':
    save_insight_data = CalDWD()
    save_insight_data.setup()
