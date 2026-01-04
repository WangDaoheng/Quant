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


    # @timing_decorator
    def cal_zt_details(self):
        """
        涨停股票的明细
        Returns:
        """
        #  1.获取日期
        # ymd = DateUtility.today()
        time_start_date = DateUtility.next_day(-2)
        time_start_date = '20250215'

        time_end_date = DateUtility.next_day(0)
        time_end_date = '20250319'

        # 2.定义 SQL 模板
        sql_statements_template = [
            """
            DELETE FROM quant.dmart_stock_zt_details WHERE ymd>= '{time_start_date}' and ymd <= '{time_end_date}';
            """,
            """
            INSERT IGNORE INTO quant.dmart_stock_zt_details
            SELECT 
                tzt.ymd
               ,tzt.stock_code
               ,tzt.stock_name
               ,tbase.concept_plate
               ,tbase.index_plate
               ,tbase.industry_plate
               ,tbase.style_plate
               ,tbase.out_plate
            FROM 
                quant.dwd_stock_zt_list                     tzt
            LEFT JOIN 
                (
                    SELECT 
                        t2.ymd
                       ,t2.stock_code
                       ,t2.concept_plate
                       ,t2.index_plate
                       ,t2.industry_plate
                       ,t2.style_plate
                       ,t2.out_plate
                    FROM 
                        quant.dwd_ashare_stock_base_info    t2
                    INNER JOIN 
                        (
                            SELECT 
                                MAX(ymd) AS latest_ymd
                            FROM 
                                quant.dwd_ashare_stock_base_info
                        ) latest 
                    ON t2.ymd = latest.latest_ymd
                ) tbase
            ON     tzt.stock_code = tbase.stock_code
            WHERE  tzt.ymd >= '{time_start_date}'  AND  tzt.ymd <= '{time_end_date}'  ;
            """
        ]

        # 3.主程序替换 {ymd} 占位符
        sql_statements = [stmt.format(time_start_date=time_start_date, time_end_date=time_end_date) for stmt in sql_statements_template]

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

    def cal_zt_details_explode(self):
        """
        涨停股票的明细的拆分
        Returns:
        """
        # 1. 获取日期范围
        time_start_date = DateUtility.next_day(-2)  # 2天前的日期
        time_start_date = '20241126'

        time_end_date = DateUtility.next_day(0)  # 当前日期
        time_end_date = '20250318'

        logging.info(f"开始处理涨停股票明细数据，日期范围：{time_start_date} 至 {time_end_date}")

        # 2. 从 MySQL 获取起止日期范围内的数据
        df = mysql_utils.data_from_mysql_to_dataframe(
            user=origin_user,
            password=origin_password,
            host=origin_host,
            database=origin_database,
            table_name='dmart_stock_zt_details',
            start_date=time_start_date,
            end_date=time_end_date,
            cols=['ymd', 'stock_code', 'stock_name', 'concept_plate', 'index_plate', 'industry_plate', 'style_plate',
                  'out_plate']
        )

        if df.empty:
            logging.warning("未获取到数据，可能日期范围内没有数据或表为空。")
            return

        logging.info(f"成功获取到 {len(df)} 条数据，开始拆解处理...")

        # 3. 定义 unpack_plates 函数
        def unpack_plates(df):
            result = []
            for _, row in df.iterrows():
                ymd = row['ymd']
                stock_code = row['stock_code']
                stock_name = row['stock_name']

                # 获取每个字段的分隔值
                fields = {
                    'concept_plate': row['concept_plate'].split(',') if pd.notna(row['concept_plate']) else [],
                    'index_plate': row['index_plate'].split(',') if pd.notna(row['index_plate']) else [],
                    'industry_plate': row['industry_plate'].split(',') if pd.notna(row['industry_plate']) else [],
                    'style_plate': row['style_plate'].split(',') if pd.notna(row['style_plate']) else [],
                    'out_plate': row['out_plate'].split(',') if pd.notna(row['out_plate']) else []
                }

                # 找到分隔值最多的字段
                max_length = max(len(fields[field]) for field in fields)

                # 按最大长度填充数据
                for i in range(max_length):
                    result_row = {
                        'ymd': ymd.strftime('%Y%m%d'),
                        'stock_code': stock_code,
                        'stock_name': stock_name,
                        'concept_plate': fields['concept_plate'][i].strip() if i < len(
                            fields['concept_plate']) else None,
                        'index_plate': fields['index_plate'][i].strip() if i < len(fields['index_plate']) else None,
                        'industry_plate': fields['industry_plate'][i].strip() if i < len(
                            fields['industry_plate']) else None,
                        'style_plate': fields['style_plate'][i].strip() if i < len(fields['style_plate']) else None,
                        'out_plate': fields['out_plate'][i].strip() if i < len(fields['out_plate']) else None
                    }
                    result.append(result_row)

            return pd.DataFrame(result)

        # 4. 调用 unpack_plates 函数处理数据
        output_df = unpack_plates(df)

        # 5. 将处理后的数据保存到 MySQL
        if platform.system() == "Windows":
            mysql_utils.data_from_dataframe_to_mysql(
                user=local_user,
                password=local_password,
                host=local_host,
                database=local_database,
                df=output_df,
                table_name="dmart_stock_zt_details_expanded",
                merge_on=['ymd', 'stock_code', 'concept_plate', 'index_plate', 'industry_plate', 'style_plate',
                          'out_plate']
            )
            logging.info(
                f"数据处理完成，已将结果保存到 {local_host} 的 {local_database}.dmart_stock_zt_details_expanded 表中。")

            mysql_utils.data_from_dataframe_to_mysql(
                user=origin_user,
                password=origin_password,
                host=origin_host,
                database=origin_database,
                df=output_df,
                table_name="dmart_stock_zt_details_expanded",
                merge_on=['ymd', 'stock_code', 'concept_plate', 'index_plate', 'industry_plate', 'style_plate',
                          'out_plate']
            )
            logging.info(
                f"数据处理完成，已将结果保存到 {origin_host} 的 {origin_database}.dmart_stock_zt_details_expanded 表中。")


        else:
            mysql_utils.data_from_dataframe_to_mysql(
                user=origin_user,
                password=origin_password,
                host=origin_host,
                database=origin_database,
                df=output_df,
                table_name="dmart_stock_zt_details_expanded",
                merge_on=['ymd', 'stock_code', 'concept_plate', 'index_plate', 'industry_plate', 'style_plate',
                          'out_plate']
            )
            logging.info(
                f"数据处理完成，已将结果保存到 {origin_host} 的 {origin_database}.dmart_stock_zt_details_expanded 表中。")



    def setup(self):

        # 涨停股票的明细
        self.cal_zt_details()

        self.cal_zt_details_explode()


if __name__ == '__main__':
    cal_dmart_data = CalDMART()
    cal_dmart_data.setup()



