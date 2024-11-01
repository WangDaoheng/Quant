# -*- coding: utf-8 -*-

import pandas as pd
from sqlalchemy import create_engine

from CommonProperties import Base_Properties
from CommonProperties.Base_utils import timing_decorator
import CommonProperties.Mysql_Utils as mysql_utils

local_user = Base_Properties.local_mysql_user
local_password = Base_Properties.local_mysql_password
local_database = Base_Properties.local_mysql_database
local_host = Base_Properties.local_mysql_host

origin_user = Base_Properties.origin_mysql_user
origin_password = Base_Properties.origin_mysql_password
origin_database = Base_Properties.origin_mysql_database
origin_host = Base_Properties.origin_mysql_host


@timing_decorator
def transfer_local_to_origin_mysql():
    """
    从 本地 向 远端 服务器刷新 mysql 数据
    Returns:
    """

    local_db_url = f'mysql+pymysql://{local_user}:{local_password}@{local_host}:3306/{local_database}'
    origin_db_url = f'mysql+pymysql://{origin_user}:{origin_password}@{origin_host}:3306/{origin_database}'

    table_all_list = ['ods_stock_code_daily_insight',
                      'ods_index_a_share_insight',
                      'ods_astock_industry_detail',
                      'ods_astock_industry_overview',
                      'ods_stock_limit_summary_insight',
                      'ods_future_inside_insight',
                      'ods_north_bound_daily',
                      'ods_shareholder_num',
                      'ods_stock_chouma_insight',
                      'ods_us_stock_daily_vantage',
                      'ods_exchange_rate_vantage_detail',
                      'ods_exchange_dxy_vantage'
                      'ods_tdx_stock_concept_plate',
                      'ods_tdx_stock_index_plate',
                      'ods_tdx_stock_industry_plate',
                      'ods_tdx_stock_region_plate',
                      'ods_tdx_stock_style_plate']

    table_temp_list = ['stock_chouma_insight']

    for tableName in table_temp_list:
        mysql_utils.full_replace_migrate(source_host=local_host,
                                         source_db_url=local_db_url,
                                         target_host=origin_host,
                                         target_db_url=origin_db_url,
                                         table_name=tableName)


@timing_decorator
def transfer_origin_to_local_mysql():
    """
    从 远端 向 本地 主机刷新 mysql 数据
    Returns:

    """

    local_db_url = f'mysql+pymysql://{local_user}:{local_password}@{local_host}:3306/{local_database}'
    origin_db_url = f'mysql+pymysql://{origin_user}:{origin_password}@{origin_host}:3306/{origin_database}'

    # 'stock_kline_daily_insight',

    table_all_list = ['ods_stock_code_daily_insight',
                      'ods_index_a_share_insight',
                      'ods_astock_industry_detail',
                      'ods_astock_industry_overview',
                      'ods_stock_limit_summary_insight',
                      'ods_future_inside_insight',
                      'ods_north_bound_daily',
                      'ods_shareholder_num',
                      'ods_stock_chouma_insight',
                      'ods_us_stock_daily_vantage',
                      'ods_exchange_rate_vantage_detail',
                      'ods_exchange_dxy_vantage'
                      'ods_tdx_stock_concept_plate',
                      'ods_tdx_stock_index_plate',
                      'ods_tdx_stock_industry_plate',
                      'ods_tdx_stock_region_plate',
                      'ods_tdx_stock_style_plate']

    table_temp_list = ['ods_stock_chouma_insight']

    for tableName in table_all_list:
        mysql_utils.full_replace_migrate(source_host=origin_host,
                                         source_db_url=origin_db_url,
                                         target_host=local_host,
                                         target_db_url=local_db_url,
                                         table_name=tableName)



@timing_decorator
def append_local_to_origin_mysql():
    """
    从 本地 向 远端 服务器刷新 mysql 数据
    Returns:
    """

    local_db_url = f'mysql+pymysql://{local_user}:{local_password}@{local_host}:3306/{local_database}'
    origin_db_url = f'mysql+pymysql://{origin_user}:{origin_password}@{origin_host}:3306/{origin_database}'

    table_now_list = ['ods_stock_code_daily_insight',
                      'ods_stock_kline_daily_insight_now',
                      'ods_index_a_share_insight_now',
                      'ods_stock_limit_summary_insight_now',
                      'ods_future_inside_insight_now',
                      'ods_stock_chouma_insight_now',
                      'ods_us_stock_daily_vantage',
                      'ods_exchange_rate_vantage_detail',
                      'ods_exchange_dxy_vantage']

    table_all_list = ['ods_stock_kline_daily_insight',
                      'ods_index_a_share_insight',
                      'ods_stock_limit_summary_insight',
                      'ods_future_inside_insight',
                      'ods_stock_chouma_insight']

    # sourceTable = 'ods_stock_chouma_insight'
    # targetTable = 'ods_stock_chouma_insight'
    # columns = ['htsc_code','ymd','exchange','last','prev_close','total_share','a_total_share','a_listed_share','listed_share','restricted_share','cost_5pct','cost_15pct','cost_50pct','cost_85pct','cost_95pct','avg_cost','max_cost','min_cost','winner_rate','diversity','pre_winner_rate','restricted_avg_cost','restricted_max_cost','restricted_min_cost','large_shareholders_avg_cost','large_shareholders_total_share','large_shareholders_total_share_pct']

    sourceTable = 'ods_stock_kline_daily_insight'
    targetTable = 'ods_stock_kline_daily_insight'
    columns = ['htsc_code', 'ymd', 'open', 'close', 'high', 'low', 'num_trades', 'volume']

    mysql_utils.cross_server_upsert(source_user=origin_user,
                                    source_password=origin_password,
                                    source_host=origin_host,
                                    source_database=origin_database,
                                    target_user=local_user,
                                    target_password=local_password,
                                    target_host=local_host,
                                    target_database=local_database,
                                    source_table=sourceTable,
                                    target_table=targetTable,
                                    columns=columns)

    # mysql_utils.cross_server_upsert(source_user=local_user,
    #                                 source_password=local_password,
    #                                 source_host=local_host,
    #                                 source_database=local_database,
    #                                 target_user=origin_user,
    #                                 target_password=origin_password,
    #                                 target_host=origin_host,
    #                                 target_database=origin_database,
    #                                 source_table=sourceTable,
    #                                 target_table=targetTable,
    #                                 columns=columns)










if __name__ == "__main__":

    #  从 本地 往 远端     msyql迁移数据
    # transfer_local_to_origin_mysql()

    # transfer_origin_to_local_mysql()

    #  跨服务器的数据append
    append_local_to_origin_mysql()



