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

    table_now_list = ['stock_code_daily_insight',
                      'stock_kline_daily_insight_now',
                      'index_a_share_insight_now',
                      'stock_limit_summary_insight_now',
                      'future_inside_insight_now',
                      'stock_chouma_insight_now',
                      'US_stock_daily_vantage',
                      'exchange_rate_vantage_detail',
                      'exchange_DXY_vantage']

    table_all_list = ['stock_kline_daily_insight',
                      'index_a_share_insight',
                      'stock_limit_summary_insight',
                      'future_inside_insight',
                      'stock_chouma_insight']

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

    table_now_list = ['stock_code_daily_insight',
                      'stock_kline_daily_insight_now',
                      'index_a_share_insight_now',
                      'stock_limit_summary_insight_now',
                      'future_inside_insight_now',
                      'stock_chouma_insight_now',
                      'US_stock_daily_vantage',
                      'exchange_rate_vantage_detail',
                      'exchange_DXY_vantage']

    table_all_list = ['stock_kline_daily_insight',
                      'index_a_share_insight',
                      'stock_limit_summary_insight',
                      'future_inside_insight',
                      'stock_chouma_insight']

    table_temp_list = ['stock_chouma_insight']

    for tableName in table_temp_list:
        mysql_utils.full_replace_migrate(source_host=origin_host,
                                         source_db_url=origin_db_url,
                                         target_host=local_host,
                                         target_db_url=local_db_url,
                                         table_name=tableName)


if __name__ == "__main__":

    #  从 本地 往 远端     msyql迁移数据
    transfer_local_to_origin_mysql()



