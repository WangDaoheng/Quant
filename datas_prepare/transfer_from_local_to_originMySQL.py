import pandas as pd
from sqlalchemy import create_engine


from CommonProperties import Base_Properties
from CommonProperties.Base_utils import timing_decorator


@timing_decorator
def transfer_data_core(local_db_url, remote_db_url, table_name):
    """
    将本地 MySQL 数据库中的表数据导入到远程 MySQL 数据库中。
    整体暴力迁移，全删全插

    Args:
        local_db_url (str): 本地 MySQL 数据库的连接 URL
        remote_db_url (str): 远程 MySQL 数据库的连接 URL
        table_name (str): 要迁移的表名
    """
    # 创建本地数据库的 SQLAlchemy 引擎
    local_engine = create_engine(local_db_url)

    # 创建远程数据库的 SQLAlchemy 引擎
    remote_engine = create_engine(remote_db_url)

    try:
        # 从本地 MySQL 数据库读取数据
        df = pd.read_sql_table(table_name, local_engine)
        print(f"成功从本地数据库读取表 {table_name} 数据。")

        # 将数据写入远程 MySQL 数据库
        df.to_sql(name=table_name, con=remote_engine, if_exists='replace', index=False)
        print(f"成功将表 {table_name} 数据写入远程数据库。")

    except Exception as e:
        print(f"数据迁移过程中发生错误: {e}")


@timing_decorator
def transfer_local_to_origin_mysql():
    local_user = Base_Properties.local_mysql_user
    local_password = Base_Properties.local_mysql_password
    local_database = Base_Properties.local_mysql_database
    local_host = Base_Properties.local_mysql_host

    origin_user = Base_Properties.origin_mysql_user
    origin_password = Base_Properties.origin_mysql_password
    origin_database = Base_Properties.origin_mysql_database
    origin_host = Base_Properties.origin_mysql_host

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


    for tableName in table_now_list:

        transfer_data_core(local_db_url=local_db_url, remote_db_url=origin_db_url, table_name=tableName)












if __name__ == "__main__":

    transfer_local_to_origin_mysql()






