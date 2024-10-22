
from sqlalchemy import create_engine, text
import pymysql
import pandas as pd
import logging


import CommonProperties.Base_Properties as base_properties
from CommonProperties.set_config import setup_logging_config

# 调用日志配置   这里要注释掉，不然日志重复打印
# setup_logging()

###################  mysql 配置   ######################
local_user = base_properties.local_mysql_user
local_password = base_properties.local_mysql_password
local_database = base_properties.local_mysql_database
local_host = base_properties.local_mysql_host

origin_user = base_properties.origin_mysql_user
origin_password = base_properties.origin_mysql_password
origin_database = base_properties.origin_mysql_database
origin_host = base_properties.origin_mysql_host



def check_data_written(total_rows, table_name, engine):
    """
    用于查询mysql写入的数据条数是否完整
    Args:
        total_rows: 要验证的表的理论上的行数
        table_name: 要验证的表的名称
        engine:     查询引擎
    Returns:  True 条数验证匹配  / False  条数验证不匹配
    """

    try:
        # 创建数据库连接
        connection = engine.raw_connection()
        cursor = connection.cursor()

        # 查询表中写入的数据总数
        check_query = f"SELECT COUNT(*) FROM {table_name}"
        cursor.execute(check_query)
        result = cursor.fetchone()[0]

        # 关闭连接
        cursor.close()
        connection.close()

        return result == total_rows
    except Exception as e:
        logging.error(f"检查数据写入时发生错误: {e}")
        return False


def data_from_dataframe_to_mysql(user, password, host, database='quant', df=pd.DataFrame(), table_name='', merge_on=[]):
    """
    把 dataframe 类型数据写入 mysql 表里面, 同时调用了
    Args:
        df:
        table_name:
        database:
    Returns:
    """

    db_url = f'mysql+pymysql://{user}:{password}@{host}:3306/{database}'
    engine = create_engine(db_url)

    # 将df中的ymd格式转换为DATE类型（如果merge_on包含'ymd'）
    if 'ymd' in merge_on:

        # df['ymd'] = df['ymd'].apply(lambda x: pd.to_datetime(str(x), format='%Y%m%d').strftime('%Y-%m-%d'))
        df['ymd'] = df['ymd'].apply(lambda x: pd.to_datetime(str(x), format='%Y%m%d') if pd.notnull(x) else pd.NaT)
        df['ymd'] = df['ymd'].apply(lambda x: x.strftime('%Y-%m-%d') if pd.notnull(x) else None)

        ymd_range = df['ymd'].unique()


    # 预取数据库中这些日期的数据
    # 动态构建SELECT语句和WHERE子句
    select_columns = ", ".join(merge_on)
    where_condition = f"ymd IN ({','.join(['%s'] * len(ymd_range))})"

    # 构建 SQL 查询
    existing_query = f"""
    SELECT {select_columns} FROM {table_name}
    WHERE {where_condition}
    """

    existing_data = pd.read_sql(existing_query, engine, params=ymd_range)
    existing_data['ymd'] = existing_data['ymd'].astype(str)

    # 从df中过滤掉已经存在的数据
    merged_df = pd.merge(df, existing_data, on=merge_on, how='left', indicator=True)
    new_data = merged_df[merged_df['_merge'] == 'left_only'].drop(columns=['_merge'])

    total_rows = new_data.shape[0]

    # 把 df 中的ymd列还原回去
    df['ymd'] = df['ymd'].apply(lambda x: pd.to_datetime(x).strftime('%Y%m%d'))

    if total_rows == 0:
        logging.info(f"所有数据已存在，无需插入新的数据到 {host} 的 {table_name} 表中。")
        return

    # 将结果批量写入 MySQL 数据库
    chunk_size = 10000  # 根据系统内存情况调整

    for i in range(0, total_rows, chunk_size):
        chunk = new_data.iloc[i:i + chunk_size]

        try:
            chunk.to_sql(name=table_name, con=engine, if_exists='append', index=False)

        except Exception as e:
            logging.error(f"写入{host} 的表：{table_name}的 第 {i // chunk_size + 1} 批次时发生错误: {e}")


# def data_from_mysql_to_dataframe(user, password, host, database='quant', table_name=''):
#     """
#     从 MySQL 表中读取数据到 DataFrame，同时进行最终的数据完整性检查和日志记录
#     Args:
#         table_name: MySQL 表名
#         database: 数据库名称
#
#     Returns:
#         df: 读取到的 DataFrame
#     """
#
#     db_url = f'mysql+pymysql://{user}:{password}@{host}:3306/{database}'
#     engine = create_engine(db_url)
#
#     # 读取 MySQL 表中的记录总数
#     query_total = f"SELECT COUNT(*) FROM {table_name}"
#     total_rows = pd.read_sql(query_total, engine).iloc[0, 0]
#
#     # 读取数据的批量大小
#     chunk_size = 10000
#     chunks = []
#
#     try:
#         for offset in range(0, total_rows, chunk_size):
#             query = f"SELECT * FROM {table_name} LIMIT {chunk_size} OFFSET {offset}"
#             chunk = pd.read_sql(query, engine)
#             chunks.append(chunk)
#
#         df = pd.concat(chunks, ignore_index=True)
#
#         # 最终的数据完整性检查
#         if df.shape[0] == total_rows:
#             logging.info(f"{host} 的 mysql表：{table_name} 数据读取成功且无遗漏，共 {total_rows} 行。")
#         else:
#             logging.warning(f"{table_name} 数据读取可能有问题，预期记录数为 {total_rows}，实际读取记录数为 {df.shape[0]}。")
#
#     except Exception as e:
#         logging.error(f"从表：{table_name} 读取数据时发生错误: {e}")
#         df = pd.DataFrame()  # 返回一个空的 DataFrame 以防出错时没有返回数据
#
#     return df


def data_from_mysql_to_dataframe(user, password, host, database='quant', table_name='', start_date=None, end_date=None, cols=None):
    """
    从 MySQL 表中读取数据到 DataFrame，同时进行最终的数据完整性检查和日志记录
    Args:
        table_name: MySQL 表名
        database: 数据库名称
        start_date: 起始日期
        end_date: 结束日期
        cols: 要选择的字段列表

    Returns:
        df: 读取到的 DataFrame
    """

    db_url = f'mysql+pymysql://{user}:{password}@{host}:3306/{database}'
    engine = create_engine(db_url)

    # 构建 SELECT 语句
    if cols:
        selected_cols = ', '.join(cols)
    else:
        selected_cols = '*'

    # 构建 WHERE 条件
    where_conditions = []
    if start_date:
        where_conditions.append(f"ymd >= '{start_date}'")
    if end_date:
        where_conditions.append(f"ymd <= '{end_date}'")

    where_clause = " AND ".join(where_conditions)

    # 读取 MySQL 表中的记录总数
    query_total = f"SELECT COUNT(*) FROM {table_name}"
    if where_clause:
        query_total += f" WHERE {where_clause}"
    total_rows = pd.read_sql(query_total, engine).iloc[0, 0]

    # 读取数据的批量大小
    chunk_size = 10000
    chunks = []

    try:
        for offset in range(0, total_rows, chunk_size):
            query = f"SELECT {selected_cols} FROM {table_name}"
            if where_clause:
                query += f" WHERE {where_clause}"
            query += f" LIMIT {chunk_size} OFFSET {offset}"
            chunk = pd.read_sql(query, engine)
            chunks.append(chunk)

        df = pd.concat(chunks, ignore_index=True)

        # 最终的数据完整性检查
        if df.shape[0] == total_rows:
            logging.info(f"{host} 的 mysql表：{table_name} 数据读取成功且无遗漏，共 {total_rows} 行。")
        else:
            logging.warning(f"{table_name} 数据读取可能有问题，预期记录数为 {total_rows}，实际读取记录数为 {df.shape[0]}。")

    except Exception as e:
        logging.error(f"从表：{table_name} 读取数据时发生错误: {e}")
        df = pd.DataFrame()  # 返回一个空的 DataFrame 以防出错时没有返回数据

    return df


# def data_from_mysql_to_dataframe_latest(user, password, host, database='quant', table_name=''):
#     """
#     从 MySQL 表中读取最新一天的数据到 DataFrame，同时进行最终的数据完整性检查和日志记录
#     Args:
#         table_name: MySQL 表名
#         database: 数据库名称
#
#     Returns:
#         df: 读取到的 DataFrame
#     """
#
#     db_url = f'mysql+pymysql://{user}:{password}@{host}:3306/{database}'
#     engine = create_engine(db_url)
#
#     try:
#         # 获取最新的 ymd 日期
#         query_latest_ymd = f"SELECT MAX(ymd) FROM {table_name}"
#         latest_ymd = pd.read_sql(query_latest_ymd, engine).iloc[0, 0]
#
#         if latest_ymd is not None:
#             # 查询最新一天的全部数据
#             query = f"SELECT * FROM {table_name} WHERE ymd = '{latest_ymd}'"
#             df = pd.read_sql(query, engine)
#
#             logging.info(f"    mysql表：{table_name} 最新一天({latest_ymd})的数据读取成功，共 {df.shape[0]} 行。")
#         else:
#             logging.warning(f"    {table_name} 表中没有找到有效的 ymd 数据。")
#             df = pd.DataFrame()  # 返回空的 DataFrame
#
#     except Exception as e:
#         logging.error(f"    从表：{table_name} 读取数据时发生错误: {e}")
#         df = pd.DataFrame()  # 返回一个空的 DataFrame 以防出错时没有返回数据
#
#     return df

def data_from_mysql_to_dataframe_latest(user, password, host, database='quant', table_name='', cols=None):
    """
    从 MySQL 表中读取最新一天的数据到 DataFrame，同时进行最终的数据完整性检查和日志记录
    Args:
        table_name: MySQL 表名
        database: 数据库名称
        cols: 要选择的字段列表

    Returns:
        df: 读取到的 DataFrame
    """

    db_url = f'mysql+pymysql://{user}:{password}@{host}:3306/{database}'
    engine = create_engine(db_url)

    try:
        # 获取最新的 ymd 日期
        query_latest_ymd = f"SELECT MAX(ymd) FROM {table_name}"
        latest_ymd = pd.read_sql(query_latest_ymd, engine).iloc[0, 0]

        if latest_ymd is not None:
            # 构建 SELECT 语句
            if cols:
                selected_cols = ', '.join(cols)
            else:
                selected_cols = '*'

            # 查询最新一天的数据
            query = f"SELECT {selected_cols} FROM {table_name} WHERE ymd = '{latest_ymd}'"
            df = pd.read_sql(query, engine)

            logging.info(f"    mysql表：{table_name} 最新一天({latest_ymd})的数据读取成功，共 {df.shape[0]} 行。")
        else:
            logging.warning(f"    {table_name} 表中没有找到有效的 ymd 数据。")
            df = pd.DataFrame()  # 返回空的 DataFrame

    except Exception as e:
        logging.error(f"    从表：{table_name} 读取数据时发生错误: {e}")
        df = pd.DataFrame()  # 返回一个空的 DataFrame 以防出错时没有返回数据

    return df



def create_partition_if_not_exists(engine, partition_name, year, month):
    next_month = month + 1
    next_year = year
    if next_month > 12:
        next_month = 1
        next_year += 1

    partition_value = next_year * 100 + next_month

    with engine.connect() as conn:
        query = text(f"""
        ALTER TABLE your_table ADD PARTITION (
            PARTITION {partition_name} VALUES LESS THAN ({partition_value})
        );
        """)
        conn.execute(query)


def upsert_table(user, password, host, database, source_table, target_table, columns):
    """
    使用 source_table 中的数据来更新或插入到 target_table 中。
    这是一种追加取并集的方式

    :param database:     默认为 quant
    :param source_table: 源表名称（字符串）
    :param target_table: 目标表名称（字符串）
    :param columns: 需要更新或插入的列名列表（列表）
    """

    db_url = f'mysql+pymysql://{user}:{password}@{host}:3306/{database}'
    engine = create_engine(db_url)

    # 构建列名部分
    columns_str = ", ".join(columns)

    # 构建 ON DUPLICATE KEY UPDATE 部分
    update_str = ", ".join([f"{col} = VALUES({col})" for col in columns])

    # 构建 SELECT 部分
    select_str = ", ".join(columns)

    # 构建完整的 SQL 语句
    sql = f"""
    INSERT INTO {target_table} ({columns_str})
    SELECT {select_str}
    FROM {source_table}
    ON DUPLICATE KEY UPDATE 
    {update_str};
    """

    # 执行 SQL 语句
    with engine.connect() as connection:
        connection.execute(text(sql))


def cross_server_upsert(source_user, source_password, source_host, source_database,
                        target_user, target_password, target_host, target_database,
                        source_table, target_table, columns):
    """
    跨服务器迁移数据，并在目标服务器上实现数据的并集。
    这是一种追加取并集的方式

    :param source_user:      源服务器的数据库用户名
    :param source_password:  源服务器的数据库密码
    :param source_host:      源服务器的主机地址
    :param source_database:  源服务器的数据库名称
    :param target_user:      目标服务器的数据库用户名
    :param target_password:  目标服务器的数据库密码
    :param target_host:      目标服务器的主机地址
    :param target_database:  目标服务器的数据库名称
    :param source_table:     源表名称（字符串）
    :param target_table:     目标表名称（字符串）
    :param columns:          需要更新或插入的列名列表（列表）
    """

    # 源服务器连接
    source_db_url = f'mysql+pymysql://{source_user}:{source_password}@{source_host}:3306/{source_database}'
    source_engine = create_engine(source_db_url)

    # 目标服务器连接
    target_db_url = f'mysql+pymysql://{target_user}:{target_password}@{target_host}:3306/{target_database}'
    target_engine = create_engine(target_db_url)

    # 从源服务器读取数据
    df = pd.read_sql_table(source_table, source_engine)

    # 在目标服务器创建临时表并插入数据
    temp_table_name = 'temp_source_data'
    df.to_sql(name=temp_table_name, con=target_engine, if_exists='replace', index=False)

    # 构建列名部分
    columns_str = ", ".join(columns)

    # 构建 ON DUPLICATE KEY UPDATE 部分
    update_str = ", ".join([f"{col} = VALUES({col})" for col in columns])

    # 构建 SELECT 部分
    select_str = ", ".join(columns)

    # 构建完整的 SQL 语句
    sql = f"""
    INSERT INTO {target_table} ({columns_str})
    SELECT {select_str}
    FROM {temp_table_name}
    ON DUPLICATE KEY UPDATE 
    {update_str};
    """

    # 在目标服务器上执行合并操作
    with target_engine.connect() as connection:
        connection.execute(text(sql))
        connection.execute(f"DROP TABLE {temp_table_name};")

    print(f"数据已从 {source_table} 迁移并合并到 {target_table}。")



def full_replace_migrate(source_host, source_db_url, target_host, target_db_url, table_name):
    """
    将本地 MySQL 数据库中的表数据导入到远程 MySQL 数据库中。
    整体暴力迁移，全删全插

    Args:
        source_host   (str): 源端 主机
        source_db_url (str): 源端 MySQL 数据库的连接 URL
        target_host   (str): 目标 主机
        target_db_url (str): 目标 MySQL 数据库的连接 URL
        table_name    (str): 要迁移的表名
    """
    # 创建 源端 数据库的 SQLAlchemy 引擎
    source_engine = create_engine(source_db_url)

    # 创建 目标 数据库的 SQLAlchemy 引擎
    target_engine = create_engine(target_db_url)

    try:
        # 从本地 MySQL 数据库读取数据
        df = pd.read_sql_table(table_name, source_engine)
        print(f"成功从{source_host} mysql库读取表 {table_name} 数据。")

        # 将数据写入远程 MySQL 数据库
        df.to_sql(name=table_name, con=target_engine, if_exists='replace', index=False)
        print(f"成功将表 {table_name} 数据写入{target_host} mysql库。")

    except Exception as e:
        print(f"数据迁移过程中发生错误: {e}")


def get_stock_codes_latest(df):
    """
    这是为了取最新的 stock_code, 首先默认从类变量里面获取 stock_code(df), 如果df为空，就从mysql里面去取最新的
    Args:
        df:
    Returns:
    """

    if df is None or df.empty:
        stock_code_df = data_from_mysql_to_dataframe_latest(user=local_user,
                                                            password=local_password,
                                                            host=local_host,
                                                            database=local_database,
                                                            table_name='ods_stock_code_daily_insight')

        mysql_stock_code_list = stock_code_df['htsc_code'].tolist()
        logging.info("    从 本地Mysql库 里读取最新的股票代码")
    else:
        mysql_stock_code_list = df['htsc_code'].tolist()
        logging.info("    从 self.stock_code 里读取最新的股票代码")

    return mysql_stock_code_list


























