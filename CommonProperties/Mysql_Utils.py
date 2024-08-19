
from sqlalchemy import create_engine, text
import pymysql
import pandas as pd
import logging


from CommonProperties import Base_Properties
from CommonProperties.logging_config import setup_logging

# 调用日志配置
setup_logging()


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






def data_from_dataframe_to_mysql(df=pd.DataFrame(), table_name='', database='quant', merge_on=[]):
    """
    把 dataframe 类型数据写入 mysql 表里面, 同时调用了
    Args:
        df:
        table_name:
        database:
    Returns:

    """
    # MySQL 数据库连接配置
    password = Base_Properties.mysql_password

    db_url = f'mysql+pymysql://root:{password}@localhost:3306/{database}'
    engine = create_engine(db_url)

    # 将df中的ymd格式转换为DATE类型（如果merge_on包含'ymd'）
    if 'ymd' in merge_on:
        df['ymd'] = df['ymd'].apply(lambda x: pd.to_datetime(str(x), format='%Y%m%d').strftime('%Y-%m-%d'))
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

    if total_rows == 0:
        logging.info(f"所有数据已存在，无需插入新的数据到 {table_name} 表中。")
        return

    # 将结果批量写入 MySQL 数据库
    chunk_size = 10000  # 根据系统内存情况调整

    for i in range(0, total_rows, chunk_size):
        chunk = new_data.iloc[i:i + chunk_size]

        try:
            chunk.to_sql(name=table_name, con=engine, if_exists='append', index=False)

        except Exception as e:
            logging.error(f"写入表：{table_name}的 第 {i // chunk_size + 1} 批次时发生错误: {e}")

    # 所有批次写入完成后检查数据写入完整性
    # if check_data_written(total_rows, table_name, engine):
    #     logging.info(f"mysql表：{table_name}  数据写入成功且无遗漏, 共 {total_rows} 行。")
    # else:
    #     logging.warning(f"{table_name} 数据写入可能有问题，记录数不匹配。")




def data_from_mysql_to_dataframe(table_name='', database='quant'):
    """
    从 MySQL 表中读取数据到 DataFrame，同时进行最终的数据完整性检查和日志记录
    Args:
        table_name: MySQL 表名
        database: 数据库名称

    Returns:
        df: 读取到的 DataFrame
    """
    # MySQL 数据库连接配置
    password = Base_Properties.mysql_password  # 从配置文件获取密码

    db_url = f'mysql+pymysql://root:{password}@localhost:3306/{database}'
    engine = create_engine(db_url)

    # 读取 MySQL 表中的记录总数
    query_total = f"SELECT COUNT(*) FROM {table_name}"
    total_rows = pd.read_sql(query_total, engine).iloc[0, 0]

    # 读取数据的批量大小
    chunk_size = 10000
    chunks = []

    try:
        for offset in range(0, total_rows, chunk_size):
            query = f"SELECT * FROM {table_name} LIMIT {chunk_size} OFFSET {offset}"
            chunk = pd.read_sql(query, engine)
            chunks.append(chunk)

        df = pd.concat(chunks, ignore_index=True)

        # 最终的数据完整性检查
        if df.shape[0] == total_rows:
            logging.info(f"mysql表：{table_name} 数据读取成功且无遗漏，共 {total_rows} 行。")
        else:
            logging.warning(f"{table_name} 数据读取可能有问题，预期记录数为 {total_rows}，实际读取记录数为 {df.shape[0]}。")

    except Exception as e:
        logging.error(f"从表：{table_name} 读取数据时发生错误: {e}")
        df = pd.DataFrame()  # 返回一个空的 DataFrame 以防出错时没有返回数据

    return df


def data_from_mysql_to_dataframe_latest(table_name='', database='quant'):
    """
    从 MySQL 表中读取最新一天的数据到 DataFrame，同时进行最终的数据完整性检查和日志记录
    Args:
        table_name: MySQL 表名
        database: 数据库名称

    Returns:
        df: 读取到的 DataFrame
    """
    # MySQL 数据库连接配置
    password = Base_Properties.mysql_password  # 从配置文件获取密码

    db_url = f'mysql+pymysql://root:{password}@localhost:3306/{database}'
    engine = create_engine(db_url)

    try:
        # 获取最新的 ymd 日期
        query_latest_ymd = f"SELECT MAX(ymd) FROM {table_name}"
        latest_ymd = pd.read_sql(query_latest_ymd, engine).iloc[0, 0]

        if latest_ymd is not None:
            # 查询最新一天的全部数据
            query = f"SELECT * FROM {table_name} WHERE ymd = '{latest_ymd}'"
            df = pd.read_sql(query, engine)

            logging.info(f"mysql表：{table_name} 最新一天({latest_ymd})的数据读取成功，共 {df.shape[0]} 行。")
        else:
            logging.warning(f"{table_name} 表中没有找到有效的 ymd 数据。")
            df = pd.DataFrame()  # 返回空的 DataFrame

    except Exception as e:
        logging.error(f"从表：{table_name} 读取数据时发生错误: {e}")
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


def upsert_table(database, source_table, target_table, columns):
    """
    使用 source_table 中的数据来更新或插入到 target_table 中。

    :param database:     默认为 quant
    :param source_table: 源表名称（字符串）
    :param target_table: 目标表名称（字符串）
    :param columns: 需要更新或插入的列名列表（列表）
    """
    # MySQL 数据库连接配置
    password = Base_Properties.mysql_password  # 从配置文件获取密码

    db_url = f'mysql+pymysql://root:{password}@localhost:3306/{database}'
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






