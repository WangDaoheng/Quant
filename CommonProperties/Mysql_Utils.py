
from sqlalchemy import create_engine, text
import pymysql
from sqlalchemy.exc import SQLAlchemyError
import gc
from datetime import datetime, timedelta
from typing import List, Optional

import pandas as pd
import numpy as np
import logging
import platform
from sqlalchemy.orm import sessionmaker

import CommonProperties.Base_Properties as base_properties
import CommonProperties.Base_utils as base_utils
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


# def data_from_dataframe_to_mysql(user, password, host, database='quant', df=pd.DataFrame(), table_name='', merge_on=[]):
#     """
#     把 dataframe 类型数据写入 mysql 表里面, 同时调用了
#     Args:
#         df:
#         table_name:
#         database:
#     Returns:
#     """
#     db_url = f'mysql+pymysql://{user}:{password}@{host}:3306/{database}'
#     engine = create_engine(db_url)
#
#     # 对输入的df的空值做处理
#     # df = df.fillna(value=None)
#     df = df.replace({np.nan: ''})
#
#     # 确保 df 中的字段列顺序与表中的列顺序一致
#     columns = df.columns.tolist()
#
#     # 检查是否存在重复数据，并将其去除
#     df.drop_duplicates(subset=merge_on, keep='first', inplace=True)
#
#     total_rows = df.shape[0]
#     if total_rows == 0:
#         logging.info(f"所有数据已存在，无需插入新的数据到 {host} 的 {table_name} 表中。")
#         return
#
#     # 使用 INSERT IGNORE 来去重
#     insert_sql = f"""
#     INSERT IGNORE INTO {table_name} ({', '.join(columns)})
#     VALUES ({', '.join(['%s'] * len(columns))});
#     """
#
#     # # 转换 df 为一个可以传递给 executemany 的列表
#     # values = df.values.tolist()
#
#     # 转换 df 为一个可以传递给 executemany 的元组列表
#     values = [tuple(row) for row in df.to_numpy()]
#
#     with engine.connect() as connection:
#         transaction = connection.begin()
#         try:
#             connection.execute(text(insert_sql), values)
#             transaction.commit()
#             logging.info(f"成功插入 {total_rows} 行数据到 {host} 的 {table_name} 表中。")
#         except Exception as e:
#             transaction.rollback()
#             logging.error(f"写入 {host} 的表：{table_name} 时发生错误: {e}")
#             raise

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

    # 对输入的df的空值做处理
    df = df.replace({np.nan: None})

    # 确保 df 中的字段列顺序与表中的列顺序一致
    columns = df.columns.tolist()

    # 检查是否存在重复数据，并将其去除
    df.drop_duplicates(subset=merge_on, keep='first', inplace=True)

    total_rows = df.shape[0]
    if total_rows == 0:
        logging.info(f"所有数据已存在，无需插入新的数据到 {host} 的 {table_name} 表中。")
        return

    # 使用 INSERT IGNORE 来去重
    insert_sql = f"""
    INSERT IGNORE INTO {table_name} ({', '.join(columns)})
    VALUES ({', '.join([f':{col}' for col in columns])});
    """

    # 转换 df 为一个可以传递给 executemany 的字典列表
    values = df.to_dict('records')

    with engine.connect() as connection:
        transaction = connection.begin()
        try:
            connection.execute(text(insert_sql), values)
            transaction.commit()
            logging.info(f"成功插入 {total_rows} 行数据到 {host} 的 {table_name} 表中。")
        except Exception as e:
            transaction.rollback()
            logging.error(f"写入 {host} 的表：{table_name} 时发生错误: {e}")
            raise


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


# def upsert_table(user, password, host, database, source_table, target_table, columns):
#     """
#     使用 source_table 中的数据来更新或插入到 target_table 中。
#     这是一种追加取并集的方式
#
#     :param database:     默认为 quant
#     :param source_table: 源表名称（字符串）
#     :param target_table: 目标表名称（字符串）
#     :param columns: 需要更新或插入的列名列表（列表）
#     """
#
#     db_url = f'mysql+pymysql://{user}:{password}@{host}:3306/{database}'
#     engine = create_engine(db_url)
#
#     # 构建列名部分
#     columns_str = ", ".join(columns)
#
#     # 构建 ON DUPLICATE KEY UPDATE 部分
#     update_str = ", ".join([f"{col} = VALUES({col})" for col in columns])
#
#     # 构建 SELECT 部分
#     select_str = ", ".join(columns)
#
#     # 构建完整的 SQL 语句
#     sql = f"""
#     INSERT INTO {target_table} ({columns_str})
#     SELECT {select_str}
#     FROM {source_table}
#     ON DUPLICATE KEY UPDATE
#     {update_str};
#     """
#
#     # 执行 SQL 语句
#     with engine.connect() as connection:
#         connection.execute(text(sql))

def upsert_table(
        user: str,
        password: str,
        host: str,
        database: str,
        source_table: str,
        target_table: str,
        columns: List[str],
        unique_key_cols: List[str],  # 必须指定联合唯一键字段（如 ['htsc_code', 'ymd']）
        sync_months: int = 6,  # 仅保留时间范围默认值（可手动传参覆盖）
        use_ignore: bool = True,  # 冲突忽略默认启用（可手动关闭）
        date_column: str = "ymd"  # 日期筛选字段（默认 ymd，可适配其他日期字段如 create_time）
) -> int:
    """
    通用 MySQL upsert 函数：增量同步指定时间范围内的数据，冲突时不中断执行
    核心功能：存在则更新，不存在则插入；支持任意表、任意联合唯一键、任意字段同步

    :param user: 数据库用户名（必填）
    :param password: 数据库密码（必填）
    :param host: 数据库主机IP（如 192.168.1.100/localhost，必填）
    :param database: 数据库名称（必填）
    :param source_table: 源表名称（必填）
    :param target_table: 目标表名称（必填）
    :param columns: 同步的字段列表（必填，如 ['col1', 'col2']）
    :param unique_key_cols: 联合唯一键字段列表（必填，如 ['code', 'date']，需与目标表唯一约束一致）
    :param sync_months: 同步时间范围（单位：月，默认 6 个月，可传参覆盖）
    :param use_ignore: 是否忽略主键冲突（默认 True，冲突时继续执行其他数据）
    :param date_column: 日期筛选字段名（默认 'ymd'，适配其他日期字段如 'create_time'）
    :return: 影响行数（插入+更新的总条数）
    """
    # -------------------------- 1. 入参合法性校验（必填项不能为空） --------------------------
    required_params = [
        ("user", user), ("password", password), ("host", host),
        ("database", database), ("source_table", source_table),
        ("target_table", target_table), ("columns", columns),
        ("unique_key_cols", unique_key_cols)
    ]
    for param_name, param_value in required_params:
        if not param_value:
            raise ValueError(f"❌ 必填参数 '{param_name}' 不能为空")

    if len(columns) == 0:
        raise ValueError("❌ 同步字段列表 columns 不能为空")

    if len(unique_key_cols) == 0:
        raise ValueError("❌ 联合唯一键列表 unique_key_cols 不能为空")

    # -------------------------- 2. 计算同步时间范围 --------------------------
    current_date = datetime.now()
    start_date = current_date - timedelta(days=sync_months * 30)  # 每月按30天简化计算
    start_date_str = start_date.strftime("%Y-%m-%d")
    end_date_str = current_date.strftime("%Y-%m-%d")

    print(f"📅 同步时间范围：{start_date_str} 至 {end_date_str}（日期字段：{date_column}）")
    print(f"📥 源表：{source_table} | 📤 目标表：{target_table}")
    print(f"🔍 同步字段：{', '.join(columns)}")
    print(f"🔑 联合唯一键：{', '.join(unique_key_cols)}")

    # -------------------------- 3. 字段安全校验（防 SQL 注入） --------------------------
    def is_valid_field(field: str) -> bool:
        """校验字段名是否合法（仅允许字母、数字、下划线，且不以数字开头）"""
        return field.replace('_', '').isalnum() and not field[0].isdigit()

    # 过滤非法字段
    valid_columns = []
    for col in columns:
        if is_valid_field(col):
            valid_columns.append(col)
        else:
            print(f"⚠️  忽略非法字段名：{col}（仅允许字母、数字、下划线，且不以数字开头）")

    valid_unique_keys = []
    for key in unique_key_cols:
        if is_valid_field(key):
            valid_unique_keys.append(key)
        else:
            print(f"⚠️  忽略非法唯一键字段名：{key}（仅允许字母、数字、下划线，且不以数字开头）")

    if not valid_columns:
        raise ValueError("❌ 无有效同步字段，请检查 columns 参数")
    if not valid_unique_keys:
        raise ValueError("❌ 无有效联合唯一键字段，请检查 unique_key_cols 参数")

    # -------------------------- 4. 构建数据库连接 --------------------------
    db_url = f'mysql+pymysql://{user}:{password}@{host}:3306/{database}'
    engine = create_engine(
        db_url,
        charset='utf8mb4',  # 支持中文和特殊字符
        pool_pre_ping=True,  # 连接前检测存活
        pool_size=5,
        max_overflow=10,
        connect_args={
            "options": "--sql_mode=NO_ENGINE_SUBSTITUTION",
            "connect_timeout": 10
        }
    )

    # -------------------------- 5. 构建 SQL 语句 --------------------------
    # 用反引号包裹表名和字段名，避免关键字冲突
    columns_str = ", ".join([f"`{col}`" for col in valid_columns])
    select_str = columns_str

    # ON DUPLICATE KEY UPDATE 部分（仅更新同步字段）
    update_str = ", ".join([f"`{col}` = VALUES(`{col}`)" for col in valid_columns])

    # 插入关键字（冲突忽略）
    insert_keyword = "INSERT IGNORE INTO" if use_ignore else "INSERT INTO"

    # 过滤条件：日期范围 + 唯一键字段非空（避免无效数据）
    non_null_conditions = " AND ".join([f"`{key}` IS NOT NULL" for key in valid_unique_keys])
    where_clause = f"`{date_column}` >= '{start_date_str}' AND `{date_column}` <= '{end_date_str}' AND {non_null_conditions}"

    # 完整 SQL
    sql = f"""
    {insert_keyword} `{target_table}` ({columns_str})
    SELECT {select_str}
    FROM `{source_table}`
    WHERE {where_clause}
    ON DUPLICATE KEY UPDATE {update_str};
    """

    print(f"\n⚙️  执行的 SQL 语句：\n{sql.strip()}")

    # -------------------------- 6. 执行 SQL 并处理结果 --------------------------
    affected_rows = 0
    try:
        with engine.connect() as connection:
            with connection.begin():  # 事务支持
                result = connection.execute(text(sql))
                connection.commit()
                affected_rows = result.rowcount
                print(f"\n✅ 执行成功！影响行数：{affected_rows}（插入+更新）")

    except SQLAlchemyError as e:
        error_msg = str(e)
        if "1062 (23000)" in error_msg:
            print(f"\n⚠️  警告：部分数据存在唯一键冲突（已自动跳过），错误摘要：{error_msg[:500]}")
            affected_rows = 0
        elif "Timeout" in error_msg or "timed out" in error_msg:
            print(f"\n❌ 错误：数据库连接超时，请检查主机 IP、端口是否可达")
            raise
        else:
            print(f"\n❌ 错误：SQL 执行失败，错误详情：{error_msg[:500]}")
            raise

    except Exception as e:
        print(f"\n❌ 错误：程序执行失败，原因：{str(e)}")
        raise

    finally:
        engine.dispose()
        print(f"\n🔌 数据库连接已关闭")

    return affected_rows




def cross_server_upsert_all(source_user, source_password, source_host, source_database,
                            target_user, target_password, target_host, target_database,
                            source_table, target_table):
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

    # 动态获取列名
    columns = df.columns.tolist()

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



def cross_server_upsert_ymd(source_user, source_password, source_host, source_database,
                            target_user, target_password, target_host, target_database,
                            source_table, target_table, start_date, end_date):
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

    # # 从源服务器读取数据
    # df = pd.read_sql_table(source_table, source_engine)

    # 从源服务器读取数据，限制 ymd 在 [start_date, end_date] 内
    query = f"""
    SELECT * FROM {source_table}
    WHERE ymd BETWEEN '{start_date}' AND '{end_date}'
    """
    df = pd.read_sql_query(query, source_engine)

    # 动态获取列名
    columns = df.columns.tolist()

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



def full_replace_migrate(source_host, source_db_url, target_host, target_db_url, table_name, chunk_size=10000):
    """
    将本地 MySQL 数据库中的表数据导入到远程 MySQL 数据库中。
    整体暴力迁移，全删全插

    Args:
        source_host   (str): 源端 主机
        source_db_url (str): 源端 MySQL 数据库的连接 URL
        target_host   (str): 目标 主机
        target_db_url (str): 目标 MySQL 数据库的连接 URL
        table_name    (str): 要迁移的表名
        chunk_size    (int): 每次读取和写入的数据块大小，默认 10000 行
    """
    # 创建 源端 数据库的 SQLAlchemy 引擎
    source_engine = create_engine(source_db_url)
    SourceSession = sessionmaker(bind=source_engine)

    # 创建 目标 数据库的 SQLAlchemy 引擎
    target_engine = create_engine(target_db_url)
    TargetSession = sessionmaker(bind=target_engine)

    try:
        # 打开源端和目标端的会话
        with SourceSession() as source_session, TargetSession() as target_session:
            # 开启事务
            with target_session.begin():
                # 第一次写入时，先清空表
                target_session.execute(text(f"TRUNCATE TABLE {table_name}"))
                print(f"成功清空目标表 {table_name}。")

            # 分批读取数据并插入目标数据库
            offset = 0
            while True:
                # 从源端数据库分批读取数据
                query = f"SELECT * FROM {table_name} LIMIT {chunk_size} OFFSET {offset}"
                chunk = pd.read_sql(query, source_session.bind)
                if chunk.empty:
                    break

                # 使用批量插入
                chunk.to_sql(name=table_name, con=target_engine, if_exists='append', index=False)
                print(f"成功写入第 {offset // chunk_size + 1} 块数据到{target_host} mysql库。")

                # 更新偏移量
                offset += chunk_size

                # 释放内存
                del chunk
                gc.collect()

        print(f"表 {table_name} 数据迁移完成。")

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

        if platform.system() == "Windows":
            user = local_user
            password = local_password
            host = local_host
            database = local_database
        else:
            user = origin_user
            password = origin_password
            host = origin_host
            database = origin_database

        stock_code_df = data_from_mysql_to_dataframe_latest(user=user,
                                                            password=password,
                                                            host=host,
                                                            database=database,
                                                            table_name='ods_stock_code_daily_insight')

        mysql_stock_code_list = stock_code_df['htsc_code'].tolist()
        logging.info("    从 本地Mysql库 里读取最新的股票代码")
    else:
        mysql_stock_code_list = df['htsc_code'].tolist()
        logging.info("    从 self.stock_code 里读取最新的股票代码")

    return mysql_stock_code_list


def execute_sql_statements(user, password, host, database, sql_statements):
    """
    连接到数据库并执行给定的 SQL 语句列表。

    参数:
    user (str): 数据库用户名。
    password (str): 数据库密码。
    host (str): 数据库主机地址。
    database (str): 数据库名称。
    sql_statements (list): 包含 SQL 语句的列表。
    """
    # 创建数据库连接 URL
    db_url = f'mysql+pymysql://{user}:{password}@{host}:3306/{database}'

    # 创建数据库引擎，设置连接池
    engine = create_engine(db_url, pool_size=10, max_overflow=20, pool_recycle=3600)

    try:
        # 使用连接池执行 SQL 语句
        with engine.connect() as connection:
            transaction = connection.begin()  # 开始事务
            for statement in sql_statements:
                # 使用 text() 来防止 SQL 注入
                connection.execute(text(statement))
            transaction.commit()  # 提交事务

    except SQLAlchemyError as e:
        # 捕获数据库相关的错误
        print(f"Error executing SQL: {e}")
    finally:
        # 确保连接被正确关闭
        engine.dispose()






















