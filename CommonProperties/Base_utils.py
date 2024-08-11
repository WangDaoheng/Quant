import os
import sys
from datetime import datetime
import time
import traceback
from functools import wraps
import shutil
from sqlalchemy import create_engine, text
import pandas as pd
import logging
import colorlog


from CommonProperties import Base_Properties


# 配置日志处理器
handler = colorlog.StreamHandler()

# 设置彩色日志的格式，包含时间、日志级别和消息内容
formatter = colorlog.ColoredFormatter(
    '%(log_color)s%(asctime)s - %(levelname)s - %(message)s',
    log_colors={
        'DEBUG': 'cyan',
        'INFO': 'green',    # 将 INFO 级别设为绿色
        'WARNING': 'yellow',
        'ERROR': 'red',
        'CRITICAL': 'bold_red',
    }
)

handler.setFormatter(formatter)

# 获取并配置 logger
logger = logging.getLogger()
logger.addHandler(handler)
logger.setLevel(logging.INFO)


def save_out_filename(filehead, file_type):
    """
    @:param filehead       文件说明
    @:param file_type      文件类型

    拼接输出文件的文件名
    """
    timestamp = datetime.now().strftime("%Y%m%d%H")
    output_filename = f"{filehead}_{timestamp}.{file_type}"
    # print("正在打印文件:{{{}}}".format(save_out_filename))
    return output_filename


def get_latest_filename(filename_dir):
    """
    返回时间戳最新的filename   file_name: stocks_codes_all_2024070818.txt
    :return:
    """
    file_names = os.listdir(filename_dir)

    latest_date = ''
    latest_file_name = ''

    # 遍历文件名列表
    for file_name in file_names:
        try:
            # 从文件名中提取时间戳部分
            timestamp = file_name.split('_')[-1].split('.')[0]

            # 检查时间戳是否是最新的
            if timestamp > latest_date:
                latest_date = timestamp
                latest_file_name = file_name
        except Exception as e:
            print(r"   在处理文件 {} 时遇到问题:{}".format(file_name, e))

    return latest_file_name



def collect_stock_items(input_list):
    """
    对stocks 的list中每个元素按照前三位做分类汇总
    :param input_list:
    :return:
    """

    result_dict = {}

    for item in input_list:
        prefix = item[:3]
        if prefix not in result_dict:
            result_dict[prefix] = [item]
        else:
            result_dict[prefix].append(item)

    return result_dict




def timing_decorator(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        start_time = time.time()
        try:
            result = func(*args, **kwargs)
        except Exception as e:
            print(f"Error in function {func.__name__}:")
            traceback.print_exc()  # 打印详细的堆栈追踪信息
            raise e  # 重新抛出异常，保持原始行为
        end_time = time.time()
        execution_time = end_time - start_time
        logging.info(f"函数: {func.__name__} 执行时间: {execution_time:.2f} 秒")
        return result
    return wrapper



def copy_and_rename_file(src_file_path, dest_dir, new_name):
    """
    将文件复制到另一个目录并重命名
    :param src_file_path: 源文件路径
    :param dest_dir: 目标目录
    :param new_name: 新文件名
    """
    # 检查目标目录是否存在，不存在则创建
    if not os.path.exists(dest_dir):
        os.makedirs(dest_dir)

    # 目标文件路径
    dest_file_path = os.path.join(dest_dir, new_name)

    # 复制文件并重命名
    shutil.copy(src_file_path, dest_file_path)
    print(f"文件已复制并重命名为: {dest_file_path}")


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





def data_from_dataframe_to_mysql(df=pd.DataFrame(), table_name='', database='quant'):
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

    total_rows = df.shape[0]

    # 将结果批量写入 MySQL 数据库
    chunk_size = 10000  # 根据系统内存情况调整

    for i in range(0, total_rows, chunk_size):
        chunk = df.iloc[i:i + chunk_size]

        try:
            chunk.to_sql(name=table_name, con=engine, if_exists='append', index=False)
        except Exception as e:
            logging.error(f"写入表：{table_name}的 第 {i // chunk_size + 1} 批次时发生错误: {e}")

    # 所有批次写入完成后检查数据写入完整性
    if check_data_written(total_rows, table_name, engine):
        logging.info(f"mysql表：{table_name}  数据写入成功且无遗漏。")
    else:
        logging.warning(f"{table_name} 数据写入可能有问题，记录数不匹配。")




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


















