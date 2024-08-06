import os
from datetime import datetime
import time
from functools import wraps
import shutil
from sqlalchemy import create_engine
import pandas as pd
import logging


logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


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
        result = func(*args, **kwargs)
        end_time = time.time()
        execution_time = end_time - start_time
        print(f"-------------   {func.__name__} 执行时间: {execution_time:.2f} 秒")
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
    用于查询写入数据条数是否完整
    Args:
        df:
        table_name:
        engine:
    Returns:
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
    把 dataframe 类型数据写入 mysql 表里面
    Args:
        df:
        table_name:
        database:
    Returns:

    """
    # MySQL 数据库连接配置
    db_url = f'mysql+pymysql://root:123456@localhost:3306/{database}'
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
        logging.info(f"{table_name} 数据写入成功且无遗漏。")
    else:
        logging.warning(f"{table_name} 数据写入可能有问题，记录数不匹配。")


