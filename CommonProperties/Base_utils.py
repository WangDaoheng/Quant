import os
import sys
from datetime import datetime
import time
import traceback
import inspect
from functools import wraps
import shutil
import pandas as pd
import logging
import requests


from CommonProperties.set_config import setup_logging_config



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
        # 获取当前函数所在的文件名和函数名
        current_frame = inspect.currentframe()
        caller_frame = inspect.getouterframes(current_frame, 2)
        file_name = os.path.basename(caller_frame[1].filename)

        # 在函数执行前打印开始日志
        logging.info(f"文件: {file_name} 函数: {func.__name__} 开始执行...")

        start_time = time.time()
        try:
            result = func(*args, **kwargs)
        except Exception as e:
            print(f"Error in function {func.__name__}:")
            traceback.print_exc()  # 打印详细的堆栈追踪信息
            raise e  # 重新抛出异常，保持原始行为
        end_time = time.time()
        execution_time = end_time - start_time

        # 在函数执行后打印执行时间日志
        logging.info(f"文件: {file_name} 函数: {func.__name__} 执行时间: {execution_time:.2f} 秒")
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





def process_in_batches(df, batch_size, processing_function, **kwargs):
    """
    通用的批次处理函数。

    Args:
        df (pd.DataFrame): 要处理的数据。
        batch_size (int): 每个批次的大小。
        processing_function (callable): 处理每个批次的函数。
        **kwargs: 传递给处理函数的参数。

    Returns:
        pd.DataFrame: 处理后的总 DataFrame。
    """
    def get_batches(df, batch_size):
        for start in range(0, len(df), batch_size):
            yield df[start:start + batch_size]

    total_batches = (len(df) + batch_size - 1) // batch_size
    total_df = pd.DataFrame()

    for i, batch_df in enumerate(get_batches(df, batch_size), start=1):
        sys.stdout.write(f"\r当前执行 {processing_function.__name__} 的 第 {i} 次循环，总共 {total_batches} 个批次")
        sys.stdout.flush()
        time.sleep(0.01)

        # 直接调用处理函数，只传递 **kwargs
        result = processing_function(**kwargs)
        total_df = pd.concat([total_df, result], ignore_index=True)

    sys.stdout.write("\n")
    return total_df


def get_with_retries(url, headers=None, timeout=10, max_retries=3, backoff_factor=1):
    """
    Args:
        url:
        headers:
        timeout:
        max_retries:      最大重试次数
        backoff_factor:

    Returns:

    """
    retries = 0
    while retries < max_retries:
        try:
            response = requests.get(url, headers=headers, timeout=timeout)
            if response.status_code == 200:
                return response
            else:
                logging.error(f"Error: {response.status_code} - {response.text}")
        except requests.exceptions.RequestException as e:
            logging.error(f"请求失败报错: {e}")

        retries += 1
        sleep_time = backoff_factor * (2 ** retries)
        logging.info(f" {sleep_time} 秒后开展重试...")
        time.sleep(sleep_time)

    logging.error(f"在经历 {max_retries} 次尝试后还是不能捕获数据")
    return None



# 调用日志配置
setup_logging_config()




