import os
from datetime import datetime



def save_out_filename(filehead, file_type):
    """
    @:param filehead       文件说明
    @:param file_type      文件类型
    订单导出列表-20240206111514.xlsx
    售后单-2024-02-22 09_50_02.xlsx
    售后单-2024-02-24 05_22_28.xlsx
    根据给定的文件标签、当前时间戳、数据源订单文件、数据源售后文件, 拼接返回打印文件名
    """
    timestamp = datetime.now().strftime("%Y%m%d%H")
    output_filename = f"{filehead}_{timestamp}.{file_type}"
    print("正在打印文件:{{{}}}".format(save_out_filename))
    return output_filename


def get_latest_filename(filename_dir):
    """
    返回时间戳最新的filename
    :param filename_dir:
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
















