# set_config.py
import logging
import colorlog
from logging.handlers import RotatingFileHandler
import os
import platform
from datetime import datetime

import json
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email import encoders

import CommonProperties.Base_Properties as base_properties
from CommonProperties.Base_Properties import log_file_linux_path, log_file_window_path, personal_linux_path, personal_window_path, personal_property_file


def get_platform_is_window():
    """
    判断当前操作系统是window 还是 其他
    Returns: True  是window平台
             Flase 是其他平台
    """
    if platform.system() == "Windows":
        return True
    else:
        return False


def read_json_file(filepath):
    """
    对 json 文件的处理, 返回一个dict
    Args:
        filepath:  文件路径
    Returns:
    """
    # 读取文件
    with open(filepath, 'r', encoding='utf-8') as file:
        data = file.read()

    # 解析 JSON 数据
    json_data = json.loads(data)

    # 输出解析结果
    return json_data


def read_personal_property():
    """
    读取私人配置文件
    Returns:
    """
    if  get_platform_is_window():
        personal_window_file = os.path.join(personal_window_path, personal_property_file)
        personal_property_dict = read_json_file(personal_window_file)

    else:
        personal_linux_file = os.path.join(personal_linux_path, personal_property_file)
        personal_property_dict = read_json_file(personal_linux_file)

    return personal_property_dict


def read_logfile():
    """
    读取日志文件的地址
    Returns:
    """
    # 获取当前日期并生成日志文件名
    current_date = datetime.now().strftime('%Y-%m-%d')

    if get_platform_is_window():
        log_file = f"log_windows_{current_date}.txt"
        log_filedir = os.path.join(log_file_window_path, log_file)

    else:
        log_file = f"log_linux_{current_date}.txt"
        log_filedir = os.path.join(log_file_linux_path, log_file)

    return log_filedir


def setup_logging_config():
    """
    日志配置模块   配置logger, 使得日志既能够在控制台打印,又能输出到.log的日志文件中
    Returns:
    """
    # 获取并配置 root logger
    logger = logging.getLogger()

    if not logger.hasHandlers():
        # 配置控制台日志处理器
        console_handler = colorlog.StreamHandler()

        # 获取当前日期并生成日志文件名
        current_date = datetime.now().strftime('%Y-%m-%d')

        # 根据操作系统类型设置日志文件路径
        if platform.system() == "Windows":
            log_file_window_filename = f'log_windows_{current_date}.txt'
            log_file_window = os.path.join(log_file_window_path, log_file_window_filename)
            log_file_path = log_file_window  # Windows 下的路径
        else:
            log_file_linux_filename = f'log_linux_{current_date}.txt'
            log_file_linux = os.path.join(log_file_linux_path, log_file_linux_filename)
            log_file_path = log_file_linux    # Linux 下的路径

        # 配置文件日志处理器（滚动日志）
        file_handler = RotatingFileHandler(log_file_path, maxBytes=1000000, backupCount=3, mode='a')

        # 设置彩色日志的格式，包含时间、日志级别和消息内容
        console_formatter = colorlog.ColoredFormatter(
            '%(log_color)s%(asctime)s - %(levelname)s - %(message)s',
            log_colors={
                'DEBUG': 'cyan',
                'INFO': 'green',  # 将 INFO 级别设为绿色
                'WARNING': 'yellow',
                'ERROR': 'red',
                'CRITICAL': 'bold_red',
            }
        )

        # 设置文件日志格式
        file_formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')

        # 将格式应用到处理器
        console_handler.setFormatter(console_formatter)
        file_handler.setFormatter(file_formatter)

        # 添加处理器到 logger
        logger.addHandler(console_handler)
        logger.addHandler(file_handler)

    # 设置日志级别
    logger.setLevel(logging.INFO)


def send_log_via_email():

    personal_property_dict = read_personal_property()

    # 发件人信息
    sender_email = personal_property_dict['sender_email']
    sender_password = personal_property_dict['sender_password']

    # 收件人信息
    receiver_email = personal_property_dict['receiver_email']

    # 获取当前日期并生成日志文件名
    current_date = datetime.now().strftime('%Y-%m-%d')

    # 构建邮件
    msg = MIMEMultipart()
    msg['From'] = sender_email
    msg['To'] = receiver_email
    msg['Subject'] = f"python 发送quant项目 {current_date} 的日志文件"

    # 邮件正文
    body = f"{current_date} 日的日志文件请见附件"
    msg.attach(MIMEText(body, 'plain'))

    # 添加附件
    filename = read_logfile()
    attachment = open(filename, "rb")
    part = MIMEBase('application', 'octet-stream')
    part.set_payload((attachment).read())
    encoders.encode_base64(part)
    part.add_header('Content-Disposition', "attachment; filename= %s" % os.path.basename(filename))
    msg.attach(part)

    # 连接到SMTP服务器
    server = smtplib.SMTP_SSL('smtp.139.com', 465)
    server.login(sender_email, sender_password)

    # 发送邮件
    text = msg.as_string()
    server.sendmail(sender_email, receiver_email, text)

    # 关闭连接
    server.quit()

    logging.info("邮件发送成功！")










if __name__ == '__main__':

    send_log_via_email()




