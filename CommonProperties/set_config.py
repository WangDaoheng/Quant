# set_config.py
import logging
import colorlog
from logging.handlers import RotatingFileHandler
import os
import platform
import CommonProperties.Base_utils as base_utils

from CommonProperties.Base_Properties import log_file_linux_path, log_file_window_path
import CommonProperties.Base_Properties as base_properties


def setup_logging_config():
    # 获取并配置 root logger
    logger = logging.getLogger()

    if not logger.hasHandlers():
        # 配置控制台日志处理器
        console_handler = colorlog.StreamHandler()

        # 根据操作系统类型设置日志文件路径
        if platform.system() == "Windows":
            log_file_window_filename = base_utils.save_out_filename(filehead='log_windows', file_type='log')
            log_file_window = os.path.join(log_file_window_path, log_file_window_filename)
            log_file_path = log_file_window  # Windows 下的路径
        else:
            log_file_linux_filename = base_utils.save_out_filename(filehead='log_linux', file_type='log')
            log_file_linux = os.path.join(log_file_linux_path, log_file_linux_filename)
            log_file_path = log_file_linux    # Linux 下的路径

        # 配置文件日志处理器（滚动日志）
        file_handler = RotatingFileHandler(log_file_path, maxBytes=1000000, backupCount=3)

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






