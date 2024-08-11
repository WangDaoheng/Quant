import sys
import time
import logging
import colorlog
from colorlog import escape_codes


if __name__ =="__main__":

    # 配置日志处理器
    handler = colorlog.StreamHandler()

    # 自定义颜色：深绿色 ANSI 转义序列
    deep_green = "\033[38;5;34m"
    reset_color = "\033[0m"


    # 创建自定义格式化器类，动态设置 INFO 级别的颜色
    class CustomColoredFormatter(colorlog.ColoredFormatter):
        def format(self, record):
            if record.levelno == logging.INFO:
                record.msg = f"{deep_green}{record.msg}{reset_color}"
            return super().format(record)


    # 设置彩色日志的格式，包含时间、日志级别和消息内容
    formatter = CustomColoredFormatter(
        '%(log_color)s%(asctime)s - %(levelname)s - %(message)s',
        log_colors={
            'DEBUG': 'cyan',
            'WARNING': 'yellow',
            'ERROR': 'red',
            'CRITICAL': 'bold_red',
        }
    )

    handler.setFormatter(formatter)

    # 获取并配置 logger
    logger = logging.getLogger("test_logger")
    logger.addHandler(handler)
    logger.setLevel(logging.INFO)

    # 记录不同级别的日志
    logger.info("循环已完成。 logging info")
    logger.warning("循环已完成。 logging warning")
    logger.error("循环已完成 logging error")

    # 确保没有使用过时的方法
    # logger.warn("循环已完成。 logging warn")  # 这行应修改为：
    logger.warning("循环已完成。 logging warn")

    # logging.INFO("logging INFO")  # 这行应修改为：
    logger.info("logging INFO")








