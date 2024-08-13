
# logging_config.py
import logging
import colorlog

def setup_logging():
    # 获取并配置 root logger
    logger = logging.getLogger()

    if not logger.hasHandlers():
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
        logger.addHandler(handler)

    logger.setLevel(logging.INFO)















