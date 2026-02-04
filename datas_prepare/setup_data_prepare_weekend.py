# -*- coding: utf-8 -*-
import sys
import os

# 获取当前脚本所在目录（/opt/quants/Quant/datas_prepare）
script_dir = os.path.dirname(os.path.abspath(__file__))
# 获取项目根目录（/opt/quants/Quant/，即 script_dir 的父目录）
project_root = os.path.dirname(script_dir)

# 将项目根目录添加到 Python 搜索路径
if project_root not in sys.path:
    sys.path.insert(0, project_root)

from datas_prepare.C02_data_download_weekend.download_akshare_history_data_weekend import SaveAkshareWeekendData

import CommonProperties.set_config as set_config


class RunDataWeekend:

    def __init__(self):
        self.save_akshare_weekend = SaveAkshareWeekendData()

    def send_logfile_email(self):
        """
        聚合后发送邮件的服务
        """
        set_config.send_log_via_email()

    def setup(self):

        #  下载 insight 当日数据
        self.save_akshare_weekend.setup()

        #  发送邮件
        self.send_logfile_email()


if __name__ == '__main__':
    run_data_prepare = RunDataWeekend()
    run_data_prepare.setup()

