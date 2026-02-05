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


from datas_prepare.C01_data_download_daily.download_insight_data_afternoon import SaveInsightData
from datas_prepare.C01_data_download_daily.download_akshare_data_afternoon import SaveAkshareDailyData
from datas_prepare.C01_data_download_daily.download_tushare_data_afternoon import SaveTushareDailyData

from datas_prepare.C03_data_merge.merge_insight_data_afternoon import MergeInsightData
from datas_prepare.C04_data_DWD.calculate_DWD_datas import CalDWD
from datas_prepare.C05_data_MART.calculate_MART_datas import CalDMART

import CommonProperties.set_config as set_config


class RunDataDaily:

    def __init__(self):
        self.save_insight_now = SaveInsightData()
        self.save_tushare_data = SaveTushareDailyData()
        self.save_akshare_data = SaveAkshareDailyData()
        # self.save_vantage_now = SaveVantageData()
        self.merge_insight = MergeInsightData()
        self.dwd_cal = CalDWD()
        self.dmart_cal = CalDMART()

    def send_logfile_email(self):
        """
        聚合后发送邮件的服务
        """
        set_config.send_log_via_email()


    def setup(self):

        #  下载 insight 当日数据
        self.save_insight_now.setup()

        #  下载 akshare 当日数据
        self.save_akshare_data.setup()

        #  下载 tushare 当日数据
        self.save_tushare_data.setup()

        # #  下载 vantage 当日数据
        # self.save_vantage_now.setup()

        #  合并 insight 当日跑批的数据至历史数据中
        self.merge_insight.setup()

        #  执行 DWD层逻辑
        self.dwd_cal.setup()

        #  执行 MART层逻辑
        self.dmart_cal.setup()

        #  发送邮件
        self.send_logfile_email()


if __name__ == '__main__':
    run_data_prepare = RunDataDaily()
    run_data_prepare.setup()

