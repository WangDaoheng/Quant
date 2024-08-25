

from datas_prepare.C01_data_download_daily.download_insight_data_afternoon import SaveInsightData
from datas_prepare.C01_data_download_daily.download_insight_data_afternoon_of_history import SaveInsightHistoryData
from datas_prepare.C01_data_download_daily.download_vantage_data_afternoon import SaveVantageData

from datas_prepare.C02_data_merge.merge_insight_data_afternoon import MergeInsightData



# ************************************************************************
# 本代码的作用是   运行整个 DataPrepare 工作
# 主要功能模块：
#   1. 数据下载        01_data_download
#      当日数据下载
#         download_insight_data_afternoon.py
#         download_vantage_data_afternoon.py
#      历史数据下载
#         download_insight_data_afternoon_of_history
#
#   2. 数据merge      C02_data_merge
#         merge_insight_data_afternoon.py
#
# ************************************************************************



class RunDataPrepare:

    def __init__(self):
        self.save_insight_now = SaveInsightData()
        # self.save_insight_history = SaveInsightHistoryData()
        self.save_vantage_now = SaveVantageData()
        self.merge_insight = MergeInsightData()

    def setup(self):

        #  下载 insight 当日数据
        self.save_insight_now.setup()

        #  合并 insight 当日数据至历史数据中
        self.merge_insight.setup()

        #  下载 vantage 当日数据
        self.save_vantage_now.setup()



if __name__ == '__main__':
    run_data_prepare = RunDataPrepare()
    run_data_prepare.setup()



























