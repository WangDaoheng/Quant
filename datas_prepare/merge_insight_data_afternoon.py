import os
from datetime import datetime

import pandas as pd
from sqlalchemy import create_engine, text
import time

# import dataprepare_properties
# import dataprepare_utils
import CommonProperties.Base_Properties as base_properties
import CommonProperties.Base_utils as base_utils
from CommonProperties.DateUtility import DateUtility
from CommonProperties.Base_utils import timing_decorator
import CommonProperties.Mysql_Utils as mysql_utils


# ************************************************************************
# 本代码的作用是下午收盘后针对 insight 行情源数据的本地保存部分开展merge
# 需要下载的数据:
# 1.上市股票代码   get_all_stocks()
# 2.筹码分布数据   get_chouma_datas()


# ************************************************************************


class SaveInsightHistoryData:

    def __init__(self):

        self.init_dirs()

        self.init_variant()

    def init_dirs(self):
        """
        关键路径初始化
        """
        #  文件路径_____insight文件当下数据基础路径
        self.dir_insight_base = base_properties.dir_insight_base

        #  文件路径_____insight文件历史数据基础路径
        self.dir_history_insight_base = base_properties.dir_history_insight_base

        #  文件路径_____insight文件merge数据基础路径
        self.dir_merge_insight_base = base_properties.dir_merge_insight_base


        ##  聚合全量的日k 数据
        #  文件路径_____上市交易股票的当下日k线数据
        self.dir_stock_kline_base = os.path.join(self.dir_insight_base, 'stock_kline')

        #  文件路径_____上市交易股票的历史日k线数据
        self.dir_history_stock_kline_base = os.path.join(self.dir_history_insight_base, 'stock_kline')

        #  文件路径_____上市交易股票的merge日k线数据
        self.dir_merge_stock_kline_base = os.path.join(self.dir_merge_insight_base, 'stock_kline')



        #  文件路径_____关键大盘指数
        self.dir_history_index_a_share_base = os.path.join(self.dir_history_insight_base, 'index_a_share')

        #  文件路径_____涨跌停数量
        self.dir_history_limit_summary_base = os.path.join(self.dir_history_insight_base, 'limit_summary')

        #  文件路径_____内盘期货
        self.dir_history_future_inside_base = os.path.join(self.dir_history_insight_base, 'future_inside')

        #  文件路径_____筹码数据
        self.dir_history_chouma_base = os.path.join(self.dir_history_insight_base, 'chouma')


    def init_variant(self):
        """
        结果变量初始化
        """
        #  除去 ST|退|B 的五要素   [ymd	htsc_code	name	exchange]
        self.stock_code_df = pd.DataFrame()

        #  获取上述股票的历史数据   日K级别
        self.kline_total_history = pd.DataFrame()

        #  获得A股市场的股指 [htsc_code 	time	frequency	open	close	high	low	volume	value]
        self.index_a_share = pd.DataFrame()

        #  大盘涨跌停数量          [time	name	今日涨停	今日跌停	昨日涨停	昨日跌停	昨日涨停表现]
        self.limit_summary_df = pd.DataFrame()

        #  期货市场数据    原油  贵金属  有色
        self.future_index = pd.DataFrame()

        #  可以获取筹码的股票数据
        self.stock_chouma_available = ""




    @timing_decorator
    def merge_stock_kline(self):
        """
        将 stock_kline 的历史数据和当月数据做merge
        :return:
         stock_kline_df  [ymd	htsc_code	name	exchange]
        """

        source_table = 'stock_kline_daily_insight_now'
        target_table = 'stock_kline_daily_insight'
        columns = []



        mysql_utils.upsert_table(source_table='', target_table='', columns=[], database='quant')


    @timing_decorator
    def merge_index_a_share(self):
        """
        000001.SH    上证指数
        399006.SZ	 创业板指
        000016.SH    上证50
        000300.SH    沪深300
        000849.SH    沪深300非银行金融指数
        000905.SH	 中证500
        399852.SZ    中证1000
        000688.SH    科创50

        Returns:
             index_a_share   [htsc_code 	time	frequency	open	close	high	low	volume	value]
        """



    @timing_decorator
    def get_limit_summary(self):
        """
        大盘涨跌停分析数据
        Args:
            market:
                1	sh_a_share	上海A股
                2	sz_a_share	深圳A股
                3	a_share	A股
                4	a_share	B股
                5	gem	创业
                6	sme	中小板
                7	star	科创板
            trading_day: List<datetime>	交易日期范围，[start_date, end_date]

        Returns: ups_downs_limit_count_up_limits
                 ups_downs_limit_count_down_limits
                 ups_downs_limit_count_pre_up_limits
                 ups_downs_limit_count_pre_down_limits
                 ups_downs_limit_count_pre_up_limits_average_change_percent

                 [time	name	今日涨停	今日跌停	昨日涨停	昨日跌停	昨日涨停表现]

        """




    @timing_decorator
    def get_future_inside(self):
        """
        期货市场数据
        贵金属,  有色数据
        国际市场  国内市场
        AU9999.SHF    沪金主连
        AU2409.SHF	  沪金
        AG9999.SHF    沪银主连
        AG2409.SHF    沪银
        CU9999.SHF    沪铜主连
        CU2409.SHF    沪铜

        EC9999.INE    欧线集运主连
        EC2410.INE    欧线集运
        SC9999.INE    原油主连
        SC2410.INE    原油

        V9999.DCE     PVC主连
        V2409.DCE     PVC
        MA9999.ZCE    甲醇主连      (找不到)
        MA2409.ZCE    甲醇         (找不到)
        目前主连找不到数据，只有月份的，暂时用 t+2 月去代替主连吧

        Returns:
        """



    @timing_decorator
    def get_chouma_datas(self):
        """
        1.获取每日的筹码分布数据
        2.找到那些当日能够拿到筹码数据的codes
        :return:
        """



    def setup(self):

        #  获取当前已上市股票过去3年到今天的历史kline
        self.merge_stock_kline()


        #  获取主要股指
        # self.get_index_a_share()

        #  大盘涨跌概览
        # self.get_limit_summary()

        #  期货__内盘
        # self.get_future_inside()

        #  筹码概览
        # self.get_chouma_datas()


if __name__ == '__main__':
    save_insight_data = SaveInsightHistoryData()
    save_insight_data.setup()
