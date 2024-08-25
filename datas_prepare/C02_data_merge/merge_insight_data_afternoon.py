import os
from datetime import datetime

import pandas as pd
from sqlalchemy import create_engine, text
import time

# import dataprepare_properties
# import dataprepare_utils
from CommonProperties import Base_Properties
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

######################  mysql 配置信息  本地和远端服务器  ####################
local_user = Base_Properties.local_mysql_user
local_password = Base_Properties.local_mysql_password
local_database = Base_Properties.local_mysql_database
local_host = Base_Properties.local_mysql_host

origin_user = Base_Properties.origin_mysql_user
origin_password = Base_Properties.origin_mysql_password
origin_database = Base_Properties.origin_mysql_database
origin_host = Base_Properties.origin_mysql_host





class MergeInsightData:

    def __init__(self):
        pass


    @timing_decorator
    def merge_stock_kline(self):
        """
        将 stock_kline 的历史数据和当月数据做merge
        :return:
         stock_kline_df  [ymd	htsc_code	name	exchange]
        """
        source_table = 'stock_kline_daily_insight_now'
        target_table = 'stock_kline_daily_insight'
        columns = ['htsc_code', 'ymd', 'open', 'close', 'high', 'low', 'num_trades', 'volume']

        # 对本地 Mysql 做数据聚合
        mysql_utils.upsert_table(user=local_user,
                                 password=local_password,
                                 host=local_host,
                                 database=local_database,
                                 source_table=source_table,
                                 target_table=target_table,
                                 columns=columns)

        # 对远端 Mysql 做数据聚合
        mysql_utils.upsert_table(user=origin_user,
                                 password=origin_password,
                                 host=origin_host,
                                 database=origin_database,
                                 source_table=source_table,
                                 target_table=target_table,
                                 columns=columns)


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
        source_table = 'index_a_share_insight_now'
        target_table = 'index_a_share_insight'
        columns = ['htsc_code', 'name', 'ymd', 'open', 'close', 'high', 'low', 'volume']

        # 对本地 Mysql 做数据聚合
        mysql_utils.upsert_table(user=local_user,
                                 password=local_password,
                                 host=local_host,
                                 database=local_database,
                                 source_table=source_table,
                                 target_table=target_table,
                                 columns=columns)

        # 对远端 Mysql 做数据聚合
        mysql_utils.upsert_table(user=origin_user,
                                 password=origin_password,
                                 host=origin_host,
                                 database=origin_database,
                                 source_table=source_table,
                                 target_table=target_table,
                                 columns=columns)


    @timing_decorator
    def merge_limit_summary(self):
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
        source_table = 'stock_limit_summary_insight_now'
        target_table = 'stock_limit_summary_insight'
        columns = ['ymd', 'name', 'today_ZT', 'today_DT', 'yesterday_ZT', 'yesterday_DT', 'yesterday_ZT_rate']

        # 对本地 Mysql 做数据聚合
        mysql_utils.upsert_table(user=local_user,
                                 password=local_password,
                                 host=local_host,
                                 database=local_database,
                                 source_table=source_table,
                                 target_table=target_table,
                                 columns=columns)

        # 对远端 Mysql 做数据聚合
        mysql_utils.upsert_table(user=origin_user,
                                 password=origin_password,
                                 host=origin_host,
                                 database=origin_database,
                                 source_table=source_table,
                                 target_table=target_table,
                                 columns=columns)


    @timing_decorator
    def merge_future_inside(self):
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
        source_table = 'future_inside_insight_now'
        target_table = 'future_inside_insight'
        columns = ['htsc_code', 'ymd', 'open', 'close', 'high', 'low', 'volume', 'open_interest', 'settle']

        # 对本地 Mysql 做数据聚合
        mysql_utils.upsert_table(user=local_user,
                                 password=local_password,
                                 host=local_host,
                                 database=local_database,
                                 source_table=source_table,
                                 target_table=target_table,
                                 columns=columns)

        # 对远端 Mysql 做数据聚合
        mysql_utils.upsert_table(user=origin_user,
                                 password=origin_password,
                                 host=origin_host,
                                 database=origin_database,
                                 source_table=source_table,
                                 target_table=target_table,
                                 columns=columns)


    def setup(self):

        #  获取当前已上市股票过去3年到今天的历史kline
        self.merge_stock_kline()

        #  获取主要股指
        self.merge_index_a_share()

        #  大盘涨跌概览
        self.merge_limit_summary()

        #  期货__内盘
        self.merge_future_inside()



if __name__ == '__main__':
    save_insight_data = MergeInsightData()
    save_insight_data.setup()
