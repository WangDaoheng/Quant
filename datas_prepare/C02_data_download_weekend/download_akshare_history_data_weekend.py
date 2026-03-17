# -*- coding: utf-8 -*-

import random
import time
import requests
import pandas as pd
import akshare as ak
import logging

from datas_prepare.Downloaders.akshareDownloader import AkshareDownloader

import CommonProperties.Base_Properties as base_properties
import CommonProperties.Mysql_Utils as mysql_utils
from CommonProperties.DateUtility import DateUtility
from CommonProperties.Base_utils import timing_decorator
from CommonProperties.set_config import setup_logging_config

# ************************************************************************
# 本代码的作用是周末使用akshare数据源下载历史数据, 写入mysql表
# 需要下载的akshare数据表（从ODS层SQL定义）:
# 1. ods_akshare_stock_value_em          股票基本面数据_估值数据
# 2. ods_akshare_stock_zh_a_gdhs_detail_em 股票基本面数据_股东数据
# 3. ods_akshare_stock_cyq_em            股票基本面数据_筹码数据
# 4. ods_akshare_stock_yjkb_em           股票基本面数据_业绩快报数据
# 5. ods_akshare_stock_yjyg_em           股票基本面数据_业绩预告数据
# 6. ods_akshare_stock_a_high_low_statistics 大盘情绪数据
# 7. ods_akshare_stock_zh_a_spot_em      行情数据_个股行情数据
# 8. ods_akshare_stock_board_concept_name_em 行情数据_板块行情数据
# 9. ods_akshare_stock_board_concept_cons_em 行情数据_板块内个股行情数据
# 10. ods_akshare_stock_board_concept_hist_em 行情数据_板块历史行情数据
# ************************************************************************

# 调用日志配置
setup_logging_config()

######################  mysql 配置信息  本地和远端服务器  ####################
local_user = base_properties.local_mysql_user
local_password = base_properties.local_mysql_password
local_database = base_properties.local_mysql_database
local_host = base_properties.local_mysql_host

origin_user = base_properties.origin_mysql_user
origin_password = base_properties.origin_mysql_password
origin_database = base_properties.origin_mysql_database
origin_host = base_properties.origin_mysql_host


class SaveAkshareWeekendData:
    """周末执行，下载akshare历史数据到MySQL"""

    def __init__(self):
        """
        结果变量初始化
        """
        # 用于存储当前处理的数据
        self.current_df = pd.DataFrame()
        # 股票代码列表缓存
        self.stock_codes = []
        # 初始化 Akshare下载器
        self.downloader = AkshareDownloader()

    @timing_decorator
    def get_stock_codes(self):
        """
        获取最新的股票代码列表
        Returns: 股票代码列表
        """
        try:
            # 从MySQL获取最新的股票代码
            stock_df = mysql_utils.data_from_mysql_to_dataframe_latest(
                user=origin_user,
                password=origin_password,
                host=origin_host,
                database=origin_database,
                table_name="ods_stock_code_daily_insight",
                cols=['stock_code']
            )

            if not stock_df.empty:
                # 清理股票代码，移除后缀
                stock_df['clean_code'] = stock_df['stock_code'].str.split('.').str[0]
                self.stock_codes = stock_df['clean_code'].tolist()
                self.downloader.set_stock_codes(self.stock_codes)

                logging.info(f"获取到 {len(self.stock_codes)} 个股票代码（已清理后缀）")
            else:
                logging.warning("股票代码表为空")
                self.stock_codes = []

        except Exception as e:
            logging.error(f"获取股票代码失败: {str(e)}")
            self.stock_codes = []


    # @timing_decorator
    def download_stock_value_em(self):
        """
        下载股票估值数据 - ods_akshare_stock_value_em
        流式处理：边下载边处理边保存，避免内存溢出
        """
        column_mapping = {
            '数据日期': 'ymd',
            '当日收盘价': 'close',
            '当日涨跌幅': 'change_pct',
            '总市值': 'total_market',
            '流通市值': 'circulation_market',
            '总股本': 'total_shares',
            '流通股本': 'circulation_shares',
            'PE(TTM)': 'pe_ttm',
            'PE(静)': 'pe_static',
            '市净率': 'pb',
            'PEG值': 'peg',
            '市现率': 'pcf',
            '市销率': 'ps'
        }

        numeric_columns = [
            'close', 'change_pct', 'total_market', 'circulation_market',
            'total_shares', 'circulation_shares', 'pe_ttm', 'pe_static',
            'pb', 'peg', 'pcf', 'ps'
        ]

        # 直接调用优化后的下载方法
        return self.downloader.download_to_mysql_stream(
            ak_function_name='stock_value_em',
            table_name='ods_akshare_stock_value_em',
            column_mapping=column_mapping,
            numeric_columns=numeric_columns,
            date_format='%Y-%m-%d',
            merge_on=['ymd', 'stock_code'],
            auto_add_stock_code=True
        )


    # @timing_decorator
    def download_stock_zh_a_gdhs_detail_em(self):
        """
        下载股东户数数据 - ods_akshare_stock_zh_a_gdhs_detail_em
        接口: stock_zh_a_gdhs_detail_em
        说明: 个股的全量历史数据，不可选定日期   建议周末跑
        """
        column_mapping = {
            '股东户数统计截止日': 'ymd',
            '代码': 'stock_code',
            '名称': 'stock_name',
            '区间涨跌幅': 'range_change_pct',
            '股东户数-本次': 'holder_num_current',
            '股东户数-上次': 'holder_num_last',
            '股东户数-增减': 'holder_num_change',
            '股东户数-增减比例': 'holder_num_change_pct',
            '户均持股市值': 'avg_holder_market',
            '户均持股数量': 'avg_holder_share_num',
            '总市值': 'total_market',
            '总股本': 'total_shares',
            '股本变动': 'share_change',
            '股本变动原因': 'share_change_reason',
            '股东户数公告日期': 'holder_num_announce_date'
        }

        numeric_columns = [
            'range_change_pct', 'holder_num_current', 'holder_num_last',
            'holder_num_change', 'holder_num_change_pct', 'avg_holder_market',
            'avg_holder_share_num', 'total_market', 'total_shares', 'share_change'
        ]

        return self.downloader.download_to_mysql(
            ak_function_name='stock_zh_a_gdhs_detail_em',
            table_name='ods_akshare_stock_zh_a_gdhs_detail_em',
            column_mapping=column_mapping,
            numeric_columns=numeric_columns,
            date_format='%Y-%m-%d',
            merge_on=['ymd', 'stock_code'],
            auto_add_stock_code=False
        )


    # @timing_decorator
    def setup(self):
        """
        主执行函数，按顺序下载所有akshare数据
        注意：由于akshare接口的限制，部分数据需要分批次或指定参数获取
        """
        logging.info("======= 开始下载akshare历史数据 =======")

        # 1. 获取股票代码列表（用于需要股票代码的接口）
        self.get_stock_codes()

        # 2. 下载股票估值数据                          可用   【周末跑】  只能跑全量 800w+条
        self.download_stock_value_em()

        # 3. 下载股东户数数据（需要股票代码，分批次处理）   可用   【周末跑】
        self.download_stock_zh_a_gdhs_detail_em()
        #


if __name__ == '__main__':

    # # 只在周末执行
    # if DateUtility.is_weekend():
    #     logging.info("今天是周末，开始执行akshare历史数据下载任务")
    #     saver = SaveAkshareWeekendData()
    #     saver.setup()
    # else:
    #     logging.info("今天不是周末，跳过akshare历史数据下载")

    saver = SaveAkshareWeekendData()
    saver.setup()





