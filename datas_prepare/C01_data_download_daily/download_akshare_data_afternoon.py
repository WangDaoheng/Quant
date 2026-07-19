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
from CommonProperties.Base_utils import timing_decorator, script_run
from CommonProperties.set_config import setup_logging_config

# ************************************************************************
# 本代码的作用是周末使用akshare数据源下载历史数据, 写入mysql表
# 需要下载的akshare数据表（从ODS层SQL定义）:
# 1. ods_akshare_stock_value_em          股票基本面数据_估值数据
# 2. ods_akshare_stock_zh_a_gdhs_detail_em 股票基本面数据_股东数据
# 4. ods_akshare_stock_yjkb_em           股票基本面数据_业绩快报数据
# 5. ods_akshare_stock_yjyg_em           股票基本面数据_业绩预告数据
# 6. ods_akshare_stock_a_high_low_statistics 大盘情绪数据
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


class SaveAkshareDailyData:
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


    @timing_decorator
    def download_stock_value_em(self):
        """
        下载股票估值数据 - ods_akshare_stock_value_em
        接口: stock_value_em
        说明: 个股的全量历史数据，需要逐个股票获取    封堵IP  不可用
        """

        # 根据实际列名进行映射
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

        # 需要转化为数字类型的列
        numeric_columns = [
            'close', 'change_pct', 'total_market', 'circulation_market',
            'total_shares', 'circulation_shares', 'pe_ttm', 'pe_static',
            'pb', 'peg', 'pcf', 'ps'
        ]

        return self.downloader.download_to_mysql(
            ak_function_name='stock_value_em',
            table_name='ods_akshare_stock_value_em',
            column_mapping=column_mapping,
            numeric_columns=numeric_columns,
            date_format='%Y-%m-%d',
            merge_on=['ymd', 'stock_code'],
            auto_add_stock_code=True
        )


    @timing_decorator
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


    @timing_decorator
    def download_stock_yjkb_em(self):
        """
        下载业绩快报数据 - ods_akshare_stock_yjkb_em
        接口: stock_yjkb_em
        说明: 全量的每日切片数据，需要指定日期（YYYY0331, YYYY0630, YYYY0930, YYYY1231）   日跑
        """
        try:
            # 获取当前年份和过去几年的数据
            current_year = int(DateUtility.today()[:4])
            years = list(range(2025, current_year + 1))  # 从2025年开始，库里已有2020数据

            # 季度对应的日期后缀
            quarter_dates = ["0331", "0630", "0930", "1231"]

            all_data = pd.DataFrame()

            # 先收集所有数据
            for year in years:
                for date_suffix in quarter_dates:
                    date_str = f"{year}{date_suffix}"
                    try:
                        df = ak.stock_yjkb_em(date=date_str)
                        if not df.empty:
                            all_data = pd.concat([all_data, df], ignore_index=True)
                    except:
                        continue

            if all_data.empty:
                logging.warning("业绩快报数据为空")
                return False

            # 列映射
            column_mapping = {
                '序号': 'serial_num',
                '股票代码': 'stock_code',
                '股票简称': 'stock_name',
                '每股收益': 'eps',
                '营业收入-营业收入': 'income',
                '营业收入-去年同期': 'income_last_year',
                '营业收入-同比增长': 'income_yoy',
                '营业收入-季度环比增长': 'income_qoq',
                '净利润-净利润': 'profit',
                '净利润-去年同期': 'profit_last_year',
                '净利润-同比增长': 'profit_yoy',
                '净利润-季度环比增长': 'profit_qoq',
                '每股净资产': 'asset_per_share',
                '净资产收益率': 'roe',
                '所处行业': 'industry',
                '公告日期': 'ymd'
            }

            numeric_columns = [
                'serial_num', 'eps', 'income', 'income_last_year', 'income_yoy', 'income_qoq',
                'profit', 'profit_last_year', 'profit_yoy', 'profit_qoq', 'asset_per_share', 'roe'
            ]

            # 处理数据
            processed_df = self.downloader._process_data(
                all_data=all_data,
                column_mapping=column_mapping,
                date_column='ymd',
                date_format='%Y-%m-%d',
                numeric_columns=numeric_columns,
                table_name='ods_akshare_stock_yjkb_em'
            )

            if processed_df.empty:
                return False

            # 保存到MySQL
            return self.downloader._save_to_mysql(
                df=processed_df,
                table_name='ods_akshare_stock_yjkb_em',
                merge_on=['ymd', 'stock_code']
            )

        except Exception as e:
            logging.error(f"下载业绩快报数据失败: {str(e)}")
            return False


    @timing_decorator
    def download_stock_yjyg_em(self):
        """
        下载业绩预告数据 - ods_akshare_stock_yjyg_em
        接口: stock_yjyg_em
        说明: 全量的每日切片数据，需要指定日期（YYYY0331, YYYY0630, YYYY0930, YYYY1231）   日跑
        """
        try:
            # 获取当前年份和过去几年的数据
            current_year = int(DateUtility.today()[:4])
            years = list(range(2026, current_year + 1))  # 从2026年开始，库里已有2020数据

            # 季度对应的日期后缀
            quarter_dates = ["0331", "0630", "0930", "1231"]

            all_data = pd.DataFrame()

            # 先收集所有数据
            for year in years:
                for date_suffix in quarter_dates:
                    date_str = f"{year}{date_suffix}"
                    try:
                        df = ak.stock_yjyg_em(date=date_str)
                        if df is not None and not df.empty:
                            all_data = pd.concat([all_data, df], ignore_index=True)
                    except:
                        continue

            if all_data.empty:
                logging.warning("业绩预告数据为空")
                return False

            # 列映射
            column_mapping = {
                '公告日期': 'ymd',
                '序号': 'serial_num',
                '股票代码': 'stock_code',
                '股票简称': 'stock_name',
                '预测指标': 'forecast_index',
                '业绩变动': 'performance_change',
                '预测数值': 'forecast_value',
                '业绩变动幅度': 'change_pct',
                '业绩变动原因': 'change_reason',
                '预告类型': 'forecast_type',
                '上年同期值': 'last_year_value'
            }

            numeric_columns = ['serial_num', 'forecast_value', 'change_pct', 'last_year_value']

            # 处理数据
            processed_df = self.downloader._process_data(
                all_data=all_data,
                column_mapping=column_mapping,
                date_column='ymd',
                date_format='%Y-%m-%d',
                numeric_columns=numeric_columns,
                table_name='ods_akshare_stock_yjyg_em'
            )

            if processed_df.empty:
                return False

            # 保存到MySQL
            return self.downloader._save_to_mysql(
                df=processed_df,
                table_name='ods_akshare_stock_yjyg_em',
                merge_on=['ymd', 'stock_code']
            )

        except Exception as e:
            logging.error(f"下载业绩预告数据失败: {str(e)}")
            return False


    @timing_decorator
    def download_stock_a_high_low_statistics(self):
        """
        下载大盘高低统计数据 - ods_akshare_stock_a_high_low_statistics
        下载所有市场类型：全部A股、上证50、沪深300、中证500
        """
        # 复用通用的数据处理和保存逻辑
        markets = ["all", "sz50", "hs300", "zz500"]

        # 获取数据
        all_data = pd.DataFrame()
        for market in markets:
            try:
                df = ak.stock_a_high_low_statistics(symbol=market)
                if df is not None and not df.empty:
                    df['market'] = market
                    all_data = pd.concat([all_data, df], ignore_index=True)
            except Exception as e:
                logging.warning(f"获取 {market} 数据失败: {str(e)[:100]}")
                continue

        if all_data.empty:
            logging.warning("大盘高低统计数据为空")
            return False

        # 直接使用downloader的数据处理和保存方法
        column_mapping = {
            'date': 'ymd',
            'close': 'close',
            'high20': 'high20',
            'low20': 'low20',
            'high60': 'high60',
            'low60': 'low60',
            'high120': 'high120',
            'low120': 'low120'
        }

        numeric_columns = [
            'close', 'high20', 'low20', 'high60',
            'low60', 'high120', 'low120'
        ]

        # 使用_process_data处理数据
        processed_df = self.downloader._process_data(
            all_data=all_data,
            column_mapping=column_mapping,
            date_column='ymd',
            date_format='%Y%m%d',
            numeric_columns=numeric_columns,
            table_name='ods_akshare_stock_a_high_low_statistics'
        )

        # 使用_save_to_mysql保存数据
        if not processed_df.empty:
            return self.downloader._save_to_mysql(
                df=processed_df,
                table_name='ods_akshare_stock_a_high_low_statistics',
                merge_on=['ymd', 'market']
            )
        return False


    # @timing_decorator
    # def download_stock_board_concept_name_ths(self):
    #     """
    #     下载同花顺概念板块基本信息 - ods_akshare_board_concept_name_ths
    #     接口: stock_board_concept_name_ths
    #     说明: 获取同花顺所有概念板块的基本信息
    #     """
    #     try:
    #         logging.info("开始下载同花顺概念板块数据...")
    #
    #         # 获取所有同花顺概念板块数据
    #         df = ak.stock_board_concept_name_ths()
    #
    #         if df.empty:
    #             logging.warning("同花顺概念板块数据为空")
    #             return False
    #
    #         # 添加日期列
    #         today = DateUtility.today()
    #         df['ymd'] = today
    #
    #         logging.info(f"同花顺概念板块数据获取完成，共 {len(df)} 条记录")
    #
    #         # 列映射
    #         column_mapping = {
    #             'name': 'board_name',
    #             'code': 'board_code'
    #         }
    #
    #         # 使用downloader的数据处理方法
    #         processed_df = self.downloader._process_data(
    #             all_data=df,
    #             column_mapping=column_mapping,
    #             date_column='ymd',
    #             date_format='%Y%m%d',
    #             numeric_columns=None,
    #             table_name='ods_akshare_board_concept_name_ths'
    #         )
    #
    #         if processed_df.empty:
    #             logging.warning("同花顺概念板块数据处理后为空")
    #             return False
    #
    #         # # 在数据处理后清理board_name列的空格
    #         # if 'board_name' in processed_df.columns:
    #         #     # 移除所有空格
    #         #     processed_df['board_name'] = processed_df['board_name'].str.replace(' ', '')
    #
    #         # 删除重复记录
    #         if 'ymd' in processed_df.columns and 'board_code' in processed_df.columns:
    #             processed_df = processed_df.drop_duplicates(subset=['ymd', 'board_code'], keep='first')
    #
    #         # 使用downloader的保存方法
    #         success = self.downloader._save_to_mysql(
    #             df=processed_df,
    #             table_name='ods_akshare_board_concept_name_ths',
    #             merge_on=['ymd', 'board_code']
    #         )
    #
    #         if success:
    #             logging.info(
    #                 f"同花顺概念板块数据保存成功，共 {len(processed_df)} 条记录，{processed_df['board_code'].nunique()} 个概念")
    #         else:
    #             logging.error("同花顺概念板块数据保存失败")
    #
    #         return success
    #
    #     except Exception as e:
    #         logging.error(f"下载同花顺概念板块数据失败: {str(e)}")
    #         import traceback
    #         logging.error(traceback.format_exc())
    #         return False


    # @timing_decorator
    # def download_stock_board_concept_index_ths(self, start_date=None, end_date=None):
    #     """
    #     下载同花顺概念板块指数数据 - ods_akshare_stock_board_concept_index_ths
    #     接口: stock_board_concept_index_ths
    #     说明: 遍历所有概念板块的历史指数数据
    #     """
    #     try:
    #         # 如果没有指定日期，使用默认范围
    #         if start_date is None:
    #             start_date = DateUtility.first_day_of_month()  # 本月第一天
    #         if end_date is None:
    #             end_date = DateUtility.today()
    #
    #         # 从MySQL获取最新的同花顺概念板块列表
    #         logging.info("开始获取同花顺概念板块列表...")
    #
    #         board_df = mysql_utils.data_from_mysql_to_dataframe_latest(
    #             user=self.downloader.origin_user,
    #             password=self.downloader.origin_password,
    #             host=self.downloader.origin_host,
    #             database=self.downloader.origin_database,
    #             table_name="ods_akshare_board_concept_name_ths",
    #             cols=['board_name', 'board_code']
    #         )
    #
    #         # 去重并获取概念名称列表
    #         board_names = board_df['board_name'].dropna().unique().tolist()
    #         total = len(board_names)
    #         logging.info(f"开始下载 {total} 个概念板块的指数数据，日期: {start_date}~{end_date}")
    #
    #         all_data = pd.DataFrame()
    #         success_concepts = []
    #         failed_concepts = []
    #
    #         for i, board_name in enumerate(board_names):
    #             try:
    #                 # 跳过可能为空的板块名
    #                 if pd.isna(board_name) or not str(board_name).strip():
    #                     continue
    #
    #                 # 改为 DEBUG 级别，避免日志刷屏
    #                 logging.debug(f"下载概念板块 [{i + 1}/{total}]: {board_name}")
    #
    #                 # 获取概念板块指数数据
    #                 df = ak.stock_board_concept_index_ths(
    #                     symbol=str(board_name).strip(),
    #                     start_date=start_date,
    #                     end_date=end_date
    #                 )
    #
    #                 if not df.empty:
    #                     # 添加概念板块信息
    #                     df['board_name'] = str(board_name).strip()
    #                     # 查找对应的concept_code
    #                     concept_code_row = board_df[board_df['board_name'] == board_name]
    #                     if not concept_code_row.empty:
    #                         df['board_code'] = concept_code_row.iloc[0]['board_code']
    #                     else:
    #                         df['board_code'] = str(board_name).strip()
    #
    #                     all_data = pd.concat([all_data, df], ignore_index=True)
    #                     success_concepts.append(board_name)
    #
    #                     logging.debug(f"  {board_name}: 获取到 {len(df)} 条记录")
    #                 else:
    #                     logging.warning(f"  {board_name}: 指数数据为空")
    #                     failed_concepts.append(board_name)
    #
    #                 # 添加延迟以避免封IP
    #                 time.sleep(random.uniform(0.5, 1.5))
    #
    #                 # 每50个打印一次进度，或最后几个
    #                 if (i + 1) % 50 == 0 or (i + 1) == total:
    #                     logging.info(f"进度: [{i + 1}/{total}] 成功 {len(success_concepts)} 失败 {len(failed_concepts)}")
    #
    #             except Exception as e:
    #                 error_msg = str(e)
    #                 if "404" in error_msg or "无法获取" in error_msg:
    #                     logging.warning(f"  {board_name}: 可能不存在或无法访问")
    #                 else:
    #                     logging.error(f"  下载 {board_name} 失败: {error_msg[:100]}")
    #                 failed_concepts.append(board_name)
    #                 time.sleep(2)
    #                 continue
    #
    #         # 最终汇总
    #         logging.info(f"概念板块下载完成: 成功 {len(success_concepts)} 个, 失败 {len(failed_concepts)} 个, 总记录 {len(all_data)} 条")
    #
    #         # 列映射
    #         column_mapping = {
    #             '日期': 'ymd',
    #             '开盘价': 'open',
    #             '最高价': 'high',
    #             '最低价': 'low',
    #             '收盘价': 'close',
    #             '成交量': 'trading_volume',
    #             '成交额': 'trading_amount'
    #         }
    #
    #         numeric_columns = [
    #             'open', 'close', 'high', 'low',
    #             'trading_volume', 'trading_amount'
    #         ]
    #
    #         # 使用downloader的数据处理方法
    #         processed_df = self.downloader._process_data(
    #             all_data=all_data,
    #             column_mapping=column_mapping,
    #             date_column='ymd',
    #             date_format='%Y-%m-%d',
    #             numeric_columns=numeric_columns,
    #             table_name='ods_akshare_stock_board_concept_index_ths'
    #         )
    #
    #         # 使用downloader的保存方法
    #         success = self.downloader._save_to_mysql(
    #             df=processed_df,
    #             table_name='ods_akshare_stock_board_concept_index_ths',
    #             merge_on=['ymd', 'board_code']
    #         )
    #         return success
    #
    #     except Exception as e:
    #         logging.error(f"下载概念板块指数数据失败: {str(e)}")
    #         import traceback
    #         logging.error(traceback.format_exc())
    #         return False



    @script_run(script_name="download_akshare_data_afternoon.py")
    def setup(self):
        """
        主执行函数，按顺序下载所有akshare数据
        注意：由于akshare接口的限制，部分数据需要分批次或指定参数获取
        """
        logging.info("======= 开始下载akshare历史数据 =======")

        # 1. 获取股票代码列表（用于需要股票代码的接口）
        self.get_stock_codes()

        # # 2. 下载股票估值数据            封堵IP 办公IP可用 但下载800w+ 记录 【周末跑】
        # self.download_stock_value_em()
        #
        # # 3. 下载股东户数数据（需要股票代码，分批次处理）   可用              【周末跑】
        # self.download_stock_zh_a_gdhs_detail_em()

        # 5. 下载业绩快报数据（指定日期）         【可用】  日跑
        self.download_stock_yjkb_em()

        # 6. 下载业绩预告数据（指定日期）         【可用】  日跑
        self.download_stock_yjyg_em()

        # 7. 下载大盘高低统计数据               【可用】  日跑
        self.download_stock_a_high_low_statistics()


        # # 12. 同花顺板块码值                  废弃改用tushare    日跑
        # self.download_stock_board_concept_name_ths()
        #
        # # 13. 同花顺板块日K行情数据            废弃改用tushare    日跑
        # self.download_stock_board_concept_index_ths()



if __name__ == '__main__':
    downloader = SaveAkshareDailyData()
    downloader.setup()



