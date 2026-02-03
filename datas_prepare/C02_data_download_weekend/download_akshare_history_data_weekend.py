# -*- coding: utf-8 -*-

import time
import random
import requests
import os
import sys
import time
import platform
import pandas as pd
import akshare as ak
from datetime import datetime
import logging

from datas_prepare.Downloaders.akshareDownloader import AkshareDownloader

import CommonProperties.Base_Properties as base_properties
import CommonProperties.Base_utils as base_utils
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


class SaveAkshareHistoryData:
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

    # @timing_decorator
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
    def download_stock_cyq_em(self):
        """
        下载筹码数据 - ods_akshare_stock_cyq_em
        接口: stock_cyq_em
        说明: 个股的全量历史数据，需逐一遍历，不可选定日期      封堵IP   不可用
        """
        column_mapping = {
            '日期': 'ymd',
            '获利比例': 'profit_ratio',
            '平均成本': 'avg_cost',
            '90成本-低': 'cost_low_90',
            '90成本-高': 'cost_high_90',
            '90集中度': 'concentration_90',
            '70成本-低': 'cost_low_70',
            '70成本-高': 'cost_high_70',
            '70集中度': 'concentration_70'
        }

        numeric_columns = [
            'profit_ratio', 'avg_cost', 'cost_low_90', 'cost_high_90',
            'concentration_90', 'cost_low_70', 'cost_high_70', 'concentration_70'
        ]

        return self.downloader.download_to_mysql(
            ak_function_name='stock_cyq_em',
            table_name='ods_akshare_stock_cyq_em',
            column_mapping=column_mapping,
            numeric_columns=numeric_columns,
            date_format='%Y-%m-%d',
            merge_on=['ymd', 'stock_code'],
            auto_add_stock_code=True,
            adjust="qfq"  # 添加复权参数，前复权
        )


    # @timing_decorator
    def download_stock_yjkb_em(self):
        """
        下载业绩快报数据 - ods_akshare_stock_yjkb_em
        接口: stock_yjkb_em
        说明: 全量的每日切片数据，需要指定日期（YYYY0331, YYYY0630, YYYY0930, YYYY1231）   日跑
        """
        try:
            # 获取当前年份和过去几年的数据
            current_year = int(DateUtility.today()[:4])
            years = list(range(2020, current_year + 1))  # 从2020年开始

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
                '公告日期': 'ymd',
                '市场板块': 'market_board',
                '证券类型': 'securities_type'
            }

            numeric_columns = [
                'serial_num', 'eps', 'income', 'income_last_year', 'income_yoy', 'income_qoq',
                'profit', 'profit_last_year', 'profit_yoy', 'profit_qoq',
                'asset_per_share', 'roe'
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



    # @timing_decorator
    def download_stock_yjyg_em(self):
        """
        下载业绩预告数据 - ods_akshare_stock_yjyg_em
        接口: stock_yjyg_em
        说明: 全量的每日切片数据，需要指定日期（YYYY0331, YYYY0630, YYYY0930, YYYY1231）   日跑
        """
        try:
            # 获取当前年份和过去几年的数据
            current_year = int(DateUtility.today()[:4])
            years = list(range(2020, current_year + 1))  # 从2020年开始

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

            numeric_columns = [
                'serial_num', 'forecast_index', 'performance_change', 'forecast_value',
                'change_pct', 'last_year_value'
            ]

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


    # @timing_decorator
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
    def download_stock_zh_a_spot_em(self):
        """
        下载个股行情数据 - ods_akshare_stock_zh_a_spot_em
        接口: stock_zh_a_spot_em
        说明: 单次返回所有沪深京 A 股上市公司的实时行情数据，不可指定日期     目前只能返回100条  不可用
        """
        try:
            logging.info("开始下载个股行情数据...")

            # 获取实时行情数据
            df = ak.stock_zh_a_spot_em()

            if not df.empty:
                # 添加日期列（今天）
                today = DateUtility.today()
                df['ymd'] = today

                # 数据清洗和转换
                column_mapping = {
                    '序号': 'serial_num',
                    '代码': 'stock_code',
                    '名称': 'stock_name',
                    '最新价': 'close',
                    '涨跌幅': 'change_pct',
                    '涨跌额': 'change_amt',
                    '成交量': 'trading_volume',
                    '成交额': 'trading_amount',
                    '振幅': 'amplitude',
                    '最高': 'high',
                    '最低': 'low',
                    '今开': 'open',
                    '昨收': 'prev_close',
                    '量比': 'volume_ratio',
                    '换手率': 'turnover_rate',
                    '市盈率-动态': 'pe_dynamic',
                    '市净率': 'pb',
                    '总市值': 'total_market',
                    '流通市值': 'circulation_market',
                    '涨速': 'price_rise_speed',
                    '5分钟涨跌': 'five_min_price_change',
                    '60日涨跌幅': 'sixty_day_price_change',
                    '年初至今涨跌幅': 'ytd_price_change'
                }

                df = df.rename(columns=column_mapping)
                available_columns = [col for col in column_mapping.values() if col in df.columns]
                available_columns.insert(0, 'ymd')
                df = df[available_columns]

                # 数值类型转换（处理百分比和特殊字符）
                percent_columns = ['change_pct', 'amplitude', 'turnover_rate', 'five_min_price_change',
                                   'sixty_day_price_change', 'ytd_price_change']
                for col in percent_columns:
                    if col in df.columns:
                        df[col] = df[col].astype(str).str.replace('%', '').astype(float)

                numeric_columns = ['serial_num', 'close', 'change_amt', 'trading_volume', 'trading_amount',
                                   'high', 'low', 'open', 'prev_close', 'volume_ratio', 'pe_dynamic', 'pb',
                                   'total_market', 'circulation_market', 'price_rise_speed']
                for col in numeric_columns:
                    if col in df.columns:
                        df[col] = pd.to_numeric(df[col], errors='coerce')

                # 删除重复记录
                df = df.drop_duplicates(subset=['ymd', 'stock_code'], keep='first')

                logging.info(f"个股行情数据下载完成，共 {len(df)} 条记录")

                # 保存到MySQL
                if platform.system() == "Windows":
                    mysql_utils.data_from_dataframe_to_mysql(
                        user=local_user,
                        password=local_password,
                        host=local_host,
                        database=local_database,
                        df=df,
                        table_name="ods_akshare_stock_zh_a_spot_em",
                        merge_on=['ymd', 'stock_code']
                    )

                mysql_utils.data_from_dataframe_to_mysql(
                    user=origin_user,
                    password=origin_password,
                    host=origin_host,
                    database=origin_database,
                    df=df,
                    table_name="ods_akshare_stock_zh_a_spot_em",
                    merge_on=['ymd', 'stock_code']
                )

            else:
                logging.warning("个股行情数据为空")

        except Exception as e:
            logging.error(f"下载个股行情数据失败: {str(e)}")


    # @timing_decorator
    def download_stock_board_concept_name_em(self):
        """
        下载板块概念数据 - ods_akshare_board_concept_name_em
        接口: stock_board_concept_name_em
        说明: 所有板块概念的基本信息，获取全部板块
        """
        try:
            logging.info("开始下载板块概念数据...")

            # 获取所有板块数据
            df = ak.stock_board_concept_name_em()

            if df.empty:
                logging.warning("板块概念数据为空")
                return False

            # 添加日期列
            today = DateUtility.today()
            df['ymd'] = today

            logging.info(f"板块概念数据获取完成，共 {len(df)} 条记录")

            # 列映射
            column_mapping = {
                '排名': 'ranking',
                '板块名称': 'board_name',
                '板块代码': 'board_code',
                '最新价': 'close',
                '涨跌额': 'change_amt',
                '涨跌幅': 'change_pct',
                '总市值': 'total_market',
                '换手率': 'turnover_rate',
                '上涨家数': 'rising_stocks_num',
                '下跌家数': 'falling_stocks_num',
                '领涨股票': 'leading_stock',
                '领涨股票-涨跌幅': 'leading_stock_pct'
            }

            numeric_columns = [
                'ranking', 'close', 'change_amt', 'total_market',
                'rising_stocks_num', 'falling_stocks_num',
                'change_pct', 'turnover_rate', 'leading_stock_pct'
            ]

            # 使用downloader的数据处理方法
            processed_df = self.downloader._process_data(
                all_data=df,
                column_mapping=column_mapping,
                date_column='ymd',
                date_format='%Y%m%d',
                numeric_columns=numeric_columns,
                table_name='ods_akshare_board_concept_name_em'
            )

            if processed_df.empty:
                logging.warning("板块概念数据处理后为空")
                return False

            # 使用downloader的保存方法
            success = self.downloader._save_to_mysql(
                df=processed_df,
                table_name='ods_akshare_board_concept_name_em',
                merge_on=['ymd', 'board_code']
            )

            if success:
                logging.info(
                    f"板块概念数据保存成功，共 {len(processed_df)} 条记录，{processed_df['board_code'].nunique()} 个板块")
            else:
                logging.error("板块概念数据保存失败")

            return success

        except Exception as e:
            logging.error(f"下载板块概念数据失败: {str(e)}")
            import traceback
            logging.error(traceback.format_exc())
            return False



    # @timing_decorator
    def download_stock_board_concept_cons_em(self):
        """
        下载板块内个股行情数据 - ods_akshare_stock_board_concept_cons_em
        接口: stock_board_concept_cons_em
        说明: 全量的每日切片数据，遍历所有板块（从数据库获取板块列表）
        """
        try:
            # 从MySQL获取最新的板块列表
            logging.info("开始获取板块列表...")

            board_df = mysql_utils.data_from_mysql_to_dataframe_latest(
                user=self.downloader.origin_user,
                password=self.downloader.origin_password,
                host=self.downloader.origin_host,
                database=self.downloader.origin_database,
                table_name="ods_akshare_board_concept_name_em",  # 板块信息表
                cols=['board_name', 'board_code']  # 获取板块名称和代码
            )

            if board_df.empty:
                logging.warning("数据库中没有板块列表数据，请先运行 download_stock_board_concept_name_em()")
                return False

            # 去重并获取板块名称列表
            board_names = board_df['board_name'].dropna().unique().tolist()
            logging.info(f"从数据库获取到 {len(board_names)} 个板块")

            if not board_names:
                logging.warning("数据库中没有有效的板块名称")
                return False

            logging.info(f"开始下载 {len(board_names)} 个板块的成分股数据")

            all_data = pd.DataFrame()
            success_boards = []
            failed_boards = []

            for i, board_name in enumerate(board_names):
                try:
                    # 跳过可能为空的板块名
                    if pd.isna(board_name) or not str(board_name).strip():
                        continue

                    logging.info(f"下载板块 [{i + 1}/{len(board_names)}]: {board_name}")

                    # 获取板块成分股数据
                    df = ak.stock_board_concept_cons_em(symbol=str(board_name).strip())

                    if not df.empty:
                        # 添加日期和板块信息
                        today = DateUtility.today()
                        df['ymd'] = today
                        df['board_name'] = str(board_name).strip()

                        # 查找对应的board_code
                        board_code_row = board_df[board_df['board_name'] == board_name]
                        if not board_code_row.empty:
                            df['board_code'] = board_code_row.iloc[0]['board_code']
                        else:
                            df['board_code'] = str(board_name).strip()  # 降级处理

                        all_data = pd.concat([all_data, df], ignore_index=True)
                        success_boards.append(board_name)

                        logging.info(f"  {board_name}: 获取到 {len(df)} 条记录")
                    else:
                        logging.warning(f"  {board_name}: 成分股数据为空")
                        failed_boards.append(board_name)

                except Exception as e:
                    logging.error(f"  下载 {board_name} 失败: {str(e)[:100]}")
                    failed_boards.append(board_name)
                    continue

            if all_data.empty:
                logging.warning("所有板块的成分股数据都为空")
                return False

            logging.info(f"板块成分股数据获取完成:")
            logging.info(f"  成功板块: {len(success_boards)} 个")
            logging.info(f"  失败板块: {len(failed_boards)} 个")
            logging.info(f"  总记录数: {len(all_data)} 条")

            # 列映射
            column_mapping = {
                '序号': 'serial_num',
                '代码': 'stock_code',
                '名称': 'stock_name',
                '最新价': 'close',
                '涨跌幅': 'change_pct',
                '涨跌额': 'change_amt',
                '成交量': 'trading_volume',
                '成交额': 'trading_amount',
                '振幅': 'amplitude',
                '最高': 'high',
                '最低': 'low',
                '今开': 'open',
                '昨收': 'prev_close',
                '换手率': 'turnover_rate',
                '市盈率-动态': 'pe_dynamic',
                '市净率': 'pb'
            }

            numeric_columns = [
                'serial_num', 'close', 'change_amt', 'trading_volume', 'trading_amount',
                'high', 'low', 'open', 'prev_close', 'pe_dynamic', 'pb',
                'change_pct', 'amplitude', 'turnover_rate'
            ]

            # 使用downloader的数据处理方法
            processed_df = self.downloader._process_data(
                all_data=all_data,
                column_mapping=column_mapping,
                date_column='ymd',
                date_format='%Y%m%d',
                numeric_columns=numeric_columns,
                table_name='ods_akshare_stock_board_concept_cons_em'
            )

            if processed_df.empty:
                logging.warning("板块成分股数据处理后为空")
                return False

            # 使用downloader的保存方法
            success = self.downloader._save_to_mysql(
                df=processed_df,
                table_name='ods_akshare_stock_board_concept_cons_em',
                merge_on=['ymd', 'stock_code']
            )

            if success:
                logging.info(
                    f"板块成分股数据保存成功，共 {len(processed_df)} 条记录，{processed_df['board_code'].nunique()} 个板块")
            else:
                logging.error("板块成分股数据保存失败")

            return success

        except Exception as e:
            logging.error(f"下载板块成分股数据失败: {str(e)}")
            import traceback
            logging.error(traceback.format_exc())
            return False




    # @timing_decorator
    def download_stock_board_concept_hist_em(self, start_date=None, end_date=None):
        """
        下载板块历史行情数据 - ods_akshare_stock_board_concept_hist_em
        接口: stock_board_concept_hist_em
        说明: 遍历所有板块的历史行情数据（从数据库获取板块列表）
        """
        try:
            # 如果没有指定日期，使用默认范围
            if start_date is None:
                start_date = DateUtility.first_day_of_year(-1)  # 去年第一天
            if end_date is None:
                end_date = DateUtility.today()

            # 从MySQL获取最新的板块列表
            logging.info("开始获取板块列表...")

            board_df = mysql_utils.data_from_mysql_to_dataframe_latest(
                user=self.downloader.origin_user,
                password=self.downloader.origin_password,
                host=self.downloader.origin_host,
                database=self.downloader.origin_database,
                table_name="ods_akshare_board_concept_name_em",  # 板块信息表
                cols=['board_name', 'board_code']  # 获取板块名称和代码
            )

            if board_df.empty:
                logging.warning("数据库中没有板块列表数据，请先运行 download_stock_board_concept_name_em()")
                return False

            # 去重并获取板块名称列表
            board_names = board_df['board_name'].dropna().unique().tolist()
            logging.info(f"从数据库获取到 {len(board_names)} 个板块")

            if not board_names:
                logging.warning("数据库中没有有效的板块名称")
                return False

            logging.info(f"开始下载 {len(board_names)} 个板块的历史数据，日期: {start_date}~{end_date}")

            all_data = pd.DataFrame()
            success_boards = []
            failed_boards = []

            for i, board_name in enumerate(board_names):
                try:
                    # 跳过可能为空的板块名
                    if pd.isna(board_name) or not str(board_name).strip():
                        continue

                    logging.info(f"下载板块 [{i + 1}/{len(board_names)}]: {board_name}")

                    # 获取板块历史数据
                    df = ak.stock_board_concept_hist_em(
                        symbol=str(board_name).strip(),
                        start_date=start_date,
                        end_date=end_date
                    )

                    if not df.empty:
                        # 添加板块信息
                        df['board_name'] = str(board_name).strip()

                        # 查找对应的board_code
                        board_code_row = board_df[board_df['board_name'] == board_name]
                        if not board_code_row.empty:
                            df['board_code'] = board_code_row.iloc[0]['board_code']
                        else:
                            df['board_code'] = str(board_name).strip()  # 降级处理

                        all_data = pd.concat([all_data, df], ignore_index=True)
                        success_boards.append(board_name)

                        logging.info(f"  {board_name}: 获取到 {len(df)} 条记录")
                    else:
                        logging.warning(f"  {board_name}: 历史数据为空")
                        failed_boards.append(board_name)

                except Exception as e:
                    logging.error(f"  下载 {board_name} 失败: {str(e)[:100]}")
                    failed_boards.append(board_name)
                    continue

            if all_data.empty:
                logging.warning("所有板块的历史行情数据都为空")
                return False

            logging.info(f"板块历史数据获取完成:")
            logging.info(f"  成功板块: {len(success_boards)} 个")
            logging.info(f"  失败板块: {len(failed_boards)} 个")
            logging.info(f"  总记录数: {len(all_data)} 条")

            # 列映射
            column_mapping = {
                '日期': 'ymd',
                '开盘': 'open',
                '收盘': 'close',
                '最高': 'high',
                '最低': 'low',
                '涨跌幅': 'change_pct',
                '涨跌额': 'change_amt',
                '成交量': 'trading_volume',
                '成交额': 'trading_amount',
                '振幅': 'amplitude',
                '换手率': 'turnover_rate'
            }

            numeric_columns = [
                'open', 'close', 'high', 'low', 'change_amt',
                'trading_volume', 'trading_amount',
                'change_pct', 'amplitude', 'turnover_rate'
            ]

            # 使用downloader的数据处理方法
            processed_df = self.downloader._process_data(
                all_data=all_data,
                column_mapping=column_mapping,
                date_column='ymd',
                date_format='%Y-%m-%d',  # akshare返回的是YYYY-MM-DD格式
                numeric_columns=numeric_columns,
                table_name='ods_akshare_stock_board_concept_hist_em'
            )

            if processed_df.empty:
                logging.warning("板块历史行情数据处理后为空")
                return False

            # 使用downloader的保存方法
            success = self.downloader._save_to_mysql(
                df=processed_df,
                table_name='ods_akshare_stock_board_concept_hist_em',
                merge_on=['ymd', 'board_code']
            )

            if success:
                logging.info(
                    f"板块历史数据保存成功，共 {len(processed_df)} 条记录，{processed_df['board_code'].nunique()} 个板块")
            else:
                logging.error("板块历史数据保存失败")

            return success

        except Exception as e:
            logging.error(f"下载板块历史行情数据失败: {str(e)}")
            import traceback
            logging.error(traceback.format_exc())
            return False

    # @timing_decorator
    def download_stock_board_concept_name_ths(self):
        """
        下载同花顺概念板块基本信息 - ods_akshare_board_concept_name_ths
        接口: stock_board_concept_name_ths
        说明: 获取同花顺所有概念板块的基本信息
        """
        try:
            logging.info("开始下载同花顺概念板块数据...")

            # 获取所有同花顺概念板块数据
            df = ak.stock_board_concept_name_ths()

            if df.empty:
                logging.warning("同花顺概念板块数据为空")
                return False

            # 添加日期列
            today = DateUtility.today()
            df['ymd'] = today

            logging.info(f"同花顺概念板块数据获取完成，共 {len(df)} 条记录")

            # 列映射
            column_mapping = {
                'name': 'board_name',
                'code': 'board_code'
            }

            # 使用downloader的数据处理方法
            processed_df = self.downloader._process_data(
                all_data=df,
                column_mapping=column_mapping,
                date_column='ymd',
                date_format='%Y%m%d',
                numeric_columns=None,
                table_name='ods_akshare_board_concept_name_ths'
            )

            if processed_df.empty:
                logging.warning("同花顺概念板块数据处理后为空")
                return False

            # 删除重复记录
            if 'ymd' in processed_df.columns and 'board_code' in processed_df.columns:
                processed_df = processed_df.drop_duplicates(subset=['ymd', 'board_code'], keep='first')

            # 使用downloader的保存方法
            success = self.downloader._save_to_mysql(
                df=processed_df,
                table_name='ods_akshare_board_concept_name_ths',
                merge_on=['ymd', 'board_code']
            )

            if success:
                logging.info(
                    f"同花顺概念板块数据保存成功，共 {len(processed_df)} 条记录，{processed_df['board_code'].nunique()} 个概念")
            else:
                logging.error("同花顺概念板块数据保存失败")

            return success

        except Exception as e:
            logging.error(f"下载同花顺概念板块数据失败: {str(e)}")
            import traceback
            logging.error(traceback.format_exc())
            return False

    # @timing_decorator
    def download_stock_board_concept_index_ths(self, start_date=None, end_date=None):
        """
        下载同花顺概念板块指数数据 - ods_akshare_stock_board_concept_index_ths
        接口: stock_board_concept_index_ths
        说明: 遍历所有概念板块的历史指数数据
        """
        try:
            # 如果没有指定日期，使用默认范围
            if start_date is None:
                start_date = DateUtility.first_day_of_year(-2)  # 去年第一天
            if end_date is None:
                end_date = DateUtility.today()

            # 从MySQL获取最新的同花顺概念板块列表
            logging.info("开始获取同花顺概念板块列表...")

            board_df = mysql_utils.data_from_mysql_to_dataframe_latest(
                user=self.downloader.origin_user,
                password=self.downloader.origin_password,
                host=self.downloader.origin_host,
                database=self.downloader.origin_database,
                table_name="ods_akshare_board_concept_name_ths",  # 使用新表的同花顺概念信息
                cols=['board_name', 'board_code']  # 获取概念名称和代码
            )

            # 去重并获取概念名称列表
            board_names = board_df['board_name'].dropna().unique().tolist()
            logging.info(f"开始下载 {len(board_names)} 个概念板块的指数数据，日期: {start_date}~{end_date}")

            all_data = pd.DataFrame()
            success_concepts = []
            failed_concepts = []

            for i, board_name in enumerate(board_names):
                try:
                    # 跳过可能为空的板块名
                    if pd.isna(board_name) or not str(board_name).strip():
                        continue

                    logging.info(f"下载概念板块 [{i + 1}/{len(board_names)}]: {board_name}")

                    # 获取概念板块指数数据
                    df = ak.stock_board_concept_index_ths(
                        symbol=str(board_name).strip(),
                        start_date=start_date,
                        end_date=end_date
                    )

                    if not df.empty:
                        # 添加概念板块信息
                        df['board_name'] = str(board_name).strip()

                        # 查找对应的concept_code
                        concept_code_row = board_df[board_df['board_name'] == board_name]
                        if not concept_code_row.empty:
                            df['board_code'] = concept_code_row.iloc[0]['board_code']
                        else:
                            df['board_code'] = str(board_name).strip()  # 降级处理

                        all_data = pd.concat([all_data, df], ignore_index=True)
                        success_concepts.append(board_name)

                        logging.info(f"  {board_name}: 获取到 {len(df)} 条记录")
                    else:
                        logging.warning(f"  {board_name}: 指数数据为空")
                        failed_concepts.append(board_name)

                    # 添加延迟以避免封IP
                    time.sleep(random.uniform(0.5, 1.5))

                except Exception as e:
                    error_msg = str(e)
                    if "404" in error_msg or "无法获取" in error_msg:
                        logging.warning(f"  {board_name}: 可能不存在或无法访问")
                    else:
                        logging.error(f"  下载 {board_name} 失败: {error_msg[:100]}")
                    failed_concepts.append(board_name)
                    time.sleep(2)  # 失败后等待更长时间
                    continue

            # 列映射
            column_mapping = {
                '日期': 'ymd',
                '开盘价': 'open',
                '最高价': 'high',
                '最低价': 'low',
                '收盘价': 'close',
                '成交量': 'trading_volume',
                '成交额': 'trading_amount'
            }

            numeric_columns = [
                'open', 'close', 'high', 'low',
                'trading_volume', 'trading_amount'
            ]

            # 使用downloader的数据处理方法
            processed_df = self.downloader._process_data(
                all_data=all_data,
                column_mapping=column_mapping,
                date_column='ymd',
                date_format='%Y-%m-%d',  # akshare返回的是YYYY-MM-DD格式
                numeric_columns=numeric_columns,
                table_name='ods_akshare_stock_board_concept_index_ths'
            )

            # 使用downloader的保存方法
            success = self.downloader._save_to_mysql(
                df=processed_df,
                table_name='ods_akshare_stock_board_concept_index_ths',
                merge_on=['ymd', 'board_code']
            )
            return success

        except Exception as e:
            logging.error(f"下载概念板块指数数据失败: {str(e)}")
            import traceback
            logging.error(traceback.format_exc())
            return False



    # @timing_decorator
    def setup(self):
        """
        主执行函数，按顺序下载所有akshare数据
        注意：由于akshare接口的限制，部分数据需要分批次或指定参数获取
        """
        logging.info("======= 开始下载akshare历史数据 =======")

        # 1. 获取股票代码列表（用于需要股票代码的接口）
        self.get_stock_codes()

        # # 2. 下载股票估值数据                       封堵IP    不可用
        # self.download_stock_value_em()
        #
        # # 3. 下载股东户数数据（需要股票代码，分批次处理）   可用但周末跑
        self.download_stock_zh_a_gdhs_detail_em()
        #
        # # 4. 下载筹码数据（需要股票代码，分批次处理）    封堵IP   不可用
        # self.download_stock_cyq_em()
        #
        # # 5. 下载业绩快报数据（指定日期）         可用
        # self.download_stock_yjkb_em()
        #
        # # 6. 下载业绩预告数据（指定日期）         可用
        # self.download_stock_yjyg_em()
        #
        # # 7. 下载大盘高低统计数据（默认沪深300）   可用
        # self.download_stock_a_high_low_statistics()
        #
        # # 8. 下载个股行情数据（实时数据）     目前只能返回100条  不可用
        # self.download_stock_zh_a_spot_em()

        # # 9. 下载板块行情数据               封堵IP   不可用
        # self.download_stock_board_concept_name_em()

        # # 10. 下载板块内个股行情数据       封堵IP   不可用
        # self.download_stock_board_concept_cons_em()
        #
        # # 11. 下载板块历史行情数据         封堵IP   不可用
        # self.download_stock_board_concept_hist_em()

        # # 12. 同花顺板块数据
        # self.download_stock_board_concept_name_ths()

        # # 12. 同花顺板块数据
        # self.download_stock_board_concept_index_ths()


if __name__ == '__main__':

    # 只在周末执行
    if DateUtility.is_weekend():
        logging.info("今天是周末，开始执行akshare历史数据下载任务")
        saver = SaveAkshareHistoryData()
        saver.setup()
    else:
        logging.info("今天不是周末，跳过akshare历史数据下载")





