# -*- coding: utf-8 -*-

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
        说明: 个股的全量历史数据，需要逐个股票获取
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
            merge_on=['ymd', 'stock_code']
        )

    @timing_decorator
    def download_stock_zh_a_gdhs_detail_em(self):
        """
        下载股东户数数据 - ods_akshare_stock_zh_a_gdhs_detail_em
        接口: stock_zh_a_gdhs_detail_em
        说明: 个股的全量历史数据，不可选定日期
        """
        try:
            logging.info("开始下载股东户数数据...")

            # 批次处理，避免内存过大
            if not self.stock_codes:
                self.get_stock_codes()

            if not self.stock_codes:
                logging.warning("无股票代码，跳过股东户数数据下载")
                return

            batch_size = 50
            total_batches = (len(self.stock_codes) + batch_size - 1) // batch_size
            all_data = pd.DataFrame()

            for i in range(0, len(self.stock_codes), batch_size):
                batch = self.stock_codes[i:i + batch_size]
                batch_data = pd.DataFrame()

                for stock_code in batch:
                    try:
                        # 获取单个股票的股东户数数据
                        df = ak.stock_zh_a_gdhs_detail_em(symbol=stock_code)
                        if not df.empty:
                            df['stock_code'] = stock_code
                            batch_data = pd.concat([batch_data, df], ignore_index=True)
                    except Exception as e:
                        logging.warning(f"股票 {stock_code} 股东户数数据获取失败: {str(e)}")
                        continue

                all_data = pd.concat([all_data, batch_data], ignore_index=True)

                # 进度显示
                current_batch = i // batch_size + 1
                sys.stdout.write(f"\r下载股东户数数据进度: {current_batch}/{total_batches} 批次")
                sys.stdout.flush()
                time.sleep(0.5)  # 避免请求过于频繁

            sys.stdout.write("\n")

            if not all_data.empty:
                # 数据清洗和转换
                column_mapping = {
                    '股东户数统计截止日': 'ymd',
                    '股票代码': 'stock_code',
                    '股票简称': 'stock_name',
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

                df = all_data.rename(columns=column_mapping)
                available_columns = [col for col in column_mapping.values() if col in df.columns]
                df = df[available_columns]

                # 日期格式转换
                date_columns = ['ymd', 'holder_num_announce_date']
                for col in date_columns:
                    if col in df.columns:
                        df[col] = pd.to_datetime(df[col], errors='coerce').dt.strftime('%Y%m%d')

                # 数值类型转换
                numeric_columns = ['range_change_pct', 'holder_num_current', 'holder_num_last',
                                   'holder_num_change', 'holder_num_change_pct', 'avg_holder_market',
                                   'avg_holder_share_num', 'total_market', 'total_shares', 'share_change']
                for col in numeric_columns:
                    if col in df.columns:
                        df[col] = pd.to_numeric(df[col], errors='coerce')

                # 删除重复记录
                df = df.drop_duplicates(subset=['ymd', 'stock_code'], keep='first')

                logging.info(f"股东户数数据下载完成，共 {len(df)} 条记录")

                # 保存到MySQL
                if platform.system() == "Windows":
                    mysql_utils.data_from_dataframe_to_mysql(
                        user=local_user,
                        password=local_password,
                        host=local_host,
                        database=local_database,
                        df=df,
                        table_name="ods_akshare_stock_zh_a_gdhs_detail_em",
                        merge_on=['ymd', 'stock_code']
                    )

                mysql_utils.data_from_dataframe_to_mysql(
                    user=origin_user,
                    password=origin_password,
                    host=origin_host,
                    database=origin_database,
                    df=df,
                    table_name="ods_akshare_stock_zh_a_gdhs_detail_em",
                    merge_on=['ymd', 'stock_code']
                )

            else:
                logging.warning("股东户数数据为空")

        except Exception as e:
            logging.error(f"下载股东户数数据失败: {str(e)}")

    @timing_decorator
    def download_stock_cyq_em(self):
        """
        下载筹码数据 - ods_akshare_stock_cyq_em
        接口: stock_cyq_em
        说明: 个股的全量历史数据，不可选定日期
        """
        try:
            logging.info("开始下载筹码数据...")

            # 获取股票代码
            if not self.stock_codes:
                self.get_stock_codes()

            if not self.stock_codes:
                logging.warning("无股票代码，跳过筹码数据下载")
                return

            all_data = pd.DataFrame()

            for i, stock_code in enumerate(self.stock_codes):
                try:
                    # 获取单个股票的筹码数据
                    df = ak.stock_cyq_em(symbol=stock_code)
                    if not df.empty:
                        df['stock_code'] = stock_code
                        all_data = pd.concat([all_data, df], ignore_index=True)

                    # 进度显示
                    if (i + 1) % 100 == 0 or i == len(self.stock_codes) - 1:
                        sys.stdout.write(f"\r下载筹码数据进度: {i + 1}/{len(self.stock_codes)} 只股票")
                        sys.stdout.flush()

                    time.sleep(0.1)  # 避免请求过于频繁

                except Exception as e:
                    logging.warning(f"股票 {stock_code} 筹码数据获取失败: {str(e)}")
                    continue

            sys.stdout.write("\n")

            if not all_data.empty:
                # 数据清洗和转换
                column_mapping = {
                    '日期': 'ymd',
                    '股票代码': 'stock_code',
                    '股票简称': 'stock_name',
                    '获利比例': 'profit_ratio',
                    '平均成本': 'avg_cost',
                    '90成本-低': 'cost_low_90',
                    '90成本-高': 'cost_high_90',
                    '90集中度': 'concentration_90',
                    '70成本-低': 'cost_low_70',
                    '70成本-高': 'cost_high_70',
                    '70集中度': 'concentration_70'
                }

                df = all_data.rename(columns=column_mapping)
                available_columns = [col for col in column_mapping.values() if col in df.columns]
                df = df[available_columns]

                # 日期格式转换
                if 'ymd' in df.columns:
                    df['ymd'] = pd.to_datetime(df['ymd'], errors='coerce').dt.strftime('%Y%m%d')

                # 数值类型转换
                numeric_columns = ['profit_ratio', 'avg_cost', 'cost_low_90', 'cost_high_90',
                                   'concentration_90', 'cost_low_70', 'cost_high_70', 'concentration_70']
                for col in numeric_columns:
                    if col in df.columns:
                        df[col] = pd.to_numeric(df[col], errors='coerce')

                # 删除重复记录
                df = df.drop_duplicates(subset=['ymd', 'stock_code'], keep='first')

                logging.info(f"筹码数据下载完成，共 {len(df)} 条记录")

                # 保存到MySQL
                if platform.system() == "Windows":
                    mysql_utils.data_from_dataframe_to_mysql(
                        user=local_user,
                        password=local_password,
                        host=local_host,
                        database=local_database,
                        df=df,
                        table_name="ods_akshare_stock_cyq_em",
                        merge_on=['ymd', 'stock_code']
                    )

                mysql_utils.data_from_dataframe_to_mysql(
                    user=origin_user,
                    password=origin_password,
                    host=origin_host,
                    database=origin_database,
                    df=df,
                    table_name="ods_akshare_stock_cyq_em",
                    merge_on=['ymd', 'stock_code']
                )

            else:
                logging.warning("筹码数据为空")

        except Exception as e:
            logging.error(f"下载筹码数据失败: {str(e)}")

    @timing_decorator
    def download_stock_yjkb_em(self, date=None):
        """
        下载业绩快报数据 - ods_akshare_stock_yjkb_em
        接口: stock_yjkb_em
        说明: 全量的每日切片数据，可选定日期
        """
        try:
            # 如果没有指定日期，使用今天的日期
            if date is None:
                date = DateUtility.today()

            logging.info(f"开始下载业绩快报数据，日期: {date}")

            # 转换日期格式
            date_obj = datetime.strptime(date, '%Y%m%d')
            year = date_obj.year
            quarter = (date_obj.month - 1) // 3 + 1

            # 获取业绩快报数据
            df = ak.stock_yjkb_em(date=f"{year}年一季报")  # 这里需要根据实际情况调整

            if not df.empty:
                # 数据清洗和转换
                column_mapping = {
                    '公告日期': 'ymd',
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
                    '市场板块': 'market_board',
                    '证券类型': 'securities_type'
                }

                df = df.rename(columns=column_mapping)
                available_columns = [col for col in column_mapping.values() if col in df.columns]
                df = df[available_columns]

                # 日期格式转换
                if 'ymd' in df.columns:
                    df['ymd'] = pd.to_datetime(df['ymd'], errors='coerce').dt.strftime('%Y%m%d')

                # 数值类型转换
                numeric_columns = ['eps', 'income', 'income_last_year', 'income_qoq',
                                   'profit', 'profit_last_year', 'profit_qoq', 'asset_per_share', 'roe']
                for col in numeric_columns:
                    if col in df.columns:
                        # 处理百分比和特殊字符
                        if col in ['income_yoy', 'profit_yoy']:
                            df[col] = df[col].astype(str).str.replace('%', '').astype(float)
                        else:
                            df[col] = pd.to_numeric(df[col], errors='coerce')

                # 删除重复记录
                df = df.drop_duplicates(subset=['ymd', 'stock_code'], keep='first')

                logging.info(f"业绩快报数据下载完成，共 {len(df)} 条记录")

                # 保存到MySQL
                if platform.system() == "Windows":
                    mysql_utils.data_from_dataframe_to_mysql(
                        user=local_user,
                        password=local_password,
                        host=local_host,
                        database=local_database,
                        df=df,
                        table_name="ods_akshare_stock_yjkb_em",
                        merge_on=['ymd', 'stock_code']
                    )

                mysql_utils.data_from_dataframe_to_mysql(
                    user=origin_user,
                    password=origin_password,
                    host=origin_host,
                    database=origin_database,
                    df=df,
                    table_name="ods_akshare_stock_yjkb_em",
                    merge_on=['ymd', 'stock_code']
                )

            else:
                logging.warning(f"日期 {date} 的业绩快报数据为空")

        except Exception as e:
            logging.error(f"下载业绩快报数据失败: {str(e)}")

    @timing_decorator
    def download_stock_yjyg_em(self, date=None):
        """
        下载业绩预告数据 - ods_akshare_stock_yjyg_em
        接口: stock_yjyg_em
        说明: 全量的每日切片数据，可选定日期
        """
        try:
            # 如果没有指定日期，使用今天的日期
            if date is None:
                date = DateUtility.today()

            logging.info(f"开始下载业绩预告数据，日期: {date}")

            # 获取业绩预告数据
            df = ak.stock_yjyg_em(date=date)

            if not df.empty:
                # 数据清洗和转换
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

                df = df.rename(columns=column_mapping)
                available_columns = [col for col in column_mapping.values() if col in df.columns]
                df = df[available_columns]

                # 日期格式转换
                if 'ymd' in df.columns:
                    df['ymd'] = pd.to_datetime(df['ymd'], errors='coerce').dt.strftime('%Y%m%d')

                # 数值类型转换
                numeric_columns = ['forecast_index', 'performance_change', 'forecast_value',
                                   'change_pct', 'last_year_value']
                for col in numeric_columns:
                    if col in df.columns:
                        if col == 'change_pct':
                            df[col] = df[col].astype(str).str.replace('%', '').astype(float)
                        else:
                            df[col] = pd.to_numeric(df[col], errors='coerce')

                # 删除重复记录
                df = df.drop_duplicates(subset=['ymd', 'stock_code'], keep='first')

                logging.info(f"业绩预告数据下载完成，共 {len(df)} 条记录")

                # 保存到MySQL
                if platform.system() == "Windows":
                    mysql_utils.data_from_dataframe_to_mysql(
                        user=local_user,
                        password=local_password,
                        host=local_host,
                        database=local_database,
                        df=df,
                        table_name="ods_akshare_stock_yjyg_em",
                        merge_on=['ymd', 'stock_code']
                    )

                mysql_utils.data_from_dataframe_to_mysql(
                    user=origin_user,
                    password=origin_password,
                    host=origin_host,
                    database=origin_database,
                    df=df,
                    table_name="ods_akshare_stock_yjyg_em",
                    merge_on=['ymd', 'stock_code']
                )

            else:
                logging.warning(f"日期 {date} 的业绩预告数据为空")

        except Exception as e:
            logging.error(f"下载业绩预告数据失败: {str(e)}")

    @timing_decorator
    def download_stock_a_high_low_statistics(self, symbol="沪深300"):
        """
        下载大盘高低统计数据 - ods_akshare_stock_a_high_low_statistics
        接口: stock_a_high_low_statistics
        说明: 全量的每日切片数据，不可指定日期
        """
        try:
            logging.info(f"开始下载大盘高低统计数据，指数: {symbol}")

            # 获取数据
            df = ak.stock_a_high_low_statistics(symbol=symbol)

            if not df.empty:
                # 数据清洗和转换
                column_mapping = {
                    '交易日': 'ymd',
                    '股票代码': 'stock_code',
                    '股票名称': 'stock_name',
                    '相关指数收盘价': 'close',
                    '20日新高': 'high20',
                    '20日新低': 'low20',
                    '60日新高': 'high60',
                    '60日新低': 'low60',
                    '120日新高': 'high120',
                    '120日新低': 'low120'
                }

                df = df.rename(columns=column_mapping)
                available_columns = [col for col in column_mapping.values() if col in df.columns]
                df = df[available_columns]

                # 日期格式转换
                if 'ymd' in df.columns:
                    df['ymd'] = pd.to_datetime(df['ymd'], errors='coerce').dt.strftime('%Y%m%d')

                # 数值类型转换
                numeric_columns = ['close', 'high20', 'low20', 'high60', 'low60', 'high120', 'low120']
                for col in numeric_columns:
                    if col in df.columns:
                        df[col] = pd.to_numeric(df[col], errors='coerce')

                # 删除重复记录
                df = df.drop_duplicates(subset=['ymd', 'stock_code'], keep='first')

                logging.info(f"大盘高低统计数据下载完成，共 {len(df)} 条记录")

                # 保存到MySQL
                if platform.system() == "Windows":
                    mysql_utils.data_from_dataframe_to_mysql(
                        user=local_user,
                        password=local_password,
                        host=local_host,
                        database=local_database,
                        df=df,
                        table_name="ods_akshare_stock_a_high_low_statistics",
                        merge_on=['ymd', 'stock_code']
                    )

                mysql_utils.data_from_dataframe_to_mysql(
                    user=origin_user,
                    password=origin_password,
                    host=origin_host,
                    database=origin_database,
                    df=df,
                    table_name="ods_akshare_stock_a_high_low_statistics",
                    merge_on=['ymd', 'stock_code']
                )

            else:
                logging.warning(f"指数 {symbol} 的大盘高低统计数据为空")

        except Exception as e:
            logging.error(f"下载大盘高低统计数据失败: {str(e)}")

    @timing_decorator
    def download_stock_zh_a_spot_em(self):
        """
        下载个股行情数据 - ods_akshare_stock_zh_a_spot_em
        接口: stock_zh_a_spot_em
        说明: 全量的每日切片数据，不可指定日期
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

    @timing_decorator
    def download_stock_board_concept_name_em(self):
        """
        下载板块行情数据 - ods_akshare_stock_board_concept_name_em
        接口: stock_board_concept_name_em
        说明: 全量的每日切片数据，不可指定日期
        """
        try:
            logging.info("开始下载板块行情数据...")

            # 获取板块数据
            df = ak.stock_board_concept_name_em()

            if not df.empty:
                # 添加日期列（今天）
                today = DateUtility.today()
                df['ymd'] = today

                # 数据清洗和转换
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

                df = df.rename(columns=column_mapping)
                available_columns = [col for col in column_mapping.values() if col in df.columns]
                available_columns.insert(0, 'ymd')
                df = df[available_columns]

                # 数值类型转换（处理百分比）
                percent_columns = ['change_pct', 'turnover_rate', 'leading_stock_pct']
                for col in percent_columns:
                    if col in df.columns:
                        df[col] = df[col].astype(str).str.replace('%', '').astype(float)

                numeric_columns = ['ranking', 'close', 'change_amt', 'total_market',
                                   'rising_stocks_num', 'falling_stocks_num']
                for col in numeric_columns:
                    if col in df.columns:
                        df[col] = pd.to_numeric(df[col], errors='coerce')

                # 删除重复记录
                df = df.drop_duplicates(subset=['ymd', 'board_code'], keep='first')

                logging.info(f"板块行情数据下载完成，共 {len(df)} 条记录")

                # 保存到MySQL
                if platform.system() == "Windows":
                    mysql_utils.data_from_dataframe_to_mysql(
                        user=local_user,
                        password=local_password,
                        host=local_host,
                        database=local_database,
                        df=df,
                        table_name="ods_akshare_stock_board_concept_name_em",
                        merge_on=['ymd', 'board_code']
                    )

                mysql_utils.data_from_dataframe_to_mysql(
                    user=origin_user,
                    password=origin_password,
                    host=origin_host,
                    database=origin_database,
                    df=df,
                    table_name="ods_akshare_stock_board_concept_name_em",
                    merge_on=['ymd', 'board_code']
                )

            else:
                logging.warning("板块行情数据为空")

        except Exception as e:
            logging.error(f"下载板块行情数据失败: {str(e)}")

    @timing_decorator
    def download_stock_board_concept_cons_em(self, symbol="阿兹海默"):
        """
        下载板块内个股行情数据 - ods_akshare_stock_board_concept_cons_em
        接口: stock_board_concept_cons_em
        说明: 全量的每日切片数据，不可指定日期
        """
        try:
            logging.info(f"开始下载板块内个股行情数据，板块: {symbol}")

            # 获取板块成分股数据
            df = ak.stock_board_concept_cons_em(symbol=symbol)

            if not df.empty:
                # 添加日期列（今天）
                today = DateUtility.today()
                df['ymd'] = today
                df['board_name'] = symbol
                df['board_code'] = symbol  # 这里可以根据需要映射为实际代码

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
                    '换手率': 'turnover_rate',
                    '市盈率-动态': 'pe_dynamic',
                    '市净率': 'pb'
                }

                df = df.rename(columns=column_mapping)
                available_columns = ['ymd', 'board_name', 'board_code'] + [col for col in column_mapping.values() if
                                                                           col in df.columns]
                df = df[available_columns]

                # 数值类型转换（处理百分比）
                percent_columns = ['change_pct', 'amplitude', 'turnover_rate']
                for col in percent_columns:
                    if col in df.columns:
                        df[col] = df[col].astype(str).str.replace('%', '').astype(float)

                numeric_columns = ['serial_num', 'close', 'change_amt', 'trading_volume', 'trading_amount',
                                   'high', 'low', 'open', 'prev_close', 'pe_dynamic', 'pb']
                for col in numeric_columns:
                    if col in df.columns:
                        df[col] = pd.to_numeric(df[col], errors='coerce')

                # 删除重复记录
                df = df.drop_duplicates(subset=['ymd', 'stock_code'], keep='first')

                logging.info(f"板块内个股行情数据下载完成，共 {len(df)} 条记录")

                # 保存到MySQL
                if platform.system() == "Windows":
                    mysql_utils.data_from_dataframe_to_mysql(
                        user=local_user,
                        password=local_password,
                        host=local_host,
                        database=local_database,
                        df=df,
                        table_name="ods_akshare_stock_board_concept_cons_em",
                        merge_on=['ymd', 'stock_code']
                    )

                mysql_utils.data_from_dataframe_to_mysql(
                    user=origin_user,
                    password=origin_password,
                    host=origin_host,
                    database=origin_database,
                    df=df,
                    table_name="ods_akshare_stock_board_concept_cons_em",
                    merge_on=['ymd', 'stock_code']
                )

            else:
                logging.warning(f"板块 {symbol} 的成分股数据为空")

        except Exception as e:
            logging.error(f"下载板块内个股行情数据失败: {str(e)}")

    @timing_decorator
    def download_stock_board_concept_hist_em(self, symbol="阿兹海默",
                                             start_date=None, end_date=None):
        """
        下载板块历史行情数据 - ods_akshare_stock_board_concept_hist_em
        接口: stock_board_concept_hist_em
        说明: 可指定日期范围
        """
        try:
            # 如果没有指定日期，使用默认范围
            if start_date is None:
                start_date = DateUtility.first_day_of_year(-1)
            if end_date is None:
                end_date = DateUtility.today()

            logging.info(f"开始下载板块历史行情数据，板块: {symbol}，日期: {start_date}~{end_date}")

            # 获取板块历史数据
            df = ak.stock_board_concept_hist_em(symbol=symbol,
                                                start_date=start_date,
                                                end_date=end_date)

            if not df.empty:
                # 添加板块代码列
                df['board_code'] = symbol

                # 数据清洗和转换
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

                df = df.rename(columns=column_mapping)
                available_columns = ['board_code'] + [col for col in column_mapping.values() if col in df.columns]
                df = df[available_columns]

                # 日期格式转换
                if 'ymd' in df.columns:
                    df['ymd'] = pd.to_datetime(df['ymd'], errors='coerce').dt.strftime('%Y%m%d')

                # 数值类型转换（处理百分比）
                percent_columns = ['change_pct', 'amplitude', 'turnover_rate']
                for col in percent_columns:
                    if col in df.columns:
                        df[col] = df[col].astype(str).str.replace('%', '').astype(float)

                numeric_columns = ['open', 'close', 'high', 'low', 'change_amt',
                                   'trading_volume', 'trading_amount']
                for col in numeric_columns:
                    if col in df.columns:
                        df[col] = pd.to_numeric(df[col], errors='coerce')

                # 删除重复记录
                df = df.drop_duplicates(subset=['ymd', 'board_code'], keep='first')

                logging.info(f"板块历史行情数据下载完成，共 {len(df)} 条记录")

                # 保存到MySQL
                if platform.system() == "Windows":
                    mysql_utils.data_from_dataframe_to_mysql(
                        user=local_user,
                        password=local_password,
                        host=local_host,
                        database=local_database,
                        df=df,
                        table_name="ods_akshare_stock_board_concept_hist_em",
                        merge_on=['ymd', 'board_code']
                    )

                mysql_utils.data_from_dataframe_to_mysql(
                    user=origin_user,
                    password=origin_password,
                    host=origin_host,
                    database=origin_database,
                    df=df,
                    table_name="ods_akshare_stock_board_concept_hist_em",
                    merge_on=['ymd', 'board_code']
                )

            else:
                logging.warning(f"板块 {symbol} 的历史行情数据为空")

        except Exception as e:
            logging.error(f"下载板块历史行情数据失败: {str(e)}")

    # @timing_decorator
    def setup(self):
        """
        主执行函数，按顺序下载所有akshare数据
        注意：由于akshare接口的限制，部分数据需要分批次或指定参数获取
        """
        logging.info("======= 开始下载akshare历史数据 =======")

        # 1. 获取股票代码列表（用于需要股票代码的接口）
        self.get_stock_codes()

        # 2. 下载股票估值数据
        self.download_stock_value_em()

        # # 3. 下载股东户数数据（需要股票代码，分批次处理）
        # self.download_stock_zh_a_gdhs_detail_em()
        #
        # # 4. 下载筹码数据（需要股票代码，分批次处理）
        # self.download_stock_cyq_em()
        #
        # # 5. 下载业绩快报数据（指定日期）
        # self.download_stock_yjkb_em()
        #
        # # 6. 下载业绩预告数据（指定日期）
        # self.download_stock_yjyg_em()
        #
        # # 7. 下载大盘高低统计数据（默认沪深300）
        # self.download_stock_a_high_low_statistics()
        #
        # # 8. 下载个股行情数据（实时数据）
        # self.download_stock_zh_a_spot_em()
        #
        # # 9. 下载板块行情数据
        # self.download_stock_board_concept_name_em()

        # # 10. 下载板块内个股行情数据（示例板块）
        # # 可以根据需要下载多个板块
        # popular_boards = ["新能源汽车", "人工智能", "半导体", "医药", "白酒"]
        # for board in popular_boards[:2]:  # 先下载前2个板块避免耗时过长
        #     try:
        #         self.download_stock_board_concept_cons_em(symbol=board)
        #     except Exception as e:
        #         logging.warning(f"板块 {board} 数据下载失败: {str(e)}")
        #         continue
        #
        # # 11. 下载板块历史行情数据（示例板块）
        # for board in popular_boards[:1]:  # 先下载1个板块的历史数据
        #     try:
        #         self.download_stock_board_concept_hist_em(symbol=board)
        #     except Exception as e:
        #         logging.warning(f"板块 {board} 历史数据下载失败: {str(e)}")
        #         continue
        #
        # logging.info("======= akshare历史数据下载完成 =======")


if __name__ == '__main__':
    # 只在周末执行
    # if DateUtility.is_weekend():
    #     logging.info("今天是周末，开始执行akshare历史数据下载任务")
    #     saver = SaveAkshareHistoryData()
    #     saver.setup()
    # else:
    #     logging.info("今天不是周末，跳过akshare历史数据下载")

    saver = SaveAkshareHistoryData()
    saver.setup()


