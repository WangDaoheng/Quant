# -*- coding: utf-8 -*-

import os
import sys
import time
import platform
import pandas as pd
import akshare as ak
from datetime import datetime
import logging
import random

import CommonProperties.Base_Properties as base_properties
import CommonProperties.Base_utils as base_utils
import CommonProperties.Mysql_Utils as mysql_utils
from CommonProperties.DateUtility import DateUtility
from CommonProperties.Base_utils import timing_decorator


class AkshareDownloader:
    """akshare数据下载器 - 通用工具类"""

    def __init__(self, stock_codes=None):
        """
        初始化下载器
        Args:
            stock_codes: 股票代码列表，如果不提供则从数据库获取
        """
        self.stock_codes = stock_codes or []

        # MySQL配置
        self.local_user = base_properties.local_mysql_user
        self.local_password = base_properties.local_mysql_password
        self.local_database = base_properties.local_mysql_database
        self.local_host = base_properties.local_mysql_host

        self.origin_user = base_properties.origin_mysql_user
        self.origin_password = base_properties.origin_mysql_password
        self.origin_database = base_properties.origin_mysql_database
        self.origin_host = base_properties.origin_mysql_host

    def set_stock_codes(self, stock_codes):
        """设置股票代码列表"""
        self.stock_codes = stock_codes

    # @timing_decorator
    def download_to_mysql(self,
                          ak_function_name,  # akshare函数名
                          table_name,  # MySQL表名
                          column_mapping,  # 列名映射字典
                          merge_on=None,  # 去重字段，默认['ymd', 'stock_code']
                          batch_size=100,  # 批次大小
                          sleep_time=0.1,  # 请求间隔
                          symbol_param='symbol',  # 股票代码参数名
                          date_column='ymd',  # 日期列名
                          date_format='%Y-%m-%d',  # 日期格式
                          numeric_columns=None,  # 数值列列表，自动识别
                          auto_add_stock_code=True,  # 新增：是否自动添加stock_code列
                          special_processing=None,  # 特殊处理函数
                          **kwargs):  # 传递给akshare函数的其他参数
        """
        通用下载函数：逐个股票下载akshare数据并保存到MySQL

        Args:
            ak_function_name: akshare函数名，如 'stock_value_em'
            table_name: MySQL表名
            column_mapping: 列名映射字典 {原列名: 新列名}
            merge_on: 去重字段，默认 ['ymd', 'stock_code']
            batch_size: 批次处理大小
            sleep_time: 请求间隔时间（秒）
            symbol_param: 股票代码参数名
            date_column: 日期列名
            date_format: 日期格式
            numeric_columns: 数值列列表，None则自动识别
            auto_add_stock_code: 是否自动添加stock_code列（默认True）
            special_processing: 特殊处理函数，用于额外的数据处理
            **kwargs: 传递给akshare函数的其他参数
        """
        try:
            if not self.stock_codes:
                logging.warning(f"无股票代码，跳过{table_name}数据下载")
                return False

            # 获取akshare函数
            try:
                ak_function = getattr(ak, ak_function_name)
            except AttributeError:
                logging.error(f"akshare函数 {ak_function_name} 不存在")
                return False

            all_data = pd.DataFrame()
            total_batches = (len(self.stock_codes) + batch_size - 1) // batch_size

            logging.info(f"开始下载{table_name}数据，共{len(self.stock_codes)}只股票，{total_batches}个批次")

            MAX_RETRY = 2
            # 分批下载
            for batch_idx in range(0, len(self.stock_codes), batch_size):
                batch_codes = self.stock_codes[batch_idx:batch_idx + batch_size]
                batch_data = pd.DataFrame()
                current_batch = batch_idx // batch_size + 1

                for i, stock_code in enumerate(batch_codes):
                    df = None
                    last_error = None

                    for retry in range(MAX_RETRY + 1):
                        try:
                            params = {symbol_param: stock_code, **kwargs}
                            df = ak_function(**params)
                            sleep_time = random.uniform(2.1, 2.6)
                            time.sleep(sleep_time)

                            if df is not None:
                                break  # 成功获取数据，跳出重试循环

                        except Exception as e:
                            last_error = e
                            if retry < MAX_RETRY:
                                time.sleep(0.5 * (retry + 1))

                    # 检查最终结果
                    if df is None:
                        error_msg = str(last_error) if last_error else "接口返回None"
                        logging.warning(f"股票 {stock_code} 数据获取失败: {error_msg}")
                        continue

                    if df.empty:
                        logging.debug(f"股票 {stock_code} 返回空DataFrame")
                        continue

                    # 处理成功获取的数据
                    if auto_add_stock_code and 'stock_code' not in df.columns:
                        df['stock_code'] = stock_code
                    batch_data = pd.concat([batch_data, df], ignore_index=True)

                all_data = pd.concat([all_data, batch_data], ignore_index=True)
                logging.info(f"批次 {current_batch}/{total_batches} 完成，累计获取 {len(all_data)} 条记录")

            sys.stdout.write("\n")

            if all_data.empty:
                logging.warning(f"{table_name}数据为空")
                return False

            # 数据处理
            df = self._process_data(
                all_data, column_mapping, date_column,
                date_format, numeric_columns, table_name
            )

            if df.empty:
                logging.warning(f"{table_name}数据处理后为空")
                return False

            # 特殊处理（如果有）
            if special_processing and callable(special_processing):
                try:
                    df = special_processing(df)
                    logging.info(f"特殊处理完成，处理后记录数: {len(df)}")
                except Exception as e:
                    logging.warning(f"特殊处理失败: {str(e)}")

            # 保存到MySQL
            success = self._save_to_mysql(df, table_name, merge_on or [date_column, 'stock_code'])

            return success

        except Exception as e:
            logging.error(f"下载{table_name}数据失败: {str(e)}")
            import traceback
            logging.error(traceback.format_exc())
            return False


    def _process_data(self, all_data, column_mapping, date_column,
                      date_format, numeric_columns, table_name):
        """数据处理：重命名、日期转换、数值转换"""
        try:
            # 1. 重命名
            df = all_data.rename(columns=column_mapping)

            # 2. 日期标准化
            if date_column in df.columns:
                df[date_column] = pd.to_datetime(df[date_column]).dt.strftime('%Y%m%d')

            # 3. 数值转换
            if numeric_columns:
                for col in numeric_columns:
                    if col in df.columns:
                        df[col] = pd.to_numeric(df[col], errors='coerce')

            # 4. 去重
            id_cols = [c for c in ['stock_code', 'board_code', 'concept_code'] if c in df.columns]
            if date_column in df.columns and id_cols:
                df = df.drop_duplicates(subset=[date_column] + id_cols[:1])

            return df.reset_index(drop=True)

        except Exception as e:
            logging.error(f"{table_name} 处理失败: {e}")
            return pd.DataFrame()


    # def _save_to_mysql(self, df, table_name, merge_on):
    #     """保存数据到MySQL"""
    #     try:
    #         mysql_utils.data_from_dataframe_to_mysql(
    #             user=self.origin_user,
    #             password=self.origin_password,
    #             host=self.origin_host,
    #             database=self.origin_database,
    #             df=df,
    #             table_name=table_name,
    #             merge_on=merge_on
    #         )
    #
    #         return True
    #
    #     except Exception as e:
    #         logging.error(f"保存到MySQL失败: {str(e)}")
    #         import traceback
    #         logging.error(traceback.format_exc())
    #         return False

    def _save_to_mysql(self, df, table_name, merge_on):
        """保存数据到MySQL，失败时自动保存csv"""
        try:
            if df.empty:
                logging.warning(f"{table_name} DataFrame为空，跳过插入")
                return True

            # 尝试插入数据
            inserted_count = mysql_utils.data_from_dataframe_to_mysql(
                user=self.origin_user,
                password=self.origin_password,
                host=self.origin_host,
                database=self.origin_database,
                df=df,
                table_name=table_name,
                merge_on=merge_on,
                batch_size=20000
            )

            logging.info(f"成功插入 {inserted_count} 行数据到 {table_name}")
            return True

        except Exception as e:
            # 保存失败的数据到csv
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            filename = f"failed_data_{table_name}_{timestamp}.csv"

            try:
                df.to_csv(filename, index=False, encoding='utf-8')
                logging.error(f"数据插入失败，已保存到文件: {filename}")
                logging.error(f"失败原因: {str(e)}")
            except:
                # 如果连保存csv都失败，至少记录行数
                logging.error(f"数据插入失败，且无法保存csv，DataFrame行数: {len(df)}")

            # 记录完整错误堆栈
            import traceback
            logging.error(f"完整错误信息:\n{traceback.format_exc()}")

            return False


    def download_single_to_mysql(self,
                                 ak_function_name,
                                 table_name,
                                 column_mapping,
                                 merge_on=None,
                                 date_column='ymd',
                                 date_format='%Y-%m-%d',
                                 numeric_columns=None,
                                 **kwargs):
        """
        下载不需要股票代码的单一数据表到MySQL

        Args:
            ak_function_name: akshare函数名
            table_name: MySQL表名
            column_mapping: 列名映射字典
            merge_on: 去重字段
            date_column: 日期列名
            date_format: 日期格式
            numeric_columns: 数值列列表
            **kwargs: 传递给akshare函数的参数
        """
        try:
            # 获取akshare函数
            try:
                ak_function = getattr(ak, ak_function_name)
            except AttributeError:
                logging.error(f"akshare函数 {ak_function_name} 不存在")
                return False

            logging.info(f"开始下载{table_name}数据")

            # 获取数据
            df = ak_function(**kwargs)

            if df.empty:
                logging.warning(f"{table_name}数据为空")
                return False

            # 数据处理
            df = self._process_data(
                df, column_mapping, date_column,
                date_format, numeric_columns, table_name
            )

            if df.empty:
                logging.warning(f"{table_name}数据处理后为空")
                return False

            # 保存到MySQL
            success = self._save_to_mysql(df, table_name, merge_on or [date_column])

            return success

        except Exception as e:
            logging.error(f"下载{table_name}数据失败: {str(e)}")
            import traceback
            logging.error(traceback.format_exc())
            return False


    def batch_download_with_date(self,
                                 ak_function_name,
                                 table_name,
                                 column_mapping,
                                 start_date,
                                 end_date,
                                 merge_on=None,
                                 date_column='ymd',
                                 date_format='%Y-%m-%d',
                                 numeric_columns=None,
                                 **kwargs):
        """
        按日期批量下载数据到MySQL

        Args:
            ak_function_name: akshare函数名
            table_name: MySQL表名
            column_mapping: 列名映射字典
            start_date: 开始日期
            end_date: 结束日期
            merge_on: 去重字段
            date_column: 日期列名
            date_format: 日期格式
            numeric_columns: 数值列列表
            **kwargs: 其他参数
        """
        try:
            # 获取akshare函数
            try:
                ak_function = getattr(ak, ak_function_name)
            except AttributeError:
                logging.error(f"akshare函数 {ak_function_name} 不存在")
                return False

            logging.info(f"开始批量下载{table_name}数据，日期范围: {start_date} ~ {end_date}")

            # 获取数据
            df = ak_function(start_date=start_date, end_date=end_date, **kwargs)

            if df.empty:
                logging.warning(f"{table_name}数据为空")
                return False

            # 数据处理
            df = self._process_data(
                df, column_mapping, date_column,
                date_format, numeric_columns, table_name
            )

            if df.empty:
                logging.warning(f"{table_name}数据处理后为空")
                return False

            # 保存到MySQL
            success = self._save_to_mysql(df, table_name, merge_on or [date_column])

            return success

        except Exception as e:
            logging.error(f"批量下载{table_name}数据失败: {str(e)}")
            import traceback
            logging.error(traceback.format_exc())
            return False