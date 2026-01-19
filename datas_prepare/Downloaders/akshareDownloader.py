# -*- coding: utf-8 -*-

import os
import sys
import time
import platform
import pandas as pd
import akshare as ak
from datetime import datetime
import logging

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

    @timing_decorator
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

            # 分批下载
            for batch_idx in range(0, len(self.stock_codes), batch_size):
                batch_codes = self.stock_codes[batch_idx:batch_idx + batch_size]
                batch_data = pd.DataFrame()
                current_batch = batch_idx // batch_size + 1

                for i, stock_code in enumerate(batch_codes):
                    try:
                        # 构造请求参数
                        params = {symbol_param: stock_code, **kwargs}
                        df = ak_function(**params)

                        if not df.empty:
                            # 只在需要时添加stock_code列
                            if auto_add_stock_code and 'stock_code' not in df.columns:
                                df['stock_code'] = stock_code
                            batch_data = pd.concat([batch_data, df], ignore_index=True)

                    except Exception as e:
                        logging.warning(f"股票 {stock_code} {table_name}数据获取失败: {str(e)}")
                        continue

                    # 进度显示
                    if (i + 1) % 10 == 0:
                        sys.stdout.write(
                            f"\r批次 {current_batch}/{total_batches}: 已处理 {i + 1}/{len(batch_codes)} 只股票")
                        sys.stdout.flush()

                    time.sleep(sleep_time)

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
        """数据处理：重命名、日期转换、数值转换等"""
        try:
            # 1. 显示原始列名（调试用）
            logging.debug(f"{table_name}原始列名: {all_data.columns.tolist()}")

            # 2. 重命名列（只重命名存在的列）
            existing_mapping = {k: v for k, v in column_mapping.items() if k in all_data.columns}
            df = all_data.rename(columns=existing_mapping)

            # 3. 日期格式转换
            if date_column in df.columns:
                try:
                    df[date_column] = pd.to_datetime(df[date_column], format=date_format).dt.strftime('%Y%m%d')
                except Exception as e:
                    logging.warning(f"日期转换失败，尝试自动转换: {str(e)}")
                    df[date_column] = pd.to_datetime(df[date_column], errors='coerce').dt.strftime('%Y%m%d')

            # 4. 数值列转换
            if numeric_columns:
                # 使用指定的数值列
                num_cols_to_convert = [col for col in numeric_columns if col in df.columns]
            else:
                # 自动识别：排除非数值列
                exclude_cols = ['stock_code', date_column, 'stock_name', '名称', '股票简称']
                num_cols_to_convert = [
                    col for col in df.columns
                    if col not in exclude_cols and df[col].dtype == 'object'
                ]

            for col in num_cols_to_convert:
                try:
                    # 处理百分比符号
                    if df[col].dtype == 'object' and df[col].astype(str).str.contains('%').any():
                        df[col] = df[col].astype(str).str.replace('%', '').astype(float)
                    else:
                        df[col] = pd.to_numeric(df[col], errors='coerce')
                except Exception as e:
                    logging.warning(f"列 {col} 数值转换失败: {str(e)}")

            # 5. 删除重复记录
            if date_column in df.columns and 'stock_code' in df.columns:
                df = df.drop_duplicates(subset=[date_column, 'stock_code'], keep='first')

            # 6. 删除关键字段为空的行
            required_cols = [col for col in [date_column, 'stock_code'] if col in df.columns]
            if required_cols:
                df = df.dropna(subset=required_cols)

            # 7. 重置索引
            df = df.reset_index(drop=True)

            logging.info(f"{table_name}数据清洗完成，共 {len(df)} 条记录，{df['stock_code'].nunique()} 只股票")

            return df

        except Exception as e:
            logging.error(f"数据处理失败: {str(e)}")
            import traceback
            logging.error(traceback.format_exc())
            return pd.DataFrame()

    def _save_to_mysql(self, df, table_name, merge_on):
        """保存数据到MySQL"""
        try:
            if platform.system() == "Windows":
                mysql_utils.data_from_dataframe_to_mysql(
                    user=self.local_user,
                    password=self.local_password,
                    host=self.local_host,
                    database=self.local_database,
                    df=df,
                    table_name=table_name,
                    merge_on=merge_on
                )

            mysql_utils.data_from_dataframe_to_mysql(
                user=self.origin_user,
                password=self.origin_password,
                host=self.origin_host,
                database=self.origin_database,
                df=df,
                table_name=table_name,
                merge_on=merge_on
            )

            logging.info(f"数据已保存到MySQL表 {table_name}")
            return True

        except Exception as e:
            logging.error(f"保存到MySQL失败: {str(e)}")
            import traceback
            logging.error(traceback.format_exc())
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