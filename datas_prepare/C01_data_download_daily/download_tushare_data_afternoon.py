# -*- coding: utf-8 -*-

import tushare as ts
import pandas as pd
import sys
import time
import logging
import warnings


import CommonProperties.Base_Properties as base_properties
import CommonProperties.Mysql_Utils as mysql_utils
from CommonProperties.DateUtility import DateUtility
from CommonProperties.Base_utils import timing_decorator
from CommonProperties import set_config

# 方法1：屏蔽所有 FutureWarning（最简单有效）
warnings.filterwarnings('ignore', category=FutureWarning)


class SaveTushareDailyData:
    def __init__(self):
        """
        初始化Tushare
        :param tushare_token: 你的Tushare API Token
        """
        # 设置Tushare Token
        ts.set_token(base_properties.ts_token)
        self.pro = ts.pro_api()

    def get_stock_kline_tushare(self):
        """
        使用Tushare获取全部股票的历史日K线数据，并存入数据库
        完全复用现有逻辑，仅替换数据源
        """
        # 1. 获取日期范围
        today = DateUtility.today()

        if int(today[6:8]) > 15:
            time_start_date = DateUtility.next_day(-15)  # 15天前
        else:
            time_start_date = DateUtility.first_day_of_month()  # 当月1号
        time_end_date = today

        # 2. 获取股票代码列表
        stock_code_list = mysql_utils.get_stock_codes_latest(None)  # 修改点1：传入None

        # 3. 分批处理设置
        batch_size = 80  # 修改点2：减小批量大小，从100改为80

        def get_batches(lst, batch_size):
            for start in range(0, len(lst), batch_size):
                yield lst[start:start + batch_size]

        total_batches = (len(stock_code_list) + batch_size - 1) // batch_size
        kline_total_df = pd.DataFrame()

        # 4. 下载tushare数据
        for i, batch_list in enumerate(get_batches(stock_code_list, batch_size), start=1):
            sys.stdout.write(f"\r当前执行get_stock_kline_tushare的第{i}次循环，总共{total_batches}个批次")
            sys.stdout.flush()

            # 关键：构建Tushare所需的代码格式（如000001.SZ）
            tushare_codes = batch_list

            # 使用Tushare的pro_bar接口（日线数据）
            try:
                # 这里循环单个股票调用，避免Tushare批量接口限制
                for ts_code in tushare_codes:
                    df_batch = ts.pro_bar(
                        ts_code=ts_code,
                        start_date=time_start_date,
                        end_date=time_end_date,
                        adj='qfq',  # 前复权，对应你的fq="pre"
                        freq='D'  # 日线
                    )

                    if df_batch is not None and not df_batch.empty:
                        # 添加股票代码列（去除后缀）
                        kline_total_df = pd.concat([kline_total_df, df_batch], ignore_index=True)

                # 控制请求频率，避免触发限制（非常重要！）
                time.sleep(0.5)  # 修改点3：增加等待时间，从0.2改为0.5秒

            except Exception as e:
                # 修改点4：记录详细错误
                logging.warning(f"批次{i}获取失败: {str(e)[:100]}")  # 截取前100字符避免过长
                time.sleep(2)  # 失败后等待更久
                continue

        sys.stdout.write("\n")

        # 5. 数据处理：对齐现有数据结构
        if not kline_total_df.empty:
            # 检查实际返回的列名
            logging.info(f"Tushare返回的列名: {kline_total_df.columns.tolist()}")

            # 重命名列以匹配你的数据库schema
            # 注意：Tushare返回的列名可能是'trade_date'或'date'，需要确认
            column_mapping = {}

            # 自动检测列名
            if 'trade_date' in kline_total_df.columns:
                column_mapping['trade_date'] = 'ymd'
            elif 'date' in kline_total_df.columns:
                column_mapping['date'] = 'ymd'

            if 'ts_code' in kline_total_df.columns:
                column_mapping['ts_code'] = 'stock_code'

            # 应用重命名
            kline_total_df.rename(columns=column_mapping, inplace=True)

            # 如果没有重命名成功，尝试其他列名
            if 'ymd' not in kline_total_df.columns:
                # 查找可能的日期列
                date_cols = [col for col in kline_total_df.columns if 'date' in col.lower()]
                if date_cols:
                    kline_total_df.rename(columns={date_cols[0]: 'ymd'}, inplace=True)

            if 'stock_code' not in kline_total_df.columns:
                # 查找可能的代码列
                code_cols = [col for col in kline_total_df.columns if 'code' in col.lower()]
                if code_cols:
                    kline_total_df.rename(columns={code_cols[0]: 'stock_code'}, inplace=True)

            # 转换日期格式
            if 'ymd' in kline_total_df.columns:
                kline_total_df['ymd'] = pd.to_datetime(kline_total_df['ymd']).dt.strftime('%Y%m%d')
            else:
                # 添加当天日期
                kline_total_df['ymd'] = today

            # 选择需要的列（根据你的数据库表结构调整）
            required_columns = ['stock_code', 'ymd', 'open', 'close', 'high', 'low', 'change_pct', 'volume',
                                'trading_amount']
            # 只保留存在的列
            available_columns = [col for col in required_columns if col in kline_total_df.columns]
            kline_total_df = kline_total_df[available_columns]

            # 去除重复（复用你的逻辑）
            kline_total_df = kline_total_df.drop_duplicates(subset=['ymd', 'stock_code'], keep='first')

            # 输出统计信息
            date_counts = kline_total_df['ymd'].value_counts()
            logging.info("各日期数据量统计:")
            for date, count in date_counts.items():
                logging.info(f"  {date}: {count}条")

            # 6. 存入数据库（完全复用你的函数）
            mysql_utils.data_from_dataframe_to_mysql(
                user=base_properties.origin_mysql_user,  # 你的数据库配置
                password=base_properties.origin_mysql_password,
                host=base_properties.origin_mysql_host,
                database=base_properties.origin_mysql_database,
                df=kline_total_df,
                table_name="ods_stock_kline_daily_ts",  # 建议新表名
                merge_on=['ymd', 'stock_code']
            )

            logging.info(f"成功获取{len(kline_total_df)}条日K线数据")
            return kline_total_df
        else:
            logging.info('get_stock_kline_tushare的返回值为空')
            return pd.DataFrame()


    def setup(self):

        # 下载每日收盘后的日K线行情数据
        self.get_stock_kline_tushare()

        # df = self.pro.query('user', token=base_properties.ts_token)
        # print(df)


if __name__ == '__main__':
    # 1. 初始化（替换为你的真实Token）
    fetcher = SaveTushareDailyData()

    # 2. 获取数据
    kline_data = fetcher.setup()

