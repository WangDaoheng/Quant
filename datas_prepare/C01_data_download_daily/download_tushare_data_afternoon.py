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
    def __init__(self, stock_code_df=None):
        """
        初始化Tushare
        :param tushare_token: 你的Tushare API Token
        :param stock_code_df: 股票代码DataFrame，可选
        """
        # 设置Tushare Token
        ts.set_token(base_properties.ts_token)
        self.pro = ts.pro_api()
        self.stock_code_df = stock_code_df

    def get_stock_kline_tushare(self):
        """
        使用Tushare获取全部股票的历史日K线数据，并存入数据库
        完全复用现有逻辑，仅替换数据源
        """
        # 1. 获取日期范围（复用你的逻辑）
        today = DateUtility.today()

        if int(today[6:8]) > 15:
            time_start_date = DateUtility.next_day(-15)  # 15天前
        else:
            time_start_date = DateUtility.first_day_of_month()  # 当月1号
        time_end_date = today

        # 2. 获取股票代码列表（复用你的函数）
        stock_code_list = mysql_utils.get_stock_codes_latest(self.stock_code_df)

        # 3. 分批处理设置（完全复用）
        batch_size = 100

        def get_batches(lst, batch_size):
            for start in range(0, len(lst), batch_size):
                yield lst[start:start + batch_size]

        total_batches = (len(stock_code_list) + batch_size - 1) // batch_size
        kline_total_df = pd.DataFrame()

        # 4. 核心变更：使用Tushare替代Insight
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
                time.sleep(0.2)  # Tushare免费版限制5次/秒

            except Exception as e:
                logging.warning(f"批次{i}获取失败: {e}")
                time.sleep(2)  # 失败后等待更久
                continue

        sys.stdout.write("\n")

        # 5. 数据处理：对齐现有数据结构
        if not kline_total_df.empty:
            # 重命名列以匹配你的数据库schema
            kline_total_df.rename(columns={
                'ts_code': 'stock_code',
                'trade_date': 'ymd',
                'open': 'open',
                'close': 'close',
                'high': 'high',
                'low': 'low',
                'pct_chg': 'change_pct',
                'vol': 'volume',  # Tushare中成交量字段为vol
                'amount': 'trading_amount'  # 成交额，可选
            }, inplace=True)

            # 转换日期格式（Tushare返回YYYYMMDD格式字符串）
            kline_total_df['ymd'] = pd.to_datetime(kline_total_df['ymd']).dt.strftime('%Y%m%d')

            # 选择需要的列（根据你的数据库表结构调整）
            required_columns = ['stock_code', 'ymd', 'open', 'close', 'high', 'low', 'change_pct', 'volume', 'trading_amount']
            kline_total_df = kline_total_df[required_columns]

            # 去除重复（复用你的逻辑）
            kline_total_df = kline_total_df.drop_duplicates(subset=['ymd', 'stock_code'], keep='first')

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




if __name__ == '__main__':
    # 1. 初始化（替换为你的真实Token）
    fetcher = SaveTushareDailyData(base_properties.ts_token)

    # 2. 获取数据
    kline_data = fetcher.get_stock_kline_tushare()

    # 3. 对比验证（检查更新速度）
    print(f"数据日期范围: {kline_data['ymd'].min()} 至 {kline_data['ymd'].max()}")
    print(f"是否包含今日数据: {DateUtility.today() in kline_data['ymd'].values}")








