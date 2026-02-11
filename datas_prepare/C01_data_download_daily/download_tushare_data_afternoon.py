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

    @timing_decorator
    def get_stock_kline_tushare(self):
        """
        使用Tushare获取全部股票的历史日K线数据，并存入数据库
        简单优化：每5个批次（500次请求）后等待60秒
        """
        # 1. 获取日期范围
        today = DateUtility.today()

        if int(today[6:8]) > 15:
            time_start_date = DateUtility.next_day(-15)  # 15天前
        else:
            time_start_date = DateUtility.first_day_of_month()  # 当月1号
        time_end_date = today

        # 2. 获取股票代码列表
        stock_code_list = mysql_utils.get_stock_codes_latest(None)
        logging.info(f"获取到 {len(stock_code_list)} 支股票")

        # 3. 分批处理设置
        batch_size = 100  # 每个批次100个股票
        batches_per_sleep = 5  # 每5个批次后sleep（500次请求）

        def get_batches(lst, batch_size):
            for start in range(0, len(lst), batch_size):
                yield lst[start:start + batch_size]

        total_batches = (len(stock_code_list) + batch_size - 1) // batch_size
        kline_total_df = pd.DataFrame()
        successful_count = 0
        failed_count = 0

        # 4. 下载tushare数据
        for i, batch_list in enumerate(get_batches(stock_code_list, batch_size), start=1):
            sys.stdout.write(f"\r当前执行get_stock_kline_tushare的第{i}次循环，总共{total_batches}个批次")
            sys.stdout.flush()

            batch_data = pd.DataFrame()

            # 处理批次内的每个股票
            for ts_code in batch_list:
                try:
                    # 使用Tushare的pro_bar接口（日线数据）
                    df_batch = ts.pro_bar(
                        ts_code=ts_code,
                        start_date=time_start_date,
                        end_date=time_end_date,
                        adj='qfq',  # 前复权，对应你的fq="pre"
                        freq='D'  # 日线
                    )

                    if df_batch is not None and not df_batch.empty:
                        batch_data = pd.concat([batch_data, df_batch], ignore_index=True)
                        successful_count += 1
                    else:
                        failed_count += 1

                except Exception as e:
                    failed_count += 1
                    error_msg = str(e)
                    if "每分钟最多访问该接口500次" in error_msg:
                        logging.warning(f"批次{i}遇到频率限制: {error_msg[:50]}")
                    continue

            # 将批次数据添加到总数据
            if not batch_data.empty:
                kline_total_df = pd.concat([kline_total_df, batch_data], ignore_index=True)

            # 每5个批次后等待60秒（刚好500次请求）
            if i % batches_per_sleep == 0 and i < total_batches:
                wait_time = 60  # 等待60秒
                logging.info(f"已完成{i}个批次（约{i * batch_size}次请求），等待{wait_time}秒...")
                time.sleep(wait_time)
                logging.info("等待结束，继续执行...")

        sys.stdout.write("\n")
        logging.info(
            f"请求统计: 成功 {successful_count}, 失败 {failed_count}, 成功率: {successful_count / (successful_count + failed_count) * 100:.1f}%")

        # 5. 数据处理：对齐现有数据结构
        if not kline_total_df.empty:
            # 检查实际返回的列名
            logging.info(f"Tushare返回的列名: {kline_total_df.columns.tolist()}")

            # 重命名列以匹配你的数据库schema
            column_mapping = {
                'trade_date': 'ymd',
                'ts_code': 'stock_code',
                'pct_chg': 'change_pct',
                'vol': 'volume',
                'amount': 'trading_amount'
            }

            # 应用重命名
            kline_total_df.rename(columns=column_mapping, inplace=True)

            # 转换日期格式
            if 'ymd' in kline_total_df.columns:
                kline_total_df['ymd'] = pd.to_datetime(kline_total_df['ymd']).dt.strftime('%Y%m%d')
            else:
                # 添加当天日期
                kline_total_df['ymd'] = today

            # 选择需要的列（根据你的数据库表结构调整）
            required_columns = ['stock_code', 'ymd', 'open', 'close', 'high', 'low', 'change_pct', 'volume',
                                'trading_amount']

            # 确保列存在
            existing_columns = [col for col in required_columns if col in kline_total_df.columns]
            kline_total_df = kline_total_df[existing_columns]

            # 去除重复（复用你的逻辑）
            if 'ymd' in kline_total_df.columns and 'stock_code' in kline_total_df.columns:
                kline_total_df = kline_total_df.drop_duplicates(subset=['ymd', 'stock_code'], keep='first')

            # 输出统计信息
            if 'ymd' in kline_total_df.columns:
                date_counts = kline_total_df['ymd'].value_counts()
                logging.info("各日期数据量统计:")
                for date, count in date_counts.head(10).items():  # 只显示前10个日期
                    logging.info(f"  {date}: {count}条")
                if len(date_counts) > 10:
                    logging.info(f"  ... 共{len(date_counts)}个日期")

            # 6. 存入数据库（完全复用你的函数）
            try:
                mysql_utils.data_from_dataframe_to_mysql(
                    user=base_properties.origin_mysql_user,
                    password=base_properties.origin_mysql_password,
                    host=base_properties.origin_mysql_host,
                    database=base_properties.origin_mysql_database,
                    df=kline_total_df,
                    table_name="ods_stock_kline_daily_ts",
                    merge_on=['ymd', 'stock_code']
                )
                logging.info(f"成功获取{len(kline_total_df)}条日K线数据")
            except Exception as e:
                logging.error(f"保存到数据库失败: {str(e)}")
                return pd.DataFrame()

            return kline_total_df
        else:
            logging.warning('get_stock_kline_tushare的返回值为空')
            return pd.DataFrame()

    def setup(self):
        # 下载每日收盘后的日K线行情数据
        result = self.get_stock_kline_tushare()
        return result


if __name__ == '__main__':
    # 1. 初始化（替换为你的真实Token）
    fetcher = SaveTushareDailyData()

    # 2. 获取数据
    kline_data = fetcher.setup()