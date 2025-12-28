import pandas as pd
import logging
from CommonProperties import Mysql_Utils
from CommonProperties.Base_utils import timing_decorator, convert_ymd_format

logger = logging.getLogger(__name__)

class FactorLibrary:
    """因子计算库：基于MySQL数据计算PB/涨停/筹码等因子"""
    def __init__(self):
        # 复用MySQL配置
        self.user = Mysql_Utils.origin_user
        self.password = Mysql_Utils.origin_password
        self.host = Mysql_Utils.origin_host
        self.database = Mysql_Utils.origin_database

    @timing_decorator
    def pb_factor(self, start_date, end_date, pb_percentile=0.3):
        """
        计算PB因子：低于30分位数的股票标记为True
        :param start_date: 开始日期（YYYYMMDD）
        :param end_date: 结束日期（YYYYMMDD）
        :param pb_percentile: 分位数阈值
        :return: 包含stock_code和pb_signal的DataFrame
        """
        try:
            # 从MART层读取PB数据
            pb_df = Mysql_Utils.data_from_mysql_to_dataframe(
                user=self.user,
                password=self.password,
                host=self.host,
                database=self.database,
                table_name='mart_stock_financial_indicator_daily',
                start_date=start_date,
                end_date=end_date,
                cols=['htsc_code', 'ymd', 'pb']
            )

            # 数据预处理
            pb_df = convert_ymd_format(pb_df, 'ymd')
            pb_df.rename(columns={'htsc_code': 'stock_code'}, inplace=True)
            pb_df = pb_df.dropna(subset=['pb'])

            # 计算分位数，标记低PB股票
            pb_threshold = pb_df['pb'].quantile(pb_percentile)
            pb_df['pb_signal'] = pb_df['pb'] < pb_threshold

            logger.info(f"PB因子计算完成：共{len(pb_df)}只股票，低PB股票{pb_df['pb_signal'].sum()}只")
            return pb_df[['stock_code', 'ymd', 'pb', 'pb_signal']]
        except Exception as e:
            logger.error(f"计算PB因子失败：{str(e)}")
            return pd.DataFrame(columns=['stock_code', 'ymd', 'pb', 'pb_signal'])

    @timing_decorator
    def zt_factor(self, start_date, end_date, days=5):
        """
        计算涨停因子：近5日有涨停的股票标记为True
        :param start_date: 开始日期（YYYYMMDD）
        :param end_date: 结束日期（YYYYMMDD）
        :param days: 统计天数
        :return: 包含stock_code和zt_signal的DataFrame
        """
        try:
            # 从DWD层读取价格数据
            price_df = Mysql_Utils.data_from_mysql_to_dataframe(
                user=self.user,
                password=self.password,
                host=self.host,
                database=self.database,
                table_name='dwd_stock_price_daily',
                start_date=start_date,
                end_date=end_date,
                cols=['htsc_code', 'ymd', 'close', 'pct_change']
            )

            # 数据预处理
            price_df = convert_ymd_format(price_df, 'ymd')
            price_df.rename(columns={'htsc_code': 'stock_code'}, inplace=True)
            price_df = price_df.dropna(subset=['pct_change'])

            # 定义涨停阈值（A股涨停为9.8%以上）
            zt_threshold = 9.8
            price_df['is_zt'] = price_df['pct_change'] >= zt_threshold

            # 按股票分组，判断近days天是否有涨停
            zt_df = price_df.groupby('stock_code').agg({
                'is_zt': 'any',
                'ymd': 'last'
            }).reset_index()
            zt_df.rename(columns={'is_zt': 'zt_signal'}, inplace=True)

            logger.info(f"涨停因子计算完成：共{len(zt_df)}只股票，涨停股票{zt_df['zt_signal'].sum()}只")
            return zt_df[['stock_code', 'ymd', 'zt_signal']]
        except Exception as e:
            logger.error(f"计算涨停因子失败：{str(e)}")
            return pd.DataFrame(columns=['stock_code', 'ymd', 'zt_signal'])

    @timing_decorator
    def shareholder_factor(self, start_date, end_date):
        """
        计算筹码因子：股东数环比下降的股票标记为True
        :param start_date: 开始日期（YYYYMMDD）
        :param end_date: 结束日期（YYYYMMDD）
        :return: 包含stock_code和shareholder_signal的DataFrame
        """
        try:
            # 从MART层读取股东数据
            shareholder_df = Mysql_Utils.data_from_mysql_to_dataframe(
                user=self.user,
                password=self.password,
                host=self.host,
                database=self.database,
                table_name='mart_stock_shareholder_daily',
                start_date=start_date,
                end_date=end_date,
                cols=['htsc_code', 'ymd', 'shareholder_count']
            )

            # 数据预处理
            shareholder_df = convert_ymd_format(shareholder_df, 'ymd')
            shareholder_df.rename(columns={'htsc_code': 'stock_code'}, inplace=True)
            shareholder_df = shareholder_df.dropna(subset=['shareholder_count'])

            # 按股票分组，计算股东数环比变化
            shareholder_df = shareholder_df.sort_values(['stock_code', 'ymd'])
            shareholder_df['prev_count'] = shareholder_df.groupby('stock_code')['shareholder_count'].shift(1)
            shareholder_df['shareholder_change'] = (shareholder_df['shareholder_count'] - shareholder_df['prev_count']) / shareholder_df['prev_count']
            shareholder_df['shareholder_signal'] = shareholder_df['shareholder_change'] < 0  # 股东数下降

            # 去重，保留最新数据
            shareholder_df = shareholder_df.drop_duplicates('stock_code', keep='last')

            logger.info(f"筹码因子计算完成：共{len(shareholder_df)}只股票，股东数下降{shareholder_df['shareholder_signal'].sum()}只")
            return shareholder_df[['stock_code', 'ymd', 'shareholder_count', 'shareholder_signal']]
        except Exception as e:
            logger.error(f"计算筹码因子失败：{str(e)}")
            return pd.DataFrame(columns=['stock_code', 'ymd', 'shareholder_count', 'shareholder_signal'])