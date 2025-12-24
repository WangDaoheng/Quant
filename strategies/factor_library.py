import pandas as pd
import logging
from CommonProperties.Mysql_Utils import (
    data_from_mysql_to_dataframe,
    data_from_mysql_to_dataframe_latest,
    origin_user, origin_password, origin_host, origin_database
)
from CommonProperties.Base_utils import convert_ymd_format, timing_decorator
from CommonProperties.DateUtility import DateUtility


class FactorLibrary:
    """因子库：基于现有数据封装价值/情绪/筹码/北向资金因子"""

    def __init__(self):
        self.user = origin_user
        self.password = origin_password
        self.host = origin_host
        self.database = origin_database

    @timing_decorator
    def load_base_data(self, start_date=None, end_date=None):
        """加载基础宽表数据（DWD层）"""
        if not start_date:
            start_date = DateUtility.first_day_of_month_before_n_months(3)  # 默认近3个月
        if not end_date:
            end_date = DateUtility.today()

        # 读取DWD基础信息表
        df = data_from_mysql_to_dataframe(
            user=self.user,
            password=self.password,
            host=self.host,
            database=self.database,
            table_name='dwd_ashare_stock_base_info',
            start_date=start_date,
            end_date=end_date
        )
        # 统一日期格式
        df = convert_ymd_format(df, 'ymd')
        # 缺失值填充
        df = self._fill_missing_data(df)
        return df

    @staticmethod
    def _fill_missing_data(df):
        """缺失值填充（复用基础逻辑）"""
        fill_cols = ['close', 'market_value', 'pb', 'pe', 'shareholder_num']
        for col in fill_cols:
            df[col] = df.groupby('stock_code')[col].fillna(method='ffill')
        return df

    @timing_decorator
    def pb_factor(self, quantile=0.3, start_date=None, end_date=None):
        """价值因子：低PB选股（PB前30%分位）"""
        df = self.load_base_data(start_date, end_date)
        # 过滤有效PB（>0）
        df = df[df['pb'] > 0].copy()
        # 按日期计算PB分位数
        df['pb_quantile'] = df.groupby('ymd')['pb'].rank(pct=True)
        df['pb_signal'] = df['pb_quantile'] <= quantile
        return df[['ymd', 'stock_code', 'stock_name', 'pb', 'pb_quantile', 'pb_signal']]

    @timing_decorator
    def zt_factor(self, window=5, start_date=None, end_date=None):
        """情绪因子：近N日涨停数"""
        if not start_date:
            start_date = DateUtility.first_day_of_month_before_n_months(1)
        if not end_date:
            end_date = DateUtility.today()

        # 读取涨停清单
        zt_df = data_from_mysql_to_dataframe(
            user=self.user,
            password=self.password,
            host=self.host,
            database=self.database,
            table_name='dwd_stock_ZT_list',
            start_date=start_date,
            end_date=end_date
        )
        zt_df = convert_ymd_format(zt_df, 'ymd')

        # 统计近N日涨停次数
        zt_df['ymd'] = pd.to_datetime(zt_df['ymd'])
        zt_count = zt_df.groupby(['stock_code', pd.Grouper(key='ymd', freq='D')]).size().reset_index(name='zt_count')
        zt_count['zt_count_5d'] = zt_count.groupby('stock_code')['zt_count'].rolling(window=window,
                                                                                     min_periods=1).sum().reset_index(
            drop=True)
        zt_count['zt_signal'] = zt_count['zt_count_5d'] >= 1
        return zt_count

    @timing_decorator
    def shareholder_factor(self, start_date=None, end_date=None):
        """筹码因子：股东数环比下降（筹码集中）"""
        if not start_date:
            start_date = DateUtility.first_day_of_month_before_n_months(1)
        if not end_date:
            end_date = DateUtility.today()

        # 读取股东数数据
        shareholder_df = data_from_mysql_to_dataframe(
            user=self.user,
            password=self.password,
            host=self.host,
            database=self.database,
            table_name='ods_shareholder_num',
            start_date=start_date,
            end_date=end_date
        )
        shareholder_df['pct_of_total_sh'] = pd.to_numeric(shareholder_df['pct_of_total_sh'], errors='coerce')
        # 股东数环比下降=筹码集中
        shareholder_df['shareholder_signal'] = shareholder_df['pct_of_total_sh'] < 0
        return shareholder_df[['ymd', 'stock_code', 'pct_of_total_sh', 'shareholder_signal']]

    @timing_decorator
    def north_bound_factor(self, quantile=0.7, start_date=None, end_date=None):
        """北向资金因子：持仓占比前30%"""
        if not start_date:
            start_date = DateUtility.first_day_of_month_before_n_months(1)
        if not end_date:
            end_date = DateUtility.today()

        # 读取北向资金数据
        north_df = data_from_mysql_to_dataframe(
            user=self.user,
            password=self.password,
            host=self.host,
            database=self.database,
            table_name='ods_north_bound_daily',
            start_date=start_date,
            end_date=end_date
        )
        north_df['pct_total_share'] = pd.to_numeric(north_df['pct_total_share'], errors='coerce')
        # 按日期计算北向持仓分位数
        north_df['north_quantile'] = north_df.groupby('ymd')['pct_total_share'].rank(pct=True)
        north_df['north_signal'] = north_df['north_quantile'] >= quantile
        return north_df[['ymd', 'stock_code', 'pct_total_share', 'north_signal']]