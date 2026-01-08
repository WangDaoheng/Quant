# strategy/factor_library.py
import pandas as pd
import logging
from CommonProperties import Mysql_Utils
from CommonProperties.Base_utils import timing_decorator, convert_ymd_format

logger = logging.getLogger(__name__)


class FactorLibrary:
    """因子计算库：基于现有MySQL数据计算PB/涨停/筹码等因子"""

    def __init__(self):
        # 复用MySQL配置
        self.user = Mysql_Utils.origin_user
        self.password = Mysql_Utils.origin_password
        self.host = Mysql_Utils.origin_host
        self.database = Mysql_Utils.origin_database

    @timing_decorator
    def pb_factor(self, start_date, end_date, pb_percentile=0.3):
        """
        计算PB因子：低于 某个分位数（例如 30）的股票标记为 True 否则为 False
        使用 dwd_ashare_stock_base_info 表
        """
        try:
            # 从DWD层读取PB数据
            pb_df = Mysql_Utils.data_from_mysql_to_dataframe(
                user=self.user,
                password=self.password,
                host=self.host,
                database=self.database,
                table_name='dwd_ashare_stock_base_info',  # 使用现有表
                start_date=start_date,
                end_date=end_date,
                cols=['ymd', 'stock_code', 'pb']
            )

            if pb_df.empty:
                logger.warning(f"PB因子数据为空: {start_date}~{end_date}")
                return pd.DataFrame(columns=['stock_code', 'ymd', 'pb', 'pb_signal'])

            # 数据预处理
            pb_df = convert_ymd_format(pb_df, 'ymd')
            pb_df = pb_df.dropna(subset=['pb'])

            # 转换pb列为数值类型
            try:
                pb_df['pb'] = pd.to_numeric(pb_df['pb'], errors='coerce')
            except:
                # 如果pb列是字符串，尝试提取数值
                pb_df['pb'] = pb_df['pb'].astype(str).str.extract(r'([\d\.]+)')[0].astype(float)

            pb_df = pb_df.dropna(subset=['pb'])

            # 计算分位数，标记低PB股票
            if len(pb_df) > 0:
                pb_threshold = pb_df['pb'].quantile(pb_percentile)
                pb_df['pb_signal'] = pb_df['pb'] < pb_threshold
            else:
                pb_df['pb_signal'] = False

            logger.info(f"PB因子计算完成：共{len(pb_df)}只股票")
            return pb_df[['stock_code', 'ymd', 'pb', 'pb_signal']]
        except Exception as e:
            logger.error(f"计算PB因子失败：{str(e)}")
            return pd.DataFrame(columns=['stock_code', 'ymd', 'pb', 'pb_signal'])


    @timing_decorator
    def zt_factor(self, start_date, end_date, lookback_days=5):
        """
        计算涨停因子：在[start_date, end_date] 内 近 lookback_days 日有涨停的股票标记为 True
        使用 dwd_stock_zt_list 表
        """
        try:
            # 从DWD层读取涨停数据
            zt_df = Mysql_Utils.data_from_mysql_to_dataframe(
                user=self.user,
                password=self.password,
                host=self.host,
                database=self.database,
                table_name='dwd_stock_zt_list',  # 使用现有表
                start_date=start_date,
                end_date=end_date,
                cols=['ymd', 'stock_code']
            )

            if zt_df.empty:
                logger.warning(f"涨停因子数据为空: {start_date}~{end_date}")
                return pd.DataFrame(columns=['stock_code', 'ymd', 'zt_signal'])

            # 数据预处理
            zt_df = convert_ymd_format(zt_df, 'ymd')
            zt_df['ymd'] = pd.to_datetime(zt_df['ymd'])

            # 按股票分组，找到每个股票的最新涨停日期
            latest_zt = zt_df.groupby('stock_code')['ymd'].max().reset_index()
            latest_zt['zt_signal'] = True
            latest_zt = latest_zt.rename(columns={'ymd': 'latest_zt_date'})

            # 获取查询结束日期
            end_date_dt = pd.to_datetime(end_date, format='%Y%m%d')

            # 计算每个股票最新涨停日距离查询结束日的天数
            latest_zt['days_since_zt'] = (end_date_dt - latest_zt['latest_zt_date']).dt.days

            # 近 lookback_days 天有涨停的标记为True
            latest_zt['zt_signal'] = latest_zt['days_since_zt'] <= lookback_days

            logger.info(f"涨停因子计算完成：共{len(latest_zt)}只股票，"
                        f"近{lookback_days}天涨停{latest_zt['zt_signal'].sum()}只")

            return latest_zt[['stock_code', 'latest_zt_date', 'zt_signal']]
        except Exception as e:
            logger.error(f"计算涨停因子失败：{str(e)}")
            return pd.DataFrame(columns=['stock_code', 'ymd', 'zt_signal'])


    @timing_decorator
    def shareholder_factor(self, start_date, end_date):
        """
        计算筹码因子：股东数环比下降的股票标记为 True
        使用 ods_shareholder_num 表
        """
        try:
            # 从ODS层读取股东数据
            shareholder_df = Mysql_Utils.data_from_mysql_to_dataframe(
                user=self.user,
                password=self.password,
                host=self.host,
                database=self.database,
                table_name='ods_shareholder_num',  # 使用现有表
                start_date=start_date,
                end_date=end_date,
                cols=['htsc_code', 'ymd', 'total_sh', 'pct_of_total_sh']
            )

            if shareholder_df.empty:
                logger.warning(f"股东因子数据为空: {start_date}~{end_date}")
                return pd.DataFrame(columns=['stock_code', 'ymd', 'total_sh', 'shareholder_signal'])

            # 数据预处理
            shareholder_df = convert_ymd_format(shareholder_df, 'ymd')
            shareholder_df.rename(columns={'htsc_code': 'stock_code'}, inplace=True)

            # 清理股票代码格式（移除后缀）
            shareholder_df['stock_code'] = shareholder_df['stock_code'].astype(str)
            shareholder_df['stock_code'] = shareholder_df['stock_code'].str.split('.').str[0]

            # 转换数据为数值类型
            shareholder_df['total_sh'] = pd.to_numeric(shareholder_df['total_sh'], errors='coerce')
            shareholder_df['pct_of_total_sh'] = pd.to_numeric(shareholder_df['pct_of_total_sh'], errors='coerce')
            shareholder_df = shareholder_df.dropna(subset=['total_sh', 'pct_of_total_sh'])

            # 按股票分组，找到最新数据
            shareholder_df = shareholder_df.sort_values(['stock_code', 'ymd'], ascending=[True, False])
            latest_data = shareholder_df.drop_duplicates('stock_code', keep='first')

            # 股东数环比下降标记为True
            latest_data['shareholder_signal'] = latest_data['pct_of_total_sh'] < 0

            logger.info(f"筹码因子计算完成：共{len(latest_data)}只股票，"
                        f"股东数下降{latest_data['shareholder_signal'].sum()}只")

            return latest_data[['stock_code', 'ymd', 'total_sh', 'pct_of_total_sh', 'shareholder_signal']]
        except Exception as e:
            logger.error(f"计算筹码因子失败：{str(e)}")
            return pd.DataFrame(columns=['stock_code', 'ymd', 'total_sh', 'shareholder_signal'])

    @timing_decorator
    def get_stock_kline_data(self, stock_code, start_date, end_date):
        """
        获取股票K线数据（用于回测）
        使用 ods_stock_kline_daily_insight 表
        """
        try:
            # 处理股票代码格式
            stock_code_clean = stock_code.split('.')[0] if '.' in stock_code else stock_code

            # 读取K线数据
            kline_df = Mysql_Utils.data_from_mysql_to_dataframe(
                user=self.user,
                password=self.password,
                host=self.host,
                database=self.database,
                table_name='ods_stock_kline_daily_insight',
                start_date=start_date,
                end_date=end_date,
                cols=['htsc_code', 'ymd', 'open', 'high', 'low', 'close', 'volume']
            )

            if kline_df.empty:
                return pd.DataFrame()

            # 过滤指定股票代码
            kline_df = kline_df[kline_df['htsc_code'].str.contains(stock_code_clean)]

            # 数据预处理
            kline_df = convert_ymd_format(kline_df, 'ymd')
            kline_df.rename(columns={'htsc_code': 'stock_code'}, inplace=True)

            return kline_df
        except Exception as e:
            logger.error(f"获取K线数据失败 {stock_code}: {str(e)}")
            return pd.DataFrame()

