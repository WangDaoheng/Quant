# strategy/factor_library.py
import pandas as pd
import logging
from CommonProperties import Mysql_Utils
from CommonProperties.Base_utils import timing_decorator, convert_ymd_format

logger = logging.getLogger(__name__)


class FactorLibrary:
    """因子计算库：基于现有MySQL数据计算PB/涨停/筹码等因子（支持多日回测）"""

    def __init__(self):
        # 复用MySQL配置
        self.user = Mysql_Utils.origin_user
        self.password = Mysql_Utils.origin_password
        self.host = Mysql_Utils.origin_host
        self.database = Mysql_Utils.origin_database

    @timing_decorator
    def pb_factor(self, start_date, end_date, pb_percentile=0.3):
        """
        计算PB因子：为日期范围内的每一天计算PB信号

        返回:
            DataFrame: ymd, stock_code, pb, pb_signal
        """
        try:
            # 从DWD层读取PB数据
            pb_df = Mysql_Utils.data_from_mysql_to_dataframe(
                user=self.user,
                password=self.password,
                host=self.host,
                database=self.database,
                table_name='dwd_ashare_stock_base_info',
                start_date=start_date,
                end_date=end_date,
                cols=['ymd', 'stock_code', 'pb']
            )

            if pb_df.empty:
                logger.warning(f"PB因子数据为空: {start_date}~{end_date}")
                return pd.DataFrame(columns=['ymd', 'stock_code', 'pb', 'pb_signal'])

            # 数据预处理
            pb_df = convert_ymd_format(pb_df, 'ymd')
            pb_df = pb_df.dropna(subset=['pb'])

            # 转换pb列为数值类型
            try:
                pb_df['pb'] = pd.to_numeric(pb_df['pb'], errors='coerce')
            except:
                pb_df['pb'] = pb_df['pb'].astype(str).str.extract(r'([\d\.]+)')[0].astype(float)

            pb_df = pb_df.dropna(subset=['pb'])

            # 按日计算分位数，标记低PB股票
            # 每个元素是一个dataframe
            result_dfs = []

            # 按日期分组处理
            pb_df['ymd_dt'] = pd.to_datetime(pb_df['ymd'])
            unique_dates = pb_df['ymd_dt'].unique()

            for date in unique_dates:
                date_str = date.strftime('%Y%m%d')
                date_df = pb_df[pb_df['ymd_dt'] == date].copy()

                if len(date_df) > 0:
                    pb_threshold = date_df['pb'].quantile(pb_percentile)
                    date_df['pb_signal'] = date_df['pb'] < pb_threshold
                    date_df['ymd'] = date_str

                    result_dfs.append(date_df[['ymd', 'stock_code', 'pb', 'pb_signal']])

            if result_dfs:
                result_df = pd.concat(result_dfs, ignore_index=True)
                logger.info(f"PB因子计算完成：共{len(result_df)}条记录，日期范围{start_date}~{end_date}")
                return result_df
            else:
                return pd.DataFrame(columns=['ymd', 'stock_code', 'pb', 'pb_signal'])

        except Exception as e:
            logger.error(f"计算PB因子失败：{str(e)}")
            return pd.DataFrame(columns=['ymd', 'stock_code', 'pb', 'pb_signal'])

    # @timing_decorator
    def zt_factor(self, start_date, end_date, lookback_days=5):
        """
        计算涨停因子：为日期范围内的每一天计算涨停信号
        返回:
            DataFrame: ymd, stock_code, zt_signal, latest_zt_date
        """
        try:
            # 1. 读取日期范围内的所有涨停记录
            zt_df = Mysql_Utils.data_from_mysql_to_dataframe(
                user=self.user,
                password=self.password,
                host=self.host,
                database=self.database,
                table_name='dwd_stock_zt_list',
                start_date=start_date,
                end_date=end_date,
                cols=['ymd', 'stock_code']
            )

            if zt_df.empty:
                logger.warning(f"涨停因子数据为空: {start_date}~{end_date}")
                # 返回空DataFrame，但包含正确的列结构
                return pd.DataFrame(columns=['ymd', 'stock_code', 'zt_signal'])

            # 2. 数据预处理
            zt_df = convert_ymd_format(zt_df, 'ymd')
            zt_df['ymd_dt'] = pd.to_datetime(zt_df['ymd'])

            # 3. 获取需要计算的所有日期
            start_dt = pd.to_datetime(start_date, format='%Y%m%d')
            end_dt = pd.to_datetime(end_date, format='%Y%m%d')

            # 从PB数据或K线数据获取实际交易日
            # 简化版：先生成所有日期，后续可以优化
            all_dates = pd.date_range(start=start_dt, end=end_dt, freq='D')

            # 4. 获取所有有涨停记录的股票
            all_zt_stocks = zt_df['stock_code'].unique()

            # 5. 为每只股票构建涨停日期列表
            stock_zt_dates = {}
            for stock in all_zt_stocks:
                stock_dates = zt_df[zt_df['stock_code'] == stock]['ymd_dt'].tolist()
                stock_zt_dates[stock] = sorted(stock_dates)

            # 6. 计算每日涨停信号
            result_data = []

            for current_date in all_dates:
                date_str = current_date.strftime('%Y%m%d')

                for stock in all_zt_stocks:
                    if stock in stock_zt_dates and stock_zt_dates[stock]:
                        # 找到小于等于当前日期的涨停记录
                        zt_dates = [d for d in stock_zt_dates[stock] if d <= current_date]

                        if zt_dates:
                            latest_zt_date = max(zt_dates)
                            days_since_zt = (current_date - latest_zt_date).days

                            # 判断是否在lookback_days窗口内
                            zt_signal = 0 <= days_since_zt <= lookback_days

                            result_data.append({
                                'ymd': date_str,
                                'stock_code': stock,
                                'zt_signal': zt_signal,
                                'latest_zt_date': latest_zt_date.strftime('%Y%m%d')
                            })

            # 7. 转换为DataFrame
            result_df = pd.DataFrame(result_data) if result_data else pd.DataFrame(
                columns=['ymd', 'stock_code', 'zt_signal', 'latest_zt_date']
            )

            # 8. 按日期和股票代码排序
            result_df = result_df.sort_values(['ymd', 'stock_code']).reset_index(drop=True)

            logger.info(
                f"涨停因子计算完成：日期范围 {start_date}~{end_date}，"
                f"共{len(all_dates)}天，{len(all_zt_stocks)}只股票有涨停记录，"
                f"总记录数：{len(result_df)}，"
                f"涨停信号True占比：{result_df['zt_signal'].mean() * 100:.2f}%"
            )

            return result_df[['ymd', 'stock_code', 'zt_signal']]

        except Exception as e:
            logger.error(f"计算涨停因子失败：{str(e)}")
            return pd.DataFrame(columns=['ymd', 'stock_code', 'zt_signal'])

    @timing_decorator
    def shareholder_factor(self, start_date, end_date):
        """
        计算筹码因子：为日期范围内的每一天计算股东数信号

        返回:
            DataFrame: ymd, stock_code, shareholder_signal, total_sh, pct_of_total_sh
        """
        try:
            # 从ODS层读取股东数据
            shareholder_df = Mysql_Utils.data_from_mysql_to_dataframe(
                user=self.user,
                password=self.password,
                host=self.host,
                database=self.database,
                table_name='ods_shareholder_num',
                start_date=start_date,
                end_date=end_date,
                cols=['htsc_code', 'ymd', 'total_sh', 'pct_of_total_sh']
            )

            if shareholder_df.empty:
                logger.warning(f"股东因子数据为空: {start_date}~{end_date}")
                return pd.DataFrame(columns=['ymd', 'stock_code', 'shareholder_signal'])

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

            # 股东数环比下降标记为True
            shareholder_df['shareholder_signal'] = shareholder_df['pct_of_total_sh'] < 0

            # 按日期排序
            shareholder_df = shareholder_df.sort_values(['ymd', 'stock_code'])

            logger.info(
                f"筹码因子计算完成：共{len(shareholder_df)}条记录，"
                f"股东数下降占比：{shareholder_df['shareholder_signal'].mean() * 100:.2f}%"
            )

            return shareholder_df[['ymd', 'stock_code', 'shareholder_signal', 'total_sh', 'pct_of_total_sh']]

        except Exception as e:
            logger.error(f"计算筹码因子失败：{str(e)}")
            return pd.DataFrame(columns=['ymd', 'stock_code', 'shareholder_signal'])

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

    # @timing_decorator
    def get_trading_days(self, start_date, end_date):
        """
        获取交易日列表（优化版）
        """
        try:
            # 从K线数据中获取实际的交易日
            kline_dates = Mysql_Utils.data_from_mysql_to_dataframe(
                user=self.user,
                password=self.password,
                host=self.host,
                database=self.database,
                table_name='ods_stock_kline_daily_insight',
                cols=['ymd']
            )['ymd'].unique()

            # 转换为日期格式
            kline_dates = pd.to_datetime(kline_dates, format='%Y%m%d')

            # 筛选日期范围
            start_dt = pd.to_datetime(start_date, format='%Y%m%d')
            end_dt = pd.to_datetime(end_date, format='%Y%m%d')

            trading_days = sorted([d for d in kline_dates if start_dt <= d <= end_dt])

            # 转换为字符串格式
            trading_days_str = [d.strftime('%Y%m%d') for d in trading_days]

            logger.info(f"获取交易日：{len(trading_days_str)}天，从{trading_days_str[0]}到{trading_days_str[-1]}")
            return trading_days_str

        except Exception as e:
            logger.error(f"获取交易日失败：{str(e)}")
            # 返回所有日期作为后备
            start_dt = pd.to_datetime(start_date, format='%Y%m%d')
            end_dt = pd.to_datetime(end_date, format='%Y%m%d')
            all_dates = pd.date_range(start=start_dt, end=end_dt, freq='D')
            return [d.strftime('%Y%m%d') for d in all_dates]

if __name__=='__main__':
    factorlib = FactorLibrary()
    res = factorlib.get_trading_days(start_date='20260101', end_date='20260109')


