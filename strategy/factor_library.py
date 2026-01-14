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

    # @timing_decorator
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
        计算涨停因子：为全量股票计算过去lookback_days个交易日内是否有涨停
        返回:
            DataFrame: ymd, stock_code, zt_signal
        """
        try:
            # 1. 获取实际交易日列表
            trading_days = self.get_trading_days(start_date, end_date)
            if not trading_days:
                logger.warning(f"交易日列表为空: {start_date}~{end_date}")
                return pd.DataFrame(columns=['ymd', 'stock_code', 'zt_signal'])

            logger.info(f"获取到 {len(trading_days)} 个交易日: {trading_days[0]} ~ {trading_days[-1]}")

            # 2. 获取涨停记录（扩展到更早的日期以覆盖lookback_days窗口）
            # 计算需要查询的起始日期：开始日期的前lookback_days天
            start_dt = pd.to_datetime(start_date, format='%Y%m%d')
            query_start_dt = start_dt - pd.Timedelta(days=lookback_days + 10)  # 加10天作为缓冲
            query_start_date = query_start_dt.strftime('%Y%m%d')

            logger.info(f"查询涨停记录：{query_start_date} ~ {end_date}（包含缓冲期）")

            zt_df = Mysql_Utils.data_from_mysql_to_dataframe(
                user=self.user,
                password=self.password,
                host=self.host,
                database=self.database,
                table_name='dwd_stock_zt_list',
                start_date=query_start_date,
                end_date=end_date,
                cols=['ymd', 'stock_code']
            )

            if zt_df.empty:
                logger.warning(f"涨停因子数据为空: {query_start_date}~{end_date}")
                # 返回全量股票的False信号
                return self._get_all_false_signals(trading_days, start_date, end_date)

            # 3. 数据预处理
            zt_df = convert_ymd_format(zt_df, 'ymd')
            zt_df['ymd_dt'] = pd.to_datetime(zt_df['ymd'])

            # 4. 为每只股票构建涨停日期列表
            stock_zt_dates = {}
            for stock in zt_df['stock_code'].unique():
                stock_dates = zt_df[zt_df['stock_code'] == stock]['ymd_dt'].tolist()
                stock_zt_dates[stock] = sorted(stock_dates)

            # 5. 获取全量股票基本信息（参考pb_factor的实现）
            # 使用最新的交易日获取股票池
            latest_trading_day = trading_days[-1]
            stock_base_df = Mysql_Utils.data_from_mysql_to_dataframe(
                user=self.user,
                password=self.password,
                host=self.host,
                database=self.database,
                table_name='dwd_ashare_stock_base_info',
                start_date=latest_trading_day,
                end_date=latest_trading_day,
                cols=['ymd', 'stock_code', 'stock_name']
            )

            if stock_base_df.empty:
                logger.warning(f"股票基本信息数据为空: {latest_trading_day}")
                return pd.DataFrame(columns=['ymd', 'stock_code', 'zt_signal'])

            # 获取全量股票列表
            all_stocks = stock_base_df['stock_code'].unique()
            logger.info(f"获取到 {len(all_stocks)} 只全量股票")

            # 6. 将交易日转换为datetime对象
            trading_days_dt = [pd.to_datetime(d, format='%Y%m%d') for d in trading_days]

            # 7. 为每只股票的每个交易日计算涨停信号
            result_data = []

            # 创建交易日索引映射，用于快速查找前N个交易日
            date_to_index = {date: i for i, date in enumerate(trading_days_dt)}

            for i, current_date in enumerate(trading_days_dt):
                date_str = trading_days[i]  # 使用原始字符串格式

                for stock in all_stocks:
                    zt_signal = False

                    if stock in stock_zt_dates:
                        # 获取这只股票的所有涨停日期
                        stock_all_zt_dates = stock_zt_dates[stock]

                        # 找到小于等于当前日期的所有涨停
                        zt_dates_before = [d for d in stock_all_zt_dates if d <= current_date]

                        if zt_dates_before:
                            # 我们需要检查过去lookback_days个交易日内是否有涨停
                            # 首先找到当前日期在交易日列表中的位置
                            current_idx = date_to_index[current_date]

                            # 计算窗口起始索引（不能小于0）
                            window_start_idx = max(0, current_idx - lookback_days + 1)

                            # 获取窗口内的交易日
                            window_dates = trading_days_dt[window_start_idx:current_idx + 1]

                            # 检查窗口内是否有涨停
                            for zt_date in zt_dates_before:
                                if zt_date in window_dates:
                                    zt_signal = True
                                    break

                    result_data.append({
                        'ymd': date_str,
                        'stock_code': stock,
                        'zt_signal': zt_signal
                    })

                # 进度日志
                if (i + 1) % 5 == 0 or i == len(trading_days_dt) - 1:
                    logger.info(f"进度：已处理 {i + 1}/{len(trading_days_dt)} 个交易日")

            # 8. 转换为DataFrame
            result_df = pd.DataFrame(result_data)

            # 9. 统计信息
            total_records = len(result_df)
            signal_true_count = result_df['zt_signal'].sum()
            signal_true_pct = (signal_true_count / total_records * 100) if total_records > 0 else 0

            logger.info(
                f"涨停因子计算完成：日期范围 {start_date}~{end_date}，"
                f"共{len(trading_days)}个交易日，{len(all_stocks)}只股票，"
                f"总记录数：{total_records}，"
                f"涨停信号True数量：{signal_true_count}，"
                f"占比：{signal_true_pct:.2f}%"
            )

            return result_df[['ymd', 'stock_code', 'zt_signal']]

        except Exception as e:
            logger.error(f"计算涨停因子失败：{str(e)}")
            import traceback
            logger.error(traceback.format_exc())
            return pd.DataFrame(columns=['ymd', 'stock_code', 'zt_signal'])

    def _get_all_false_signals(self, trading_days, start_date, end_date):
        """
        当没有涨停记录时，为所有股票生成False信号
        """
        try:
            # 获取全量股票列表
            latest_trading_day = trading_days[-1] if trading_days else end_date
            stock_base_df = Mysql_Utils.data_from_mysql_to_dataframe(
                user=self.user,
                password=self.password,
                host=self.host,
                database=self.database,
                table_name='dwd_ashare_stock_base_info',
                start_date=latest_trading_day,
                end_date=latest_trading_day,
                cols=['stock_code']
            )

            if stock_base_df.empty:
                return pd.DataFrame(columns=['ymd', 'stock_code', 'zt_signal'])

            all_stocks = stock_base_df['stock_code'].unique()

            # 生成所有False信号
            result_data = []
            for date_str in trading_days:
                for stock in all_stocks:
                    result_data.append({
                        'ymd': date_str,
                        'stock_code': stock,
                        'zt_signal': False
                    })

            result_df = pd.DataFrame(result_data)
            logger.info(f"无涨停记录，为{len(all_stocks)}只股票生成False信号，共{len(result_df)}条记录")

            return result_df

        except Exception as e:
            logger.error(f"生成False信号失败：{str(e)}")
            return pd.DataFrame(columns=['ymd', 'stock_code', 'zt_signal'])

    # @timing_decorator
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

    # @timing_decorator
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
        获取交易日列表 - 从 ods_trading_days_insight 表获取
        简化版：假设ymd是MySQL date类型
        """
        try:
            # 从交易日历表获取交易日
            trading_days_df = Mysql_Utils.data_from_mysql_to_dataframe(
                user=self.user,
                password=self.password,
                host=self.host,
                database=self.database,
                table_name='ods_trading_days_insight',
                start_date=start_date,  # MySQL WHERE已筛选
                end_date=end_date,  # MySQL WHERE已筛选
                cols=['exchange', 'ymd']
            )

            if trading_days_df.empty:
                logger.warning(f"交易日历表为空: {start_date}~{end_date}")
                return []

            # 筛选 XSHG（上海交易所）的交易日
            sh_days = trading_days_df[trading_days_df['exchange'] == 'XSHG']

            if sh_days.empty:
                logger.warning(f"XSHG交易日为空: {start_date}~{end_date}")
                return []

            # 直接转换为YYYYMMDD格式并排序
            trading_days = sorted([
                date_obj.strftime('%Y%m%d')
                for date_obj in sh_days['ymd'].tolist()
            ])

            logger.info(
                f"获取交易日：{len(trading_days)}天，"
                f"从{trading_days[0] if trading_days else '无'}到{trading_days[-1] if trading_days else '无'}"
            )
            return trading_days

        except Exception as e:
            logger.error(f"获取交易日失败：{str(e)}")
            return []



if __name__ == '__main__':
    factorlib = FactorLibrary()
    # 测试修复后的交易日获取
    res = factorlib.get_trading_days(start_date='20260101', end_date='20260109')
    print(f"交易日: {res}")


