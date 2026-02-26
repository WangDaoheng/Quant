# strategy/factor_library.py
import pandas as pd
import logging
from CommonProperties import Mysql_Utils
from CommonProperties.Base_utils import timing_decorator, convert_ymd_format

logger = logging.getLogger(__name__)


class FactorLibrary:
    """因子计算库：改为百分制输出"""

    def __init__(self):
        self.user = Mysql_Utils.origin_user
        self.password = Mysql_Utils.origin_password
        self.host = Mysql_Utils.origin_host
        self.database = Mysql_Utils.origin_database

        # 简单缓存，不强制依赖
        self.cached_factors = {}

    def pb_factor_score(self, start_date, end_date, reverse=True, save_to_cache=True):
        """
        计算PB因子百分制评分（0-100分）
        简化版：有数据的计算排名，无数据的给0分
        """
        try:
            # 获取PB数据
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
                return pd.DataFrame(columns=['ymd', 'stock_code', 'pb_score'])

            # 转换数值，无效值变为NaN
            pb_df['pb'] = pd.to_numeric(pb_df['pb'], errors='coerce')

            # 按日期分组计算排名
            result_dfs = []

            for date, date_df in pb_df.groupby('ymd'):
                date_df = date_df.copy()

                # 获取有效数据的数量: valid_count
                valid_mask = date_df['pb'].notna()
                valid_count = valid_mask.sum()

                if valid_count > 0:
                    # 对有效数据计算百分制得分
                    valid_data = date_df.loc[valid_mask].copy()

                    if reverse:
                        # PB越低分越高（价值因子）
                        valid_data['pb_rank'] = valid_data['pb'].rank(method='min', ascending=True)
                    else:
                        valid_data['pb_rank'] = valid_data['pb'].rank(method='min', ascending=False)

                    max_rank = valid_data['pb_rank'].max()
                    valid_data['pb_score'] = ((max_rank - valid_data['pb_rank']) / max_rank * 100).round(2)

                    # 将得分合并回原数据框
                    for idx in valid_data.index:
                        date_df.loc[idx, 'pb_score'] = valid_data.loc[idx, 'pb_score']

                # 无效数据给0分（简单处理）
                date_df.loc[~valid_mask, 'pb_score'] = 0.0

                result_dfs.append(date_df[['ymd', 'stock_code', 'pb_score']])

            result_df = pd.concat(result_dfs, ignore_index=True)
            logger.info(f"PB因子百分制计算完成：共{len(result_df)}条记录")

            # 保存到缓存
            if save_to_cache:
                self.cached_factors['pb'] = result_df.copy()

            return result_df

        except Exception as e:
            logger.error(f"计算PB因子失败：{str(e)}")
            return pd.DataFrame(columns=['ymd', 'stock_code', 'pb_score'])

    def zt_factor_score(self, start_date, end_date, lookback_days=5, scoring_method='linear', save_to_cache=True):
        """
        计算涨停因子百分制评分（0-100分）

        Args:
            start_date: 开始日期，格式'YYYYMMDD'
            end_date: 结束日期，格式'YYYYMMDD'
            lookback_days: 回溯交易日数量，默认5天
            scoring_method: 评分方法
                - 'linear': 线性得分，每涨停一次得 100/lookback_days 分
                - 'log': 对数得分，涨停越多边际效应递减
                - 'binary': 二元得分，有涨停就得100分
            save_to_cache: 是否保存到缓存

        Returns:
            DataFrame: 包含 ymd, stock_code, zt_score 三列
        """
        try:
            # 1. 获取完整的交易日列表（包含历史数据）
            # 计算合理的起始查询日期：往前推 lookback_days * 2 + 5 天
            start_dt = pd.to_datetime(start_date)
            query_start_dt = start_dt - pd.Timedelta(days=lookback_days * 2 + 5)
            query_start_date = query_start_dt.strftime('%Y%m%d')

            # 获取从查询起始日期到 end_date 的所有交易日
            full_trading_days = self.get_trading_days(query_start_date, end_date)
            if not full_trading_days:
                logger.warning(f"未获取到交易日数据，范围: {query_start_date} - {end_date}")
                result_df = pd.DataFrame(columns=['ymd', 'stock_code', 'zt_score'])
            else:
                logger.info(
                    f"完整交易日范围: {full_trading_days[0]} 到 {full_trading_days[-1]}, 共{len(full_trading_days)}个交易日")

                # 2. 找到 start_date 在完整交易日列表中的位置
                try:
                    start_idx = full_trading_days.index(start_date)
                except ValueError:
                    # 如果 start_date 不是交易日，找第一个大于等于 start_date 的交易日
                    start_idx = None
                    for i, d in enumerate(full_trading_days):
                        if d >= start_date:
                            start_idx = i
                            break
                    if start_idx is None:
                        logger.warning(f"在交易日列表中找不到 {start_date} 及之后的日期")
                        result_df = pd.DataFrame(columns=['ymd', 'stock_code', 'zt_score'])
                    else:
                        # 3. 找到 end_date 在完整交易日列表中的位置
                        try:
                            end_idx = full_trading_days.index(end_date)
                        except ValueError:
                            # 如果 end_date 不是交易日，找最后一个小于等于 end_date 的交易日
                            end_idx = len(full_trading_days) - 1
                            for i in range(len(full_trading_days) - 1, -1, -1):
                                if full_trading_days[i] <= end_date:
                                    end_idx = i
                                    break

                        # 4. 截取需要计算的交易日范围（从 start_date 到 end_date）
                        target_trading_days = full_trading_days[start_idx:end_idx + 1]
                        logger.info(
                            f"目标计算区间: {target_trading_days[0]} 到 {target_trading_days[-1]}, 共{len(target_trading_days)}个交易日")

                        # 5. 计算需要查询涨停记录的起始日期（精确计算回溯期）
                        # 最早需要回溯的交易日索引
                        earliest_needed_idx = max(0, start_idx - lookback_days)
                        actual_query_start = full_trading_days[earliest_needed_idx]
                        logger.info(
                            f"实际查询涨停记录范围: {actual_query_start} 到 {end_date} (回溯{lookback_days}个交易日)")

                        # 6. 获取涨停记录
                        zt_df = Mysql_Utils.data_from_mysql_to_dataframe(
                            user=self.user,
                            password=self.password,
                            host=self.host,
                            database=self.database,
                            table_name='dwd_stock_zt_list',
                            start_date=actual_query_start,
                            end_date=end_date,
                            cols=['ymd', 'stock_code']
                        )

                        if zt_df.empty:
                            logger.info(f"在范围 {actual_query_start} - {end_date} 内无涨停记录，全部返回0分")
                            result_df = self._get_zero_scores(target_trading_days, start_date, end_date, 'zt_score')
                        else:
                            # 7. 确保日期列是字符串格式
                            if not pd.api.types.is_string_dtype(zt_df['ymd']):
                                zt_df['ymd'] = zt_df['ymd'].astype(str)

                            # 8. 获取全量股票列表
                            stock_base_df = Mysql_Utils.data_from_mysql_to_dataframe(
                                user=self.user,
                                password=self.password,
                                host=self.host,
                                database=self.database,
                                table_name='dwd_ashare_stock_base_info',
                                start_date=target_trading_days[-1],
                                end_date=target_trading_days[-1],
                                cols=['stock_code']
                            )

                            if stock_base_df.empty:
                                logger.warning("未获取到股票基础信息")
                                result_df = pd.DataFrame(columns=['ymd', 'stock_code', 'zt_score'])
                            else:
                                all_stocks = stock_base_df['stock_code'].unique()
                                logger.info(f"全量股票数量: {len(all_stocks)}")

                                # 9. 为每只股票构建涨停字典
                                stock_zt_dict = {}
                                for _, row in zt_df.iterrows():
                                    stock = row['stock_code']
                                    date = row['ymd']
                                    if stock not in stock_zt_dict:
                                        stock_zt_dict[stock] = []
                                    stock_zt_dict[stock].append(date)

                                logger.info(f"有涨停记录的股票数量: {len(stock_zt_dict)}")

                                # 10. 为每个目标交易日预先计算回溯起始日期（使用完整交易日列表）
                                lookback_start_dates = {}
                                date_to_idx = {date: idx for idx, date in enumerate(full_trading_days)}

                                for current_date in target_trading_days:
                                    current_idx = date_to_idx[current_date]
                                    # 计算回溯起始索引：取前 lookback_days 个交易日
                                    start_idx = max(0, current_idx - lookback_days + 1)  # +1 是因为要包含当前日
                                    lookback_start_dates[current_date] = full_trading_days[start_idx]

                                # 11. 根据评分方法计算得分函数
                                if scoring_method == 'linear':
                                    # 线性得分：每涨停一次得 100/lookback_days 分
                                    max_possible_zts = lookback_days
                                    score_per_zt = 100 / max_possible_zts if max_possible_zts > 0 else 20
                                    logger.info(f"线性评分: 每涨停一次得 {score_per_zt:.2f} 分")

                                    def calculate_score(zt_count):
                                        return min(zt_count * score_per_zt, 100)

                                elif scoring_method == 'log':
                                    # 对数得分：涨停越多边际效应递减
                                    # 公式：log2(涨停次数+1) * (100/log2(lookback_days+1))
                                    import math
                                    max_score_factor = 100 / math.log2(lookback_days + 1) if lookback_days > 0 else 20

                                    def calculate_score(zt_count):
                                        if zt_count == 0:
                                            return 0
                                        score = math.log2(zt_count + 1) * max_score_factor
                                        return min(round(score, 2), 100)

                                    logger.info(f"对数评分: 最大得分因子 {max_score_factor:.2f}")

                                elif scoring_method == 'binary':
                                    # 二元得分：有涨停就得100分
                                    def calculate_score(zt_count):
                                        return 100 if zt_count > 0 else 0

                                    logger.info("二元评分: 有涨停得100分，无涨停得0分")
                                else:
                                    # 默认线性
                                    score_per_zt = 100 / lookback_days if lookback_days > 0 else 20

                                    def calculate_score(zt_count):
                                        return min(zt_count * score_per_zt, 100)

                                    logger.info(f"默认线性评分: 每涨停一次得 {score_per_zt:.2f} 分")

                                # 12. 计算每个交易日的得分
                                result_data = []
                                total_days = len(target_trading_days)

                                for i, current_date in enumerate(target_trading_days):
                                    if i % 100 == 0 and i > 0:  # 每100个交易日打印一次进度
                                        logger.info(f"处理进度: {i}/{total_days}")

                                    lookback_start = lookback_start_dates[current_date]

                                    for stock in all_stocks:
                                        zt_score = 0

                                        if stock in stock_zt_dict:
                                            # 统计回溯期内涨停次数（使用字符串比较，精确到日）
                                            recent_zts = [d for d in stock_zt_dict[stock]
                                                          if lookback_start <= d <= current_date]

                                            zt_score = calculate_score(len(recent_zts))
                                            # 四舍五入保留2位小数
                                            zt_score = round(zt_score, 2)

                                        result_data.append({
                                            'ymd': current_date,
                                            'stock_code': stock,
                                            'zt_score': zt_score
                                        })

                                result_df = pd.DataFrame(result_data)

                                # 13. 统计得分分布
                                score_distribution = result_df['zt_score'].value_counts().sort_index()
                                logger.info(f"得分分布(前10): {dict(list(score_distribution.head(10).items()))}")

            logger.info(
                f"涨停因子计算完成：共{len(result_df)}条记录，使用{lookback_days}个交易日回溯，评分方法:{scoring_method}")

            # 保存到缓存（只添加这一行，不改变原有逻辑）
            if save_to_cache:
                self.cached_factors['zt'] = result_df.copy()

            return result_df[['ymd', 'stock_code', 'zt_score']]

        except Exception as e:
            logger.error(f"计算涨停因子失败：{str(e)}")
            import traceback
            logger.error(traceback.format_exc())
            return pd.DataFrame(columns=['ymd', 'stock_code', 'zt_score'])

    def shareholder_factor_score(self, start_date, end_date, save_to_cache=True):
        """
        计算筹码因子百分制评分（0-100分）
        使用 dwd_shareholder_num_latest 表

        评分逻辑：股东人数减少得高分，增加得低分
        使用平滑的 sigmoid 函数

        Args:
            start_date: 开始日期 (YYYYMMDD)
            end_date: 结束日期 (YYYYMMDD)
            save_to_cache: 是否保存到缓存

        Returns:
            DataFrame: 包含 ymd, stock_code, stock_name, shareholder_score
        """
        try:
            import math

            # 获取股东数据
            shareholder_df = Mysql_Utils.data_from_mysql_to_dataframe(
                user=self.user,
                password=self.password,
                host=self.host,
                database=self.database,
                table_name='dwd_shareholder_num_latest',
                start_date=start_date,
                end_date=end_date,
                cols=['ymd', 'stock_code', 'stock_name', 'pct_of_total_sh']
            )

            if shareholder_df.empty:
                result_df = pd.DataFrame(columns=['ymd', 'stock_code', 'stock_name', 'shareholder_score'])
            else:
                # 转换数值
                shareholder_df['pct_of_total_sh'] = pd.to_numeric(shareholder_df['pct_of_total_sh'], errors='coerce')

                # 定义平滑得分函数
                def smooth_score(pct):
                    if pd.isna(pct):
                        return 0.0
                    # sigmoid 变换: 股东减少(-) → 高分，股东增加(+) → 低分
                    x = pct * 0.15  # 0.15 控制曲线陡峭程度
                    sigmoid = 1 / (1 + math.exp(-x))
                    return round(100 * (1 - sigmoid), 2)

                # 计算得分
                shareholder_df['shareholder_score'] = shareholder_df['pct_of_total_sh'].apply(smooth_score)
                result_df = shareholder_df[['ymd', 'stock_code', 'stock_name', 'shareholder_score']]

            logger.info(f"股东人数因子计算完成：共{len(result_df)}条记录")

            # 保存到缓存
            if save_to_cache:
                self.cached_factors['shareholder'] = result_df.copy()

            return result_df

        except Exception as e:
            logger.error(f"计算股东数因子失败：{str(e)}")
            return pd.DataFrame(columns=['ymd', 'stock_code', 'stock_name', 'shareholder_score'])


    def _get_zero_scores(self, trading_days, start_date, end_date, score_col):
        """生成全0分数据"""
        try:
            # 获取股票列表
            stock_base_df = Mysql_Utils.data_from_mysql_to_dataframe(
                user=self.user,
                password=self.password,
                host=self.host,
                database=self.database,
                table_name='dwd_ashare_stock_base_info',
                start_date=trading_days[-1] if trading_days else end_date,
                end_date=trading_days[-1] if trading_days else end_date,
                cols=['stock_code']
            )

            if stock_base_df.empty:
                return pd.DataFrame(columns=['ymd', 'stock_code', score_col])

            all_stocks = stock_base_df['stock_code'].unique()

            result_data = []
            for date_str in trading_days:
                for stock in all_stocks:
                    result_data.append({
                        'ymd': date_str,
                        'stock_code': stock,
                        score_col: 0.0
                    })

            return pd.DataFrame(result_data)

        except Exception as e:
            logger.error(f"生成零分数据失败：{str(e)}")
            return pd.DataFrame(columns=['ymd', 'stock_code', score_col])

    def get_trading_days(self, start_date, end_date):
        """获取交易日列表"""
        try:
            trading_days_df = Mysql_Utils.data_from_mysql_to_dataframe(
                user=self.user,
                password=self.password,
                host=self.host,
                database=self.database,
                table_name='ods_trading_days_insight',
                start_date=start_date,
                end_date=end_date,
                cols=['ymd']
            )

            if trading_days_df.empty:
                return []

            # 直接返回排序后的日期列表
            trading_days = sorted(trading_days_df['ymd'].unique())
            return [d.strftime('%Y%m%d') for d in trading_days]

        except Exception as e:
            logger.error(f"获取交易日失败：{str(e)}")
            return []

    def volume_shrinkage_factor(self, start_date, end_date,  save_to_cache=True):
        """
        计算缩量下跌因子（0-100分）
        使用预计算的均线表，固定使用60日均量作为长期基准

        评分逻辑：
        1. 成交量条件（60分）：
           - 近5日均量低于60日均量（30分）
           - 近1-3天连续缩量（30分）
        2. 价格条件（40分）：
           - 连续阴线天数越多，得分越高
           - 连续3天阴线得满分40分
        """
        try:
            # 1. 从技术指标表获取数据
            tech_df = Mysql_Utils.data_from_mysql_to_dataframe(
                user=self.user,
                password=self.password,
                host=self.host,
                database=self.database,
                table_name='dwd_stock_technical_indicators',
                start_date=start_date,
                end_date=end_date,
                cols=['ymd', 'stock_code', 'stock_name', 'close', 'volume',
                      'vol_ma5', 'vol_ma60', 'volume_vs_ma5']
            )

            # 定义固定的列顺序（与表结构完全一致）
            fixed_columns = [
                'ymd', 'stock_code', 'stock_name',
                'close', 'volume',
                'vol_ma5', 'vol_ma60', 'volume_vs_ma5',
                'is_shrink_today', 'consecutive_shrink_days',
                'is_down', 'consecutive_down_days',
                'volume_score', 'price_score', 'composite_score', 'signal_level'
            ]

            if tech_df.empty:
                logger.warning(f"技术指标数据为空: {start_date}~{end_date}")
                return pd.DataFrame(columns=fixed_columns)

            # 2. 获取阴线数据   关于引线 todo  最好是连阴但累计跌幅却有限的
            down_df = self.get_down_days(start_date, end_date)

            if down_df.empty:
                logger.warning(f"阴线数据为空: {start_date}~{end_date}")
                return pd.DataFrame(columns=fixed_columns)

            # 3. 合并数据
            merged_df = pd.merge(
                tech_df,
                down_df[['ymd', 'stock_code', 'is_down']],
                on=['ymd', 'stock_code'],
                how='inner'
            )

            # 4. 按股票分组计算连续指标
            result_dfs = []

            for stock, stock_df in merged_df.groupby('stock_code'):
                stock_df = stock_df.copy()
                stock_df = stock_df.sort_values('ymd')

                # 计算连续缩量（volume_vs_ma5 < 0 表示当日成交量低于5日均量）
                stock_df['is_shrink_today'] = stock_df['volume_vs_ma5'] < 0

                # 计算连续缩量天数
                def count_consecutive(series):
                    """计算连续True的天数（从最新往旧统计）"""
                    count = 0
                    for val in series[::-1]:
                        if val:
                            count += 1
                        else:
                            break
                    return count

                # 连续缩量天数（最近5天内）
                stock_df['consecutive_shrink_days'] = 0
                for i in range(len(stock_df)):
                    start_idx = max(0, i - 4)  # 最近5天
                    window = stock_df['is_shrink_today'].iloc[start_idx:i + 1]
                    stock_df.iloc[i, stock_df.columns.get_loc('consecutive_shrink_days')] = \
                        count_consecutive(window.values)

                # 连续阴线天数（最近5天内）
                stock_df['consecutive_down_days'] = 0
                for i in range(len(stock_df)):
                    start_idx = max(0, i - 4)  # 最近5天
                    window = stock_df['is_down'].iloc[start_idx:i + 1]
                    stock_df.iloc[i, stock_df.columns.get_loc('consecutive_down_days')] = \
                        count_consecutive(window.values)

                result_dfs.append(stock_df)

            final_df = pd.concat(result_dfs, ignore_index=True)

            # 5. 计算成交量得分（0-60分）
            def calculate_volume_score(row):
                score = 0

                # 条件1：5日均量 < 60日均量（30分）
                if not pd.isna(row['vol_ma5']) and not pd.isna(row['vol_ma60']):
                    if row['vol_ma5'] < row['vol_ma60'] * 0.8:  # 低于80%
                        score += 30
                    elif row['vol_ma5'] < row['vol_ma60']:  # 低于100%
                        score += 20
                    elif row['vol_ma5'] < row['vol_ma60'] * 1.2:  # 低于120%
                        score += 10

                # 条件2：连续缩量天数（30分）
                if row['consecutive_shrink_days'] >= 3:
                    score += 30
                elif row['consecutive_shrink_days'] == 2:
                    score += 20
                elif row['consecutive_shrink_days'] == 1:
                    score += 10

                return min(score, 60)

            # 6. 计算价格得分（0-40分）
            def calculate_price_score(row):
                if row['consecutive_down_days'] >= 3:
                    return 40
                elif row['consecutive_down_days'] == 2:
                    return 30
                elif row['consecutive_down_days'] == 1:
                    return 20
                else:
                    return 0

            final_df['volume_score'] = final_df.apply(calculate_volume_score, axis=1)
            final_df['price_score'] = final_df.apply(calculate_price_score, axis=1)
            final_df['composite_score'] = (final_df['volume_score'] + final_df['price_score']).round(2)

            # 7. 添加评分等级
            def get_score_level(score):
                if score >= 80:
                    return 'A'  # 强烈信号
                elif score >= 60:
                    return 'B'  # 明显信号
                elif score >= 40:
                    return 'C'  # 一般信号
                elif score >= 20:
                    return 'D'  # 弱信号
                else:
                    return 'E'  # 无信号

            final_df['signal_level'] = final_df['composite_score'].apply(get_score_level)

            # 6. 按固定列顺序选择数据
            result_df = final_df[fixed_columns].copy()
            logger.info(f"缩量下跌因子计算完成：共{len(result_df)}条记录")

            # 7. 保存到缓存
            if save_to_cache:
                self.cached_factors['volume'] = result_df.copy()

            # 8. 保存到数据库
            if save_to_cache:
                try:
                    Mysql_Utils.data_from_dataframe_to_mysql(
                        user=self.user,
                        password=self.password,
                        host=self.host,
                        database=self.database,
                        df=result_df,
                        table_name="dwb_factor_volume_shrinkage",
                        merge_on=['ymd', 'stock_code']
                    )
                    logger.info(f"缩量下跌因子已保存到 dwb_factor_volume_shrinkage，共{len(result_df)}条")
                except Exception as e:
                    logger.error(f"保存缩量下跌因子失败：{str(e)}")

            return result_df

        except Exception as e:
            logger.error(f"计算缩量下跌因子失败：{str(e)}")
            import traceback
            logger.error(traceback.format_exc())
            return pd.DataFrame(columns=fixed_columns)


    def explain_volume_shrinkage(self, stock_code, date):
        """
        详细解释某只股票某天的缩量下跌因子
        使用 ods_stock_kline_daily_ts 表
        """
        try:
            # 获取该股票的历史数据
            target_date = pd.to_datetime(date)
            start_dt = target_date - pd.Timedelta(days=100)

            df = Mysql_Utils.data_from_mysql_to_dataframe(
                user=self.user,
                password=self.password,
                host=self.host,
                database=self.database,
                table_name='ods_stock_kline_daily_ts',
                start_date=start_dt.strftime('%Y%m%d'),
                end_date=date,
                cols=['stock_code', 'ymd', 'close', 'volume', 'change_pct']
            )

            if df.empty:
                print(f"没有找到 {stock_code} 的数据")
                return

            df = df[df['stock_code'] == stock_code].sort_values('ymd')
            df['ymd'] = pd.to_datetime(df['ymd'])

            # 计算指标
            df['is_down'] = df['change_pct'] < 0
            df['volume_ma90'] = df['volume'].rolling(90).mean()
            df['volume_ma5'] = df['volume'].rolling(5).mean()
            df['volume_decrease'] = df['volume'].diff() < 0

            # 计算连续天数
            def count_consecutive(series):
                count = 0
                for val in series[::-1]:
                    if val:
                        count += 1
                    else:
                        break
                return count

            # 获取目标日期的数据
            target_idx = df[df['ymd'] == target_date].index
            if len(target_idx) == 0:
                print(f"没有找到 {stock_code} 在 {date} 的数据")
                return

            target_idx = target_idx[0]
            start_idx = max(0, target_idx - 4)

            recent_down = df.loc[start_idx:target_idx, 'is_down']
            recent_volume = df.loc[start_idx:target_idx, 'volume_decrease']

            consecutive_down = count_consecutive(recent_down.values)
            consecutive_volume = count_consecutive(recent_volume.values)

            target = df.loc[target_idx]

            print("=" * 70)
            print(f"缩量下跌因子详解 - {stock_code} @ {date}")
            print("=" * 70)

            # 显示最近5天的K线
            print("\n【最近5天走势】")
            print(f"{'日期':<10} {'涨跌幅':>8} {'收盘':>8} {'成交量':>12} {'状态':>6}")
            print("-" * 50)

            recent = df.loc[max(0, target_idx - 4):target_idx]
            for _, row in recent.iterrows():
                status = "阴线" if row['is_down'] else "阳线"
                print(f"{row['ymd'].strftime('%m-%d'):<10} "
                      f"{row['change_pct']:>7.2f}% "
                      f"{row['close']:>8.2f} "
                      f"{row['volume']:>12.0f} "
                      f"{status:>6}")

            # 成交量分析
            print("\n【成交量分析】")
            if not pd.isna(target['volume_ma5']) and not pd.isna(target['volume_ma90']):
                ratio = target['volume_ma5'] / target['volume_ma90']
                print(f"  当前成交量: {target['volume']:.0f}")
                print(f"  5日均量: {target['volume_ma5']:.0f}")
                print(f"  90日均量: {target['volume_ma90']:.0f}")
                print(f"  短期/长期均量比: {ratio:.2f}")

                vol_score = 0
                if target['volume_ma5'] < target['volume_ma90'] * 0.8:
                    vol_score += 30
                    print("  ✓ 条件1: 5日均量 < 90日均量80% (+30分)")
                elif target['volume_ma5'] < target['volume_ma90']:
                    vol_score += 20
                    print("  ✓ 条件1: 5日均量 < 90日均量 (+20分)")
                elif target['volume_ma5'] < target['volume_ma90'] * 1.2:
                    vol_score += 10
                    print("  ✓ 条件1: 5日均量 < 90日均量120% (+10分)")
                else:
                    print("  ✗ 条件1: 成交量未达标")

                print(f"\n【连续缩量】")
                print(f"  连续缩量天数: {consecutive_volume}")
                if consecutive_volume >= 3:
                    vol_score += 30
                    print(f"  ✓ 条件2: 连续{consecutive_volume}天缩量 (+30分)")
                elif consecutive_volume == 2:
                    vol_score += 20
                    print(f"  ✓ 条件2: 连续2天缩量 (+20分)")
                elif consecutive_volume == 1:
                    vol_score += 10
                    print(f"  ✓ 条件2: 连续1天缩量 (+10分)")
                else:
                    print("  ✗ 条件2: 无连续缩量")

                print(f"\n【成交量得分】: {min(vol_score, 60)}/60")

                # 价格分析
                print("\n【价格分析 - 连续阴线】")
                print(f"  连续阴线天数: {consecutive_down}")

                price_score = 0
                if consecutive_down >= 3:
                    price_score = 40
                    print(f"  ✓ 连续{consecutive_down}天阴线 (+40分)")
                elif consecutive_down == 2:
                    price_score = 30
                    print(f"  ✓ 连续2天阴线 (+30分)")
                elif consecutive_down == 1:
                    price_score = 20
                    print(f"  ✓ 连续1天阴线 (+20分)")
                else:
                    print("  ✗ 无连续阴线")

                print(f"\n【价格得分】: {price_score}/40")

                # 综合得分
                composite = vol_score + price_score
                print(f"\n【综合得分】: {composite:.0f}/100")

                if composite >= 80:
                    print("【信号等级】: A - 强烈信号")
                elif composite >= 60:
                    print("【信号等级】: B - 明显信号")
                elif composite >= 40:
                    print("【信号等级】: C - 一般信号")
                elif composite >= 20:
                    print("【信号等级】: D - 弱信号")
                else:
                    print("【信号等级】: E - 无信号")

            print("=" * 70)

        except Exception as e:
            print(f"分析失败: {str(e)}")
            import traceback
            traceback.print_exc()

    def get_down_days(self, start_date, end_date):
        """
        获取每日的阴线标记
        阴线定义：基于 ods_stock_kline_daily_ts 表中的 is_down 字段
        is_down = 1 表示阴线（收盘价 < 开盘价）
        Args:
            start_date: 开始日期 (YYYYMMDD)
            end_date: 结束日期 (YYYYMMDD)
        Returns:
            DataFrame: ymd, stock_code, is_down (True/False)
        """
        try:
            # 从 ods_stock_kline_daily_ts 表获取阴线数据
            down_df = Mysql_Utils.data_from_mysql_to_dataframe(
                user=self.user,
                password=self.password,
                host=self.host,
                database=self.database,
                table_name='ods_stock_kline_daily_ts',
                start_date=start_date,
                end_date=end_date,
                cols=['ymd', 'stock_code', 'is_down']
            )

            if down_df.empty:
                logger.warning(f"阴线数据为空: {start_date}~{end_date}")
                return pd.DataFrame(columns=['ymd', 'stock_code', 'is_down'])

            # 将 is_down 从 0/1 转换为 False/True
            down_df['is_down'] = down_df['is_down'].astype(bool)

            logger.info(f"获取阴线数据完成：共{len(down_df)}条记录")

            return down_df[['ymd', 'stock_code', 'is_down']]

        except Exception as e:
            logger.error(f"获取阴线数据失败：{str(e)}")
            return pd.DataFrame(columns=['ymd', 'stock_code', 'is_down'])

    def aggregate_factors(self, start_date, end_date, factors=None, save_to_db=True):
        """
        简单的因子汇总 - 有什么算什么
        Args:
            start_date: 开始日期
            end_date: 结束日期
            factors: 要汇总的因子列表，如 ['pb', 'zt', 'shareholder', 'volume']
                     如果不传，则汇总所有已缓存的因子
            save_to_db: 是否保存到数据库
        Returns:
            DataFrame: 汇总后的因子得分
        """
        # 如果没有指定因子，用缓存中有的
        if factors is None:
            factors = list(self.cached_factors.keys())

        if not factors:
            logger.warning("没有指定因子，且缓存为空")
            return pd.DataFrame(columns=['ymd', 'stock_code'])

        # 定义因子得分列的映射（硬编码，简单直接）
        score_col_map = {
            'pb': 'pb_score',
            'zt': 'zt_score',
            'shareholder': 'shareholder_score',
            'volume': 'composite_score'  # volume因子返回的就是 composite_score
        }

        # 逐个处理因子
        summary_df = None

        for factor_name in factors:
            # 检查是否在缓存中
            if factor_name not in self.cached_factors:
                logger.warning(f"因子 {factor_name} 不在缓存中，跳过")
                continue

            df = self.cached_factors[factor_name]
            score_col = score_col_map.get(factor_name)

            if score_col not in df.columns:
                logger.warning(f"因子 {factor_name} 的DataFrame中没有 {score_col} 列")
                continue

            # 只取需要的列
            factor_df = df[['ymd', 'stock_code', score_col]].copy()
            factor_df = factor_df.rename(columns={score_col: f'{factor_name}_score'})

            # 合并
            if summary_df is None:
                summary_df = factor_df
            else:
                summary_df = pd.merge(
                    summary_df, factor_df,
                    on=['ymd', 'stock_code'],
                    how='outer'
                )

        if summary_df is None:
            logger.warning("没有成功合并任何因子")
            return pd.DataFrame(columns=['ymd', 'stock_code'])

        # 填充空值为0
        score_cols = [col for col in summary_df.columns if col.endswith('_score')]
        for col in score_cols:
            summary_df[col] = summary_df[col].fillna(0)

        # 保存到数据库
        if save_to_db:
            try:
                Mysql_Utils.data_from_dataframe_to_mysql(
                    user=self.user,
                    password=self.password,
                    host=self.host,
                    database=self.database,
                    df=summary_df,
                    table_name="dwb_factor_summary",
                    merge_on=['ymd', 'stock_code']
                )
                logger.info(f"因子汇总已保存到 dwb_factor_summary，共{len(summary_df)}条")
            except Exception as e:
                logger.error(f"保存因子汇总失败：{str(e)}")

        logger.info(f"因子汇总完成，共{len(summary_df)}条，包含因子: {factors}")

        return summary_df


    def setup(self):

        #  pb 因子计算
        self.pb_factor_score(start_date='20250101', end_date='20260215')

        #  涨停 因子计算
        self.zt_factor_score(start_date='20250101', end_date='20260215')

        #  股东数 因子计算
        self.shareholder_factor_score(start_date='20250101', end_date='20260215')

        #  缩量因子计算
        self.volume_shrinkage_factor(start_date='20250101', end_date='20260215')

        #  因子汇总
        self.aggregate_factors(start_date='20250101', end_date='20260215')



if __name__ == '__main__':
    factorlib = FactorLibrary()
    # factorlib.setup()

    # 测试修复后的交易日获取
    # res = factorlib.get_trading_days(start_date='20260101', end_date='20260109')
    # print(f"交易日: {res}")
    #
    # pb_score = factorlib.pb_factor_score(start_date='20260101', end_date='20260109')
    # print(pb_score)

    # share_score = factorlib.shareholder_factor_score(start_date='20260101', end_date='20260109')
    # print(share_score)

    # 1. 计算因子
    factor_df = factorlib.volume_shrinkage_factor(
        start_date='2026-02-01',
        end_date='2026-02-24'
    )

    # 2. 查看高分股票（连续阴线+缩量）
    high_score = factor_df[factor_df['signal_level'].isin(['A', 'B'])].sort_values(
        'composite_score', ascending=False
    )
    print("强烈信号股票：")
    print(high_score[['ymd', 'stock_code', 'stock_name', 'consecutive_down_days',
                      'composite_score', 'signal_level']].head(10))

    # 3. 分析单只股票
    factorlib.explain_volume_shrinkage('000001', '2026-02-22')

    # 4. 统计连续3天阴线的股票
    three_days_down = factor_df[factor_df['consecutive_down_days'] >= 3]
    print(f"\n连续3天阴线的股票数量: {len(three_days_down)}")
    print(three_days_down[['ymd', 'stock_code', 'consecutive_down_days',
                           'volume_score', 'composite_score']].head())
















