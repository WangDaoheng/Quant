# strategy/factor_library.py
import pandas as pd
import logging
import math
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

        # 获取全量股票列表（stock_code, stock_name）
        self.stocks_df = Mysql_Utils.get_stock_codes_latest()

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
            # 1. 获取交易日
            query_start = (pd.to_datetime(start_date) - pd.Timedelta(days=lookback_days * 2 + 10)).strftime('%Y%m%d')
            all_days = self.get_trading_days(query_start, end_date)

            if not all_days:
                return pd.DataFrame(columns=['ymd', 'stock_code', 'zt_score'])

            # 2. 确定目标区间
            # 找到 >= start_date 的第一个交易日
            start_idx = next((i for i, d in enumerate(all_days) if d >= start_date), None)
            if start_idx is None:
                return pd.DataFrame(columns=['ymd', 'stock_code', 'zt_score'])

            # 找到 <= end_date 的最后一个交易日
            end_idx = next((i for i in range(len(all_days) - 1, -1, -1) if all_days[i] <= end_date), None)
            if end_idx is None:
                return pd.DataFrame(columns=['ymd', 'stock_code', 'zt_score'])

            target_days = all_days[start_idx:end_idx + 1]

            # 3. 获取涨停数据（需要回溯 lookback_days-1 天）
            earliest_idx = max(0, start_idx - lookback_days + 1)
            query_start_zt = all_days[earliest_idx]

            zt_df = Mysql_Utils.data_from_mysql_to_dataframe(
                user=self.user, password=self.password, host=self.host, database=self.database,
                table_name='dwd_stock_zt_list',
                start_date=query_start_zt, end_date=end_date,
                cols=['ymd', 'stock_code']
            )

            # 4. 构建 (date, stock) -> 是否涨停 的标记
            zt_df['zt'] = 1
            zt_pivot = zt_df.pivot_table(index='ymd', columns='stock_code', values='zt', fill_value=0)

            # 5. 滚动求和（滑动窗口）
            # 对齐到完整交易日索引，填充0
            zt_pivot.index = pd.to_datetime(zt_pivot.index).strftime('%Y%m%d')
            zt_pivot = zt_pivot.reindex(index=all_days, fill_value=0)

            # 滚动窗口求和（包含当前日，往前 lookback_days 天）
            rolling_zt = zt_pivot.rolling(window=lookback_days, min_periods=1).sum()

            # 6. 截取目标区间
            target_rolling = rolling_zt.loc[target_days]

            # 7. 转长格式
            result = target_rolling.reset_index().melt(
                id_vars=['ymd'],
                var_name='stock_code',
                value_name='zt_count'
            )

            # 8. 计算得分
            if scoring_method == 'linear':
                result['zt_score'] = (result['zt_count'] * (100 / lookback_days)).clip(upper=100).round(2)
            elif scoring_method == 'log':
                factor = 100 / math.log2(lookback_days + 1)
                result['zt_score'] = result['zt_count'].apply(
                    lambda x: min(round(math.log2(x + 1) * factor, 2), 100) if x > 0 else 0
                )
            elif scoring_method == 'binary':
                result['zt_score'] = result['zt_count'].apply(lambda x: 100 if x > 0 else 0)
            else:
                result['zt_score'] = (result['zt_count'] * (100 / lookback_days)).clip(upper=100).round(2)

            result = result[['ymd', 'stock_code', 'zt_score']]

            if save_to_cache:
                self.cached_factors['zt'] = result.copy()

            return result

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
            try:
                Mysql_Utils.data_from_dataframe_to_mysql(
                    user=self.user,
                    password=self.password,
                    host=self.host,
                    database=self.database,
                    df=result_df,
                    table_name="dwd_factor_volume_shrinkage",
                    merge_on=['ymd', 'stock_code']
                )
                logger.info(f"缩量下跌因子已保存到 dwd_factor_volume_shrinkage，共{len(result_df)}条")
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
                print(f"没有找到 {start_dt} ~ {date} 之间的数据")
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

    def aggregate_factors(self, start_date, end_date, factors=None):
        """
        因子汇总 - 直接写入MySQL，严格按照表结构
        """
        import pandas as pd

        # 如果没有指定因子，用缓存中有的
        if factors is None:
            factors = list(self.cached_factors.keys())

        if not factors:
            logger.warning("没有指定因子，且缓存为空")
            return

        # 获取交易日列表
        trading_days = self.get_trading_days(start_date, end_date)
        if not trading_days:
            logger.warning(f"没有交易日数据: {start_date}~{end_date}")
            return

        # 构建基础DataFrame：所有股票 * 所有交易日
        base_data = []
        for date in trading_days:
            # 统一日期格式为 YYYY-MM-DD
            if isinstance(date, str) and len(date) == 8 and date.isdigit():
                date_str = f"{date[:4]}-{date[4:6]}-{date[6:8]}"
            else:
                date_str = str(date)

            for _, row in self.stocks_df.iterrows():
                base_data.append({
                    'ymd': date_str,
                    'stock_code': row['stock_code'],
                    'stock_name': row['stock_name']
                })

        summary_df = pd.DataFrame(base_data)

        # 初始化所有得分为0
        summary_df['pb_score'] = 0
        summary_df['zt_score'] = 0
        summary_df['shareholder_score'] = 0
        summary_df['volume_score'] = 0
        summary_df['price_score'] = 0
        summary_df['composite_score'] = 0
        summary_df['signal_level'] = ''

        # 定义因子列映射
        factor_cols = {
            'pb': ['pb_score'],
            'zt': ['zt_score'],
            'shareholder': ['shareholder_score'],
            'volume': ['volume_score', 'price_score', 'composite_score', 'signal_level']
        }

        # 逐个因子合并更新
        for factor_name in factors:
            if factor_name not in self.cached_factors:
                logger.warning(f"因子 {factor_name} 不在缓存中，跳过")
                continue

            df = self.cached_factors[factor_name].copy()

            # 统一因子数据的日期格式为 YYYY-MM-DD
            if 'ymd' in df.columns:
                if pd.api.types.is_datetime64_any_dtype(df['ymd']):
                    df['ymd'] = df['ymd'].dt.strftime('%Y-%m-%d')
                elif df['ymd'].dtype == 'object' and df['ymd'].astype(str).str.match(r'^\d{8}$').any():
                    df['ymd'] = pd.to_datetime(df['ymd'], format='%Y%m%d').dt.strftime('%Y-%m-%d')
                else:
                    df['ymd'] = df['ymd'].astype(str)

            cols_to_merge = factor_cols.get(factor_name, [])

            # 左连接合并
            merged = pd.merge(
                summary_df[['ymd', 'stock_code']],
                df[['ymd', 'stock_code'] + cols_to_merge],
                on=['ymd', 'stock_code'],
                how='left'
            )

            # 更新对应的列 - 修复版本
            for col in cols_to_merge:
                if col in merged.columns:
                    # 创建合并数据的副本用于更新
                    update_data = merged[['ymd', 'stock_code', col]].copy()
                    update_data = update_data[update_data[col].notna()]

                    if not update_data.empty:
                        # 直接使用布尔索引更新，避免索引问题
                        for _, row in update_data.iterrows():
                            mask = (summary_df['ymd'] == row['ymd']) & (summary_df['stock_code'] == row['stock_code'])
                            summary_df.loc[mask, col] = row[col]

        # 写入MySQL
        try:
            Mysql_Utils.data_from_dataframe_to_mysql(
                user=self.user,
                password=self.password,
                host=self.host,
                database=self.database,
                df=summary_df[['ymd', 'stock_code', 'stock_name', 'pb_score', 'zt_score',
                               'shareholder_score', 'volume_score', 'price_score',
                               'composite_score', 'signal_level']],
                table_name="dwd_factor_summary",
                merge_on=['ymd', 'stock_code']
            )
            logger.info(f"因子汇总已保存到 dwd_factor_summary，共{len(summary_df)}条")
        except Exception as e:
            logger.error(f"保存因子汇总失败：{str(e)}")

        logger.info(f"因子汇总完成，共{len(summary_df)}条，包含因子: {factors}")


    def setup(self):

        # #  pb 因子计算
        # self.pb_factor_score(start_date='20240101', end_date='20260227')
        #
        # #  涨停 因子计算
        self.zt_factor_score(start_date='20260801', end_date='20260827')

        #  股东数 因子计算
        # self.shareholder_factor_score(start_date='20260801', end_date='20260828')

        # #  缩量因子计算
        # self.volume_shrinkage_factor(start_date='20240101', end_date='20260227')
        #
        # #  因子汇总
        # self.aggregate_factors(start_date='20240101', end_date='20260227')



if __name__ == '__main__':
    factorlib = FactorLibrary()
    factorlib.setup()

    # 测试修复后的交易日获取
    # res = factorlib.get_trading_days(start_date='20260101', end_date='20260109')
    # print(f"交易日: {res}")
    #
    # pb_score = factorlib.pb_factor_score(start_date='20260101', end_date='20260109')
    # print(pb_score)

    # share_score = factorlib.shareholder_factor_score(start_date='20260101', end_date='20260109')
    # print(share_score)

    # 1. 计算因子
    # factor_df = factorlib.volume_shrinkage_factor(
    #     start_date='2026-02-01',
    #     end_date='2026-02-24'
    # )
    #
    # # 2. 查看高分股票（连续阴线+缩量）
    # high_score = factor_df[factor_df['signal_level'].isin(['A', 'B'])].sort_values(
    #     'composite_score', ascending=False
    # )
    # print("强烈信号股票：")
    # print(high_score[['ymd', 'stock_code', 'stock_name', 'consecutive_down_days',
    #                   'composite_score', 'signal_level']].head(10))

    # 3. 分析单只股票
    # factorlib.explain_volume_shrinkage('000001.SZ', '2026-02-24')

    # # 4. 统计连续3天阴线的股票
    # three_days_down = factor_df[factor_df['consecutive_down_days'] >= 3]
    # print(f"\n连续3天阴线的股票数量: {len(three_days_down)}")
    # print(three_days_down[['ymd', 'stock_code', 'consecutive_down_days',
    #                        'volume_score', 'composite_score']].head())
















