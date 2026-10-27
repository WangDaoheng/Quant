# strategy/factor_library.py
import pandas as pd
import numpy as np
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


    def pb_factor_score(self, start_date, end_date, reverse=True, save_to_cache=True):
        """
        计算 PB 因子百分制评分
        source_table: dwd_ashare_stock_base_info
        简化版：对于 pb >0 的股票，根据每日截面计算 pb 排名。
        reverse为 True（默认）, pb升序排名, pb越小, 排名值越小, 打分越高（有效数据得分[1, 100]，无效数据得分0）
        reverse为 False, pb降序排名, pb越大, 排名越小, 打分越高（有效数据得分[1, 100]，无效数据得分0）
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
            # 新增：pb<=0 视为无效（0 是 dwd 层缺失填充值，负 PB 是净资产为负的困难企业）
            # 之后Pe也要这么处理，pe<=视为无效
            pb_df['pb'] = pb_df['pb'].mask(pb_df['pb'] <= 0)

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
                    valid_data['pb_score'] = (100.0 if max_rank == 1 else (
                                1 + 99 * (max_rank - valid_data['pb_rank']) / (max_rank - 1)).round(2))
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


    def zt_factor_score(self, start_date, end_date, lookback_days=10,
                        type_weight=None, decay='exp', half_life=5,
                        streak_bonus=5, max_streak=5,
                        reverse=False, save_to_cache=True):
        """
        计算涨停因子百分制评分
        source_table: dwd_stock_zt_list
        逻辑：统计每只股票过去 lookback_days 个交易日内的涨停强度 raw_score，由三部分构成：
            1) 【涨停类型】：一字板 yizi=1.0 / T字板 tzi=0.9 / 实体板 shiti=0.8 / 炸板 zha=0.2
               （封板质量越高分越高；炸板为弱势涨停，大幅弱化但仍保留少量动量信息）；
            2) 【时间衰减】：距今越近权重越高。decay='exp' 指数衰减 exp(-age/half_life)，
               其中，age 指某次涨停事件距离"当前打分日"隔了几个交易日,
               half_life 半衰期,影响力衰减到原来一半所需要的交易日数。
               decay='linear' 线性衰减 (lookback_days-age)/lookback_days，
               decay='none' 不衰减；
            3) 【连板加成】：截至当日连续封板天数 streak（炸板打断连板），首板无加成，每多一板加 streak_bonus 分，封顶 max_streak 板
               （连板高度是短线最强信号，边际递增）。
                连板加成 = (连板高度 - 1) × streak_bonus，封顶 max_streak
                streak_bonus：每一板值多少分
                max_streak：连板加成的封顶板数（防止极端值垄断）
        raw_score(t) = Σ [ 涨停类型权重 × 时间衰减权重 ]   ← 对窗口内每次涨停求和
                       + 连板加成                       ← 独立相加
        每日截面上对 raw_score 排名映射到 [1,100]：
            reverse=False（默认）：raw_score 越大，排名越靠前，得分越高；
            reverse=True：反向打分（用于观察涨停后的反转/回落效应）。
        异常值处理：
            - zt_type 缺失的涨停记录按实体板 shiti=0.8 计；
            - 区间内无任何涨停记录的股票得分 0（与 pb_score 的无效=0 口径一致）；
            - 仅炸板(zha)无封板的股票 raw_score 很低，截面排名自然靠后。
        股票池：以 ods_stock_code_daily_insight（每日已上市股票码表）为全量底表，
            窗口内无涨停记录的股票 raw_score 补 0，保证输出覆盖全市场，
            与 pb_score 等因子的股票池口径一致，便于合成。
        """
        try:
            if type_weight is None:
                type_weight = {'yizi': 1.0, 'tzi': 0.9, 'shiti': 0.8, 'zha': 0.2}

            # ========== 1. 交易日与目标区间 ==========
            query_start = (pd.to_datetime(start_date)
                           - pd.Timedelta(days=lookback_days * 2 + 20)).strftime('%Y%m%d')
            all_days = self.get_trading_days(query_start, end_date)
            if not all_days:
                return pd.DataFrame(columns=['ymd', 'stock_code', 'zt_score'])

            start_idx = next((i for i, d in enumerate(all_days) if d >= start_date), None)
            end_idx = next((i for i in range(len(all_days) - 1, -1, -1) if all_days[i] <= end_date), None)
            if start_idx is None or end_idx is None:
                return pd.DataFrame(columns=['ymd', 'stock_code', 'zt_score'])

            target_days = all_days[start_idx:end_idx + 1]
            # 连板计算需要额外回溯（连板最长可能超过 lookback_days）
            earliest_idx = max(0, start_idx - lookback_days - max_streak)
            query_start_zt = all_days[earliest_idx]

            # ========== 2. 读取涨停数据 ==========
            zt_df = Mysql_Utils.data_from_mysql_to_dataframe(
                user=self.user, password=self.password, host=self.host, database=self.database,
                table_name='dwd_stock_zt_list',
                start_date=query_start_zt, end_date=end_date,
                cols=['ymd', 'stock_code', 'zt_type']
            )
            if zt_df.empty:
                return pd.DataFrame(columns=['ymd', 'stock_code', 'zt_score'])

            zt_df['ymd'] = pd.to_datetime(zt_df['ymd']).dt.strftime('%Y%m%d')
            # zt_type 缺失按实体板计
            zt_df['type_w'] = zt_df['zt_type'].map(type_weight).fillna(type_weight['shiti'])
            # 封板标记：炸板不算封板（用于连板计算，炸板会打断连板）
            zt_df['is_sealed'] = (zt_df['zt_type'] != 'zha').astype(int)

            # ========== 3. 构建事件矩阵（行=交易日，列=股票） ==========
            event = zt_df.pivot_table(index='ymd', columns='stock_code',
                                      values='type_w', aggfunc='max')
            event = event.reindex(index=all_days).fillna(0)

            sealed = zt_df.pivot_table(index='ymd', columns='stock_code',
                                       values='is_sealed', aggfunc='max')
            sealed = sealed.reindex(index=all_days).fillna(0)

            # ========== 4. 时间衰减加权的涨停强度 ==========
            lags = np.arange(lookback_days)
            if decay == 'exp':
                decay_w = np.exp(-lags / half_life)
            elif decay == 'linear':
                decay_w = (lookback_days - lags) / lookback_days
            else:  # 'none'
                decay_w = np.ones(lookback_days)

            # raw = Σ_lag 事件权重(lag天前) × 衰减(lag)，向量化实现
            raw = pd.DataFrame(0.0, index=all_days, columns=event.columns)
            for lag in range(lookback_days):
                raw = raw.add(event.shift(lag).fillna(0) * decay_w[lag], fill_value=0)

            # ========== 5. 连板加成 ==========
            def _streak(s):
                # 截至当日的连续封板天数：0/1 序列上的连续1计数
                return s * s.groupby((s == 0).cumsum()).cumsum()

            streak = sealed.apply(_streak, axis=0)
            # 首板不加成，每多一板 +streak_bonus，封顶 max_streak 板
            raw = raw + (streak - 1).clip(lower=0).clip(upper=max_streak - 1) * streak_bonus

            # ========== 6. 截取目标区间并转长格式 ==========
            target_raw = raw.loc[target_days]
            long_df = (target_raw.rename_axis('ymd')
                       .reset_index()
                       .melt(id_vars='ymd', var_name='stock_code', value_name='raw_score'))

            # ========== 6.1 取全量股票底表（每日已上市股票） ==========
            universe_df = Mysql_Utils.data_from_mysql_to_dataframe(
                user=self.user, password=self.password, host=self.host, database=self.database,
                table_name='ods_stock_code_daily_insight',
                start_date=target_days[0], end_date=target_days[-1],
                cols=['ymd', 'stock_code']
            )
            if universe_df.empty:
                # 码表不可用时退化为只输出有涨停记录的股票
                logger.warning("ods_stock_code_daily_insight 无数据，因子仅覆盖窗口内有涨停记录的股票")
            else:
                universe_df['ymd'] = pd.to_datetime(universe_df['ymd']).dt.strftime('%Y%m%d')
                universe_df = universe_df[['ymd', 'stock_code']].drop_duplicates()
                # 左连接到底表：窗口内无涨停记录的股票 raw_score 补 0
                long_df = universe_df.merge(long_df, on=['ymd', 'stock_code'], how='left')
                long_df['raw_score'] = long_df['raw_score'].fillna(0)

            # ========== 7. 每日截面排名 → [1,100]，无涨停记录=0 ==========
            result_dfs = []
            for date, date_df in long_df.groupby('ymd'):
                date_df = date_df.copy()
                valid_mask = date_df['raw_score'] > 0
                valid_count = valid_mask.sum()

                if valid_count > 0:
                    valid = date_df.loc[valid_mask].copy()
                    # rank 升序：raw 越大 rank 越大
                    valid['raw_rank'] = valid['raw_score'].rank(method='min', ascending=True)
                    max_rank = valid['raw_rank'].max()

                    if max_rank == 1:
                        valid['zt_score'] = 100.0
                    elif reverse:
                        # 反向：raw 越大得分越低
                        valid['zt_score'] = (1 + 99 * (max_rank - valid['raw_rank'])
                                             / (max_rank - 1)).round(2)
                    else:
                        # 正向：raw 越大得分越高
                        valid['zt_score'] = (1 + 99 * (valid['raw_rank'] - 1)
                                             / (max_rank - 1)).round(2)

                    date_df.loc[valid.index, 'zt_score'] = valid['zt_score']

                date_df.loc[~valid_mask, 'zt_score'] = 0.0
                result_dfs.append(date_df[['ymd', 'stock_code', 'zt_score']])

            result_df = pd.concat(result_dfs, ignore_index=True)
            logger.info(f"涨停因子百分制计算完成：共{len(result_df)}条记录")

            if save_to_cache:
                self.cached_factors['zt'] = result_df.copy()

            return result_df

        except Exception as e:
            logger.error(f"计算涨停因子失败：{str(e)}")
            import traceback
            logger.error(traceback.format_exc())
            return pd.DataFrame(columns=['ymd', 'stock_code', 'zt_score'])


    def shareholder_factor_score(self, start_date, end_date, save_to_cache=True):
        """
        计算股东户数因子百分制评分（0-100分）
        source_table: dwd_shareholder_num_daily
        评分逻辑（三阶段）：
        1) 【截面分位基础分】：每日在全部股票内，对 latest_qoq_sh
           （季度对齐环比%）升序排名——股东数降幅越大排名越靠前——映射到 [1,100]。
           截面排名而非固定 sigmoid，保证不同交易日的得分分布可比（与 pb_score 口径一致）；
        2) 【新鲜度衰减】：score × (0.5 + 0.5 × fresh_weight)
           fresh_weight = exp(-days_since_load/10)，刚披露权重 1.0，之后指数衰减，
           陈旧数据保底 50%（季度数据本身时效性有限，不清零）；
        3) 【边际加速】：latest_mom_sh < 0（季内最新一次披露仍在减少）→ 得分 × 1.1，
           体现"筹码持续集中"的确认信号。
        shareholder_score(t) = ( 1 + 99 × (max_rank − qoq_rank) / (max_rank − 1) )  ← 第1阶段：截面分位基础分
                               × ( 0.5 + 0.5 × exp(−days_since_load / 10) )         ← 第2阶段：新鲜度衰减（保底0.5）
                               × 1.1（若 latest_mom_sh < 0，否则 × 1.0）              ← 第3阶段：边际加速
                               全程封顶 100
        其中：qoq_rank = 当日截面内 latest_qoq_sh 升序排名（降幅最大者 rank=1）；
              max_rank = 当日参与排名的股票总数。
        股票池（重要）：全市场覆盖。dwd_shareholder_num_daily 是每日对全量股票的
           前向填充快照（5000+ 只，绝大多数股票的值为最近一次披露，陈旧度由
           days_since_load / fresh_weight 刻画）；仅过滤 latest_qoq_sh 为 NULL 的股票
           （首次披露、无季度基期，align_flag='first'），这类股票当日不参与打分。
           与其他因子（pb/zt）输出同为全市场口径，可直接按 (ymd, stock_code) 合成。
        数据特性提示：股东数披露随财报季聚集（4月底/8月底/10月底为高峰），
           因子信号强度呈脉冲式，回测时建议按月份分层观察 IC。
        异常值处理：
           - latest_qoq_sh 缺失的股票跳过，不出现在输出中；
           - 加速系数作用后封顶 100 分。
        """
        try:
            # 获取股东数据（新宽表：每日前向填充 + 新鲜度特征）
            shareholder_df = Mysql_Utils.data_from_mysql_to_dataframe(
                user=self.user,
                password=self.password,
                host=self.host,
                database=self.database,
                table_name='dwd_shareholder_num_daily',
                start_date=start_date,
                end_date=end_date,
                cols=['ymd', 'stock_code', 'latest_qoq_sh', 'latest_mom_sh',
                      'days_since_load', 'fresh_weight']
            )

            if shareholder_df.empty:
                result_df = pd.DataFrame(columns=['ymd', 'stock_code', 'shareholder_score'])
            else:
                # 转换数值
                for col in ['latest_qoq_sh', 'latest_mom_sh', 'days_since_load', 'fresh_weight']:
                    shareholder_df[col] = pd.to_numeric(shareholder_df[col], errors='coerce')

                # 仅过滤无有效环比的股票（首次披露、无季度基期），其余全量参与
                shareholder_df = shareholder_df[shareholder_df['latest_qoq_sh'].notna()].copy()

                # ========== 1. 截面分位基础分 [1,100] ==========
                # qoq 越小（户数降幅越大）→ rank 越靠前 → 得分越高
                shareholder_df['qoq_rank'] = shareholder_df.groupby('ymd')['latest_qoq_sh'].rank(
                    method='min', ascending=True)
                shareholder_df['max_rank'] = shareholder_df.groupby('ymd')['qoq_rank'].transform('max')

                shareholder_df['base_score'] = 100.0
                mask_multi = shareholder_df['max_rank'] > 1
                # rank=1（户数降幅最大）→ 100 分；rank=max（户数增加最多）→ 1 分
                shareholder_df.loc[mask_multi, 'base_score'] = (
                        1 + 99 * (shareholder_df.loc[mask_multi, 'max_rank']
                                  - shareholder_df.loc[mask_multi, 'qoq_rank'])
                        / (shareholder_df.loc[mask_multi, 'max_rank'] - 1)
                ).round(2)

                # ========== 2. 新鲜度衰减系数：0.5 ~ 1.0 ==========
                shareholder_df['fresh_coef'] = (
                        0.5 + 0.5 * shareholder_df['fresh_weight'].fillna(0)
                ).clip(upper=1.0)

                shareholder_df['shareholder_score'] = (
                        shareholder_df['base_score'] * shareholder_df['fresh_coef']
                )

                # ========== 3. 边际加速：季内最新披露仍在减少 → ×1.1 ==========
                mask_accel = shareholder_df['latest_mom_sh'] < 0
                shareholder_df.loc[mask_accel, 'shareholder_score'] = (
                        shareholder_df.loc[mask_accel, 'shareholder_score'] * 1.1
                ).clip(upper=100)

                shareholder_df['shareholder_score'] = shareholder_df['shareholder_score'].round(2)

                result_df = shareholder_df[['ymd', 'stock_code', 'shareholder_score']]

            logger.info(f"股东人数因子计算完成：共{len(result_df)}条记录")

            if save_to_cache:
                self.cached_factors['shareholder'] = result_df.copy()

            return result_df

        except Exception as e:
            logger.error(f"计算股东数因子失败：{str(e)}")
            return pd.DataFrame(columns=['ymd', 'stock_code', 'shareholder_score'])

    def volume_shrink_factor(self, start_date, end_date, baseline='ma20',
                             min_streak=3, max_streak=5,
                             reverse=False, save_to_cache=True):
        """
        计算连续缩量因子百分制评分（0-100分）
        source_table: dwd_stock_technical_indicators
        定义：连续缩量 = 截至今日，连续 min_streak 个交易日以上，每日成交量均低于均量基准线；
              窗口内一旦某天放量（超均线），连续计数立刻清零重新积累。
        计算公式：
            shortfall(t) = max(0, -volume_vs_maXX(t))        ← 当日低于均量的幅度(%)，未低于记 0
            S(t)         = 截至今日连续低于均量的天数（放量日清零，封顶 max_streak）
            raw(t)       = Σ_{lag=0}^{S(t)-1} shortfall(t-lag)  ← 只累计"当前连续段"的缩量深度
            raw(t)       = 0，若 S(t) < min_streak               ← 新一段不足 min_streak 天判 0
        参数：
            baseline: 均量基准线，'ma5'（最松）/ 'ma20'（推荐）。
                      注意：偏离度只有 ma5/ma20 预计算，ma60 不可用；
            min_streak: 连续缩量的最少天数，默认 3（放量打断后重新积累不足此天数 → 0 分）；
            max_streak: 连续缩量累计封顶天数，默认 5（更早的缩量不再额外加分；
                        想不清封顶可传大值如 20）；
        每日截面上对 raw_score 排名映射到 [1,100]：
            raw=0（未达 min_streak）得 0 分；raw 越大（缩量越深、连续越久）得分越高；
            reverse=True 反向。
        示例（每天缩量幅度 5%，min_streak=3）：
            5天全缩        → S=5, raw=25，高分
            缩缩放缩缩     → S=2, raw=0（第3天放量清零，新段不足3天）
            5天全缩后又缩2天 → S=5(封顶), raw=最近5天累计，高分
        异常值处理：
            - 基准均线为 NULL（上市初期）视为未低于均量（不打断也不加分）；
            - 停牌日无数据，视为段外（打断连续），属合理处理。
        """
        try:
            vs_col = {'ma5': 'volume_vs_ma5', 'ma20': 'volume_vs_ma20'}.get(baseline)
            if vs_col is None:
                raise ValueError(f"baseline 只支持 'ma5'/'ma20'（偏离度未预计算 ma60），收到: {baseline}")

            # ========== 1. 交易日与目标区间 ==========
            query_start = (pd.to_datetime(start_date)
                           - pd.Timedelta(days=max_streak * 2 + 20)).strftime('%Y%m%d')
            all_days = self.get_trading_days(query_start, end_date)
            if not all_days:
                return pd.DataFrame(columns=['ymd', 'stock_code', 'volume_shrink_score'])

            start_idx = next((i for i, d in enumerate(all_days) if d >= start_date), None)
            end_idx = next((i for i in range(len(all_days) - 1, -1, -1) if all_days[i] <= end_date), None)
            if start_idx is None or end_idx is None:
                return pd.DataFrame(columns=['ymd', 'stock_code', 'volume_shrink_score'])

            target_days = all_days[start_idx:end_idx + 1]
            # 深度累计最多回溯 max_streak 天
            earliest_idx = max(0, start_idx - max_streak)
            query_start_data = all_days[earliest_idx]

            # ========== 2. 读取偏离度数据 ==========
            tech_df = Mysql_Utils.data_from_mysql_to_dataframe(
                user=self.user, password=self.password, host=self.host, database=self.database,
                table_name='dwd_stock_technical_indicators',
                start_date=query_start_data, end_date=end_date,
                cols=['ymd', 'stock_code', vs_col]
            )
            if tech_df.empty:
                return pd.DataFrame(columns=['ymd', 'stock_code', 'volume_shrink_score'])

            tech_df['ymd'] = pd.to_datetime(tech_df['ymd']).dt.strftime('%Y%m%d')
            # 低于均量的幅度：vs<0 时取 -vs（正数），否则 0；NULL 视为 0
            tech_df['shortfall'] = (-tech_df[vs_col]).clip(lower=0).fillna(0)
            # 当日是否在均量下方（NULL 计为否，不打断连续也不加分）
            tech_df['is_below'] = (tech_df[vs_col] < 0).fillna(False).astype(float)

            # ========== 3. 矩阵化：连续计数 S + 分段深度累计 ==========
            short_m = tech_df.pivot_table(index='ymd', columns='stock_code',
                                          values='shortfall', aggfunc='max')
            short_m = short_m.reindex(index=all_days).fillna(0)
            below_m = tech_df.pivot_table(index='ymd', columns='stock_code',
                                          values='is_below', aggfunc='max')
            below_m = below_m.reindex(index=all_days).fillna(0)

            # S：连续低于均量的天数（放量日 below=0 → 连续计数清零），封顶 max_streak
            S = below_m.apply(lambda s: s * s.groupby((s == 0).cumsum()).cumsum(), axis=0)
            S = S.clip(upper=max_streak)

            # 分段深度：只累加"当前连续段"内每天的 shortfall
            # shortfall(t-lag) 计入 raw(t) 当且仅当 lag < S(t)（t-lag 还在当前段内）
            raw = pd.DataFrame(0.0, index=all_days, columns=short_m.columns)
            for lag in range(max_streak):
                raw = raw.add(short_m.shift(lag).fillna(0) * (S > lag), fill_value=0)

            # 连续段不足 min_streak 天 → 0 分
            raw = raw.where(S >= min_streak, 0.0)

            # ========== 4. 截目标区间、转长格式、补全市场 ==========
            target_raw = raw.loc[target_days]
            long_df = (target_raw.rename_axis('ymd')
                       .reset_index()
                       .melt(id_vars='ymd', var_name='stock_code', value_name='raw_score'))

            universe_df = Mysql_Utils.data_from_mysql_to_dataframe(
                user=self.user, password=self.password, host=self.host, database=self.database,
                table_name='ods_stock_code_daily_insight',
                start_date=target_days[0], end_date=target_days[-1],
                cols=['ymd', 'stock_code']
            )
            if universe_df.empty:
                logger.warning("ods_stock_code_daily_insight 无数据，因子仅覆盖窗口内有记录的股票")
            else:
                universe_df['ymd'] = pd.to_datetime(universe_df['ymd']).dt.strftime('%Y%m%d')
                universe_df = universe_df[['ymd', 'stock_code']].drop_duplicates()
                long_df = universe_df.merge(long_df, on=['ymd', 'stock_code'], how='left')
                long_df['raw_score'] = long_df['raw_score'].fillna(0)

            # ========== 5. 每日截面排名 → [1,100]，raw=0 得 0 分 ==========
            result_dfs = []
            for date, date_df in long_df.groupby('ymd'):
                date_df = date_df.copy()
                valid_mask = date_df['raw_score'] > 0
                if valid_mask.sum() > 0:
                    valid = date_df.loc[valid_mask].copy()
                    valid['raw_rank'] = valid['raw_score'].rank(method='min', ascending=True)
                    max_rank = valid['raw_rank'].max()
                    if max_rank == 1:
                        valid['volume_shrink_score'] = 100.0
                    elif reverse:
                        valid['volume_shrink_score'] = (1 + 99 * (max_rank - valid['raw_rank'])
                                                        / (max_rank - 1)).round(2)
                    else:
                        valid['volume_shrink_score'] = (1 + 99 * (valid['raw_rank'] - 1)
                                                        / (max_rank - 1)).round(2)
                    date_df.loc[valid.index, 'volume_shrink_score'] = valid['volume_shrink_score']
                date_df.loc[~valid_mask, 'volume_shrink_score'] = 0.0
                result_dfs.append(date_df[['ymd', 'stock_code', 'volume_shrink_score']])

            result_df = pd.concat(result_dfs, ignore_index=True)
            logger.info(f"连续缩量因子计算完成：共{len(result_df)}条记录")

            if save_to_cache:
                self.cached_factors['volume_shrink'] = result_df.copy()
            return result_df

        except Exception as e:
            logger.error(f"计算连续缩量因子失败：{str(e)}")
            import traceback
            logger.error(traceback.format_exc())
            return pd.DataFrame(columns=['ymd', 'stock_code', 'volume_shrink_score'])


    def price_shrink_factor(self, start_date, end_date,
                            min_streak=3, max_streak=4,
                            reverse=False, save_to_cache=True):
        """
        计算连续阴线因子百分制评分（0-100分）
        source_table: ods_stock_kline_daily_ts（is_down + today_pct）
        定义：连续阴线 = 截至今日，连续 min_streak 个交易日以上，每日均为阴线（close<open）；
              一旦某天收阳，连续计数立刻清零重新积累。
        计算公式：
            down_depth(t) = max(0, -today_pct(t))            ← 当日阴线下跌幅度(%)，阳线记 0
                            （today_pct = (close-open)/open*100，阴线日自带负值）
            S(t)          = 截至今日连续阴线天数（阳线日清零，封顶 max_streak）
            raw(t)        = Σ_{lag=0}^{S(t)-1} down_depth(t-lag)  ← 只累计"当前连阴段"的累计跌幅
            raw(t)        = 0，若 S(t) < min_streak                  ← 新一段不足 min_streak 天判 0
        参数：
            min_streak: 连续阴线的最少天数，默认 3（3~4 天为经验最优区；
                        1~2 天太常见无区分度，5 天以上多为阴跌庄股）；
            max_streak: 连阴累计封顶天数，默认 4（更早的阴线不再额外加分）；
        每日截面上对 raw_score 排名映射到 [1,100]：
            raw=0（未达 min_streak）得 0 分；raw 越大（跌得越深、连阴越久）得分越高；
            reverse=True 反向（用于观察超跌反转）。
        与旧版（按连阴天数封顶打分）的区别：
            raw 为连续值（累计跌幅），消除"4连阴与8连阴同分"的并列；
            且一根阳线即清零，避免"连阴中夹阳线"的票冒充纯连阴。
        注意：
            - 阴线判定用 K 线颜色（close<open），不是涨跌幅：低开高走的票
              change_pct<0 但收阳，本因子不计入也不打断连阴；
            - 停牌日无数据，视为段外（打断连续），属合理处理。
        """
        try:
            # ========== 1. 交易日与目标区间 ==========
            query_start = (pd.to_datetime(start_date)
                           - pd.Timedelta(days=max_streak * 2 + 20)).strftime('%Y%m%d')
            all_days = self.get_trading_days(query_start, end_date)
            if not all_days:
                return pd.DataFrame(columns=['ymd', 'stock_code', 'price_shrink_score'])

            start_idx = next((i for i, d in enumerate(all_days) if d >= start_date), None)
            end_idx = next((i for i in range(len(all_days) - 1, -1, -1) if all_days[i] <= end_date), None)
            if start_idx is None or end_idx is None:
                return pd.DataFrame(columns=['ymd', 'stock_code', 'price_shrink_score'])

            target_days = all_days[start_idx:end_idx + 1]
            # 深度累计最多回溯 max_streak 天
            earliest_idx = max(0, start_idx - max_streak)
            query_start_data = all_days[earliest_idx]

            # ========== 2. 读取阴线数据 ==========
            kline_df = Mysql_Utils.data_from_mysql_to_dataframe(
                user=self.user, password=self.password, host=self.host, database=self.database,
                table_name='ods_stock_kline_daily_ts',
                start_date=query_start_data, end_date=end_date,
                cols=['ymd', 'stock_code', 'is_down', 'today_pct']
            )
            if kline_df.empty:
                return pd.DataFrame(columns=['ymd', 'stock_code', 'price_shrink_score'])

            kline_df['ymd'] = pd.to_datetime(kline_df['ymd']).dt.strftime('%Y%m%d')
            # 阴线下跌幅度：阴线日 today_pct<0 取 -today_pct（正数），阳线记 0；NULL 视为 0
            kline_df['down_depth'] = (-kline_df['today_pct']).clip(lower=0).fillna(0)
            # 阴线标记（NULL 计为否）
            kline_df['is_down_f'] = kline_df['is_down'].fillna(0).astype(float)

            # ========== 3. 矩阵化：连续计数 S + 分段深度累计 ==========
            depth_m = kline_df.pivot_table(index='ymd', columns='stock_code',
                                           values='down_depth', aggfunc='max')
            depth_m = depth_m.reindex(index=all_days).fillna(0)
            down_m = kline_df.pivot_table(index='ymd', columns='stock_code',
                                          values='is_down_f', aggfunc='max')
            down_m = down_m.reindex(index=all_days).fillna(0)

            # S：连续阴线天数（阳线日 down=0 → 清零），封顶 max_streak
            S = down_m.apply(lambda s: s * s.groupby((s == 0).cumsum()).cumsum(), axis=0)
            S = S.clip(upper=max_streak)

            # 分段深度：只累加"当前连阴段"内每天的跌幅
            raw = pd.DataFrame(0.0, index=all_days, columns=depth_m.columns)
            for lag in range(max_streak):
                raw = raw.add(depth_m.shift(lag).fillna(0) * (S > lag), fill_value=0)

            # 连阴段不足 min_streak 天 → 0 分
            raw = raw.where(S >= min_streak, 0.0)

            # ========== 4. 截目标区间、转长格式、补全市场 ==========
            target_raw = raw.loc[target_days]
            long_df = (target_raw.rename_axis('ymd')
                       .reset_index()
                       .melt(id_vars='ymd', var_name='stock_code', value_name='raw_score'))

            universe_df = Mysql_Utils.data_from_mysql_to_dataframe(
                user=self.user, password=self.password, host=self.host, database=self.database,
                table_name='ods_stock_code_daily_insight',
                start_date=target_days[0], end_date=target_days[-1],
                cols=['ymd', 'stock_code']
            )
            if universe_df.empty:
                logger.warning("ods_stock_code_daily_insight 无数据，因子仅覆盖窗口内有记录的股票")
            else:
                universe_df['ymd'] = pd.to_datetime(universe_df['ymd']).dt.strftime('%Y%m%d')
                universe_df = universe_df[['ymd', 'stock_code']].drop_duplicates()
                long_df = universe_df.merge(long_df, on=['ymd', 'stock_code'], how='left')
                long_df['raw_score'] = long_df['raw_score'].fillna(0)

            # ========== 5. 每日截面排名 → [1,100]，raw=0 得 0 分 ==========
            result_dfs = []
            for date, date_df in long_df.groupby('ymd'):
                date_df = date_df.copy()
                valid_mask = date_df['raw_score'] > 0
                if valid_mask.sum() > 0:
                    valid = date_df.loc[valid_mask].copy()
                    valid['raw_rank'] = valid['raw_score'].rank(method='min', ascending=True)
                    max_rank = valid['raw_rank'].max()
                    if max_rank == 1:
                        valid['price_shrink_score'] = 100.0
                    elif reverse:
                        valid['price_shrink_score'] = (1 + 99 * (max_rank - valid['raw_rank'])
                                                       / (max_rank - 1)).round(2)
                    else:
                        valid['price_shrink_score'] = (1 + 99 * (valid['raw_rank'] - 1)
                                                       / (max_rank - 1)).round(2)
                    date_df.loc[valid.index, 'price_shrink_score'] = valid['price_shrink_score']
                date_df.loc[~valid_mask, 'price_shrink_score'] = 0.0
                result_dfs.append(date_df[['ymd', 'stock_code', 'price_shrink_score']])

            result_df = pd.concat(result_dfs, ignore_index=True)
            logger.info(f"连续阴线因子计算完成：共{len(result_df)}条记录")

            if save_to_cache:
                self.cached_factors['price_shrink'] = result_df.copy()
            return result_df

        except Exception as e:
            logger.error(f"计算连续阴线因子失败：{str(e)}")
            import traceback
            logger.error(traceback.format_exc())
            return pd.DataFrame(columns=['ymd', 'stock_code', 'price_shrink_score'])


    def aggregate_factors(self, start_date, end_date, factors=None):
        """
        因子汇总 - 直接写入MySQL，严格按照表结构
        """
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
        # self.pb_factor_score(start_date='20260901', end_date='20260928')

        # #  涨停 因子计算
        # self.zt_factor_score(start_date='20260801', end_date='20260827')

        # #  股东数 因子计算
        # self.shareholder_factor_score(start_date='20260801', end_date='20261024')

        #  成交量缩量 因子计算
        self.volume_shrink_factor('20260901', '20261024')  # 连续缩量

        # #  日K连续阴线 因子计算
        # self.price_shrink_factor('20260901', '20261024')  # 连续阴线


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
















