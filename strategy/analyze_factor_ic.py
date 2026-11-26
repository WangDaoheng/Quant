# strategy/analyze_factor_ic.py
# 因子IC检验：单因子 + 各版本composite统一对比
import pandas as pd
import numpy as np
from scipy import stats
import logging
from CommonProperties import Mysql_Utils
import CommonProperties.Base_Properties as base_properties
from CommonProperties.Base_utils import timing_decorator, script_run

logger = logging.getLogger(__name__)


class FactorICAnalyzer:
    """因子IC检验：单因子 + 各版本composite统一对比"""

    def __init__(self):
        self.user = base_properties.origin_mysql_user
        self.password = base_properties.origin_mysql_password
        self.host = base_properties.origin_mysql_host
        self.database = base_properties.origin_mysql_database

    @timing_decorator
    def load_factor_scores(self, start_date, end_date):
        """加载单因子得分（dwd_factor_summary）"""
        logger.info(f"加载单因子得分: {start_date} ~ {end_date}")

        df = Mysql_Utils.data_from_mysql_to_dataframe(
            user=self.user, password=self.password, host=self.host, database=self.database,
            table_name='dwd_factor_summary',
            start_date=start_date, end_date=end_date
        )

        if df.empty:
            logger.error("dwd_factor_summary 为空")
            return None

        df['ymd'] = pd.to_datetime(df['ymd']).dt.strftime('%Y%m%d')
        logger.info(f"加载了 {len(df)} 条记录，{df['ymd'].nunique()} 个交易日")
        return df

    @timing_decorator
    def load_composite_scores(self, start_date, end_date):
        """加载各版本composite得分（mart_composite_daily）"""
        logger.info(f"加载composite得分: {start_date} ~ {end_date}")

        df = Mysql_Utils.data_from_mysql_to_dataframe(
            user=self.user, password=self.password, host=self.host, database=self.database,
            table_name='mart_composite_daily',
            start_date=start_date, end_date=end_date
        )

        if df.empty:
            logger.warning("mart_composite_daily 为空，跳过composite IC")
            return None

        df['ymd'] = pd.to_datetime(df['ymd']).dt.strftime('%Y%m%d')
        logger.info(f"加载了 {len(df)} 条记录，版本: {df['weights_version'].unique().tolist()}")
        return df

    @timing_decorator
    def load_future_returns(self, start_date, end_date, horizons=(1, 3, 5)):
        """加载未来N日收益"""
        logger.info(f"加载行情数据: {start_date} ~ {end_date}")

        end_dt = pd.to_datetime(end_date) + pd.Timedelta(days=max(horizons) * 2 + 10)
        end_date_extended = end_dt.strftime('%Y%m%d')

        kline_df = Mysql_Utils.data_from_mysql_to_dataframe(
            user=self.user, password=self.password, host=self.host, database=self.database,
            table_name='ods_stock_kline_daily_ts',
            start_date=start_date, end_date=end_date_extended,
            cols=['ymd', 'stock_code', 'close']
        )

        if kline_df.empty:
            logger.error("行情数据为空")
            return None

        kline_df['ymd'] = pd.to_datetime(kline_df['ymd']).dt.strftime('%Y%m%d')
        kline_df = kline_df.sort_values(['stock_code', 'ymd'])

        for h in horizons:
            kline_df[f'ret_{h}d'] = kline_df.groupby('stock_code')['close'].transform(
                lambda x: x.shift(-h) / x - 1
            ) * 100

        logger.info(f"行情数据加载完成，共 {len(kline_df)} 条")
        return kline_df[['ymd', 'stock_code'] + [f'ret_{h}d' for h in horizons]]

    def _calc_daily_ic(self, score_df, return_df, score_col, horizons=(1, 3, 5)):
        """通用：对某得分列按日算IC"""
        merged = score_df[['ymd', 'stock_code', score_col]].merge(
            return_df, on=['ymd', 'stock_code'], how='inner'
        )

        ic_records = []
        for ymd, day_df in merged.groupby('ymd'):
            if len(day_df) < 100:
                continue
            for h in horizons:
                ret_col = f'ret_{h}d'
                valid = day_df[[score_col, ret_col]].dropna()
                if len(valid) < 100:
                    continue
                ic, _ = stats.spearmanr(valid[score_col], valid[ret_col])
                ic_records.append({
                    'ymd': ymd, 'horizon': h,
                    'ic_value': round(ic, 4),
                    'stock_count': len(valid)
                })
        return pd.DataFrame(ic_records)

    @timing_decorator
    def calculate_all_ic(self, factor_df, composite_df, return_df, horizons=(1, 3, 5)):
        """计算所有IC：单因子 + 各版本composite"""
        all_ic = []

        # 1. 单因子 IC
        if factor_df is not None:
            factor_cols = [c for c in factor_df.columns if c.endswith('_score')]
            logger.info(f"识别到单因子列: {factor_cols}")

            for col in factor_cols:
                factor_name = col.replace('_score', '')
                ic_df = self._calc_daily_ic(factor_df, return_df, col, horizons)
                if not ic_df.empty:
                    ic_df['factor_name'] = factor_name
                    ic_df['factor_type'] = 'single'
                    all_ic.append(ic_df)

        # 2. composite IC（按版本）
        if composite_df is not None:
            for version in composite_df['weights_version'].unique():
                version_df = composite_df[composite_df['weights_version'] == version]
                ic_df = self._calc_daily_ic(version_df, return_df, 'composite_score', horizons)
                if not ic_df.empty:
                    ic_df['factor_name'] = f'composite_{version}'
                    ic_df['factor_type'] = 'composite'
                    all_ic.append(ic_df)

        if not all_ic:
            logger.error("无有效IC数据")
            return None

        result = pd.concat(all_ic, ignore_index=True)
        logger.info(f"IC计算完成，共 {len(result)} 条记录")
        return result

    @timing_decorator
    def save_to_mysql(self, ic_df):
        """保存IC结果"""
        if ic_df.empty:
            logger.warning("无IC数据可保存")
            return

        out_df = ic_df.copy()
        out_df['ymd'] = pd.to_datetime(out_df['ymd'], format='%Y%m%d').dt.strftime('%Y-%m-%d')

        Mysql_Utils.data_from_dataframe_to_mysql(
            user=self.user, password=self.password, host=self.host, database=self.database,
            df=out_df[['ymd', 'factor_name', 'factor_type', 'horizon', 'ic_value', 'stock_count']],
            table_name='mart_factor_ic_daily',
            merge_on=['ymd', 'factor_name', 'factor_type', 'horizon']
        )
        logger.info(f"已保存 {len(out_df)} 条IC记录")

    @timing_decorator
    def print_report(self, ic_df):
        """打印对比报告"""
        print("\n" + "=" * 75)
        print("因子IC检验报告（单因子 vs 各版本composite）")
        print("=" * 75)

        for ftype in ['single', 'composite']:
            subset = ic_df[ic_df['factor_type'] == ftype]
            if subset.empty:
                continue

            title = "【单因子】" if ftype == 'single' else "【composite（按权重版本）】"
            print(f"\n{title}")
            print("-" * 75)
            print(f"{'因子名':<25}{'周期':<6}{'IC均值':<10}{'ICIR':<10}{'IC>0占比':<12}{'样本'}")
            print("-" * 75)

            for fname in subset['factor_name'].unique():
                fdata = subset[subset['factor_name'] == fname]
                for h in [1, 3, 5]:
                    sub = fdata[fdata['horizon'] == h]['ic_value'].dropna()
                    if len(sub) == 0:
                        continue
                    ic_mean, ic_std = sub.mean(), sub.std()
                    icir = ic_mean / ic_std if ic_std > 0 else 0
                    pos = (sub > 0).mean() * 100

                    if abs(ic_mean) > 0.03 and icir > 0.3:
                        rating = "✅"
                    elif abs(ic_mean) > 0.02 and icir > 0.2:
                        rating = "🟡"
                    elif abs(ic_mean) > 0.01:
                        rating = "🟠"
                    else:
                        rating = "🔴"

                    name_display = fname if h == 1 else ''
                    print(f"{name_display:<25}{h}日    {ic_mean:<10.4f}{icir:<10.4f}{pos:<12.0f}%{len(sub)}  {rating}")

        print("\n" + "=" * 75)

    @script_run(script_name="analyze_factor_ic.py")
    def run(self, start_date, end_date, save_to_mysql=True):
        """主流程"""
        logger.info("开始因子IC检验")

        factor_df = self.load_factor_scores(start_date, end_date)
        composite_df = self.load_composite_scores(start_date, end_date)

        if factor_df is None and composite_df is None:
            logger.error("无数据可分析")
            return None

        return_df = self.load_future_returns(start_date, end_date)
        if return_df is None:
            return None

        ic_df = self.calculate_all_ic(factor_df, composite_df, return_df)
        if ic_df is None or ic_df.empty:
            logger.error("IC计算失败")
            return None

        self.print_report(ic_df)

        if save_to_mysql:
            self.save_to_mysql(ic_df)

        return ic_df


if __name__ == '__main__':
    from CommonProperties.DateUtility import DateUtility
    from CommonProperties import set_config

    set_config.setup_logging_config()

    end_date = DateUtility.today()
    start_date = DateUtility.first_day_of_month(-10)

    analyzer = FactorICAnalyzer()
    ic_df = analyzer.run(start_date, end_date, save_to_mysql=True)

    if ic_df is not None:
        ic_df.to_csv('factor_ic_analysis.csv', index=False, encoding='utf-8-sig')
        print("\n详细结果已保存到 factor_ic_analysis.csv")
