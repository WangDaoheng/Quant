# strategy/analyze_factor_ic.py
# 因子IC检验：验证因子得分与未来收益的相关性
import pandas as pd
import numpy as np
from scipy import stats
import logging
from CommonProperties import Mysql_Utils
import CommonProperties.Base_Properties as base_properties
from CommonProperties.Base_utils import timing_decorator, script_run

logger = logging.getLogger(__name__)


class FactorICAnalyzer:
    """因子IC检验：验证因子得分与未来收益的相关性"""

    def __init__(self):
        self.user = base_properties.origin_mysql_user
        self.password = base_properties.origin_mysql_password
        self.host = base_properties.origin_mysql_host
        self.database = base_properties.origin_mysql_database

    @timing_decorator
    def load_factor_scores(self, start_date, end_date):
        """加载因子得分"""
        logger.info(f"加载因子得分: {start_date} ~ {end_date}")

        df = Mysql_Utils.data_from_mysql_to_dataframe(
            user=self.user, password=self.password, host=self.host, database=self.database,
            table_name='dwd_factor_summary',
            start_date=start_date, end_date=end_date,
            cols=['ymd', 'stock_code', 'pb_score', 'zt_score',
                  'shareholder_score', 'composite_score', 'combo_score']
        )

        if df.empty:
            logger.error("因子得分表为空")
            return None

        df['ymd'] = pd.to_datetime(df['ymd']).dt.strftime('%Y%m%d')
        logger.info(f"加载了 {len(df)} 条因子记录，覆盖 {df['ymd'].nunique()} 个交易日")
        return df

    @timing_decorator
    def load_future_returns(self, start_date, end_date, horizons=(1, 3, 5)):
        """加载未来N日收益"""
        logger.info(f"加载行情数据: {start_date} ~ {end_date}")

        # 多取数据用于计算未来收益（粗略按自然日*2）
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

        # 计算未来N日收益（百分比）
        for h in horizons:
            kline_df[f'ret_{h}d'] = kline_df.groupby('stock_code')['close'].transform(
                lambda x: x.shift(-h) / x - 1
            ) * 100

        logger.info(f"行情数据加载完成，共 {len(kline_df)} 条")
        return kline_df[['ymd', 'stock_code'] + [f'ret_{h}d' for h in horizons]]

    @timing_decorator
    def calculate_ic(self, factor_df, return_df, horizons=(1, 3, 5)):
        """计算每日 RankIC（Spearman 相关系数）"""
        # 合并因子和收益
        merged = factor_df.merge(return_df, on=['ymd', 'stock_code'], how='inner')
        logger.info(f"合并后数据量: {len(merged)}")

        factor_cols = ['pb_score', 'zt_score', 'shareholder_score', 'composite_score', 'combo_score']
        ic_records = []

        for ymd, day_df in merged.groupby('ymd'):
            if len(day_df) < 100:
                continue

            for factor in factor_cols:
                for h in horizons:
                    ret_col = f'ret_{h}d'
                    valid = day_df[[factor, ret_col]].dropna()

                    if len(valid) < 100:
                        continue

                    ic, _ = stats.spearmanr(valid[factor], valid[ret_col])

                    ic_records.append({
                        'ymd': ymd,
                        'factor_name': factor.replace('_score', ''),
                        'horizon': h,
                        'ic_value': round(ic, 4),
                        'stock_count': len(valid)
                    })

        ic_df = pd.DataFrame(ic_records)
        logger.info(f"IC计算完成，共 {len(ic_df)} 条记录")
        return ic_df

    @timing_decorator
    def save_to_mysql(self, ic_df):
        """保存IC结果到 mart_factor_ic_daily"""
        if ic_df.empty:
            logger.warning("无IC数据可保存")
            return

        # ymd 转回 DATE 格式
        out_df = ic_df.copy()
        out_df['ymd'] = pd.to_datetime(out_df['ymd'], format='%Y%m%d').dt.strftime('%Y-%m-%d')

        Mysql_Utils.data_from_dataframe_to_mysql(
            user=self.user, password=self.password, host=self.host, database=self.database,
            df=out_df,
            table_name='mart_factor_ic_daily',
            merge_on=['ymd', 'factor_name', 'horizon']
        )
        logger.info(f"已保存 {len(out_df)} 条IC记录到 mart_factor_ic_daily")

    @timing_decorator
    def print_report(self, ic_df):
        """打印因子质量分析报告"""
        print("\n" + "=" * 70)
        print("因子IC检验报告")
        print("=" * 70)

        for factor in ['pb', 'zt', 'shareholder', 'composite', 'combo']:
            factor_data = ic_df[ic_df['factor_name'] == factor]
            if factor_data.empty:
                continue

            print(f"\n【{factor}】")
            print("-" * 50)
            print(f"{'周期':<6} {'IC均值':<10} {'ICIR':<10} {'IC>0占比':<12} {'样本数'}")
            print("-" * 50)

            for h in (1, 3, 5):
                sub = factor_data[factor_data['horizon'] == h]['ic_value'].dropna()
                if len(sub) == 0:
                    continue

                ic_mean = sub.mean()
                ic_std = sub.std()
                icir = ic_mean / ic_std if ic_std > 0 else 0
                pos_rate = (sub > 0).mean() * 100

                # 评级
                if abs(ic_mean) > 0.03 and icir > 0.3:
                    rating = "✅优秀"
                elif abs(ic_mean) > 0.02 and icir > 0.2:
                    rating = "🟡良好"
                elif abs(ic_mean) > 0.01:
                    rating = "🟠一般"
                else:
                    rating = "🔴无效"

                print(f"{h}日    {ic_mean:<10.4f} {icir:<10.4f} {pos_rate:<12.1f}% {len(sub)}  {rating}")

        print("\n" + "=" * 70)

    @script_run(script_name="analyze_factor_ic.py")
    def run(self, start_date, end_date, save_to_mysql=True):
        """主流程"""
        logger.info("开始因子IC检验")

        # 1. 加载因子得分
        factor_df = self.load_factor_scores(start_date, end_date)
        if factor_df is None:
            return None

        # 2. 加载未来收益
        return_df = self.load_future_returns(start_date, end_date)
        if return_df is None:
            return None

        # 3. 计算IC
        ic_df = self.calculate_ic(factor_df, return_df)
        if ic_df is None or ic_df.empty:
            logger.error("IC计算失败")
            return None

        # 4. 打印报告
        self.print_report(ic_df)

        # 5. 保存到MySQL
        if save_to_mysql:
            self.save_to_mysql(ic_df)

        return ic_df


if __name__ == '__main__':
    from CommonProperties.DateUtility import DateUtility
    from CommonProperties import set_config

    set_config.setup_logging_config()

    end_date = DateUtility.today()
    start_date = DateUtility.first_day_of_month(-10)  # 最近3个月

    analyzer = FactorICAnalyzer()
    ic_df = analyzer.run(start_date, end_date, save_to_mysql=True)

    if ic_df is not None:
        ic_df.to_csv('factor_ic_analysis.csv', index=False, encoding='utf-8-sig')
        print("\n详细结果已保存到 factor_ic_analysis.csv")
