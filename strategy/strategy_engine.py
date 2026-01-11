import pandas as pd
import logging
from CommonProperties.Base_utils import timing_decorator


class StrategyEngine:
    """策略引擎：支持单策略/组合策略选股"""

    def __init__(self, factor_lib):
        self.factor_lib = factor_lib      # 注入因子库实例（依赖注入，解耦）
        self.strategies = {}              # 存储已注册的策略（字典：策略名 → 策略函数+参数）
        # 结构示例：{
        #     '低PB策略': {'func': self.pb_strategy, 'params': {'quantile': 0.3}},
        #     '涨停策略': {'func': self.zt_strategy, 'params': {'window': 5}}
        # }

    def register_strategy(self, name, func, params=None):
        """注册策略到策略字典 self.strategies 中 """
        self.strategies[name] = {
            'func': func,                  # 策略函数（如低PB+筹码+涨停）
            'params': params or {}         # 策略参数（如PB分位数、涨停窗口）
        }
        logging.info(f"策略[{name}]注册成功")

    @timing_decorator
    def value_chip_zt_strategy(self, start_date=None, end_date=None, pb_quantile=0.3, zt_window=5):
        """低PB+筹码集中+涨停 组合因子策略"""
        # 1. 加载各因子数据（复用因子库）
        pb_df = self.factor_lib.pb_factor(pb_percentile=pb_quantile, start_date=start_date, end_date=end_date)
        zt_df = self.factor_lib.zt_factor(lookback_days=zt_window, start_date=start_date, end_date=end_date)
        shareholder_df = self.factor_lib.shareholder_factor(start_date=start_date, end_date=end_date)

        # 2. 合并因子数据（按日期+股票代码对齐）
        # 获取所有日期-股票组合
        base_df = pb_df[['ymd', 'stock_code']].drop_duplicates()

        # 左连接zt_signal（注意：这里所有日期使用相同的信号！）
        merge_df = base_df.merge(
            zt_df[['stock_code', 'zt_signal']],
            on='stock_code',
            how='left'
        ).merge(
            pb_df[['ymd', 'stock_code', 'pb_signal']],
            on=['ymd', 'stock_code'],
            how='left'
        ).merge(
            shareholder_df[['ymd', 'stock_code', 'shareholder_signal']],
            on=['ymd', 'stock_code'],
            how='left'
        )
        merge_df['zt_signal'] = merge_df['zt_signal'].fillna(False)
        merge_df['shareholder_signal'] = merge_df['shareholder_signal'].fillna(False)

        # 3. 生成最终选股信号（三个因子都满足：且逻辑）
        merge_df['final_signal'] = merge_df['pb_signal'] & merge_df['zt_signal'] & merge_df['shareholder_signal']

        # 4. 筛选结果（只保留选中的股票，返回核心字段）
        selected = merge_df[merge_df['final_signal']][['ymd', 'stock_code', 'stock_name']].reset_index(drop=True)
        logging.info(f"低PB+筹码+涨停策略选出{len(selected)}只股票")
        return selected

    @timing_decorator
    def north_bound_strategy(self, start_date=None, end_date=None, quantile=0.7):
        """北向资金重仓策略（独立策略）"""
        # 1. 加载北向资金因子
        north_df = self.factor_lib.north_bound_factor(quantile=quantile, start_date=start_date, end_date=end_date)
        # 2. 筛选北向持仓前30%的股票
        selected = north_df[north_df['north_signal']][['ymd', 'stock_code']].reset_index(drop=True)
        # 3. 补充股票名称（因子库返回的北向数据可能没有名称，合并基础数据）
        base_df = self.factor_lib.load_base_data(start_date, end_date)
        selected = selected.merge(
            base_df[['stock_code', 'stock_name']].drop_duplicates(),
            on='stock_code',
            how='left'
        )
        logging.info(f"北向资金策略选出{len(selected)}只股票")
        return selected

    @timing_decorator
    def run_strategy_combination(self, strategy_names, start_date=None, end_date=None, weight_threshold=0.5):
        """多策略加权组合选股（核心：融合多个策略的结果）"""
        if not strategy_names:
            raise ValueError("请选择至少一个策略")

        all_selected = []
        # 1. 执行每个注册的策略
        for name in strategy_names:
            if name not in self.strategies:
                raise ValueError(f"策略[{name}]未注册")
            strat = self.strategies[name]
            selected = strat['func'](start_date=start_date, end_date=end_date, **strat['params'])
            selected['strategy_name'] = name              # 标记股票来自哪个策略
            selected['weight'] = 1 / len(strategy_names)  # 等权分配（比如2个策略，每个权重0.5）
            all_selected.append(selected)

        # 2. 合并所有策略结果，计算股票的总权重
        combined_df = pd.concat(all_selected)
        score_df = combined_df.groupby(['ymd', 'stock_code']).agg({
            'weight': 'sum',                              # 总权重：某只股票被多个策略选中时，权重累加
            'stock_name': 'first'                         # 取第一个策略中的股票名称（避免重复）
        }).reset_index()

        # 按权重阈值筛选
        score_df['final_signal'] = score_df['weight'] >= weight_threshold
        final_selected = score_df[score_df['final_signal']][['ymd', 'stock_code', 'stock_name']].reset_index(drop=True)
        logging.info(f"组合策略选出{len(final_selected)}只股票")
        return final_selected


if __name__ == '__main__':
    # 1. 初始化因子库和策略引擎
    from strategy.factor_library import FactorLibrary
    from strategy.strategy_engine import StrategyEngine

    factor_lib = FactorLibrary()
    engine = StrategyEngine(factor_lib)

    # 2. 注册策略
    engine.register_strategy(
        '低PB+筹码+涨停',
        engine.value_chip_zt_strategy,
        {'pb_quantile': 0.3, 'zt_window': 5}
    )
    engine.register_strategy(
        '北向资金重仓',
        engine.north_bound_strategy,
        {'quantile': 0.7}
    )

    # 3. 执行单策略
    single_selected = engine.value_chip_zt_strategy(start_date='20250101', end_date='20250131')

    # 4. 执行组合策略
    combined_selected = engine.run_strategy_combination(
        strategy_names=['低PB+筹码+涨停', '北向资金重仓'],
        start_date='20250101',
        end_date='20250131',
        weight_threshold=0.5
    )

