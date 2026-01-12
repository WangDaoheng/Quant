# strategy/strategy_engine.py
import pandas as pd
import logging
from CommonProperties.Base_utils import timing_decorator

logger = logging.getLogger(__name__)


class StrategyEngine:
    """策略执行引擎:支持多日回测，负责执行多因子策略"""

    def __init__(self, factor_lib):
        self.factor_lib = factor_lib  # 注入因子库实例
        self.strategies = {}  # 存储已注册的策略

    def register_strategy(self, name, func, params=None):
        """注册策略"""
        self.strategies[name] = {
            'func': func,
            'params': params or {}
        }
        logger.info(f"策略[{name}]注册成功")

    @timing_decorator
    def value_chip_zt_strategy(self, start_date=None, end_date=None, pb_quantile=0.3, zt_window=5,
                               min_factor_count=2):
        """
        三因子策略：低PB+筹码集中+涨停 组合因子策略（支持多日回测）

        参数:
            start_date: 开始日期
            end_date: 结束日期
            pb_quantile: PB分位数阈值
            zt_window: 涨停窗口天数
            min_factor_count: 最少满足的因子数量（1-3）
        """
        logger.info(f"开始执行三因子策略：{start_date} ~ {end_date}")

        # 1. 获取交易日列表
        trading_days = self.factor_lib.get_trading_days(start_date, end_date)

        if not trading_days:
            logger.error("没有找到交易日数据")
            return pd.DataFrame(columns=['ymd', 'stock_code', 'stock_name'])

        all_selected = []

        # 2. 按每个交易日处理
        for i, current_date in enumerate(trading_days):
            logger.debug(f"处理交易日 {i + 1}/{len(trading_days)}: {current_date}")

            try:
                # 3. 获取当日的因子数据
                # 3.1 PB因子
                pb_df_day = self.factor_lib.pb_factor(
                    start_date=current_date,
                    end_date=current_date,
                    pb_percentile=pb_quantile
                )

                if pb_df_day.empty:
                    logger.warning(f"{current_date}: PB因子数据为空")
                    continue

                # 3.2 涨停因子
                zt_df_day = self.factor_lib.zt_factor(
                    start_date=current_date,
                    end_date=current_date,
                    lookback_days=zt_window
                )

                # 3.3 筹码因子
                shareholder_df_day = self.factor_lib.shareholder_factor(
                    start_date=current_date,
                    end_date=current_date
                )

                # 4. 合并因子数据（左连接，以PB数据为基准）
                merged = pb_df_day[['stock_code', 'pb_signal']].copy()

                # 4.1 合并涨停因子
                if not zt_df_day.empty:
                    merged = merged.merge(
                        zt_df_day[['stock_code', 'zt_signal']],
                        on='stock_code',
                        how='left'
                    )
                else:
                    merged['zt_signal'] = False

                # 4.2 合并筹码因子
                if not shareholder_df_day.empty:
                    merged = merged.merge(
                        shareholder_df_day[['stock_code', 'shareholder_signal']],
                        on='stock_code',
                        how='left'
                    )
                else:
                    merged['shareholder_signal'] = False

                # 5. 处理缺失值
                merged['zt_signal'] = merged['zt_signal'].fillna(False)
                merged['shareholder_signal'] = merged['shareholder_signal'].fillna(False)

                # 6. 计算因子得分
                merged['factor_count'] = (
                        merged['pb_signal'].astype(int) +
                        merged['zt_signal'].astype(int) +
                        merged['shareholder_signal'].astype(int)
                )

                # 7. 筛选股票
                selected_day = merged[merged['factor_count'] >= min_factor_count].copy()

                if not selected_day.empty:
                    # 添加日期信息
                    selected_day['ymd'] = current_date

                    # 添加股票名称（从PB数据获取）
                    if 'stock_name' in pb_df_day.columns:
                        stock_names = pb_df_day.set_index('stock_code')['stock_name'].to_dict()
                        selected_day['stock_name'] = selected_day['stock_code'].map(stock_names)

                    all_selected.append(selected_day[['ymd', 'stock_code', 'stock_name', 'factor_count']])

                    logger.debug(f"{current_date}: 选中 {len(selected_day)} 只股票")

            except Exception as e:
                logger.error(f"处理交易日 {current_date} 失败: {str(e)}")
                continue

        # 8. 合并所有交易日结果
        if all_selected:
            final_result = pd.concat(all_selected, ignore_index=True)

            # 统计信息
            unique_stocks = final_result['stock_code'].nunique()
            avg_selected_per_day = len(final_result) / len(trading_days)

            logger.info(
                f"策略执行完成：\n"
                f"  - 回测期间：{start_date} ~ {end_date}，共{len(trading_days)}个交易日\n"
                f"  - 选中股票总数：{len(final_result)}条记录\n"
                f"  - 唯一股票数：{unique_stocks}只\n"
                f"  - 平均每日选中：{avg_selected_per_day:.1f}只\n"
                f"  - 筛选条件：至少满足{min_factor_count}个因子"
            )

            return final_result
        else:
            logger.warning("策略未选中任何股票")
            return pd.DataFrame(columns=['ymd', 'stock_code', 'stock_name', 'factor_count'])

    @timing_decorator
    def run_strategy_combination(self, strategy_names, start_date=None, end_date=None,
                                 weight_threshold=0.5, min_factor_count=2):
        """
        多策略加权组合选股

        参数:
            strategy_names: 策略名称列表
            weight_threshold: 权重阈值
            min_factor_count: 最少满足的因子数量
        """
        if not strategy_names:
            raise ValueError("请选择至少一个策略")

        logger.info(f"开始执行组合策略：{strategy_names}")

        # 1. 执行每个策略
        strategy_results = {}
        for name in strategy_names:
            if name not in self.strategies:
                raise ValueError(f"策略[{name}]未注册")

            strat = self.strategies[name]
            logger.info(f"执行策略: {name}")

            # 执行策略
            selected = strat['func'](
                start_date=start_date,
                end_date=end_date,
                min_factor_count=min_factor_count,
                **strat['params']
            )

            strategy_results[name] = selected

        # 2. 合并策略结果
        all_dates = self.factor_lib.get_trading_days(start_date, end_date)
        combined_results = []

        for current_date in all_dates:
            date_results = []

            for strategy_name, result_df in strategy_results.items():
                # 获取该策略在当前日期的选股
                day_stocks = result_df[result_df['ymd'] == current_date]['stock_code'].tolist()

                for stock in day_stocks:
                    date_results.append({
                        'ymd': current_date,
                        'stock_code': stock,
                        'strategy_name': strategy_name
                    })

            if date_results:
                date_df = pd.DataFrame(date_results)

                # 计算权重
                strategy_count = len(strategy_names)
                date_df['weight'] = 1.0 / strategy_count

                # 按股票汇总权重
                stock_weights = date_df.groupby(['ymd', 'stock_code'])['weight'].sum().reset_index()

                # 按权重阈值筛选
                selected_stocks = stock_weights[stock_weights['weight'] >= weight_threshold]

                if not selected_stocks.empty:
                    combined_results.append(selected_stocks)

        # 3. 合并最终结果
        if combined_results:
            final_result = pd.concat(combined_results, ignore_index=True)

            # 添加股票名称
            try:
                # 从任意策略结果获取股票名称
                sample_strategy = list(strategy_results.values())[0]
                stock_names = sample_strategy.drop_duplicates('stock_code').set_index('stock_code')[
                    'stock_name'].to_dict()
                final_result['stock_name'] = final_result['stock_code'].map(stock_names)
            except:
                final_result['stock_name'] = ''

            logger.info(f"组合策略完成：选中 {len(final_result)} 只股票")
            return final_result[['ymd', 'stock_code', 'stock_name', 'weight']]
        else:
            logger.warning("组合策略未选中任何股票")
            return pd.DataFrame(columns=['ymd', 'stock_code', 'stock_name', 'weight'])

    @timing_decorator
    def run_backtest_for_strategy(self, strategy_name, start_date, end_date,
                                  initial_cash=100000, commission=0.0003):
        """
        为策略运行回测（简化版）
        实际回测应该使用专门的backtest模块
        """
        logger.info(f"为策略 {strategy_name} 运行回测")

        if strategy_name not in self.strategies:
            raise ValueError(f"策略[{strategy_name}]未注册")

        # 执行策略获取选股
        strat = self.strategies[strategy_name]
        selected_stocks = strat['func'](
            start_date=start_date,
            end_date=end_date,
            **strat['params']
        )

        if selected_stocks.empty:
            logger.warning("策略未选中任何股票，无法回测")
            return None

        # 这里应该调用backtest模块进行实际回测
        # 目前只返回选股统计信息

        stats = {
            'strategy_name': strategy_name,
            'backtest_period': f"{start_date} ~ {end_date}",
            'total_selected': len(selected_stocks),
            'unique_stocks': selected_stocks['stock_code'].nunique(),
            'trading_days': selected_stocks['ymd'].nunique(),
            'avg_stocks_per_day': len(selected_stocks) / selected_stocks['ymd'].nunique(),
            'selected_stocks_sample': selected_stocks.head(10).to_dict('records')
        }

        logger.info(f"回测统计：{stats}")
        return stats