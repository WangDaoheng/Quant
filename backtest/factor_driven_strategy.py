# backtest/factor_driven_strategy.py

import backtrader as bt
import logging
from typing import Optional
from CommonProperties.Base_utils import timing_decorator
# 导入引擎类，让IDE能识别类型
from backtest.backtest_engine import StockBacktestEngine

logger = logging.getLogger(__name__)

class FactorDrivenStrategy(bt.Strategy):
    """
    因子驱动策略（百分制版本）：
    每日获取PB/涨停/筹码因子的百分制得分，综合计算后动态决定买卖
    """
    # 声明参数
    params = (
        ('backtest_engine', Optional[StockBacktestEngine], None),
        ('pb_weight', 0.4),           # PB因子权重
        ('zt_weight', 0.3),            # 涨停因子权重
        ('shareholder_weight', 0.3),    # 筹码因子权重
        ('buy_threshold', 70),          # 买入阈值（0-100）
        ('sell_threshold', 30),         # 卖出阈值（0-100）
        ('hold_threshold', 50),         # 持仓阈值（高于此值才持仓）
    )

    @timing_decorator
    def next(self):
        # 每个交易日执行一次
        current_date = self.datas[0].datetime.date(0) if self.datas else None
        if not current_date:
            return

        # 1. 校验回测引擎参数是否传递成功
        engine: StockBacktestEngine = self.p.backtest_engine
        if not engine:
            logger.error("回测引擎实例未传递，无法查询因子")
            return

        # 2. 遍历所有股票，逐只判断因子得分
        for data in self.datas:
            stock_code = data._name
            if not stock_code:
                continue

            # 3. 查询当日因子百分制得分
            pb_score = engine.get_factor_score(stock_code, current_date, 'pb')
            zt_score = engine.get_factor_score(stock_code, current_date, 'zt')
            shareholder_score = engine.get_factor_score(stock_code, current_date, 'shareholder')

            # 4. 计算综合得分（加权平均）
            composite_score = (
                pb_score * self.p.pb_weight +
                zt_score * self.p.zt_weight +
                shareholder_score * self.p.shareholder_weight
            )

            # 5. 获取当前持仓
            current_pos = self.getposition(data).size

            # 6. 根据综合得分决定买卖
            if composite_score >= self.p.buy_threshold and current_pos == 0:
                # 买入：得分高于买入阈值且无持仓
                total_cash = self.broker.getcash() * 0.9
                position_size = total_cash / len(self.datas) / data.close[0]
                self.buy(data, size=position_size)
                logger.info(
                    f"[{current_date}] 买入 {stock_code} | "
                    f"综合得分：{composite_score:.1f} (PB:{pb_score:.0f} 涨停:{zt_score:.0f} 筹码:{shareholder_score:.0f}) | "
                    f"买入数量：{position_size:.0f}股"
                )

            elif composite_score <= self.p.sell_threshold and current_pos > 0:
                # 卖出：得分低于卖出阈值且有持仓
                self.close(data)
                logger.info(
                    f"[{current_date}] 卖出 {stock_code} | "
                    f"综合得分：{composite_score:.1f} (PB:{pb_score:.0f} 涨停:{zt_score:.0f} 筹码:{shareholder_score:.0f}) | "
                    f"持仓数量：{current_pos}股"
                )

            elif current_pos > 0 and composite_score < self.p.hold_threshold:
                # 持仓但得分低于持有阈值，考虑减仓（可选）
                # 这里简单处理：得分低于持有阈值也卖出
                self.close(data)
                logger.info(
                    f"[{current_date}] 减仓/清仓 {stock_code} | "
                    f"综合得分：{composite_score:.1f} 低于持有阈值 {self.p.hold_threshold}"
                )

            elif current_pos > 0:
                # 继续持仓，记录得分
                logger.info(
                    f"[{current_date}] 持仓 {stock_code} | "
                    f"综合得分：{composite_score:.1f} (PB:{pb_score:.0f} 涨停:{zt_score:.0f} 筹码:{shareholder_score:.0f})"
                )