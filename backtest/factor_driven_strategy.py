import backtrader as bt
import logging
from typing import Optional
from CommonProperties.Base_utils import timing_decorator
# 导入引擎类，让IDE能识别类型
from backtest.backtest_engine import StockBacktestEngine

logger = logging.getLogger(__name__)

class FactorDrivenStrategy(bt.Strategy):
    """
    因子驱动策略：每日查询PB/涨停/筹码因子，动态决定买卖
    适用于验证因子的实际交易价值
    """
    # 声明参数 + 类型注解（解决IDE跳转问题）
    params = (
        ('backtest_engine', Optional[StockBacktestEngine], None),
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

        # 2. 遍历所有股票，逐只判断因子信号
        for data in self.datas:
            stock_code = data._name
            if not stock_code:
                continue

            # 3. 查询当日因子信号
            pb_signal = engine.get_factor_value(stock_code, current_date, 'pb')
            zt_signal = engine.get_factor_value(stock_code, current_date, 'zt')
            shareholder_signal = engine.get_factor_value(stock_code, current_date, 'shareholder')

            # 4. 生成买卖信号（三个因子同时满足才买入）
            buy_signal = pb_signal and zt_signal and shareholder_signal
            sell_signal = not buy_signal

            # 5. 获取当前持仓，避免重复交易
            current_pos = self.getposition(data).size

            # 6. 执行买入
            if buy_signal and current_pos == 0:
                # 等权分配仓位：90%现金 / 股票数量 / 收盘价
                total_cash = self.broker.getcash() * 0.9
                position_size = total_cash / len(self.datas) / data.close[0]
                self.buy(data, size=position_size)
                logger.info(
                    f"[{current_date}] 买入 {stock_code} | "
                    f"PB：{pb_signal} | 涨停：{zt_signal} | 筹码：{shareholder_signal} | "
                    f"买入数量：{position_size:.0f}股"
                )

            # 7. 执行卖出
            elif sell_signal and current_pos > 0:
                self.close(data)
                logger.info(
                    f"[{current_date}] 卖出 {stock_code} | "
                    f"PB：{pb_signal} | 涨停：{zt_signal} | 筹码：{shareholder_signal} | "
                    f"持仓数量：{current_pos}股"
                )