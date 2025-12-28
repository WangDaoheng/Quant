import backtrader as bt
import logging
from CommonProperties.Base_utils import timing_decorator

logger = logging.getLogger(__name__)


class SimpleStrategy(bt.Strategy):
    """
    简易调仓策略：每月调仓一次，卖出所有持仓后买入第一只股票
    适用于快速验证选股结果的整体收益
    """

    @timing_decorator
    def next(self):
        # 每月第一个交易日调仓（20个交易日≈1个月）
        if len(self) % 20 == 0:
            current_date = self.datas[0].datetime.date(0) if self.datas else None
            if not current_date:
                return
            logger.info(f"[{current_date}] 开始月度调仓")

            # 1. 卖出所有持仓
            for data in self.datas:
                if self.getposition(data).size > 0:
                    self.close(data)
                    logger.info(f"[{current_date}] 卖出 {data._name}")

            # 2. 买入第一只股票（90%仓位）
            if self.datas:
                # 计算买入数量：(可用现金×90%) / 当前收盘价
                total_cash = self.broker.getcash() * 0.9
                position_size = total_cash / self.datas[0].close[0]
                self.buy(self.datas[0], size=position_size)
                logger.info(
                    f"[{current_date}] 买入 {self.datas[0]._name} | "
                    f"可用现金：{self.broker.getcash():.2f}元 | "
                    f"买入数量：{position_size:.0f}股 | 仓位占比：90%"
                )