import backtrader as bt
import logging


class SimpleStrategy(bt.Strategy):
    """
    简易调仓策略：每月调仓，持有选股结果第一只股票
    适用于快速验证选股结果的整体收益
    """

    def next(self):
        # 每月第一个交易日调仓（20个交易日≈1个月）
        if len(self) % 20 == 0:
            # 先卖出所有持仓
            for data in self.datas:
                if self.getposition(data).size > 0:
                    self.close(data)
                    logging.info(f"[{self.datas[0].datetime.date(0)}] 卖出 {data._name}")

            # 买入第一个股票（90%仓位）
            # self.broker 是Backtrader为策略内置的「虚拟经纪商」对象
            # 核心作用：管理账户资金、持仓、交易佣金、成交规则（比如以什么价格成交），所有交易操作（买入 / 卖出）最终都由它执行
            # self.broker.getcash()：获取账户「可用现金」
            if self.datas:
                # 计算买入数量：(当前可用现金 × 90%) / 第一只股票的当前收盘价
                position_size = (self.broker.getcash() * 0.9) / self.datas[0].close[0]
                # 执行买入：买入第一只股票，数量为position_size
                self.buy(self.datas[0], size=position_size)
                # 日志记录：买入时间、股票代码、仓位比例
                logging.info(f"[{self.datas[0].datetime.date(0)}] 买入 {self.datas[0]._name}，仓位90%")








