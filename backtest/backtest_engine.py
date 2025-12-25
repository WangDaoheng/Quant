import backtrader as bt
import pandas as pd
import logging
from CommonProperties.Mysql_Utils import (
    data_from_mysql_to_dataframe,
    origin_user, origin_password, origin_host, origin_database
)
from CommonProperties.Base_utils import timing_decorator, convert_ymd_format


class StockBacktestEngine:
    """回测引擎：适配现有数据结构"""

    def __init__(self):
        self.user = origin_user
        self.password = origin_password
        self.host = origin_host
        self.database = origin_database

    def _prepare_feed(self, stock_code, start_date, end_date):
        """准备Backtrader数据馈送"""
        # 1. 从MySQL读取K线数据
        kline_df = data_from_mysql_to_dataframe(
            user=self.user,
            password=self.password,
            host=self.host,
            database=self.database,
            table_name='ods_stock_kline_daily_insight',
            start_date=start_date,
            end_date=end_date,
            cols=['htsc_code', 'ymd', 'open', 'close', 'high', 'low', 'volume']
        )
        # 2. 股票代码格式适配（去除后缀，比如 600000.SH → 600000）
        if '.' in stock_code:
            stock_code = stock_code.split('.')[0]
        kline_df = kline_df[kline_df['htsc_code'].str.startswith(stock_code)]
        if kline_df.empty:
            logging.warning(f"股票[{stock_code}]无K线数据")
            return None

        # 3. 日期格式统一（复用你的Base_utils工具）
        kline_df = convert_ymd_format(kline_df, 'ymd')
        kline_df['ymd'] = pd.to_datetime(kline_df['ymd'])

        # 4. 列名适配Backtrader要求（必须包含datetime/open/high/low/close/volume）
        kline_df = kline_df.rename(columns={
            'ymd': 'datetime',
            'htsc_code': 'stock_code',
            'open': 'open',
            'high': 'high',
            'low': 'low',
            'close': 'close',
            'volume': 'volume'
        }).set_index('datetime')

        # 5. 转换为Backtrader可识别的DataFeed格式
        feed = bt.feeds.PandasData(dataname=kline_df)
        return feed

    @timing_decorator
    def run_backtest(self, stock_codes, start_date, end_date, initial_cash=100000):
        """
        执行回测
        :param stock_codes: 选股结果列表
        :param start_date: 回测开始日期
        :param end_date: 回测结束日期
        :param initial_cash: 初始资金
        :return: 绩效指标
        """
        # 1. 初始化Backtrader核心引擎Cerebro
        cerebro = bt.Cerebro()
        cerebro.broker.setcash(initial_cash)             # 设置初始资金
        cerebro.broker.setcommission(commission=0.0003)  # 佣金千分之0.3

        # 2. 为选股结果添加数据馈送（限制前5只，避免回测过慢）
        valid_codes = []
        for code in stock_codes[:5]:  # 限制数量，避免回测过慢
            feed = self._prepare_feed(code, start_date, end_date)
            if feed:
                cerebro.adddata(feed, name=code)
                valid_codes.append(code)

        if not valid_codes:
            logging.error("无有效股票数据，回测终止")
            return None

        # 3. 定义极简调仓策略（核心逻辑）
        class SimpleStrategy(bt.Strategy):
            # next() 方法中 → 每次行情更新（比如新的交易日）都会执行
            def next(self):
                # 每月第一个交易日调仓（20个交易日≈1个月）
                if len(self) % 20 == 0:
                    # 先卖出所有持仓
                    # self.datas 是 Backtrader Strategy 类的内置属性，存储所有加入回测的股票数据源(每只股票对应一个 data 对象)
                    for data in self.datas:
                        # getposition(data)：获取该股票的持仓对象
                        # .size：持仓数量（正数 = 多头持仓，负数 = 空头持仓，0 = 无持仓）
                        if self.getposition(data).size > 0:
                            # 对该股票执行平仓操作
                            # 等价于 self.sell(data, size=self.getposition(data).size)
                            self.close(data)
                    # 买入第一个股票（90%仓位）
                    self.buy(self.datas[0], size=0.9)  # 90%仓位

        # 4. 添加策略和绩效分析器
        cerebro.addstrategy(SimpleStrategy)                            # 加载自定义策略
        # 添加夏普比率分析器（衡量风险调整后收益）
        cerebro.addanalyzer(bt.analyzers.SharpeRatio, _name='sharpe')
        # 添加收益率分析器（总收益/年化收益）
        cerebro.addanalyzer(bt.analyzers.Returns, _name='returns')
        # 添加最大回撤分析器（衡量策略风险）
        cerebro.addanalyzer(bt.analyzers.DrawDown, _name='drawdown')

        # 5. 运行回测
        results = cerebro.run()
        strat = results[0]

        # 6. 提取核心绩效指标（格式化输出）
        perf = {
            '初始资金': initial_cash,
            '最终资金': round(cerebro.broker.getvalue(), 2),
            '总收益率': round(strat.analyzers.returns.get_analysis()['rtot'] * 100, 2),
            '年化收益率': round(strat.analyzers.returns.get_analysis()['rnorm'] * 100, 2),
            '夏普比率': round(strat.analyzers.sharpe.get_analysis()['sharperatio'], 2),
            '最大回撤': round(strat.analyzers.drawdown.get_analysis()['max']['drawdown'], 2)
        }
        logging.info(f"回测完成，最终资金：{perf['最终资金']}元")
        return perf