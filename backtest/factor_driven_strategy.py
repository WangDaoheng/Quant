import backtrader as bt
import logging
from datetime import datetime

#
# self.datas 里的 data 是数据馈送对象（Data Feed），核心对应单只股票的全量行情数据，其属性可分为「核心行情属性」「时间属性」「自定义 / 元数据属性」三大类
# 「核心行情属性」
# 属性名	           含义      	            示例（A 股场景）	             用法举例
# close	        收盘价序列	            data.close[0] → 10.5 元	    计算仓位：cash / data.close[0]
# open	        开盘价序列	            data.open[0] → 10.2 元	    判断跳空：data.open[0] > data.close[-1]*1.02
# high	        最高价序列	            data.high[0] → 10.8 元	    判断突破：data.close[0] > data.high[-5:-1].max()
# low	        最低价序列	            data.low[0] → 10.1 元	    判断支撑：data.close[0] < data.low[-3]
# volume	    成交量序列（股数 / 手）	data.volume[0] → 1000000 股	判断放量：data.volume[0] > data.volume[-1]*2
# openinterest	持仓量（期货专用，A 股无意义）	0（A 股恒为 0）	              忽略
#
#「时间属性」
# 用于获取当前 Bar 的时间，是策略中「定时调仓」「查询当日因子」的核心，常用格式为 data.datetime.xxx(0)
# 属性用法	                                      含义	                           示例
# data.datetime.date(0)	                 当前 Bar 的日期（date 对象）	            2025-12-26
# data.datetime.datetime(0)	             当前 Bar 的完整时间（datetime 对象）	    2025-12-26 15:00:00
# data.datetime.num2date(0)	             数字时间戳转 date 对象（等价于 date (0)）	2025-12-26
# data.datetime.strftime(0, '%Y%m%d')	 格式化日期字符串	                        20251226
# len(data)	                             该股票的总 Bar 数（总交易日数）	        252（1 年交易日）
#
#「自定义 / 元数据属性」
# 这类属性用于标识股票、存储自定义信息，是区分不同 data 对象的关键
# 属性名	             含义	                               你的场景用法
# data._name	  股票代码（你 addata 时指定的 name）	stock_code = data._name → 获取 600000
# data.name	      等价于 _name（官方推荐用法）	        建议替换为 data.name，更规范
# data._id	      Backtrader 内部唯一 ID（数字）	    区分不同 data 对象，如 0/1/2
# data.p	      数据馈送的参数配置	                data.p.timeframe → 获取周期（日线 / 小时线）
# data.params	  等价于 p，参数字典形式	            data.params.timeframe
# data.p.timeframe 可判断数据周期：bt.TimeFrame.Days（日线）、bt.TimeFrame.Hours（小时线）
#
#「交易相关属性（持仓 / 订单）」
# 用法	                             含义	                 示例
# self.getposition(data).size	该股票的持仓数量	        >0 有持仓，0 空仓
# self.getposition(data).price	持仓成本价（加权平均）	    10.5元
# self.getposition(data).value	持仓市值（数量 × 当前价）	10000股 × 10.5 = 105000元
# data._orders	                该股票的未成交订单列表	调试用，不建议业务使用
# 补充self.getposition()
# 逐字拆解 self.getposition(data) 的属性
# 属性名	             真实含义	                   示例（A 股场景）	          你的策略用法
# size	    持仓数量（正数 = 多头，负数 = 空头）	10000 股（买入 1 万股）	    判断是否持仓：size > 0
# price	    平均持仓成本价（加权平均）	            10.5 元 / 股	                计算浮盈：(当前价 - price) × size
# value	    持仓市值（size × 当前市场价）	        10000 × 10.8 = 108000 元	查看当前持仓的市场价值
# pnl	    持仓浮盈（value - size × price）	    108000 - 105000 = 3000 元	实时查看盈亏
# pnlcomm	扣除佣金后的浮盈	                    3000 - 32.4 = 2967.6 元	    更贴近实盘的盈亏计算
#
#
#
#


class FactorDrivenStrategy(bt.Strategy):
    """
    因子驱动策略：基于PB/涨停/筹码因子动态交易
    适用于精细化验证因子的交易价值
    """
    # 接收回测引擎实例（用于调用因子库）
    params = (('backtest_engine', None),)

    def next(self):
        # 获取当前日期
        current_date = self.datas[0].datetime.date(0)
        # 遍历所有股票
        for data in self.datas:
            stock_code = data._name  # 获取股票代码
            if not stock_code:
                continue

            # 查询当前股票的因子信号（PB+涨停+筹码）
            pb_signal = self.p.backtest_engine.get_factor_value(stock_code, current_date, 'pb')
            zt_signal = self.p.backtest_engine.get_factor_value(stock_code, current_date, 'zt')
            shareholder_signal = self.p.backtest_engine.get_factor_value(stock_code, current_date, 'shareholder')

            # 因子组合信号：三个因子都满足 → 买入；任一不满足 → 卖出
            buy_signal = pb_signal & zt_signal & shareholder_signal
            sell_signal = not buy_signal

            # 执行交易
            current_position = self.getposition(data).size
            if buy_signal and current_position == 0:
                # 买入：等权分配仓位（90%总资金 / 股票数量）
                position_size = (self.broker.getcash() * 0.9) / len(self.datas) / data.close[0]
                self.buy(data, size=position_size)
                logging.info(
                    f"[{current_date}] 买入 {stock_code} | "
                    f"PB因子：{pb_signal} | 涨停因子：{zt_signal} | 筹码因子：{shareholder_signal}"
                )
            elif sell_signal and current_position > 0:
                self.close(data)
                logging.info(
                    f"[{current_date}] 卖出 {stock_code} | "
                    f"PB因子：{pb_signal} | 涨停因子：{zt_signal} | 筹码因子：{shareholder_signal}"
                )




