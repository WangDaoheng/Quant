import backtrader as bt
import pandas as pd
import logging
from CommonProperties import Mysql_Utils
from CommonProperties.Base_utils import timing_decorator, convert_ymd_format
from strategy.factor_library import FactorLibrary
from backtest.simple_strategy import SimpleStrategy
from backtest.factor_driven_strategy import FactorDrivenStrategy

# 复用你的日志配置
logger = logging.getLogger(__name__)


class StockBacktestEngine:
    """回测引擎核心：完全复用现有MySQL工具类和装饰器"""

    def __init__(self):
        # 复用远程MySQL配置（从你的Base_Properties读取）
        self.user = Mysql_Utils.origin_user
        self.password = Mysql_Utils.origin_password
        self.host = Mysql_Utils.origin_host
        self.database = Mysql_Utils.origin_database
        # 初始化因子库
        self.factor_lib = FactorLibrary()

    @timing_decorator  # 复用你的计时装饰器
    def _prepare_feed(self, stock_code, start_date, end_date):
        """
        准备Backtrader数据馈送：复用Mysql_Utils读取数据
        :param stock_code: 股票代码（如600000）
        :param start_date: 开始日期（YYYYMMDD）
        :param end_date: 结束日期（YYYYMMDD）
        :return: Backtrader PandasData对象
        """
        try:
            # 1. 复用你的Mysql_Utils读取K线数据
            kline_df = Mysql_Utils.data_from_mysql_to_dataframe(
                user=self.user,
                password=self.password,
                host=self.host,
                database=self.database,
                table_name='ods_stock_kline_daily_insight',
                start_date=start_date,
                end_date=end_date,
                cols=['htsc_code', 'ymd', 'open', 'close', 'high', 'low', 'volume']
            )

            # 2. 股票代码格式适配（去除.SH/.SZ后缀）
            if '.' in stock_code:
                stock_code = stock_code.split('.')[0]
            kline_df = kline_df[kline_df['htsc_code'].str.startswith(stock_code)]

            if kline_df.empty:
                logger.warning(f"股票[{stock_code}]在{start_date}-{end_date}无数据")
                return None

            # 3. 复用你的日期格式化函数
            kline_df = convert_ymd_format(kline_df, 'ymd')
            kline_df['ymd'] = pd.to_datetime(kline_df['ymd'])
            kline_df = kline_df.rename(columns={
                'ymd': 'datetime',
                'htsc_code': 'stock_code',
                'open': 'open',
                'high': 'high',
                'low': 'low',
                'close': 'close',
                'volume': 'volume'
            }).set_index('datetime')

            # 4. 转换为Backtrader数据格式
            feed = bt.feeds.PandasData(dataname=kline_df)
            return feed
        except Exception as e:
            logger.error(f"准备{stock_code}数据失败：{str(e)}")
            return None

    @timing_decorator
    def get_factor_value(self, stock_code, date, factor_type='pb'):
        """
        查询指定股票/日期的因子信号
        :param stock_code: 股票代码
        :param date: 日期（datetime.date格式）
        :param factor_type: 因子类型（pb/zt/shareholder/north）
        :return: 因子信号（True/False）
        """
        try:
            date_str = date.strftime('%Y%m%d')

            if factor_type == 'pb':
                # PB因子：低PB返回True
                pb_df = self.factor_lib.pb_factor(start_date=date_str, end_date=date_str)
                pb_df = pb_df[pb_df['stock_code'].str.startswith(stock_code)]
                return pb_df['pb_signal'].iloc[0] if not pb_df.empty else False

            elif factor_type == 'zt':
                # 涨停因子：近5日涨停返回True
                zt_df = self.factor_lib.zt_factor(start_date=date_str, end_date=date_str)
                zt_df = zt_df[zt_df['stock_code'].str.startswith(stock_code)]
                return zt_df['zt_signal'].iloc[0] if not zt_df.empty else False

            elif factor_type == 'shareholder':
                # 筹码因子：股东数下降返回True
                shareholder_df = self.factor_lib.shareholder_factor(start_date=date_str, end_date=date_str)
                shareholder_df = shareholder_df[shareholder_df['stock_code'].str.startswith(stock_code)]
                return shareholder_df['shareholder_signal'].iloc[0] if not shareholder_df.empty else False

            else:
                logger.warning(f"不支持的因子类型：{factor_type}")
                return False
        except Exception as e:
            logger.error(f"查询{stock_code}@{date_str}的{factor_type}因子失败：{str(e)}")
            return False

    @timing_decorator
    def update_datas(self, cerebro, new_stock_codes, start_date, end_date, current_date):
        """
        动态更新股票数据（适配月度调仓）
        :param cerebro: Backtrader Cerebro实例
        :param new_stock_codes: 新选股列表
        :param start_date: 回测开始日期
        :param end_date: 回测结束日期
        :param current_date: 当前调仓日期（YYYYMMDD）
        :return: 有效股票列表
        """
        # 清空旧数据
        cerebro.datas.clear()
        valid_codes = []
        for code in new_stock_codes[:5]:  # 限制数量，提升回测速度
            feed = self._prepare_feed(code, current_date, end_date)
            if feed:
                cerebro.adddata(feed, name=code)
                valid_codes.append(code)
        logger.info(f"动态加载新股票数据：{valid_codes}")
        return valid_codes

    @timing_decorator
    def run_backtest(self,
                     stock_codes,
                     start_date,
                     end_date,
                     initial_cash=100000,
                     strategy_type='simple',
                     stock_selection_func=None):
        """
        执行回测主逻辑
        :param stock_codes: 初始选股列表
        :param start_date: 回测开始日期（YYYYMMDD）
        :param end_date: 回测结束日期（YYYYMMDD）
        :param initial_cash: 初始资金（默认10万）
        :param strategy_type: 策略类型（simple/factor_driven）
        :param stock_selection_func: 动态选股函数（仅dynamic_pool策略需要）
        :return: 绩效指标字典
        """
        # 1. 初始化Backtrader核心引擎
        self.cerebro = bt.Cerebro()  # 保存cerebro实例供外部调用
        self.cerebro.broker.setcash(initial_cash)  # 设置初始资金
        self.cerebro.broker.setcommission(commission=0.0003)  # 佣金：千分之0.3
        self.cerebro.broker.set_coc(True)  # 以收盘价成交（贴近实盘）

        # 2. 加载初始股票数据
        valid_codes = []
        for code in stock_codes[:5]:
            feed = self._prepare_feed(code, start_date, end_date)
            if feed:
                self.cerebro.adddata(feed, name=code)
                valid_codes.append(code)

        if not valid_codes:
            logger.error("无有效股票数据，终止回测")
            return None

        # 3. 加载策略（核心：传递参数）
        if strategy_type == 'simple':
            self.cerebro.addstrategy(SimpleStrategy)
            logger.info("加载简易调仓策略")

        elif strategy_type == 'factor_driven':
            # 传递回测引擎实例给因子策略
            self.cerebro.addstrategy(
                FactorDrivenStrategy,
                backtest_engine=self  # 关键：把引擎实例传给策略
            )
            logger.info("加载因子驱动策略")

        else:
            logger.error(f"不支持的策略类型：{strategy_type}")
            return None

        # 4. 添加绩效分析器（含胜率/夏普比率/最大回撤）
        self.cerebro.addanalyzer(bt.analyzers.SharpeRatio, _name='sharpe', riskfreerate=0.03)
        self.cerebro.addanalyzer(bt.analyzers.Returns, _name='returns', tann=252)
        self.cerebro.addanalyzer(bt.analyzers.DrawDown, _name='drawdown')
        self.cerebro.addanalyzer(bt.analyzers.TradeAnalyzer, _name='trade_analyzer')
        self.cerebro.addanalyzer(bt.analyzers.SQN, _name='sqn')

        # 5. 运行回测
        logger.info(f"开始回测：{start_date} ~ {end_date}，初始资金：{initial_cash}元")
        results = self.cerebro.run()
        if not results:
            logger.error("回测执行失败")
            return None
        strat = results[0]

        # 6. 提取绩效指标
        perf = self._extract_performance_metrics(strat, initial_cash, self.cerebro)
        logger.info(f"回测完成，最终资金：{perf['最终资金']}元")
        return perf

    def _extract_performance_metrics(self, strat, initial_cash, cerebro):
        """提取标准化绩效指标（含胜率）"""
        # 基础收益指标
        final_cash = round(cerebro.broker.getvalue(), 2)
        returns_ana = strat.analyzers.returns.get_analysis()
        sharpe_ana = strat.analyzers.sharpe.get_analysis()
        drawdown_ana = strat.analyzers.drawdown.get_analysis()

        # 交易胜率指标
        trade_ana = strat.analyzers.trade_analyzer.get_analysis()
        sqn_ana = strat.analyzers.sqn.get_analysis()

        # 计算核心指标
        total_return = round((final_cash - initial_cash) / initial_cash * 100, 2)
        annual_return = round(returns_ana.get('rnorm', 0) * 100, 2)
        sharpe_ratio = round(sharpe_ana.get('sharperatio', 0), 2)
        max_drawdown = round(drawdown_ana.get('max', {}).get('drawdown', 0), 2)

        # 胜率/盈亏比计算（容错）
        try:
            total_trades = trade_ana.total.closed
            winning_trades = trade_ana.won.total if hasattr(trade_ana, 'won') else 0
            losing_trades = trade_ana.lost.total if hasattr(trade_ana, 'lost') else 0

            win_rate = round(winning_trades / total_trades * 100, 2) if total_trades > 0 else 0
            avg_win = trade_ana.won.pnl.average if winning_trades > 0 else 0
            avg_loss = abs(trade_ana.lost.pnl.average) if losing_trades > 0 else 1
            profit_loss_ratio = round(avg_win / avg_loss, 2)
        except Exception as e:
            logger.warning(f"计算胜率失败：{str(e)}")
            total_trades = 0
            win_rate = 0
            profit_loss_ratio = 0

        # 封装结果
        return {
            # 基础信息
            '初始资金': initial_cash,
            '最终资金': final_cash,
            '回测周期': f"{start_date} ~ {end_date}",
            # 收益指标
            '总收益率': total_return,
            '年化收益率': annual_return,
            '夏普比率': sharpe_ratio,
            '最大回撤': max_drawdown,
            # 胜率指标
            '总交易次数': total_trades,
            '胜率': win_rate,
            '盈亏比': profit_loss_ratio,
            '策略质量得分(SQN)': round(sqn_ana.get('sqn', 0), 2)
        }