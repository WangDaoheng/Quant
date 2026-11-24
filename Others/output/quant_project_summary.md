# 量化工程V1.0 代码梳理文档
*生成时间: 2026-11-24 16:27:34*
*选中的目录: backtest, CommonProperties, dashboard, strategy*
*特殊包含: datas_prepare/C00_SQL 目录（无论是否选中）*

## 项目统计信息
- 项目根目录: F:\Quant\Backtrader_PJ1
- 总文件数: 23 (其中包含 0 个 C00_SQL 特殊文件)
- Python文件数: 20
- SQL文件数: 3
- Shell文件数: 0
- 有效目录数: 7

# Backtrader_PJ1 项目目录结构
*生成时间: 2026-11-24 16:27:34*
*选中的目录: backtest, CommonProperties, dashboard, strategy*
*特殊包含: datas_prepare/C00_SQL 目录（无论是否选中）*

📁 Backtrader_PJ1/
    📄 main.py
    📁 backtest/
        📄 __init__.py
        📄 backtest_engine.py
        📄 factor_driven_strategy.py
        📄 performance_analysis.py
        📄 simple_strategy.py
    📁 CommonProperties/
        📄 Base_Properties.py
        📄 Base_utils.py
        📄 DateUtility.py
        📄 Mysql_Utils.py
        📄 __init__.py
        📄 set_config.py
    📁 dashboard/
        📄 __init__.py
        📄 dapan_qingxu.py
        📄 strategy_dashboard.py
        📁 data_jsons/
            📄 __init__.py
        📁 C00_SQL 🔸
            📄 DWD_mysql_tables.sql 🔸
            📄 MART_mysql_tables.sql 🔸
            📄 ODS_mysql_tables.sql 🔸
            📄 __init__.py 🔸
    📁 strategy/
        📄 __init__.py
        📄 factor_library.py
        📄 strategy_engine.py

# 项目代码内容
*选中的目录: backtest, CommonProperties, dashboard, strategy*
*特殊包含: datas_prepare/C00_SQL 目录（无论是否选中）*

--------------------------------------------------------------------------------
## main.py

```python
#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
量化策略主程序入口 - 优化版
修复了回测周期显示bug，增强了错误处理

主要功能：
1. 运行因子驱动策略回测
2. 实时监控策略信号
3. 生成每日复盘报告
4. 创建可视化仪表盘
"""

import logging
import traceback
from datetime import datetime, timedelta
from typing import Optional, Dict, Any

# 导入自定义模块
from backtest import StockBacktestEngine, PerformanceAnalyzer
from monitor.realtime_monitor import RealtimeMonitor
from monitor.alert_system import AlertSystem
from review.daily_review import DailyReview
from dashboard.strategy_dashboard import StrategyDashboard
from CommonProperties.DateUtility import DateUtility


# ============================================================================
# 日志配置
# ============================================================================
def setup_logging():
    """配置日志系统"""
    log_format = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    log_date_format = '%Y-%m-%d %H:%M:%S'

    # 创建格式化器
    formatter = logging.Formatter(log_format, datefmt=log_date_format)

    # 文件处理器（按日期滚动）
    try:
        file_handler = logging.FileHandler(
            f'quant_strategy_{datetime.now().strftime("%Y%m%d")}.log',
            encoding='utf-8'
        )
        file_handler.setFormatter(formatter)
        file_handler.setLevel(logging.INFO)
    except Exception as e:
        print(f"创建日志文件失败: {e}")
        file_handler = None

    # 控制台处理器
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)
    console_handler.setLevel(logging.INFO)

    # 配置根日志记录器
    root_logger = logging.getLogger()
    root_logger.setLevel(logging.INFO)

    # 移除可能存在的旧处理器
    for handler in root_logger.handlers[:]:
        root_logger.removeHandler(handler)

    # 添加新处理器
    root_logger.addHandler(console_handler)
    if file_handler:
        root_logger.addHandler(file_handler)

    return root_logger


# ============================================================================
# 主函数
# ============================================================================
def main():
    """主程序入口"""
    # 1. 初始化日志
    logger = setup_logging()
    logger.info("=" * 60)
    logger.info("🚀 量化策略分析系统启动")
    logger.info(f"启动时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info("=" * 60)

    # 用于存储回测结果的变量
    factor_perf: Optional[Dict[str, Any]] = None
    engine: Optional[StockBacktestEngine] = None
    cerebro = None

    try:
        # 2. 初始化核心组件
        logger.info("📦 初始化核心组件...")
        engine = StockBacktestEngine()
        alert_system = AlertSystem()

        # 3. 回测参数配置
        logger.info("⚙️ 配置回测参数...")
        start_date = DateUtility.first_day_of_month_before_n_months(6)  # 6个月前
        end_date = DateUtility.today()  # 今天

        # 验证日期格式
        if not (start_date.isdigit() and len(start_date) == 8):
            raise ValueError(f"开始日期格式错误: {start_date}")
        if not (end_date.isdigit() and len(end_date) == 8):
            raise ValueError(f"结束日期格式错误: {end_date}")

        initial_cash = 100000  # 初始资金10万元
        initial_stock_codes = ['600000', '000001', '601318', '002594', '300059']  # 测试股票池

        logger.info(f"回测周期: {start_date} ~ {end_date}")
        logger.info(f"初始资金: {initial_cash:,}元")
        logger.info(f"股票池: {initial_stock_codes}")

        # 4. 运行回测（因子驱动策略）
        logger.info("=" * 60)
        logger.info("📈 开始因子驱动策略回测")
        logger.info("=" * 60)

        factor_perf = engine.run_backtest(
            stock_codes=initial_stock_codes,
            start_date=start_date,
            end_date=end_date,
            initial_cash=initial_cash,
            strategy_type='factor_driven'
        )

        if not factor_perf:
            logger.error("❌ 回测失败，终止程序")
            return

        # 5. 生成回测报告
        logger.info("📊 生成回测报告...")
        analyzer = PerformanceAnalyzer()
        factor_report = analyzer.generate_report(
            backtest_result=factor_perf,
            strategy_name="因子驱动策略",
            start_date=start_date,
            end_date=end_date
        )

        logger.info("\n" + "=" * 60)
        logger.info("📋 因子驱动策略回测报告")
        logger.info("=" * 60)

        # 逐行输出报告，避免日志截断
        for line in factor_report.split('\n'):
            logger.info(line)

        # 6. 获取Cerebro实例用于监控/复盘
        if hasattr(engine, 'get_cerebro'):
            cerebro = engine.get_cerebro()
        elif hasattr(engine, 'cerebro'):
            cerebro = engine.cerebro
        else:
            logger.warning("⚠️ 无法获取Cerebro实例，跳过监控和复盘")
            cerebro = None

        # 7. 实时监控（如果Cerebro可用）
        if cerebro:
            logger.info("\n" + "=" * 60)
            logger.info("👁️ 开始实时监控")
            logger.info("=" * 60)

            monitor = RealtimeMonitor(engine, initial_stock_codes)

            # 单次监控（非循环）
            logger.info("🔍 监控因子信号...")
            factor_alerts = monitor.monitor_factor_signals()

            logger.info("🔍 监控持仓绩效...")
            position_alerts = monitor.monitor_position_performance(cerebro)

            logger.info("🔍 监控价格波动...")
            price_alerts = monitor.monitor_price_volatility()

            # 触发预警
            if factor_alerts or position_alerts or price_alerts:
                logger.warning("🚨 检测到预警信号，触发预警系统")
                alert_system.trigger_alert('all', {
                    'factor': factor_alerts,
                    'position': position_alerts,
                    'price': price_alerts
                })
            else:
                logger.info("✅ 无预警信号，监控正常")
        else:
            logger.info("⏭️ 跳过实时监控（Cerebro不可用）")

        # 8. 每日复盘（如果Cerebro可用）
        if cerebro:
            logger.info("\n" + "=" * 60)
            logger.info("📝 生成每日复盘报告")
            logger.info("=" * 60)

            review = DailyReview(engine, cerebro, 'factor_driven')
            review_report = review.generate_daily_review_report()

            logger.info("📄 复盘报告摘要:")
            # 只输出报告的前几行作为摘要
            lines = review_report.split('\n')[:15]
            for line in lines:
                logger.info(line)

            if len(review_report.split('\n')) > 15:
                logger.info("... (完整报告已保存至文件)")
        else:
            logger.info("⏭️ 跳过每日复盘（Cerebro不可用）")

        # 9. 生成可视化仪表盘（如果回测结果可用）
        if factor_perf and cerebro:
            logger.info("\n" + "=" * 60)
            logger.info("📊 生成可视化仪表盘")
            logger.info("=" * 60)

            dashboard = StrategyDashboard(engine, factor_perf, 'factor_driven')
            dashboard_path = dashboard.generate_dashboard(cerebro)

            if dashboard_path:
                logger.info(f"✅ 仪表盘已生成: {dashboard_path}")
                logger.info(f"💡 请用浏览器打开查看: file://{dashboard_path}")
            else:
                logger.error("❌ 仪表盘生成失败")
        else:
            logger.info("⏭️ 跳过仪表盘生成（数据不足）")

        # 10. 显示关键绩效指标
        logger.info("\n" + "=" * 60)
        logger.info("🎯 关键绩效指标汇总")
        logger.info("=" * 60)

        if factor_perf:
            metrics = [
                ("总收益率", f"{factor_perf.get('总收益率', 0):.2f}%"),
                ("年化收益率", f"{factor_perf.get('年化收益率', 0):.2f}%"),
                ("夏普比率", f"{factor_perf.get('夏普比率', 0):.2f}"),
                ("最大回撤", f"{factor_perf.get('最大回撤', 0):.2f}%"),
                ("胜率", f"{factor_perf.get('胜率', 0):.2f}%"),
                ("盈亏比", f"{factor_perf.get('盈亏比', 0):.2f}"),
                ("最终资金", f"{factor_perf.get('最终资金', 0):,.2f}元"),
            ]

            for name, value in metrics:
                logger.info(f"  {name:<10} : {value}")

            # 简单评估
            total_return = factor_perf.get('总收益率', 0)
            max_drawdown = factor_perf.get('最大回撤', 100)

            if total_return > 20 and max_drawdown < 15:
                logger.info("🌟 策略表现优秀！")
            elif total_return > 10 and max_drawdown < 20:
                logger.info("👍 策略表现良好")
            elif total_return > 0:
                logger.info("🤔 策略表现一般，有待优化")
            else:
                logger.info("⚠️ 策略亏损，需要重新评估")

        logger.info("\n" + "=" * 60)
        logger.info("✅ 量化策略分析流程完成")
        logger.info("=" * 60)

    except KeyboardInterrupt:
        logger.warning("\n⚠️ 用户中断程序执行")
    except Exception as e:
        logger.error(f"\n❌ 程序执行出错: {str(e)}")
        logger.error("详细错误信息:")
        logger.error(traceback.format_exc())

        # 尝试保存部分结果
        try:
            if factor_perf:
                logger.info("\n💾 尝试保存已生成的回测结果...")
                # 这里可以添加保存到文件的逻辑
                pass
        except:
            pass

        logger.error("❌ 程序异常终止")
    finally:
        # 清理资源
        logger.info("🧹 清理资源...")
        # 可以添加资源清理逻辑，如关闭数据库连接等


# ============================================================================
# 程序入口
# ============================================================================
if __name__ == "__main__":
    # 记录启动信息
    print("=" * 60)
    print("🎯 量化策略分析系统 v1.0")
    print(f"📅 启动时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 60)
    print("")

    # 运行主程序
    main()

    # 程序结束
    print("")
    print("=" * 60)
    print("🏁 程序执行完毕")
    print("=" * 60)
```

--------------------------------------------------------------------------------
## backtest\__init__.py

```python
from .backtest_engine import StockBacktestEngine
from .simple_strategy import SimpleStrategy
from .factor_driven_strategy import FactorDrivenStrategy
from .performance_analysis import PerformanceAnalyzer

__all__ = [
    'StockBacktestEngine',
    'SimpleStrategy',
    'FactorDrivenStrategy',
    'PerformanceAnalyzer'
]
```

--------------------------------------------------------------------------------
## backtest\backtest_engine.py

```python
# backtest/backtest_engine.py

import backtrader as bt
import pandas as pd
import logging
from CommonProperties import Mysql_Utils
from CommonProperties.Base_utils import timing_decorator
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
        # 提前初始化cerebro（但要注意线程安全）
        self.cerebro = None

    @timing_decorator
    def _prepare_feed(self, stock_code, start_date, end_date):
        """
        准备Backtrader数据馈送
        """
        try:
            # 使用factor_lib获取K线数据
            kline_df = self.factor_lib.get_stock_kline_data(
                stock_code=stock_code,
                start_date=start_date,
                end_date=end_date
            )

            if kline_df.empty:
                logger.warning(f"股票[{stock_code}]在{start_date}-{end_date}无数据")
                return None

            # 确保数据按日期排序
            kline_df = kline_df.sort_values('ymd')

            # 数据格式转换
            kline_df['ymd'] = pd.to_datetime(kline_df['ymd'])
            kline_df = kline_df.set_index('ymd')
            kline_df.index.name = 'datetime'

            # 确保列名正确
            kline_df = kline_df.rename(columns={
                'open': 'open',
                'high': 'high',
                'low': 'low',
                'close': 'close',
                'volume': 'volume'
            })

            # 转换为Backtrader数据格式
            feed = bt.feeds.PandasData(dataname=kline_df)
            return feed
        except Exception as e:
            logger.error(f"准备{stock_code}数据失败：{str(e)}")
            return None

    @timing_decorator
    def get_factor_value(self, stock_code, date, factor_type='pb'):
        """
        查询指定股票/日期的因子信号（兼容旧版二元信号）
        """
        try:
            date_str = date.strftime('%Y%m%d')

            # 清理股票代码格式
            stock_code_clean = stock_code.split('.')[0] if '.' in stock_code else stock_code

            if factor_type == 'pb':
                # PB因子
                pb_df = self.factor_lib.pb_factor(start_date=date_str, end_date=date_str)
                if not pb_df.empty:
                    # 精确匹配股票代码
                    pb_df_filtered = pb_df[pb_df['stock_code'] == stock_code_clean]
                    if not pb_df_filtered.empty:
                        return bool(pb_df_filtered['pb_signal'].iloc[0])
                return False

            elif factor_type == 'zt':
                # 涨停因子
                zt_df = self.factor_lib.zt_factor(start_date=date_str, end_date=date_str)
                if not zt_df.empty:
                    zt_df_filtered = zt_df[zt_df['stock_code'] == stock_code_clean]
                    if not zt_df_filtered.empty:
                        return bool(zt_df_filtered['zt_signal'].iloc[0])
                return False

            elif factor_type == 'shareholder':
                # 筹码因子
                shareholder_df = self.factor_lib.shareholder_factor(start_date=date_str, end_date=date_str)
                if not shareholder_df.empty:
                    shareholder_df_filtered = shareholder_df[shareholder_df['stock_code'] == stock_code_clean]
                    if not shareholder_df_filtered.empty:
                        return bool(shareholder_df_filtered['shareholder_signal'].iloc[0])
                return False

            else:
                logger.warning(f"不支持的因子类型：{factor_type}")
                return False
        except Exception as e:
            logger.error(f"查询{stock_code}@{date_str}的{factor_type}因子失败：{str(e)}")
            return False

    @timing_decorator
    def get_factor_score(self, stock_code, date, factor_type='pb'):
        """
        获取因子百分制得分（新版）

        Args:
            stock_code: 股票代码
            date: 日期
            factor_type: 因子类型 (pb/zt/shareholder)

        Returns:
            float: 0-100的得分
        """
        try:
            date_str = date.strftime('%Y%m%d')
            stock_code_clean = stock_code.split('.')[0] if '.' in stock_code else stock_code

            if factor_type == 'pb':
                # PB因子百分制
                pb_df = self.factor_lib.pb_factor_score(start_date=date_str, end_date=date_str)
                if not pb_df.empty:
                    filtered = pb_df[pb_df['stock_code'] == stock_code_clean]
                    if not filtered.empty:
                        return float(filtered['pb_score'].iloc[0])
                return 0.0

            elif factor_type == 'zt':
                # 涨停因子百分制
                zt_df = self.factor_lib.zt_factor_score(start_date=date_str, end_date=date_str)
                if not zt_df.empty:
                    filtered = zt_df[zt_df['stock_code'] == stock_code_clean]
                    if not filtered.empty:
                        return float(filtered['zt_score'].iloc[0])
                return 0.0

            elif factor_type == 'shareholder':
                # 筹码因子百分制
                sh_df = self.factor_lib.shareholder_factor_score(start_date=date_str, end_date=date_str)
                if not sh_df.empty:
                    filtered = sh_df[sh_df['stock_code'] == stock_code_clean]
                    if not filtered.empty:
                        return float(filtered['shareholder_score'].iloc[0])
                return 0.0

            else:
                logger.warning(f"不支持的因子类型：{factor_type}")
                return 0.0

        except Exception as e:
            logger.error(f"获取{stock_code}@{date_str}的{factor_type}因子得分失败：{str(e)}")
            return 0.0

    @timing_decorator
    def get_factor_scores(self, stock_code, date):
        """
        一次性获取所有因子的百分制得分

        Args:
            stock_code: 股票代码
            date: 日期

        Returns:
            dict: {'pb': score, 'zt': score, 'shareholder': score}
        """
        return {
            'pb': self.get_factor_score(stock_code, date, 'pb'),
            'zt': self.get_factor_score(stock_code, date, 'zt'),
            'shareholder': self.get_factor_score(stock_code, date, 'shareholder')
        }

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

        # 3. 加载策略
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

        # 4. 添加绩效分析器
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
        perf = self._extract_performance_metrics(strat, initial_cash, self.cerebro, start_date, end_date)
        logger.info(f"回测完成，最终资金：{perf['最终资金']}元")
        return perf

    def _extract_performance_metrics(self, strat, initial_cash, cerebro, start_date, end_date):
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

    def get_cerebro(self):
        """
        获取Cerebro实例（供外部调用）
        """
        return self.cerebro

    @timing_decorator
    def validate_trading_days(self, start_date, end_date):
        """
        验证回测期间的交易日数量
        """
        try:
            trading_days = self.factor_lib.get_trading_days(start_date, end_date)
            if not trading_days:
                logger.error(f"回测期间 {start_date}~{end_date} 无交易日数据")
                return False

            logger.info(f"回测期间交易日数量: {len(trading_days)} 天")
            logger.info(f"交易日范围: {trading_days[0]} 到 {trading_days[-1]}")
            return True
        except Exception as e:
            logger.error(f"验证交易日失败: {str(e)}")
            return False

    @timing_decorator
    def get_stock_historical_data(self, stock_code, start_date, end_date):
        """
        获取股票历史数据（包括K线和因子数据）
        """
        try:
            # 获取K线数据
            kline_df = self.factor_lib.get_stock_kline_data(stock_code, start_date, end_date)

            if kline_df.empty:
                return None

            # 获取因子数据
            factor_data = {}

            # PB因子
            pb_df = self.factor_lib.pb_factor(start_date, end_date)
            if not pb_df.empty:
                stock_pb_df = pb_df[pb_df['stock_code'] == stock_code.split('.')[0]]
                if not stock_pb_df.empty:
                    factor_data['pb'] = stock_pb_df

            # 涨停因子
            zt_df = self.factor_lib.zt_factor(start_date, end_date)
            if not zt_df.empty:
                stock_zt_df = zt_df[zt_df['stock_code'] == stock_code.split('.')[0]]
                if not stock_zt_df.empty:
                    factor_data['zt'] = stock_zt_df

            # 筹码因子
            shareholder_df = self.factor_lib.shareholder_factor(start_date, end_date)
            if not shareholder_df.empty:
                stock_shareholder_df = shareholder_df[shareholder_df['stock_code'] == stock_code.split('.')[0]]
                if not stock_shareholder_df.empty:
                    factor_data['shareholder'] = stock_shareholder_df

            return {
                'kline': kline_df,
                'factors': factor_data
            }

        except Exception as e:
            logger.error(f"获取股票历史数据失败 {stock_code}: {str(e)}")
            return None

    def clear_data_cache(self):
        """
        清理数据缓存
        """
        try:
            # 如果有缓存机制，可以在这里清理
            pass
        except Exception as e:
            logger.warning(f"清理数据缓存失败: {str(e)}")

    @timing_decorator
    def run_comprehensive_backtest(self,
                                   stock_codes,
                                   start_date,
                                   end_date,
                                   initial_cash=100000,
                                   strategy_type='factor_driven'):
        """
        运行综合回测（包含数据验证）
        """
        logger.info("======= 开始综合回测 =======")

        # 1. 验证交易日
        if not self.validate_trading_days(start_date, end_date):
            logger.error("交易日验证失败，终止回测")
            return None

        # 2. 验证股票数据
        valid_stocks = []
        for code in stock_codes:
            data = self.get_stock_historical_data(code, start_date, end_date)
            if data and not data['kline'].empty:
                valid_stocks.append(code)
                logger.info(f"股票 {code} 数据有效，共 {len(data['kline'])} 个交易日")
            else:
                logger.warning(f"股票 {code} 数据无效或无数据")

        if not valid_stocks:
            logger.error("无有效股票数据，终止回测")
            return None

        logger.info(f"有效股票列表: {valid_stocks}")

        # 3. 运行回测
        return self.run_backtest(
            stock_codes=valid_stocks,
            start_date=start_date,
            end_date=end_date,
            initial_cash=initial_cash,
            strategy_type=strategy_type
        )


# 测试函数
if __name__ == "__main__":
    # 配置日志
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )

    # 创建回测引擎
    engine = StockBacktestEngine()

    # 测试交易日验证
    print("测试交易日验证...")
    engine.validate_trading_days('20250101', '20250131')

    # 测试股票数据获取
    print("\n测试股票数据获取...")
    stock_data = engine.get_stock_historical_data('600000', '20250101', '20250131')
    if stock_data:
        print(f"获取到 {len(stock_data['kline'])} 条K线数据")
        if 'pb' in stock_data['factors']:
            print(f"获取到 {len(stock_data['factors']['pb'])} 条PB因子数据")

    # 测试百分制因子获取
    print("\n测试百分制因子获取...")
    from datetime import datetime

    test_date = datetime.now().date()

    pb_score = engine.get_factor_score('600000', test_date, 'pb')
    zt_score = engine.get_factor_score('600000', test_date, 'zt')
    sh_score = engine.get_factor_score('600000', test_date, 'shareholder')

    print(f"PB得分: {pb_score}")
    print(f"涨停得分: {zt_score}")
    print(f"筹码得分: {sh_score}")

    # 测试批量获取
    scores = engine.get_factor_scores('600000', test_date)
    print(f"所有因子得分: {scores}")
```

--------------------------------------------------------------------------------
## backtest\factor_driven_strategy.py

```python
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
```

--------------------------------------------------------------------------------
## backtest\performance_analysis.py

```python
import logging
from CommonProperties.Base_utils import timing_decorator

logger = logging.getLogger(__name__)

class PerformanceAnalyzer:
    """绩效分析工具：生成标准化回测报告（含胜率/因子效果分析）"""
    @staticmethod
    @timing_decorator
    def generate_report(backtest_result, strategy_name, start_date, end_date):
        """
        生成结构化回测报告
        :param backtest_result: 回测绩效字典
        :param strategy_name: 策略名称
        :param start_date: 开始日期
        :param end_date: 结束日期
        :return: 格式化报告字符串
        """
        if not backtest_result:
            return "❌ 回测失败，无有效绩效数据"

        # 生成Markdown格式报告
        report = f"""
# 📈 {strategy_name} 回测报告
## 🕒 回测周期
{start_date} ~ {end_date}

## 💰 核心收益指标
| 指标         | 数值       | 说明                     |
|--------------|------------|--------------------------|
| 初始资金     | {backtest_result['初始资金']} 元 | -                        |
| 最终资金     | {backtest_result['最终资金']} 元 | 回测结束后账户总资金     |
| 总收益率     | {backtest_result['总收益率']} % | 累计收益（含手续费）|
| 年化收益率   | {backtest_result['年化收益率']} % | 按252个交易日年化        |
| 夏普比率     | {backtest_result['夏普比率']} | 风险调整后收益（越高越好）|
| 最大回撤     | {backtest_result['最大回撤']} % | 最大浮亏比例（越低越好）|

## 🎯 交易胜率指标
| 指标         | 数值       | 说明                     |
|--------------|------------|--------------------------|
| 总交易次数   | {backtest_result['总交易次数']} | 完整买卖次数             |
| 胜率         | {backtest_result['胜率']} % | 盈利交易占比             |
| 盈亏比       | {backtest_result['盈亏比']} | 平均盈利/平均亏损        |
| 策略质量得分 | {backtest_result['策略质量得分(SQN)']} | >1.6优秀 / <0.5较差      |

## 📝 策略优化建议
{PerformanceAnalyzer._generate_suggestion(backtest_result)}
        """
        return report

    @staticmethod
    def _generate_suggestion(backtest_result):
        """根据绩效生成优化建议"""
        suggestions = []

        # 收益率维度
        if backtest_result['年化收益率'] > 15:
            suggestions.append("✅ 年化收益率>15%，策略收益能力优秀")
        elif backtest_result['年化收益率'] < 5:
            suggestions.append("⚠️ 年化收益率<5%，建议优化因子组合或调仓频率")

        # 风险维度
        if backtest_result['最大回撤'] > 20:
            suggestions.append("⚠️ 最大回撤>20%，建议添加止损规则（如亏损8%止损）")
        else:
            suggestions.append("✅ 最大回撤<20%，风险控制良好")

        # 胜率维度
        if backtest_result['胜率'] > 60:
            suggestions.append("✅ 胜率>60%，因子择时能力优秀")
        elif backtest_result['胜率'] < 40:
            suggestions.append("⚠️ 胜率<40%，建议提高因子筛选严格度（如PB分位数从0.3→0.2）")

        # 盈亏比维度
        if backtest_result['盈亏比'] > 2:
            suggestions.append("✅ 盈亏比>2，单次盈利覆盖多次亏损，稳定性高")
        elif backtest_result['盈亏比'] < 1:
            suggestions.append("⚠️ 盈亏比<1，建议优化卖出规则（如盈利10%止盈）")

        # 夏普比率维度
        if backtest_result['夏普比率'] > 1.5:
            suggestions.append("✅ 夏普比率>1.5，风险收益比优秀，可实盘验证")
        elif backtest_result['夏普比率'] < 0.5:
            suggestions.append("⚠️ 夏普比率<0.5，建议更换因子组合（如添加北向资金因子）")

        return "\n".join(suggestions) if suggestions else "📌 策略表现中性，建议持续跟踪"
```

--------------------------------------------------------------------------------
## backtest\simple_strategy.py

```python
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
```

--------------------------------------------------------------------------------
## CommonProperties\Base_Properties.py

```python


######################  insight 账号信息  #############################
user = "USER019331L1"
password = "F_Y+.3mtc4tU"

######################  本地 mysql 账号信息  #############################
local_mysql_user = 'root'
local_mysql_password = "123456"
local_mysql_database = 'quant'
local_mysql_host = 'localhost'


######################  远程 mysql 账号信息  #############################
origin_mysql_user = "root"
# origin_mysql_password = "000000"
origin_mysql_password = "WZHwzh123!!!"
origin_mysql_host = "117.72.162.13"
origin_mysql_database = "quant"


######################  tushare   #############################
ts_token = "300919ac6f3f72efe445092de7643f7e40f8458096149c315c0e467a"
# ts_token = "ab0fc47587ec2b284dd7a604befdd4fecb5f324554545abe0dc1cdf8abd2"


######################  京东云 日志文件 留存地址  #############################
log_file_window_path = r"F:\QDatas\logs"
log_file_linux_path = r"/opt/Logs"
log_file_mac_path = r"/Users/wanghuihui/QuantsProject/Logs"



######################  个人 配置 留存地址  #############################
personal_window_path = r"F:\QDatas\ss_property"
personal_linux_path = r"/opt/ss_property"
personal_mac_path = r"/Users/wanghuihui/QuantsProject/ss_property"

personal_property_file = r"personal_property.txt"



```

--------------------------------------------------------------------------------
## CommonProperties\Base_utils.py

```python
import os
import sys
from datetime import datetime,date
import time
import traceback
import inspect
from functools import wraps
import shutil
import pandas as pd
import logging
import requests

from CommonProperties.set_config import setup_logging_config

# ========== 新增：终端颜色常量 ==========
class Colors:
    GREEN = '\033[92m'      # 亮绿
    RED = '\033[91m'        # 亮红
    YELLOW = '\033[93m'     # 亮黄
    CYAN = '\033[96m'       # 青色
    RESET = '\033[0m'       # 重置
    BOLD = '\033[1m'        # 加粗


def save_out_filename(filehead, file_type):
    """
    @:param filehead       文件说明
    @:param file_type      文件类型

    拼接输出文件的文件名
    """
    timestamp = datetime.now().strftime("%Y%m%d%H")
    output_filename = f"{filehead}_{timestamp}.{file_type}"
    # print("正在打印文件:{{{}}}".format(save_out_filename))
    return output_filename


def get_latest_filename(filename_dir):
    """
    返回时间戳最新的filename   file_name: stocks_codes_all_2024070818.txt
    :return:
    """
    file_names = os.listdir(filename_dir)

    latest_date = ''
    latest_file_name = ''

    # 遍历文件名列表
    for file_name in file_names:
        try:
            # 从文件名中提取时间戳部分
            timestamp = file_name.split('_')[-1].split('.')[0]

            # 检查时间戳是否是最新的
            if timestamp > latest_date:
                latest_date = timestamp
                latest_file_name = file_name
        except Exception as e:
            logging.error(r"   在处理文件 {} 时遇到问题:{}".format(file_name, e))

    return latest_file_name



def collect_stock_items(input_list):
    """
    对stocks 的list中每个元素按照前三位做分类汇总
    :param input_list:
    :return:
    """

    result_dict = {}

    for item in input_list:
        prefix = item[:3]
        if prefix not in result_dict:
            result_dict[prefix] = [item]
        else:
            result_dict[prefix].append(item)

    return result_dict


def timing_decorator(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        # 获取当前函数所在的文件名和函数名
        current_frame = inspect.currentframe()
        caller_frame = inspect.getouterframes(current_frame, 2)
        file_name = os.path.basename(caller_frame[1].filename)

        # 在函数执行前打印开始日志
        logging.info(f"======【START】  文件: {file_name} 函数: {func.__name__} 开始执行  ======")

        start_time = time.time()
        try:
            result = func(*args, **kwargs)
        except Exception as e:
            logging.error(f"Error in function {func.__name__}:")
            traceback.print_exc()  # 打印详细的堆栈追踪信息
            raise e  # 重新抛出异常，保持原始行为
        end_time = time.time()
        execution_time = end_time - start_time

        # 在函数执行后打印执行时间日志
        logging.info(f"======【END】  文件: {file_name} 函数: {func.__name__} 执行时间: {execution_time:.2f} 秒   ======")
        return result
    return wrapper


def script_run(main_func=None, *, script_name=None):
    """
    脚本级执行包装器
    用法1: @script_run          → 自动获取 sys.argv[0]
    用法2: @script_run(script_name="xxx.py")  → 指定脚本名
    """
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            # 自动获取或显式指定
            actual_script_name = script_name or os.path.basename(sys.argv[0])
            pid = os.getpid()
            separator = "=" * 60

            logging.info(f"{Colors.BOLD}{Colors.GREEN}{separator}{Colors.RESET}")
            logging.info(f"{Colors.BOLD}{Colors.GREEN}【SCRIPT START】脚本: {actual_script_name} | PID: {pid}{Colors.RESET}")
            logging.info(f"{Colors.BOLD}{Colors.GREEN}{separator}{Colors.RESET}")

            start_time = time.time()
            status = "SUCCESS"

            try:
                result = func(*args, **kwargs)
                return result
            except Exception as e:
                status = "FAILED"
                logging.error(f"{Colors.BOLD}{Colors.RED}脚本执行异常: {e}{Colors.RESET}")
                traceback.print_exc()
                raise
            finally:
                elapsed = time.time() - start_time
                status_color = Colors.GREEN if status == "SUCCESS" else Colors.RED
                logging.info(f"{Colors.BOLD}{status_color}{separator}{Colors.RESET}")
                logging.info(f"{Colors.BOLD}{status_color}【SCRIPT END】脚本: {actual_script_name} | 状态: {status} | 耗时: {elapsed:.2f}秒{Colors.RESET}")
                logging.info(f"{Colors.BOLD}{status_color}{separator}{Colors.RESET}")

        return wrapper

    # 支持 @script_run 或 @script_run(script_name="xxx.py") 两种用法
    if main_func is None:
        return decorator
    else:
        return decorator(main_func)



def copy_and_rename_file(src_file_path, dest_dir, new_name):
    """
    将文件复制到另一个目录并重命名
    :param src_file_path: 源文件路径
    :param dest_dir: 目标目录
    :param new_name: 新文件名
    """
    # 检查目标目录是否存在，不存在则创建
    if not os.path.exists(dest_dir):
        os.makedirs(dest_dir)

    # 目标文件路径
    dest_file_path = os.path.join(dest_dir, new_name)

    # 复制文件并重命名
    shutil.copy(src_file_path, dest_file_path)
    logging.info(f"文件已复制并重命名为: {dest_file_path}")


def process_in_batches(df, batch_size, processing_function, **kwargs):
    """
    通用的批次处理函数。

    Args:
        df (pd.DataFrame): 要处理的数据。
        batch_size (int): 每个批次的大小。
        processing_function (callable): 处理每个批次的函数。
        **kwargs: 传递给处理函数的参数。

    Returns:
        pd.DataFrame: 处理后的总 DataFrame。
    """
    def get_batches(df, batch_size):
        for start in range(0, len(df), batch_size):
            yield df[start:start + batch_size]

    total_batches = (len(df) + batch_size - 1) // batch_size
    total_df = pd.DataFrame()

    for i, batch_df in enumerate(get_batches(df, batch_size), start=1):
        sys.stdout.write(f"\r当前执行 {processing_function.__name__} 的 第 {i} 次循环，总共 {total_batches} 个批次")
        sys.stdout.flush()
        time.sleep(0.01)

        # 直接调用处理函数，只传递 **kwargs
        result = processing_function(**kwargs)
        total_df = pd.concat([total_df, result], ignore_index=True)

    sys.stdout.write("\n")
    return total_df


def get_with_retries(url, headers=None, timeout=10, max_retries=3, backoff_factor=1):
    """
    Args:
        url:
        headers:
        timeout:
        max_retries:      最大重试次数
        backoff_factor:

    Returns:

    """
    retries = 0
    while retries < max_retries:
        try:
            response = requests.get(url, headers=headers, timeout=timeout)
            if response.status_code == 200:
                return response
            else:
                logging.error(f"Error: {response.status_code} - {response.text}")
        except requests.exceptions.RequestException as e:
            logging.error(f"请求失败报错: {e}")

        retries += 1
        sleep_time = backoff_factor * (2 ** retries)
        logging.info(f" {sleep_time} 秒后开展重试...")
        time.sleep(sleep_time)

    logging.error(f"在经历 {max_retries} 次尝试后还是不能捕获数据")
    return None


def convert_ymd_format(df, column='ymd'):
    """
    将 ymd 列统一转换为 %Y-%m-%d 格式
    Args:
        df: 输入的 DataFrame
        column: 需要转换的列名，默认为 'ymd'
    Returns:
        df: 转换后的 DataFrame
    """
    # 检查 ymd 列的格式
    sample_value = df[column].dropna().iloc[0] if not df[column].dropna().empty else None
    print(type(sample_value))

    # 处理 sample_value 是 datetime.date 类型的情况
    if isinstance(sample_value, date):
        df.loc[:, column] = df[column].apply(
            lambda x: x.strftime('%Y-%m-%d') if pd.notnull(x) else None
        )
        return df

    # 将 sample_value 转换为字符串
    if sample_value is not None:
        sample_value = str(sample_value)

    # 如果 sample_value 是字符串且格式为 %Y%m%d，则进行转换
    if sample_value is not None and len(sample_value) == 8 and sample_value.isdigit():
        df.loc[:, column] = df[column].apply(
            lambda x: pd.to_datetime(str(x), format='%Y%m%d').strftime('%Y-%m-%d')
            if pd.notnull(x) else None
        )
    # 如果 sample_value 已经是 %Y-%m-%d 格式，则不需要转换
    elif sample_value is not None and len(sample_value) == 10 and sample_value[4] == '-' and sample_value[7] == '-':
        pass  # 已经是目标格式，无需转换
    # 如果 sample_value 是 datetime 类型，则直接格式化为 %Y-%m-%d
    elif isinstance(sample_value, pd.Timestamp):
        df.loc[:, column] = df[column].apply(
            lambda x: x.strftime('%Y-%m-%d') if pd.notnull(x) else None
        )
    else:
        raise ValueError(f"无法识别的 ymd 列格式: {sample_value}")

    return df


# 调用日志配置
# setup_logging_config()



```

--------------------------------------------------------------------------------
## CommonProperties\DateUtility.py

```python
from datetime import datetime, timedelta
import calendar


class DateUtility:
    """
    日期工具类：统一偏移规则（0=当前周期，正数=往后n周期，负数=往前n周期）
    输出格式：所有日期均返回 YYYYMMDD 字符串
    """
    @staticmethod
    def today():
        """获取今日日期"""
        return datetime.today().strftime('%Y%m%d')


    @staticmethod
    def next_day(n=0):
        """
        获取偏移n天的日期
        :param n: 天数偏移量，0=今日，正数=往后n天，负数=往前n天
        """
        next_date = datetime.today() + timedelta(days=n)
        return next_date.strftime('%Y%m%d')


    @staticmethod
    def is_monday():
        """判断今日是否是周一"""
        today = datetime.today()
        return today.weekday() == 0  # 星期一的weekday()返回值是0

    @staticmethod
    def is_friday():
        """判断今日是否是周五"""
        today = datetime.today()
        return today.weekday() == 4  # 星期五的weekday()返回值是4

    @staticmethod
    def is_weekend():
        """判断今日是否是周末（周六/周日）"""
        today = datetime.today()
        # 在大多数国家，周末是周六和周日，即weekday()返回5（周六）或6（周日）
        return today.weekday() >= 5


    @staticmethod
    def first_day_of_week(n=0):
        """
        获取指定偏移周的第一天（周一）
        :param n: 周偏移量，0=本周，1=下周，-1=上周
        """
        today = datetime.today()
        offset_days = -today.weekday() + n * 7
        start_of_week = today + timedelta(days=offset_days)
        return start_of_week.strftime('%Y%m%d')


    @staticmethod
    def last_day_of_week(n=0):
        """
        获取指定偏移周的最后一天（周日）
        :param n: 周偏移量，0=本周，1=下周，-1=上周
        """
        today = datetime.today()
        offset_days = (6 - today.weekday()) + n * 7
        last_day = today + timedelta(days=offset_days)
        return last_day.strftime('%Y%m%d')


    @staticmethod
    def first_day_of_month(n=0):
        """
        获取指定偏移月的第一天
        :param n: 月偏移量，0=本月，1=下月，-1=上月
        """
        today = datetime.today()
        month = today.month - 1 + n  ## 先转0-11月（便于计算）
        year = today.year + month // 12
        month = month % 12 + 1  ## 转回1-12月
        first_day = datetime(year, month, 1)
        return first_day.strftime('%Y%m%d')

    @staticmethod
    def last_day_of_month(n=0):
        """
        获取指定偏移月的最后一天
        :param n: 月偏移量，0=本月，1=下月，-1=上月
        """
        today = datetime.today()
        month = today.month - 1 + n
        year = today.year + month // 12
        month = month % 12 + 1
        last_day = calendar.monthrange(year, month)[1]  # 获取当月最后一天
        last_day_date = datetime(year, month, last_day)
        return last_day_date.strftime('%Y%m%d')

    # 季度相关
    @staticmethod
    def first_day_of_quarter(n=0):
        """
        获取指定偏移季度的第一天（季首：1/4/7/10月）
        :param n: 季度偏移量，0=本季度，1=下季度，-1=上季度
        """
        today = datetime.today()
        current_quarter = (today.month - 1) // 3 + 1
        target_quarter = current_quarter + n

        year = today.year + (target_quarter - 1) // 4
        quarter_month = ((target_quarter - 1) % 4) * 3 + 1
        first_day = datetime(year, quarter_month, 1)
        return first_day.strftime('%Y%m%d')

    @staticmethod
    def last_day_of_quarter(n=0):
        """
        获取指定偏移季度的最后一天（季末：3/6/9/12月）
        :param n: 季度偏移量，0=本季度，1=下季度，-1=上季度
        """
        today = datetime.today()
        current_quarter = (today.month - 1) // 3 + 1
        target_quarter = current_quarter + n

        year = today.year + (target_quarter - 1) // 4
        quarter_month = ((target_quarter - 1) % 4) * 3 + 3
        last_day = calendar.monthrange(year, quarter_month)[1]

        last_day_date = datetime(year, quarter_month, last_day)
        return last_day_date.strftime('%Y%m%d')


    @staticmethod
    def first_day_of_year(n=0):
        """
        获取指定偏移年的第一天
        :param n: 年偏移量，0=本年，1=下一年，-1=上一年
        """
        today = datetime.today()
        first_day = datetime(today.year + n, 1, 1)
        return first_day.strftime('%Y%m%d')


    @staticmethod
    def last_day_of_year(n=0):
        """
        获取指定偏移年的最后一天
        :param n: 年偏移量，0=本年，1=下一年，-1=上一年
        """
        today = datetime.today()
        last_day = datetime(today.year + n, 12, 31)
        return last_day.strftime('%Y%m%d')


# 测试
if __name__ == "__main__":
    date_utility = DateUtility()
    print("今日日期:", date_utility.today())
    print("当前是否是周末:", date_utility.is_weekend())
    print("-----------------------------------------------")
    print("本周第一天日期:", date_utility.first_day_of_week())
    print("本月第1天日期:", date_utility.first_day_of_month())
    print("本季度第一天日期:", date_utility.first_day_of_quarter())
    print("本年第一天日期:", date_utility.first_day_of_year())



```

--------------------------------------------------------------------------------
## CommonProperties\Mysql_Utils.py

```python

from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import sessionmaker
import gc
import traceback  # 用于打印详细的错误堆栈
import pandas as pd
import numpy as np
import logging
import time

import CommonProperties.Base_Properties as base_properties

###################  mysql 配置   ######################
local_user = base_properties.local_mysql_user
local_password = base_properties.local_mysql_password
local_database = base_properties.local_mysql_database
local_host = base_properties.local_mysql_host

origin_user = base_properties.origin_mysql_user
origin_password = base_properties.origin_mysql_password
origin_database = base_properties.origin_mysql_database
origin_host = base_properties.origin_mysql_host


def _mysql_engine(user, password, host, database='quant'):
    """统一的引擎工厂：带连接超时/读写超时/探活，防"静默断链挂死" """
    db_url = f'mysql+pymysql://{user}:{password}@{host}:3306/{database}'
    return create_engine(
        db_url,
        connect_args={
            'connect_timeout': 30,   # 建连 30 秒不通即报错
            'read_timeout': 300,     # recv 最长等 5 分钟（对 NAT 静默丢会话有效）
            'write_timeout': 300,
        },
        pool_pre_ping=True,          # 取连接前探活，丢弃半死连接
        pool_recycle=3600,           # 连接池 1 小时强制回收
    )

def check_data_written(total_rows, table_name, engine):
    """
    用于查询mysql写入的数据条数是否完整
    Args:
        total_rows: 要验证的表的理论上的行数
        table_name: 要验证的表的名称
        engine:     查询引擎
    Returns:  True 条数验证匹配  / False  条数验证不匹配
    """

    try:
        # 创建数据库连接
        connection = engine.raw_connection()
        cursor = connection.cursor()

        # 查询表中写入的数据总数
        check_query = f"SELECT COUNT(*) FROM {table_name}"
        cursor.execute(check_query)
        result = cursor.fetchone()[0]

        # 关闭连接
        cursor.close()
        connection.close()

        return result == total_rows
    except Exception as e:
        logging.error(f"检查数据写入时发生错误: {e}")
        return False


def data_from_dataframe_to_mysql(user, password, host, database='quant', df=pd.DataFrame(), table_name='', merge_on=[],
                                 batch_size=20000):
    """
    把 dataframe 类型数据分批写入 mysql 表里面，避免锁表
    """
    if df.empty:
        logging.info(f"DataFrame为空，跳过插入 {table_name}")
        return 0

    # 数据处理
    df = df.replace({np.nan: None})
    if merge_on:
        df = df.drop_duplicates(subset=merge_on, keep='first')

    total_rows = len(df)
    if total_rows == 0:
        return 0

    # ===== 新增：日志补充最大ymd（在drop_duplicates之后取，才是实际插入的口径）=====
    max_ymd_str = ''
    if 'ymd' in df.columns and df['ymd'].notna().any():
        max_ymd_str = f"，最大日期为 {df['ymd'].max()}"

    # 分批插入
    # db_url = f'mysql+pymysql://{user}:{password}@{host}:3306/{database}'
    # engine = create_engine(db_url)
    engine = _mysql_engine(user, password, host, database)
    columns = df.columns.tolist()

    # 修复1: 使用 :col_name 格式的占位符
    placeholders = ', '.join([f':{col}' for col in columns])
    insert_sql = f"""
    INSERT IGNORE INTO {table_name} ({', '.join(columns)})
    VALUES ({placeholders})
    """

    inserted = 0
    for i in range(0, total_rows, batch_size):
        batch = df.iloc[i:i + batch_size]
        batch_dict = batch.to_dict('records')

        # 修复2: 使用 engine.begin() 自动处理事务
        with engine.begin() as conn:
            result = conn.execute(text(insert_sql), batch_dict)
            inserted += result.rowcount

        time.sleep(0.5)
        # 只在最后一批打印进度
        if i + batch_size >= total_rows:
            logging.info(f"该批次已处理 {total_rows} 条，累计插入 {inserted} 条")

    engine.dispose()
    logging.info(f"完成：共插入 {inserted} 条到 {table_name}{max_ymd_str}")
    return inserted


# def data_from_mysql_to_dataframe(user, password, host, database='quant', table_name='', start_date=None, end_date=None, cols=None):
#     """
#     从 MySQL 表中读取数据到 DataFrame，同时进行最终的数据完整性检查和日志记录
#     Args:
#         table_name: MySQL 表名
#         database: 数据库名称
#         start_date: 起始日期
#         end_date: 结束日期
#         cols: 要选择的字段列表
#
#     Returns:
#         df: 读取到的 DataFrame
#     """
#
#     db_url = f'mysql+pymysql://{user}:{password}@{host}:3306/{database}'
#     engine = create_engine(db_url)
#
#     # 构建 SELECT 语句
#     if cols:
#         selected_cols = ', '.join(cols)
#     else:
#         selected_cols = '*'
#
#     # 构建 WHERE 条件
#     where_conditions = []
#     if start_date:
#         where_conditions.append(f"ymd >= '{start_date}'")
#     if end_date:
#         where_conditions.append(f"ymd <= '{end_date}'")
#
#     where_clause = " AND ".join(where_conditions)
#
#     # 读取 MySQL 表中的记录总数
#     query_total = f"SELECT COUNT(*) FROM {table_name}"
#     if where_clause:
#         query_total += f" WHERE {where_clause}"
#     total_rows = pd.read_sql(query_total, engine).iloc[0, 0]
#
#     # 读取数据的批量大小
#     chunk_size = 10000
#     chunks = []
#
#     try:
#         for offset in range(0, total_rows, chunk_size):
#             query = f"SELECT {selected_cols} FROM {table_name}"
#             if where_clause:
#                 query += f" WHERE {where_clause}"
#             query += f" LIMIT {chunk_size} OFFSET {offset}"
#             chunk = pd.read_sql(query, engine)
#             chunks.append(chunk)
#
#         df = pd.concat(chunks, ignore_index=True)
#
#         # 最终的数据完整性检查
#         if df.shape[0] == total_rows:
#             logging.info(f"{host} 的 mysql表：{table_name} 数据读取成功且无遗漏，共 {total_rows} 行。")
#         else:
#             logging.warning(f"{table_name} 数据读取可能有问题，预期记录数为 {total_rows}，实际读取记录数为 {df.shape[0]}。")
#
#     except Exception as e:
#         logging.error(f"从表：{table_name} 读取数据时发生错误: {e}")
#         df = pd.DataFrame()  # 返回一个空的 DataFrame 以防出错时没有返回数据
#
#     return df

def data_from_mysql_to_dataframe(user, password, host, database='quant', table_name='',
                                 start_date=None, end_date=None, cols=None,
                                 chunk_size=10000, max_retry=3):
    """
    从 MySQL 表中读取数据到 DataFrame。
    改进点（相对旧版）：
    - 去掉 COUNT(*) 前置：按键分页读到空批/不满批即结束，避免大表 COUNT 全扫
      （COUNT 是"链路沉默几分钟"触发 NAT 丢会话的元凶之一）；
    - LIMIT/OFFSET 改为 (ymd, stock_code) 键集分页：每批走主键索引，不再重复扫；
    - 读取失败断点重试：从最后成功的位置继续，不重复不遗漏；
    - 对没有 stock_code 列的表（交易日历、行情温度等）自动回退 OFFSET 模式。
    """
    engine = _mysql_engine(user, password, host, database)

    selected_cols = ', '.join(cols) if cols else '*'

    where_conditions = []
    if start_date:
        where_conditions.append(f"ymd >= '{start_date}'")
    if end_date:
        where_conditions.append(f"ymd <= '{end_date}'")
    where_clause = " AND ".join(where_conditions)
    prefix = f" WHERE {where_clause}" if where_clause else ""

    # 判断表是否有 stock_code 列，决定分页方式
    try:
        col_check = pd.read_sql(
            f"SELECT COUNT(*) FROM information_schema.COLUMNS "
            f"WHERE TABLE_SCHEMA='{database}' AND TABLE_NAME='{table_name}' "
            f"AND COLUMN_NAME='stock_code'", engine).iloc[0, 0]
        has_stock_code = col_check > 0
    except Exception:
        has_stock_code = False   # 查询失败时保守回退

    chunks = []
    total_rows = 0
    last_ymd, last_code = None, None   # 键集分页断点

    try:
        for attempt in range(max_retry):
            try:
                while True:
                    if has_stock_code:
                        if last_ymd is None:
                            cond = prefix
                        else:
                            cond = (prefix +
                                    f" AND (ymd > '{last_ymd}' OR "
                                    f"(ymd = '{last_ymd}' AND stock_code > '{last_code}'))")
                        query = (f"SELECT {selected_cols} FROM {table_name}{cond} "
                                 f"ORDER BY ymd, stock_code LIMIT {chunk_size}")
                    else:
                        # 回退：OFFSET 模式（适用于交易日历等小表）
                        query = (f"SELECT {selected_cols} FROM {table_name}{prefix} "
                                 f"LIMIT {chunk_size} OFFSET {total_rows}")

                    chunk = pd.read_sql(query, engine)

                    if chunk.empty:
                        raise StopIteration   # 正常读完，跳出重试循环

                    chunks.append(chunk)
                    total_rows += len(chunk)

                    if has_stock_code:
                        last_ymd = str(chunk['ymd'].iloc[-1])
                        last_code = str(chunk['stock_code'].iloc[-1])
                        if len(chunk) < chunk_size:
                            raise StopIteration
                    else:
                        if len(chunk) < chunk_size:
                            raise StopIteration

            except StopIteration:
                break   # 正常读完

            except Exception as e:
                if attempt < max_retry - 1:
                    logging.warning(f"读取 {table_name} 中断（{str(e)[:80]}），"
                                    f"5 秒后从断点重试（第 {attempt + 1}/{max_retry - 1} 次）...")
                    time.sleep(5)
                    engine.dispose()
                    engine = _mysql_engine(user, password, host, database)
                    # last_ymd/last_code/total_rows 保持断点，循环继续
                else:
                    raise

        df = pd.concat(chunks, ignore_index=True) if chunks else pd.DataFrame()
        logging.info(f"{host} 的 mysql表：{table_name} 数据读取成功，共 {total_rows} 行。")
        return df

    except Exception as e:
        logging.error(f"从表：{table_name} 读取数据时发生错误: {e}")
        return pd.DataFrame()
    finally:
        engine.dispose()


def data_from_mysql_to_dataframe_latest(user, password, host, database='quant', table_name='', cols=None):
    """
    从 MySQL 表中读取最新一天的数据到 DataFrame，同时进行最终的数据完整性检查和日志记录
    Args:
        table_name: MySQL 表名
        database: 数据库名称
        cols: 要选择的字段列表

    Returns:
        df: 读取到的 DataFrame
    """

    # db_url = f'mysql+pymysql://{user}:{password}@{host}:3306/{database}'
    # engine = create_engine(db_url)
    engine = _mysql_engine(user, password, host, database)

    try:
        # 获取最新的 ymd 日期
        query_latest_ymd = f"SELECT MAX(ymd) FROM {table_name}"
        latest_ymd = pd.read_sql(query_latest_ymd, engine).iloc[0, 0]

        if latest_ymd is not None:
            # 构建 SELECT 语句
            if cols:
                selected_cols = ', '.join(cols)
            else:
                selected_cols = '*'

            # 查询最新一天的数据
            query = f"SELECT {selected_cols} FROM {table_name} WHERE ymd = '{latest_ymd}'"
            df = pd.read_sql(query, engine)

            logging.info(f"    mysql表：{table_name} 最新一天({latest_ymd})的数据读取成功，共 {df.shape[0]} 行。")
        else:
            logging.warning(f"    {table_name} 表中没有找到有效的 ymd 数据。")
            df = pd.DataFrame()  # 返回空的 DataFrame

    except Exception as e:
        logging.error(f"    从表：{table_name} 读取数据时发生错误: {e}")
        df = pd.DataFrame()  # 返回一个空的 DataFrame 以防出错时没有返回数据

    return df


def create_partition_if_not_exists(engine, partition_name, year, month):
    next_month = month + 1
    next_year = year
    if next_month > 12:
        next_month = 1
        next_year += 1

    partition_value = next_year * 100 + next_month

    with engine.connect() as conn:
        query = text(f"""
        ALTER TABLE your_table ADD PARTITION (
            PARTITION {partition_name} VALUES LESS THAN ({partition_value})
        );
        """)
        conn.execute(query)


def upsert_table(user, password, host, database, source_table, target_table, columns):
    """
    使用 source_table 中的数据来更新或插入到 target_table 中（极简版）
    核心功能：存在则更新，不存在则插入；冲突时忽略（避免中断）

    :param user: 数据库用户名
    :param password: 数据库密码
    :param host: 数据库主机IP
    :param database: 数据库名称（默认为 quant）
    :param source_table: 源表名称（字符串）
    :param target_table: 目标表名称（字符串）
    :param columns: 需要更新或插入的列名列表（列表）
    """
    # 1. 构建数据库连接（原代码逻辑，无charset参数，解决TypeError错误）
    # db_url = f'mysql+pymysql://{user}:{password}@{host}:3306/{database}'
    # engine = create_engine(db_url)
    engine = _mysql_engine(user, password, host, database)

    # 2. 构建列名、更新语句、查询语句（原代码逻辑，保持不变）
    columns_str = ", ".join(columns)
    update_str = ", ".join([f"{col} = VALUES({col})" for col in columns])
    select_str = ", ".join(columns)

    # 3. 构建SQL语句（关键修改：添加IGNORE，解决唯一键冲突中断问题）
    # 原代码：INSERT INTO {target_table} ({columns_str})
    sql = f"""
    INSERT IGNORE INTO {target_table} ({columns_str})
    SELECT {select_str}
    FROM {source_table}
    ON DUPLICATE KEY UPDATE
    {update_str};
    """

    # 4. 执行SQL语句（原代码逻辑，保持不变）
    with engine.connect() as connection:
        # 添加事务提交（原代码缺少，补充后修改才会生效）
        with connection.begin():
            connection.execute(text(sql))
    # 可选：关闭引擎（非必须，但养成好习惯）
    engine.dispose()


def cross_server_upsert_all(source_user, source_password, source_host, source_database,
                            target_user, target_password, target_host, target_database,
                            source_table, target_table):
    """
    跨服务器迁移数据，并在目标服务器上实现数据的并集。
    这是一种追加取并集的方式

    :param source_user:      源服务器的数据库用户名
    :param source_password:  源服务器的数据库密码
    :param source_host:      源服务器的主机地址
    :param source_database:  源服务器的数据库名称
    :param target_user:      目标服务器的数据库用户名
    :param target_password:  目标服务器的数据库密码
    :param target_host:      目标服务器的主机地址
    :param target_database:  目标服务器的数据库名称
    :param source_table:     源表名称（字符串）
    :param target_table:     目标表名称（字符串）
    :param columns:          需要更新或插入的列名列表（列表）
    """

    # 源服务器连接
    # source_db_url = f'mysql+pymysql://{source_user}:{source_password}@{source_host}:3306/{source_database}'
    # source_engine = create_engine(source_db_url)
    source_engine = _mysql_engine(source_user, source_password, source_host, source_database)

    # 目标服务器连接
    # target_db_url = f'mysql+pymysql://{target_user}:{target_password}@{target_host}:3306/{target_database}'
    # target_engine = create_engine(target_db_url)
    target_engine = _mysql_engine(target_user, target_password, target_host, target_database)

    # 从源服务器读取数据
    df = pd.read_sql_table(source_table, source_engine)

    # 动态获取列名
    columns = df.columns.tolist()

    # 在目标服务器创建临时表并插入数据
    temp_table_name = 'temp_source_data'
    df.to_sql(name=temp_table_name, con=target_engine, if_exists='replace', index=False)

    # 构建列名部分
    columns_str = ", ".join(columns)

    # 构建 ON DUPLICATE KEY UPDATE 部分
    update_str = ", ".join([f"{col} = VALUES({col})" for col in columns])

    # 构建 SELECT 部分
    select_str = ", ".join(columns)

    # 构建完整的 SQL 语句
    sql = f"""
    INSERT INTO {target_table} ({columns_str})
    SELECT {select_str}
    FROM {temp_table_name}
    ON DUPLICATE KEY UPDATE 
    {update_str};
    """

    # 在目标服务器上执行合并操作
    with target_engine.connect() as connection:
        connection.execute(text(sql))
        connection.execute(f"DROP TABLE {temp_table_name};")

    print(f"数据已从 {source_table} 迁移并合并到 {target_table}。")



def cross_server_upsert_ymd(source_user, source_password, source_host, source_database,
                            target_user, target_password, target_host, target_database,
                            source_table, target_table, start_date, end_date):
    """
    跨服务器迁移数据，并在目标服务器上实现数据的并集。
    这是一种追加取并集的方式

    :param source_user:      源服务器的数据库用户名
    :param source_password:  源服务器的数据库密码
    :param source_host:      源服务器的主机地址
    :param source_database:  源服务器的数据库名称
    :param target_user:      目标服务器的数据库用户名
    :param target_password:  目标服务器的数据库密码
    :param target_host:      目标服务器的主机地址
    :param target_database:  目标服务器的数据库名称
    :param source_table:     源表名称（字符串）
    :param target_table:     目标表名称（字符串）
    :param columns:          需要更新或插入的列名列表（列表）
    """

    # 源服务器连接
    # source_db_url = f'mysql+pymysql://{source_user}:{source_password}@{source_host}:3306/{source_database}'
    # source_engine = create_engine(source_db_url)
    source_engine = _mysql_engine(source_user, source_password, source_host, source_database)

    # 目标服务器连接
    # target_db_url = f'mysql+pymysql://{target_user}:{target_password}@{target_host}:3306/{target_database}'
    # target_engine = create_engine(target_db_url)
    target_engine = _mysql_engine(target_user, target_password, target_host, target_database)

    # # 从源服务器读取数据
    # df = pd.read_sql_table(source_table, source_engine)

    # 从源服务器读取数据，限制 ymd 在 [start_date, end_date] 内
    query = f"""
    SELECT * FROM {source_table}
    WHERE ymd BETWEEN '{start_date}' AND '{end_date}'
    """
    df = pd.read_sql_query(query, source_engine)

    # 动态获取列名
    columns = df.columns.tolist()

    # 在目标服务器创建临时表并插入数据
    temp_table_name = 'temp_source_data'
    df.to_sql(name=temp_table_name, con=target_engine, if_exists='replace', index=False)

    # 构建列名部分
    columns_str = ", ".join(columns)
    # 构建 ON DUPLICATE KEY UPDATE 部分
    update_str = ", ".join([f"{col} = VALUES({col})" for col in columns])
    # 构建 SELECT 部分
    select_str = ", ".join(columns)

    # 构建完整的 SQL 语句
    sql = f"""
    INSERT INTO {target_table} ({columns_str})
    SELECT {select_str}
    FROM {temp_table_name}
    ON DUPLICATE KEY UPDATE 
    {update_str};
    """

    # 在目标服务器上执行合并操作
    with target_engine.connect() as connection:
        connection.execute(text(sql))
        connection.execute(f"DROP TABLE {temp_table_name};")

    print(f"数据已从 {source_table} 迁移并合并到 {target_table}。")


def full_replace_migrate(source_host, source_db_url, target_host, target_db_url, table_name, chunk_size=10000):
    """
    将本地 MySQL 数据库中的表数据导入到远程 MySQL 数据库中。
    整体暴力迁移，全删全插

    Args:
        source_host   (str): 源端 主机
        source_db_url (str): 源端 MySQL 数据库的连接 URL
        target_host   (str): 目标 主机
        target_db_url (str): 目标 MySQL 数据库的连接 URL
        table_name    (str): 要迁移的表名
        chunk_size    (int): 每次读取和写入的数据块大小，默认 10000 行
    """
    # 创建源端数据库的SQLAlchemy引擎
    source_engine = create_engine(source_db_url)
    # 创建目标数据库的SQLAlchemy引擎
    target_engine = create_engine(target_db_url)

    try:
        # 1. 清空目标表（使用text语句，避免SQL注入，且单独执行）
        # 不使用Session，直接用engine执行，避免事务隐式提交问题
        with target_engine.connect() as target_conn:
            # 开启事务执行TRUNCATE
            with target_conn.begin():
                target_conn.execute(text(f"TRUNCATE TABLE {table_name}"))
            print(f"成功清空目标表 {table_name}。")

        # 2. 分批读取源数据并插入目标库
        offset = 0
        while True:
            # 分批读取数据：使用参数化查询（虽然LIMIT/OFFSET无法参数化，但用text封装更规范）
            # 注意：table_name如果是外部传入，需做合法性校验，避免SQL注入
            query = text(f"SELECT * FROM {table_name} LIMIT :chunk_size OFFSET :offset")
            # 用pandas读取数据，直接使用engine，无需Session
            chunk = pd.read_sql(
                query,
                con=source_engine,
                params={"chunk_size": chunk_size, "offset": offset}  # 参数化传递数值，避免注入
            )

            if chunk.empty:
                break

            # 批量插入目标数据库
            chunk.to_sql(
                name=table_name,
                con=target_engine,
                if_exists='append',
                index=False,
                chunksize=chunk_size  # 再分块写入，提升大数量插入性能
            )
            print(f"成功写入第 {offset // chunk_size + 1} 块数据到{target_host} mysql库。")

            # 更新偏移量
            offset += chunk_size

            # 释放内存
            del chunk
            gc.collect()

        print(f"表 {table_name} 数据迁移完成。")

    except Exception as e:
        # 打印详细的错误信息和堆栈，方便定位问题
        print(f"数据迁移过程中发生错误: {str(e)}")
        print("错误堆栈信息：")
        traceback.print_exc()




def get_stock_codes_latest():
    """
    这是为了取最新的 stock_code, 首先默认从类变量里面获取 stock_code(df), 如果df为空，就从mysql里面去取最新的
    Args:
        df:
    Returns:
    """
    user = origin_user
    password = origin_password
    host = origin_host
    database = origin_database

    stock_code_df = data_from_mysql_to_dataframe_latest(user=user,
                                                        password=password,
                                                        host=host,
                                                        database=database,
                                                        table_name='ods_stock_code_daily_insight')

    latest_stocks_df = stock_code_df[['stock_code', 'stock_name']]

    return latest_stocks_df


def execute_sql_statements(user, password, host, database, sql_statements):
    """
    连接到数据库并执行给定的 SQL 语句列表。

    参数:
    user (str): 数据库用户名。
    password (str): 数据库密码。
    host (str): 数据库主机地址。
    database (str): 数据库名称。
    sql_statements (list): 包含 SQL 语句的列表。
    """
    # 创建数据库连接 URL
    db_url = f'mysql+pymysql://{user}:{password}@{host}:3306/{database}'

    # 创建数据库引擎，设置连接池
    engine = create_engine(db_url, pool_size=10, max_overflow=20, pool_recycle=3600)

    try:
        # 使用连接池执行 SQL 语句
        with engine.connect() as connection:
            transaction = connection.begin()  # 开始事务
            for statement in sql_statements:
                # 使用 text() 来防止 SQL 注入
                connection.execute(text(statement))
            transaction.commit()  # 提交事务

    except SQLAlchemyError as e:
        # 捕获数据库相关的错误
        print(f"Error executing SQL: {e}")
    finally:
        # 确保连接被正确关闭
        engine.dispose()


def execute_query(user, password, host, database, sql):
    """
    执行查询 SQL，返回 DataFrame
    """
    db_url = f'mysql+pymysql://{user}:{password}@{host}:3306/{database}'
    engine = create_engine(db_url, pool_size=5, max_overflow=10, pool_recycle=3600)

    try:
        with engine.connect() as connection:
            result = pd.read_sql(text(sql), connection)
            return result
    except SQLAlchemyError as e:
        print(f"Error executing query: {e}")
        return pd.DataFrame()
    finally:
        engine.dispose()

```

--------------------------------------------------------------------------------
## CommonProperties\__init__.py

```python

```

--------------------------------------------------------------------------------
## CommonProperties\set_config.py

```python
# set_config.py
import logging
import colorlog
from logging.handlers import RotatingFileHandler
import os
import platform
from datetime import datetime

import json
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email import encoders

import CommonProperties.Base_Properties as base_properties
from CommonProperties.Base_Properties import log_file_window_path, log_file_linux_path, log_file_mac_path, personal_window_path, personal_linux_path, personal_mac_path, personal_property_file


def get_platform():
    """
    判断当前操作系统是window 还是 其他
    Returns: True  是window平台
             Flase 是其他平台
    """
    if platform.system() == "Windows":
        return 'Windows'
    elif platform.system() == "Linux":
        return 'Linux'
    elif platform.system() == "Darwin":
        return 'Mac'
    else:
        return None


def read_json_file(filepath):
    """
    对 json 文件的处理, 返回一个dict
    Args:
        filepath:  文件路径
    Returns:
    """
    # 读取文件
    with open(filepath, 'r', encoding='utf-8') as file:
        data = file.read()
    # 解析 JSON 数据
    json_data = json.loads(data)
    # 输出解析结果
    return json_data


def read_personal_property():
    """
    读取私人配置文件
    Returns:
    """
    personal_property_dict = ''
    if get_platform() == 'Windows':
        personal_window_file = os.path.join(personal_window_path, personal_property_file)
        personal_property_dict = read_json_file(personal_window_file)
    elif get_platform() == 'Linux':
        personal_linux_file = os.path.join(personal_linux_path, personal_property_file)
        personal_property_dict = read_json_file(personal_linux_file)
    elif get_platform() == 'Mac':
        personal_mac_file = os.path.join(personal_mac_path, personal_property_file)
        personal_property_dict = read_json_file(personal_mac_file)
    return personal_property_dict


def read_logfile():
    """
    读取日志文件的地址
    Returns:
    """
    # 获取当前日期并生成日志文件名
    current_date = datetime.now().strftime('%Y-%m-%d')
    log_filedir = ''
    if get_platform() == 'Windows':
        log_file = f"log_windows_{current_date}.txt"
        log_filedir = os.path.join(log_file_window_path, log_file)
    elif get_platform() == 'Linux':
        log_file = f"log_linux_{current_date}.txt"
        log_filedir = os.path.join(log_file_linux_path, log_file)
    elif get_platform() == 'Mac':
        log_file = f"log_mac_{current_date}.txt"
        log_filedir = os.path.join(log_file_mac_path, log_file)
    return log_filedir


def setup_logging_config():
    """
    日志配置模块   配置logger, 使得日志既能够在控制台打印,又能输出到.log的日志文件中
    Returns:
    """
    # 获取并配置 root logger
    logger = logging.getLogger()

    if not logger.hasHandlers():
        # 配置控制台日志处理器
        console_handler = colorlog.StreamHandler()
        # 获取当前日期并生成日志文件名
        current_date = datetime.now().strftime('%Y-%m-%d')

        # 根据操作系统类型设置日志文件路径
        log_file_path = ''
        if get_platform() == 'Windows':
            log_file_window_filename = f'log_windows_{current_date}.txt'
            log_file_window = os.path.join(log_file_window_path, log_file_window_filename)
            log_file_path = log_file_window  # Windows 下的路径
        elif get_platform() == 'Linux':
            log_file_linux_filename = f'log_linux_{current_date}.txt'
            log_file_linux = os.path.join(log_file_linux_path, log_file_linux_filename)
            log_file_path = log_file_linux    # Linux 下的路径
        elif get_platform() == 'Mac':
            log_file_mac_filename = f'log_mac_{current_date}.txt'
            log_file_mac = os.path.join(log_file_mac_path, log_file_mac_filename)
            log_file_path = log_file_mac      # Mac 下的路径

        # 配置文件日志处理器（滚动日志）
        file_handler = RotatingFileHandler(log_file_path, maxBytes=1000000, backupCount=3, mode='a')

        # 设置彩色日志的格式，包含时间、日志级别和消息内容
        console_formatter = colorlog.ColoredFormatter(
            '%(log_color)s%(asctime)s - %(levelname)s - %(message)s',
            log_colors={
                'DEBUG': 'cyan',
                'INFO': 'green',  # 将 INFO 级别设为绿色
                'WARNING': 'yellow',
                'ERROR': 'red',
                'CRITICAL': 'bold_red',
            }
        )

        # 设置文件日志格式
        file_formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')

        # 将格式应用到处理器
        console_handler.setFormatter(console_formatter)
        file_handler.setFormatter(file_formatter)

        # 添加处理器到 logger
        logger.addHandler(console_handler)
        logger.addHandler(file_handler)

    # 设置日志级别
    logger.setLevel(logging.INFO)


def send_log_via_email():

    personal_property_dict = read_personal_property()

    # 发件人信息
    sender_email = personal_property_dict['sender_email']
    sender_password = personal_property_dict['sender_password']

    # 收件人信息
    receiver_email = personal_property_dict['receiver_email']

    # 获取当前日期并生成日志文件名
    current_date = datetime.now().strftime('%Y-%m-%d')

    # 构建邮件
    msg = MIMEMultipart()
    msg['From'] = sender_email
    msg['To'] = receiver_email
    msg['Subject'] = f"python 发送quant项目 {current_date} 的日志文件"

    # 邮件正文
    body = f"{current_date} 日的日志文件请见附件"
    msg.attach(MIMEText(body, 'plain'))

    # 添加附件
    logging.info("邮件开始发送........")
    filename = read_logfile()
    attachment = open(filename, "rb")
    part = MIMEBase('application', 'octet-stream')
    part.set_payload((attachment).read())
    encoders.encode_base64(part)
    part.add_header('Content-Disposition', "attachment; filename= %s" % os.path.basename(filename))
    msg.attach(part)

    # 连接到SMTP服务器
    server = smtplib.SMTP_SSL('smtp.139.com', 465)
    server.login(sender_email, sender_password)

    # 发送邮件
    text = msg.as_string()
    server.sendmail(sender_email, receiver_email, text)

    # 关闭连接
    server.quit()
    logging.info("邮件发送成功！")



if __name__ == '__main__':

    # send_log_via_email()
    sender_email = '19801322932@139.com'
    sender_password = '04b78b87377067e47800'

    try:
        server = smtplib.SMTP_SSL('smtp.139.com', 465)
        server.login(sender_email, sender_password)
        print("登录成功！")
        server.quit()
    except smtplib.SMTPAuthenticationError as e:
        print(f"登录失败: {e}")



```

--------------------------------------------------------------------------------
## dashboard\__init__.py

```python
from .strategy_dashboard import StrategyDashboard

__all__ = ['StrategyDashboard']
```

--------------------------------------------------------------------------------
## dashboard\dapan_qingxu.py

```python
# -*- coding: utf-8 -*-

import numpy as np
import pandas as pd
from datetime import datetime, timedelta
import json
import logging
from pathlib import Path

import CommonProperties.Base_Properties as base_properties
import CommonProperties.Mysql_Utils as mysql_utils
from CommonProperties.DateUtility import DateUtility
from CommonProperties.Base_utils import timing_decorator, script_run
from CommonProperties import set_config

# ************************************************************************
# 本代码的作用是生成大盘情绪仪表盘的JSON数据文件
# 输出: sentiment.json + plates.json 供前端静态页面读取
# ************************************************************************

# 调用日志配置
set_config.setup_logging_config()

# 数据库配置
local_user = base_properties.local_mysql_user
local_password = base_properties.local_mysql_password
local_database = base_properties.local_mysql_database
local_host = base_properties.local_mysql_host

origin_user = base_properties.origin_mysql_user
origin_password = base_properties.origin_mysql_password
origin_database = base_properties.origin_mysql_database
origin_host = base_properties.origin_mysql_host

# 输出目录
OUTPUT_DIR = Path('./data_jsons')
OUTPUT_DIR.mkdir(exist_ok=True)

# 指数代码映射
INDEX_CODES = {
    'sh': '000001.SH',  # 上证指数（全市场量能）
    'cyb': '399006.SZ',  # 创业板指
    'kc50': '000688.SH',  # 科创50
}

# 情绪得分权重
SCORE_WEIGHTS = {
    'zt_ratio': 0.30,  # 涨跌停比
    'premium': 0.25,  # 打板溢价
    'hl_diff': 0.20,  # 新高新低差
    'vol_pctile': 0.25,  # 量能分位
}

class GenerateSentimentData:
    """生成情绪仪表盘数据"""
    def __init__(self, n_days=60):
        self.n_days = n_days
        self.trading_days = []
        self.limit_df = pd.DataFrame()
        self.index_data = {}
        self.hl_df = pd.DataFrame()
        self.zt_consecutive = pd.Series(dtype=int)

    def get_trading_days(self):
        """获取最近N个交易日"""
        end_date = DateUtility.next_day(-1)  # 昨天（T-1）
        start_date = (datetime.strptime(end_date, '%Y%m%d') - timedelta(days=self.n_days * 2)).strftime('%Y%m%d')

        df = mysql_utils.data_from_mysql_to_dataframe(
            user=origin_user,
            password=origin_password,
            host=origin_host,
            database=origin_database,
            table_name='ods_trading_days_insight',
            start_date=start_date,
            end_date=end_date,
            cols=['ymd']
        )

        if df.empty:
            logging.error("交易日历表为空，无法获取交易日")
            return []

        self.trading_days = sorted(df['ymd'].astype(str).tolist())[-self.n_days:]
        logging.info(f"交易日范围: {self.trading_days[0]} ~ {self.trading_days[-1]}, 共{len(self.trading_days)}天")
        return self.trading_days

    def fetch_limit_summary(self):
        """获取涨跌停数据"""
        start_date = self.trading_days[0]
        end_date = self.trading_days[-1]

        self.limit_df = mysql_utils.data_from_mysql_to_dataframe(
            user=origin_user,
            password=origin_password,
            host=origin_host,
            database=origin_database,
            table_name='ods_stock_limit_summary_insight',
            start_date=start_date,
            end_date=end_date,
            cols=['ymd', 'today_ZT', 'today_DT', 'yesterday_ZT_rate']
        )

        if not self.limit_df.empty:
            self.limit_df['ymd'] = self.limit_df['ymd'].astype(str)
            self.limit_df = self.limit_df.set_index('ymd')
            logging.info(f"涨跌停数据: {len(self.limit_df)}天")
        else:
            logging.warning("涨跌停数据为空")

    def fetch_index_data(self):
        """获取指数K线数据"""
        start_date = self.trading_days[0]
        end_date = self.trading_days[-1]

        all_df = mysql_utils.data_from_mysql_to_dataframe(
            user=origin_user,
            password=origin_password,
            host=origin_host,
            database=origin_database,
            table_name='ods_index_a_share_insight',
            start_date=start_date,
            end_date=end_date,
            cols=['index_code', 'ymd', 'open', 'close', 'high', 'low', 'volume']
        )

        if all_df.empty:
            logging.error("指数数据为空")
            return

        all_df['ymd'] = all_df['ymd'].astype(str)

        for key, code in INDEX_CODES.items():
            sub_df = all_df[all_df['index_code'] == code].copy()
            if not sub_df.empty:
                sub_df = sub_df.set_index('ymd')
                self.index_data[code] = sub_df
                logging.info(f"{code} 数据: {len(sub_df)}天")
            else:
                logging.warning(f"{code} 无数据")

    def fetch_high_low(self):
        """获取新高新低数据"""
        start_date = self.trading_days[0]
        end_date = self.trading_days[-1]

        self.hl_df = mysql_utils.data_from_mysql_to_dataframe(
            user=origin_user,
            password=origin_password,
            host=origin_host,
            database=origin_database,
            table_name='ods_akshare_stock_a_high_low_statistics',
            start_date=start_date,
            end_date=end_date,
            cols=['ymd', 'market', 'high20', 'low20', 'high60', 'low60']
        )

        if not self.hl_df.empty:
            # 只保留全市场数据
            self.hl_df = self.hl_df[self.hl_df['market'] == 'all'].copy()
            self.hl_df['ymd'] = self.hl_df['ymd'].astype(str)
            self.hl_df = self.hl_df.set_index('ymd')
            logging.info(f"新高新低数据: {len(self.hl_df)}天")
        else:
            logging.warning("新高新低数据为空")

    def fetch_zt_list(self):
        """获取涨停清单，计算最高连板"""
        start_date = self.trading_days[0]
        end_date = self.trading_days[-1]

        df = mysql_utils.data_from_mysql_to_dataframe(
            user=origin_user,
            password=origin_password,
            host=origin_host,
            database=origin_database,
            table_name='dwd_stock_zt_list',
            start_date=start_date,
            end_date=end_date,
            cols=['ymd', 'stock_code']
        )

        if df.empty:
            logging.warning("涨停清单为空")
            self.zt_consecutive = pd.Series(dtype=int)
            return

        # 统一日期格式为 YYYYMMDD
        df['ymd'] = df['ymd'].astype(str).str.replace('-', '').str.replace('/', '')
        df = df.sort_values(['stock_code', 'ymd'])

        # 计算连板数
        df['consecutive'] = 1
        for stock in df['stock_code'].unique():
            mask = df['stock_code'] == stock
            stock_df = df[mask]
            consecutive = 1
            prev_ymd = None
            for idx, row in stock_df.iterrows():
                if prev_ymd is not None:
                    curr_date = datetime.strptime(row['ymd'], '%Y%m%d')
                    prev_date = datetime.strptime(prev_ymd, '%Y%m%d')
                    if (curr_date - prev_date).days <= 3:
                        consecutive += 1
                    else:
                        consecutive = 1
                df.loc[idx, 'consecutive'] = consecutive
                prev_ymd = row['ymd']

        self.zt_consecutive = df.groupby('ymd')['consecutive'].max()
        logging.info(f"连板数据: {len(self.zt_consecutive)}天")

    def calc_volume_percentile(self, volume_series):
        """计算量能分位"""
        return volume_series.rank(pct=True) * 100

    def calc_emotion_score(self, row, vol_pctile):
        """计算情绪得分"""
        zt = row.get('today_ZT', 0)
        dt = row.get('today_DT', 0)
        zt_ratio = zt / (zt + dt + 1)
        zt_score = min(zt_ratio * 150, 100)

        premium = row.get('yesterday_ZT_rate', 0) or 0
        premium_score = max(0, min((premium + 3) / 6 * 100, 100))

        high20 = row.get('high20', 0) or 0
        low20 = row.get('low20', 0) or 0
        hl_diff = high20 - low20
        hl_score = max(0, min((hl_diff + 200) / 400 * 100, 100))

        vol_score = vol_pctile if not pd.isna(vol_pctile) else 50

        total = (zt_score * SCORE_WEIGHTS['zt_ratio'] +
                 premium_score * SCORE_WEIGHTS['premium'] +
                 hl_score * SCORE_WEIGHTS['hl_diff'] +
                 vol_score * SCORE_WEIGHTS['vol_pctile'])

        return {
            'score': round(total, 1),
            'zt_score': round(zt_score, 1),
            'premium_score': round(premium_score, 1),
            'hl_score': round(hl_score, 1),
            'vol_score': round(vol_score, 1),
        }

    @timing_decorator
    def generate_sentiment_json(self):
        """生成 sentiment.json"""
        sh_df = self.index_data.get(INDEX_CODES['sh'], pd.DataFrame()).copy()
        if sh_df.empty:
            logging.error("上证指数数据为空，无法生成情绪数据")
            return

        sh_df['amount_yi'] = sh_df['volume'] * 100 / 1e8
        vol_pctile = self.calc_volume_percentile(sh_df['amount_yi'])

        result = {
            'meta': {
                'generated_at': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                'date_range': [self.trading_days[0], self.trading_days[-1]],
                'days': len(self.trading_days),
            },
            'daily': []
        }

        for ymd in self.trading_days:
            # 统一日期格式：去掉横线，兼容 2026-07-06 和 20260706
            ymd_clean = str(ymd).replace('-', '').replace('/', '')
            # 格式：0706（MMDD，无斜杠，更紧凑）
            date_str = ymd_clean[4:8]

            limit_row = self.limit_df.loc[ymd] if ymd in self.limit_df.index else pd.Series()
            hl_row = self.hl_df.loc[ymd] if ymd in self.hl_df.index else pd.Series()

            sh_row = sh_df.loc[ymd] if ymd in sh_df.index else pd.Series()
            cyb_df = self.index_data.get(INDEX_CODES['cyb'], pd.DataFrame())
            cyb_row = cyb_df.loc[ymd] if ymd in cyb_df.index else pd.Series()
            kc_df = self.index_data.get(INDEX_CODES['kc50'], pd.DataFrame())
            kc_row = kc_df.loc[ymd] if ymd in kc_df.index else pd.Series()

            emotion = self.calc_emotion_score(
                {**limit_row.to_dict(), **hl_row.to_dict()},
                vol_pctile.get(ymd, 50)
            )

            max_board = int(self.zt_consecutive.get(ymd, 0))

            daily_data = {
                'ymd': ymd_clean,
                'date': date_str,
                'zt': int(limit_row.get('today_ZT', 0)),
                'dt': int(limit_row.get('today_DT', 0)),
                'premium': round(float(limit_row.get('yesterday_ZT_rate', 0) or 0), 2),
                'hl_diff': int(hl_row.get('high20', 0) or 0) - int(hl_row.get('low20', 0) or 0),
                'high20': int(hl_row.get('high20', 0) or 0),
                'low20': int(hl_row.get('low20', 0) or 0),
                'amount_yi': round(float(sh_row.get('amount_yi', 0)), 2),
                'max_board': max_board,
                'emotion': emotion,
                'indices': {
                    'sh': {
                        'open': round(float(sh_row.get('open', 0)), 2),
                        'close': round(float(sh_row.get('close', 0)), 2),
                        'low': round(float(sh_row.get('low', 0)), 2),
                        'high': round(float(sh_row.get('high', 0)), 2),
                        'volume': round(float(sh_row.get('volume', 0)) * 100 / 1e12, 2),
                    },
                    'cyb': {
                        'open': round(float(cyb_row.get('open', 0)), 2),
                        'close': round(float(cyb_row.get('close', 0)), 2),
                        'low': round(float(cyb_row.get('low', 0)), 2),
                        'high': round(float(cyb_row.get('high', 0)), 2),
                        'volume': round(float(cyb_row.get('volume', 0)) * 100 / 1e12, 2),
                    },
                    'kc50': {
                        'open': round(float(kc_row.get('open', 0)), 2),
                        'close': round(float(kc_row.get('close', 0)), 2),
                        'low': round(float(kc_row.get('low', 0)), 2),
                        'high': round(float(kc_row.get('high', 0)), 2),
                        'volume': round(float(kc_row.get('volume', 0)) * 100 / 1e12, 2),
                    },
                }
            }

            result['daily'].append(daily_data)

        output_file = OUTPUT_DIR / 'sentiment.json'
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(result, f, ensure_ascii=False, indent=2)

        logging.info(f"生成 {output_file}，共 {len(result['daily'])} 天数据")


    @timing_decorator
    def generate_plates_json(self):
        """生成 plates.json（板块数据）"""
        result = {
            'meta': {
                'generated_at': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                'date_range': [self.trading_days[0], self.trading_days[-1]] if self.trading_days else [],
            },
            'daily_top20': {}
        }

        output_file = OUTPUT_DIR / 'plates.json'
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(result, f, ensure_ascii=False, indent=2)

        logging.info(f"生成 {output_file}（板块数据待补充）")

    @script_run(script_name="generate_sentiment_data.py")
    def setup(self):
        """主流程"""
        logging.info(f"开始生成情绪数据，最近 {self.n_days} 个交易日...")

        if not self.get_trading_days():
            logging.error("无交易日数据，退出")
            return

        self.fetch_limit_summary()
        self.fetch_index_data()
        self.fetch_high_low()
        self.fetch_zt_list()

        self.generate_sentiment_json()
        self.generate_plates_json()

        logging.info("完成！")


if __name__ == '__main__':
    # 增量：最近60个交易日
    generator = GenerateSentimentData(n_days=60)
    generator.setup()

    # 历史回填：250日（用于量能分位）
    # generator = GenerateSentimentData(n_days=250)
    # generator.setup()

```

--------------------------------------------------------------------------------
## dashboard\strategy_dashboard.py

```python
import logging
import os
import warnings
import matplotlib.pyplot as plt
import plotly.graph_objects as go
import plotly.express as px
import pandas as pd
from datetime import datetime
from CommonProperties.Base_utils import timing_decorator

warnings.filterwarnings('ignore')

# 设置中文字体
plt.rcParams['font.sans-serif'] = ['SimHei']
plt.rcParams['axes.unicode_minus'] = False

logger = logging.getLogger(__name__)


class StrategyDashboard:
    """
    可视化面板：
    1. 收益曲线可视化
    2. 因子效果可视化
    3. 风险指标可视化
    4. 生成交互式仪表盘
    """

    def __init__(self, backtest_engine, backtest_result, strategy_type):
        self.engine = backtest_engine
        self.backtest_result = backtest_result
        self.strategy_type = strategy_type
        self.dashboard_dir = "dashboard_plots"

    @timing_decorator
    def plot_equity_curve(self, cerebro, save_fig=True):
        """绘制收益曲线"""
        logger.info("绘制收益曲线")
        # 提取账户价值历史
        equity_data = []
        strat = cerebro.runstrats[0][0] if cerebro.runstrats else None
        if not strat:
            return None

        # 模拟收益曲线（实际需从Backtrader获取）
        dates = []
        values = []
        for i, data in enumerate(strat.datas[0].datetime):
            dates.append(datetime.fromordinal(int(data)))
            # 模拟账户价值变化（仅演示，实际需替换为真实回测数据）
            base_value = self.backtest_result['初始资金']
            values.append(base_value * (1 + (i % 100) / 1000 * (1 if i < 50 else -0.5)))

            # 绘制Matplotlib图
        fig, ax = plt.subplots(figsize=(12, 6))
        ax.plot(dates, values, label='账户总资产', color='#1f77b4', linewidth=2)
        ax.axhline(y=self.backtest_result['初始资金'], color='red', linestyle='--', label='初始资金')
        ax.set_title(f'{self.strategy_type} 收益曲线', fontsize=14, fontweight='bold')
        ax.set_xlabel('日期', fontsize=12)
        ax.set_ylabel('账户价值（元）', fontsize=12)
        ax.legend()
        ax.grid(True, alpha=0.3)

        # 保存图片
        if save_fig:
            os.makedirs(self.dashboard_dir, exist_ok=True)
            fig_path = f"{self.dashboard_dir}/equity_curve_{self.strategy_type}.png"
            plt.savefig(fig_path, dpi=300, bbox_inches='tight')
            logger.info(f"收益曲线已保存至：{fig_path}")
        plt.close(fig)  # 关闭画布释放内存

        return fig

    @timing_decorator
    def plot_factor_effectiveness(self, save_fig=True):
        """绘制因子效果对比图"""
        logger.info("绘制因子效果对比图")
        if self.strategy_type != 'factor_driven':
            logger.warning("非因子驱动策略，跳过因子效果绘图")
            return None

        # 模拟因子效果数据（实际需从复盘模块/数据库获取）
        factor_data = {
            '因子类型': ['PB因子', '涨停因子', '筹码因子', '组合因子'],
            '盈利胜率': [
                self.backtest_result.get('pb_win_rate', 65),
                self.backtest_result.get('zt_win_rate', 58),
                self.backtest_result.get('shareholder_win_rate', 62),
                self.backtest_result.get('combo_win_rate', 75)
            ],
            '平均收益': [2.5, 3.2, 1.8, 4.5]
        }
        factor_df = pd.DataFrame(factor_data)

        # 绘制Plotly交互式图
        fig = go.Figure()
        # 胜率柱状图
        fig.add_trace(go.Bar(
            x=factor_df['因子类型'],
            y=factor_df['盈利胜率'],
            name='盈利胜率（%）',
            yaxis='y1',
            marker_color='#2ecc71'
        ))
        # 平均收益折线图
        fig.add_trace(go.Scatter(
            x=factor_df['因子类型'],
            y=factor_df['平均收益'],
            name='平均收益（%）',
            yaxis='y2',
            line=dict(color='#e74c3c', width=3)
        ))

        # 布局设置
        fig.update_layout(
            title=f'{self.strategy_type} 因子效果对比',
            xaxis_title='因子类型',
            yaxis=dict(
                title='盈利胜率（%）',
                titlefont=dict(color='#2ecc71'),
                tickfont=dict(color='#2ecc71'),
                range=[0, 100]
            ),
            yaxis2=dict(
                title='平均收益（%）',
                titlefont=dict(color='#e74c3c'),
                tickfont=dict(color='#e74c3c'),
                overlaying='y',
                side='right',
                range=[0, 5]
            ),
            width=1000,
            height=600,
            legend=dict(x=0.02, y=0.98)
        )

        # 保存HTML文件
        if save_fig:
            os.makedirs(self.dashboard_dir, exist_ok=True)
            html_path = f"{self.dashboard_dir}/factor_effectiveness_{self.strategy_type}.html"
            fig.write_html(html_path)
            logger.info(f"因子效果图已保存至：{html_path}")

        return fig

    @timing_decorator
    def plot_risk_metrics(self, save_fig=True):
        """绘制风险指标雷达图"""
        logger.info("绘制风险指标雷达图")
        # 风险指标数据（归一化处理）
        risk_metrics = {
            '指标': ['年化收益率', '夏普比率', '胜率', '盈亏比', '最大回撤（反向）'],
            '实际值': [
                min(self.backtest_result['年化收益率'] / 20, 1),  # 20%年化=满分
                min(self.backtest_result['夏普比率'] / 2, 1),  # 夏普2=满分
                min(self.backtest_result['胜率'] / 100, 1),  # 100%胜率=满分
                min(self.backtest_result['盈亏比'] / 3, 1),  # 盈亏比3=满分
                max(1 - (self.backtest_result['最大回撤'] / 30), 0)  # 30%回撤=0分
            ],
            '优秀值': [1, 1, 1, 1, 1]  # 优秀基准
        }
        risk_df = pd.DataFrame(risk_metrics)

        # 绘制Plotly雷达图
        fig = go.Figure()
        fig.add_trace(go.Scatterpolar(
            r=risk_df['实际值'],
            theta=risk_df['指标'],
            fill='toself',
            name='实际值',
            marker_color='#3498db'
        ))
        fig.add_trace(go.Scatterpolar(
            r=risk_df['优秀值'],
            theta=risk_df['指标'],
            fill='toself',
            name='优秀基准',
            marker_color='#95a5a6',
            opacity=0.3
        ))

        fig.update_layout(
            title=f'{self.strategy_type} 风险收益指标雷达图',
            polar=dict(
                radialaxis=dict(
                    visible=True,
                    range=[0, 1],
                    tickvals=[0, 0.2, 0.4, 0.6, 0.8, 1],
                    ticktext=['0', '0.2', '0.4', '0.6', '0.8', '1']
                )
            ),
            showlegend=True,
            width=800,
            height=800
        )

        # 保存HTML文件
        if save_fig:
            os.makedirs(self.dashboard_dir, exist_ok=True)
            html_path = f"{self.dashboard_dir}/risk_metrics_{self.strategy_type}.html"
            fig.write_html(html_path)
            logger.info(f"风险指标图已保存至：{html_path}")

        return fig

    @timing_decorator
    def generate_dashboard(self, cerebro, save_fig=True):
        """生成完整可视化仪表盘"""
        logger.info("======= 生成策略可视化仪表盘 =======")
        # 确保目录存在
        os.makedirs(self.dashboard_dir, exist_ok=True)

        # 生成各类型图表
        self.plot_equity_curve(cerebro, save_fig)
        if self.strategy_type == 'factor_driven':
            self.plot_factor_effectiveness(save_fig)
        self.plot_risk_metrics(save_fig)

        # 定义因子图表的条件渲染片段
        if self.strategy_type == 'factor_driven':
            factor_chart_html = f"""
            <div class="chart-container">
                <div class="chart-title">因子效果对比</div>
                <iframe src="factor_effectiveness_{self.strategy_type}.html" class="iframe-container"></iframe>
            </div>
            """
        else:
            factor_chart_html = "<!-- 非因子策略，隐藏因子效果图表 -->"

        # 生成完整的仪表盘HTML页面
        dashboard_html = f"""
        <!DOCTYPE html>
        <html lang="zh-CN">
        <head>
            <meta charset="UTF-8">
            <title>{self.strategy_type} 策略仪表盘</title>
            <style>
                body {{ 
                    font-family: "Microsoft YaHei", Arial, sans-serif; 
                    margin: 0; 
                    padding: 20px; 
                    background-color: #f5f7fa;
                }}
                .dashboard-title {{ 
                    text-align: center; 
                    font-size: 28px; 
                    font-weight: bold; 
                    margin-bottom: 30px; 
                    color: #2c3e50;
                }}
                .chart-container {{ 
                    margin: 30px auto; 
                    padding: 20px; 
                    background: white;
                    border-radius: 12px; 
                    box-shadow: 0 2px 10px rgba(0,0,0,0.1);
                    max-width: 1200px;
                }}
                .metrics-summary {{ 
                    display: flex; 
                    justify-content: space-around; 
                    flex-wrap: wrap; 
                    margin: 20px auto;
                    max-width: 1200px;
                }}
                .metric-card {{ 
                    padding: 20px; 
                    background: white;
                    border-radius: 10px; 
                    width: 180px; 
                    text-align: center; 
                    margin: 10px;
                    box-shadow: 0 2px 8px rgba(0,0,0,0.08);
                    transition: transform 0.2s;
                }}
                .metric-card:hover {{
                    transform: translateY(-5px);
                }}
                .metric-value {{ 
                    font-size: 24px; 
                    font-weight: bold; 
                    color: #2c3e50;
                    margin: 10px 0;
                }}
                .metric-label {{ 
                    font-size: 14px; 
                    color: #7f8c8d;
                }}
                .chart-title {{
                    font-size: 18px;
                    font-weight: 600;
                    color: #34495e;
                    margin-bottom: 15px;
                    border-left: 4px solid #3498db;
                    padding-left: 10px;
                }}
                .iframe-container {{
                    width: 100%;
                    height: 600px;
                    border: none;
                    border-radius: 8px;
                }}
                .img-container {{
                    width: 100%;
                    border-radius: 8px;
                    max-height: 600px;
                    object-fit: contain;
                }}
            </style>
        </head>
        <body>
            <div class="dashboard-title">{self.strategy_type} 策略可视化仪表盘</div>

            <!-- 核心指标汇总 -->
            <div class="metrics-summary">
                <div class="metric-card">
                    <div class="metric-label">年化收益率</div>
                    <div class="metric-value">{self.backtest_result['年化收益率']}%</div>
                </div>
                <div class="metric-card">
                    <div class="metric-label">夏普比率</div>
                    <div class="metric-value">{self.backtest_result['夏普比率']}</div>
                </div>
                <div class="metric-card">
                    <div class="metric-label">胜率</div>
                    <div class="metric-value">{self.backtest_result['胜率']}%</div>
                </div>
                <div class="metric-card">
                    <div class="metric-label">最大回撤</div>
                    <div class="metric-value">{self.backtest_result['最大回撤']}%</div>
                </div>
                <div class="metric-card">
                    <div class="metric-label">盈亏比</div>
                    <div class="metric-value">{self.backtest_result['盈亏比']}</div>
                </div>
                <div class="metric-card">
                    <div class="metric-label">策略质量得分</div>
                    <div class="metric-value">{self.backtest_result['策略质量得分(SQN)']}</div>
                </div>
            </div>

            <!-- 收益曲线 -->
            <div class="chart-container">
                <div class="chart-title">收益曲线</div>
                <img src="equity_curve_{self.strategy_type}.png" class="img-container" alt="收益曲线">
            </div>

            <!-- 因子效果对比（仅因子策略显示） -->
            {factor_chart_html}

            <!-- 风险指标雷达图 -->
            <div class="chart-container">
                <div class="chart-title">风险收益指标雷达图</div>
                <iframe src="risk_metrics_{self.strategy_type}.html" class="iframe-container"></iframe>
            </div>

            <!-- 底部信息 -->
            <div style="text-align: center; margin-top: 50px; color: #95a5a6; font-size: 14px;">
                生成时间：{datetime.now().strftime('%Y-%m-%d %H:%M:%S')} | 数据来源：Quant量化系统
            </div>
        </body>
        </html>
        """

        # 保存仪表盘主页面
        dashboard_path = f"{self.dashboard_dir}/strategy_dashboard_{self.strategy_type}.html"
        with open(dashboard_path, 'w', encoding='utf-8') as f:
            f.write(dashboard_html)

        logger.info(f"✅ 完整仪表盘已保存至：{dashboard_path}")
        logger.info(f"📌 可直接用浏览器打开该文件查看可视化结果")

        return dashboard_path
```

--------------------------------------------------------------------------------
## dashboard\data_jsons\__init__.py

```python

```

--------------------------------------------------------------------------------
## datas_prepare\C00_SQL\DWD_mysql_tables.sql

```sql
--1.1
------------------  dwd_ashare_stock_base_info   股票基本信息大宽表
CREATE TABLE quant.dwd_ashare_stock_base_info (
       ymd                DATE                     COMMENT '日期'
      ,stock_code         VARCHAR(50)              COMMENT '代码'
      ,stock_name         VARCHAR(50)              COMMENT '名称'
      ,close              DOUBLE(12,2)             COMMENT '最新收盘价'
      ,change_pct         FLOAT                    COMMENT '当日涨跌幅(%)'
      ,volume             BIGINT                   COMMENT '成交量'
      ,trading_amount     DOUBLE                   COMMENT '成交额'
      ,market_value       DOUBLE                   COMMENT '流通市值(亿)'
      ,total_value        DOUBLE                   COMMENT '总市值(亿)'
      ,total_capital      DOUBLE                   COMMENT '总股本(亿)'
      ,float_capital      DOUBLE                   COMMENT '流通股(亿)'
      ,shareholder_num    BIGINT                   COMMENT '股东人数'
      ,pct_of_total_sh    DOUBLE(12,4)             COMMENT '股东数较上期环比波动百分比'
      ,pb                 FLOAT(12,2)              COMMENT '市净率'
      ,pe                 FLOAT(12,2)              COMMENT '市盈(动)'
      ,market             VARCHAR(50)              COMMENT '市场特征主板创业板等'
      ,plate_names        VARCHAR(2000)            COMMENT '板块名称'
      ,UNIQUE KEY unique_ymd_stock_code (ymd, stock_code)
) COMMENT='股票基本信息大宽表';


--1.2
------------------  dwd_stock_zt_list   涨停股票清单
CREATE TABLE `dwd_stock_zt_list` (
  `ymd`             date            NOT NULL                COMMENT '交易日期',
  `stock_code`      varchar(20)     NOT NULL                COMMENT '股票代码',
  `stock_name`      varchar(50)     DEFAULT NULL            COMMENT '股票名称',
  `open`            decimal(10,2)   DEFAULT NULL            COMMENT '开盘价',
  `high`            decimal(10,2)   DEFAULT NULL            COMMENT '最高价',
  `low`             decimal(10,2)   DEFAULT NULL            COMMENT '最低价',
  `close`           decimal(10,2)   DEFAULT NULL            COMMENT '收盘价',
  `volume`          bigint          DEFAULT NULL            COMMENT '成交量（手）',
  `last_close`      decimal(10,2)   DEFAULT NULL            COMMENT '昨日收盘价',
  `rate`            decimal(10,2)   DEFAULT NULL            COMMENT '涨跌幅（%）',
  `zt_type`         varchar(10)     DEFAULT NULL            COMMENT '涨停类型：yizi/tzi/shiti/zha',
  `zt_price`        decimal(10,2)   DEFAULT NULL            COMMENT '理论涨停价',
  `turnover_rate`   decimal(10,2)   DEFAULT NULL            COMMENT '换手率（%）',
  `market_value`    decimal(20,2)   DEFAULT NULL            COMMENT '流通市值（亿）',
  `total_value`     decimal(20,2)   DEFAULT NULL            COMMENT '总市值（亿）',
  `total_capital`   decimal(20,2)   DEFAULT NULL            COMMENT '总股本（亿）',
  `float_capital`   decimal(20,2)   DEFAULT NULL            COMMENT '流通股（亿）',
  `shareholder_num` int             DEFAULT NULL            COMMENT '股东人数',
  `pb`              decimal(10,2)   DEFAULT NULL            COMMENT '市净率',
  `pe`              decimal(10,2)   DEFAULT NULL            COMMENT '市盈（动）',
  `market`          varchar(20)     DEFAULT NULL            COMMENT '市场特征（主板/创业板/科创板）',
  `plate_names`     text                                    COMMENT '所属板块（逗号分隔）',
  `created_at`      timestamp       NULL DEFAULT CURRENT_TIMESTAMP COMMENT '写入时间',
  PRIMARY KEY (`ymd`,`stock_code`),
  KEY `idx_ymd` (`ymd`),
  KEY `idx_zt_type` (`zt_type`),
  KEY `idx_stock_code` (`stock_code`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci COMMENT='涨停明细表（含类型分类）';



--1.3
------------------  dwd_stock_dt_list   跌停股票清单
CREATE TABLE `dwd_stock_dt_list` (
  `ymd`             date            NOT NULL                COMMENT '交易日期',
  `stock_code`      varchar(20)     NOT NULL                COMMENT '股票代码',
  `stock_name`      varchar(50)     DEFAULT NULL            COMMENT '股票名称',
  `open`            decimal(10,2)   DEFAULT NULL            COMMENT '开盘价',
  `high`            decimal(10,2)   DEFAULT NULL            COMMENT '最高价',
  `low`             decimal(10,2)   DEFAULT NULL            COMMENT '最低价',
  `close`           decimal(10,2)   DEFAULT NULL            COMMENT '收盘价',
  `volume`          bigint          DEFAULT NULL            COMMENT '成交量（手）',
  `last_close`      decimal(10,2)   DEFAULT NULL            COMMENT '昨日收盘价',
  `rate`            decimal(10,2)   DEFAULT NULL            COMMENT '涨跌幅（%）',
  `dt_price`        decimal(10,2)   DEFAULT NULL            COMMENT '理论跌停价',
  `turnover_rate`   decimal(10,2)   DEFAULT NULL            COMMENT '换手率（%）',
  `market_value`    decimal(20,2)   DEFAULT NULL            COMMENT '流通市值（亿）',
  `total_value`     decimal(20,2)   DEFAULT NULL            COMMENT '总市值（亿）',
  `total_capital`   decimal(20,2)   DEFAULT NULL            COMMENT '总股本（亿）',
  `float_capital`   decimal(20,2)   DEFAULT NULL            COMMENT '流通股（亿）',
  `shareholder_num` int             DEFAULT NULL            COMMENT '股东人数',
  `pb`              decimal(10,2)   DEFAULT NULL            COMMENT '市净率',
  `pe`              decimal(10,2)   DEFAULT NULL            COMMENT '市盈（动）',
  `market`          varchar(20)     DEFAULT NULL            COMMENT '市场特征',
  `plate_names`     text                                    COMMENT '所属板块',
  `created_at`      timestamp       NULL DEFAULT CURRENT_TIMESTAMP COMMENT '写入时间',
  PRIMARY KEY (`ymd`,`stock_code`),
  KEY `idx_ymd` (`ymd`),
  KEY `idx_stock_code` (`stock_code`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci COMMENT='跌停明细表';


--1.5
------------------  dwd_stock_technical_indicators   股票技术指标预计算表（均线等）
CREATE TABLE quant.dwd_stock_technical_indicators (
     ymd                      DATE        NOT NULL    COMMENT '交易日期'
    ,stock_code               VARCHAR(50) NOT NULL    COMMENT '股票代码'
    ,stock_name               varchar(50)             COMMENT '股票名称'
    ,close                    FLOAT                   COMMENT '收盘价'
    ,volume                   BIGINT                  COMMENT '成交量'
    -- 价格均线
    ,ma5                      FLOAT                   COMMENT '5日均线'
    ,ma10                     FLOAT                   COMMENT '10日均线'
    ,ma20                     FLOAT                   COMMENT '20日均线'
    ,ma60                     FLOAT                   COMMENT '60日均线'
    ,ma120                    FLOAT                   COMMENT '120日均线（半年线）'
    ,ma250                    FLOAT                   COMMENT '250日均线（年线）'
    -- 成交量均线
    ,vol_ma5                  FLOAT                   COMMENT '5日均量'
    ,vol_ma10                 FLOAT                   COMMENT '10日均量'
    ,vol_ma20                 FLOAT                   COMMENT '20日均量'
    ,vol_ma60                 FLOAT                   COMMENT '60日均量'
    ,vol_ma120                FLOAT                   COMMENT '120日均量'
    ,vol_ma250                FLOAT                   COMMENT '250日均量'
    -- 价格偏离度
    ,price_vs_ma5             DECIMAL(10,2)           COMMENT '价格/5日均线-1, 单位%'
    ,price_vs_ma20            DECIMAL(10,2)           COMMENT '价格/20日均线-1, 单位%'
    ,price_vs_ma60            DECIMAL(10,2)           COMMENT '价格/60日均线-1, 单位%'
    -- 成交量偏离度
    ,volume_vs_ma5            DECIMAL(10,2)           COMMENT '成交量/5日均量-1, 单位%'
    ,volume_vs_ma20           DECIMAL(10,2)           COMMENT '成交量/20日均量-1, 单位%'
    ,UNIQUE KEY unique_ymd_stock_code (ymd, stock_code)
    ,INDEX idx_stock_code (stock_code)
    ,INDEX idx_ma5 (ma5)
    ,INDEX idx_vol_ma5 (vol_ma5)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci
COMMENT='股票技术指标预计算表（均线等）';


--2.2
------------------  dwd_factor_summary  因子汇总表（所有因子得分）
CREATE TABLE IF NOT EXISTS quant.dwd_factor_summary (
    ymd               DATE         NOT NULL  COMMENT '交易日期',
    stock_code        VARCHAR(50)  NOT NULL  COMMENT '股票代码',
    stock_name        VARCHAR(50)            COMMENT '股票名称',
    -- 各因子得分
    pb_score          DECIMAL(5,2) DEFAULT 0 COMMENT 'PB因子得分(0-100)',
    zt_score          DECIMAL(5,2) DEFAULT 0 COMMENT '涨停因子得分(0-100)',
    shareholder_score DECIMAL(5,2) DEFAULT 0 COMMENT '股东数因子得分(0-100)',
    -- 缩量下跌因子相关得分
    volume_score      DECIMAL(5,2) DEFAULT 0 COMMENT '缩量因子得分(0-60)',
    price_score       DECIMAL(5,2) DEFAULT 0 COMMENT '下跌因子得分(0-40)',
    composite_score   DECIMAL(5,2) DEFAULT 0 COMMENT '缩量下跌综合得分(0-100)',
    signal_level      CHAR(1)                COMMENT '缩量下跌信号等级(A/B/C/D/E)',
    -- 其他因子可继续添加
    UNIQUE KEY unique_ymd_stock (ymd, stock_code),
    INDEX idx_composite (composite_score),
    INDEX idx_pb (pb_score),
    INDEX idx_zt (zt_score)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci
COMMENT='因子汇总表（所有因子得分）';


--4.2        多渠道板块数据 -- 多渠道汇总
------------------  dwd_stock_a_total_plate
CREATE TABLE `dwd_stock_a_total_plate` (
  `ymd`             date            NOT NULL                COMMENT '日期',
  `board_code`      varchar(50)     CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci DEFAULT NULL COMMENT '板块代码',
  `board_name`      varchar(50)     CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NOT NULL COMMENT '板块名称',
  `stock_code`      varchar(50)     DEFAULT NULL            COMMENT '标的代码',
  `stock_name`      varchar(50)     DEFAULT NULL            COMMENT '标的名称',
  `source_table`    varchar(50)     DEFAULT NULL            COMMENT '来源表',
  `remark`          varchar(50)     DEFAULT NULL            COMMENT '备注',
  `stock_code_pure` varchar(50)     GENERATED ALWAYS AS (substring_index(`stock_code`,_utf8mb4'.',1)) VIRTUAL,
  UNIQUE KEY `unique_ymd_board_code` (`ymd`,`board_name`,`stock_code`),
  KEY `idx_ymd_stock_board` (`ymd`,`stock_code`,`board_name`),
  KEY `idx_ymd_pure_board` (`ymd`,`stock_code_pure`,`board_name`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci COMMENT='多渠道板块数据 -- 多渠道汇总'



```

--------------------------------------------------------------------------------
## datas_prepare\C00_SQL\MART_mysql_tables.sql

```sql
--1.1
------------------  dmart_stock_zt_details   股票涨停明细
CREATE TABLE quant.dmart_stock_zt_details (
       ymd            DATE                     COMMENT '日期'
      ,stock_code     varchar(50)              COMMENT '代码'
      ,stock_name     varchar(50)              COMMENT '名称'
      ,concept_plate  VARCHAR(500)             COMMENT '概念板块'
      ,index_plate    VARCHAR(500)             COMMENT '指数板块'
      ,industry_plate VARCHAR(500)             COMMENT '行业板块'
      ,style_plate    VARCHAR(500)             COMMENT '风格板块'
      ,out_plate      VARCHAR(500)             COMMENT '外部数据板块'
      ,UNIQUE KEY unique_ymd_stock_code (ymd, stock_code)
) COMMENT='股票涨停明细';


------------------  dmart_stock_zt_details_expanded   股票涨停明细拆分
CREATE TABLE quant.dmart_stock_zt_details_expanded (
       ymd            DATE                     COMMENT '日期'
      ,stock_code     VARCHAR(20)              COMMENT '股票代码'
      ,stock_name     VARCHAR(50)              COMMENT '股票名称'
      ,concept_plate  VARCHAR(500)             COMMENT '概念板块'
      ,index_plate    VARCHAR(500)             COMMENT '指数板块'
      ,industry_plate VARCHAR(500)             COMMENT '行业板块'
      ,style_plate    VARCHAR(500)             COMMENT '风格板块'
      ,out_plate      VARCHAR(500)             COMMENT '外部数据板块'
) COMMENT='股票涨停明细拆分';


```

--------------------------------------------------------------------------------
## datas_prepare\C00_SQL\ODS_mysql_tables.sql

```sql

--1.0
------------------  ods_trading_days_insight   交易所的交易日历
CREATE TABLE quant.ods_trading_days_insight (
     exchange                 VARCHAR(50)             COMMENT '交易所名称'
    ,ymd                      DATE        NOT NULL    COMMENT '交易日期'
    ,UNIQUE KEY unique_ymd_exchange (exchange, ymd)
) COMMENT='交易所的交易日历';


-------------------------------------------   insight 数据  ---------------------------------
--1.1   下午跑
------------------  ods_stock_code_daily_insight   当日已上市股票码表
CREATE TABLE quant.ods_stock_code_daily_insight (
     ymd                      DATE        NOT NULL    COMMENT '交易日期'
    ,stock_code               VARCHAR(50) NOT NULL    COMMENT '股票代码'
    ,stock_name               VARCHAR(50)             COMMENT '股票名'
    ,exchange                 VARCHAR(50)             COMMENT '交易所名称'
    ,UNIQUE KEY unique_ymd_stock_code (ymd, stock_code)
) COMMENT='当日已上市股票码表';

--1.2   下午跑
------------------  ods_stock_limit_summary_insight   当日大A行情温度
CREATE TABLE quant.ods_stock_limit_summary_insight (
     ymd                      DATE        NOT NULL    COMMENT '日期'
    ,name                     VARCHAR(50) NOT NULL    COMMENT '市场名称'
    ,today_ZT                 INT                     COMMENT '今日涨停股票数'
    ,today_DT                 INT                     COMMENT '今日跌停股票数'
    ,yesterday_ZT             INT                     COMMENT '昨日涨停股票数'
    ,yesterday_DT             INT                     COMMENT '昨日跌停股票数'
    ,yesterday_ZT_rate        FLOAT                   COMMENT '昨日涨停股票的今日平均涨幅'
    ,UNIQUE KEY unique_ymd_name (ymd, name)
) COMMENT='当日大A行情温度(全量表)';

--1.3   下午跑
------------------  ods_stock_chouma_insight   A股的筹码分布数据
CREATE TABLE quant.ods_stock_chouma_insight (
    stock_code                               VARCHAR(50) NOT NULL     COMMENT '证券代码'
   ,ymd                                      DATE NOT NULL            COMMENT '交易日'
   ,exchange                                 VARCHAR(50)              COMMENT '交易所'
   ,close                                    FLOAT                    COMMENT '最新价格'
   ,prev_close                               FLOAT                    COMMENT '昨收价格'
   ,total_shares                             BIGINT                   COMMENT '总股本（股）'
   ,a_total_share                            BIGINT                   COMMENT 'A股总数(股)'
   ,a_listed_share                           BIGINT                   COMMENT '流通a股（万股）'
   ,listed_share                             BIGINT                   COMMENT '流通股总数'
   ,restricted_share                         BIGINT                   COMMENT '限售股总数'
   ,cost_5pct                                FLOAT                    COMMENT '5分位持仓成本（持仓成本最低的 5%的持仓成本）'
   ,cost_15pct                               FLOAT                    COMMENT '15分位持仓成本'
   ,cost_50pct                               FLOAT                    COMMENT '50分位持仓成本'
   ,cost_85pct                               FLOAT                    COMMENT '85分位持仓成本'
   ,cost_95pct                               FLOAT                    COMMENT '95分位持仓成本'
   ,avg_cost                                 FLOAT                    COMMENT '流通股加权平均持仓成本'
   ,max_cost                                 FLOAT                    COMMENT '流通股最大持仓成本'
   ,min_cost                                 FLOAT                    COMMENT '流通股最小持仓成本'
   ,winner_rate                              FLOAT                    COMMENT '流通股获利胜率'
   ,diversity                                FLOAT                    COMMENT '流通股筹码分散程度百分比'
   ,pre_winner_rate                          FLOAT                    COMMENT '流通股昨日获利胜率'
   ,restricted_avg_cost                      FLOAT                    COMMENT '限售股平均持仓成本'
   ,restricted_max_cost                      FLOAT                    COMMENT '限售股最大持仓成本'
   ,restricted_min_cost                      FLOAT                    COMMENT '限售股最小持仓成本'
   ,large_shareholders_avg_cost              FLOAT                    COMMENT '大流通股股东持股平均持仓成本'
   ,large_shareholders_total_share           FLOAT                    COMMENT '大流通股股东持股总数'
   ,large_shareholders_total_share_pct       FLOAT                    COMMENT '大流通股股东持股占总股本的比例'
   ,UNIQUE KEY unique_ymd_stock_code (ymd, stock_code)
) COMMENT='A股的筹码分布数据';

--1.4   下午跑
------------------  ods_astock_industry_overview   行业分类，申万三级分类
CREATE TABLE quant.ods_astock_industry_overview (
    ymd                       DATE                    COMMENT '交易日期'
   ,classified                varchar(100)            COMMENT '行业分类'
   ,industry_name             varchar(100)            COMMENT '行业名称'
   ,industry_code             varchar(100)            COMMENT '行业代码'
   ,l1_code                   varchar(100)            COMMENT '一级行业代码'
   ,l1_name                   varchar(100)            COMMENT '一级行业名称'
   ,l2_code                   varchar(100)            COMMENT '二级行业代码'
   ,l2_name                   varchar(100)            COMMENT '二级行业名称'
   ,l3_code                   varchar(100)            COMMENT '三级行业代码'
   ,l3_name                   varchar(100)            COMMENT '三级行业名称'
   ,UNIQUE KEY unique_industry_code (ymd, industry_code)
) COMMENT='行业分类，申万三级分类';

--1.5   下午跑
------------------  ods_astock_industry_detail   股票&行业的关联
CREATE TABLE quant.ods_astock_industry_detail (
    ymd                       DATE                    COMMENT '交易日期'
   ,stock_code                varchar(100)            COMMENT '股票代码'
   ,stock_name                varchar(50)             COMMENT '股票名称'
   ,industry_name             varchar(100)            COMMENT '行业名称'
   ,industry_code             varchar(100)            COMMENT '行业代码'
   ,l1_code                   varchar(100)            COMMENT '一级行业代码'
   ,l1_name                   varchar(100)            COMMENT '一级行业名称'
   ,l2_code                   varchar(100)            COMMENT '二级行业代码'
   ,l2_name                   varchar(100)            COMMENT '二级行业名称'
   ,l3_code                   varchar(100)            COMMENT '三级行业代码'
   ,l3_name                   varchar(100)            COMMENT '三级行业名称'
   ,UNIQUE KEY unique_stock_code (ymd, stock_code)
) COMMENT='股票&行业的关联';

--1.6   凌晨跑
------------------  ods_index_a_share_insight   大A的主要指数日K
CREATE TABLE quant.ods_index_a_share_insight (
     index_code               VARCHAR(50) NOT NULL    COMMENT '指数代码'
    ,index_name               VARCHAR(50) NOT NULL    COMMENT '指数名称'
    ,ymd                      DATE        NOT NULL    COMMENT '交易日期'
    ,open                     FLOAT                   COMMENT '开盘价'
    ,close                    FLOAT                   COMMENT '收盘价'
    ,high                     FLOAT                   COMMENT '最高价'
    ,low                      FLOAT                   COMMENT '最低价'
    ,volume                   BIGINT                  COMMENT '成交量'
    ,UNIQUE KEY unique_ymd_stock_code (ymd, index_code)
) COMMENT='大A的主要指数日K(全量表)';

--1.7   凌晨跑
------------------  ods_future_inside_insight   内盘主要期货数据日K
CREATE TABLE quant.ods_future_inside_insight (
     stock_code               VARCHAR(50) NOT NULL    COMMENT '期货标的代码'
    ,ymd                      DATE        NOT NULL    COMMENT '交易日期'
    ,open                     FLOAT                   COMMENT '开盘价'
    ,close                    FLOAT                   COMMENT '收盘价'
    ,high                     FLOAT                   COMMENT '最高价'
    ,low                      FLOAT                   COMMENT '最低价'
    ,volume                   BIGINT                  COMMENT '成交量'
    ,open_interest            BIGINT                  COMMENT '持仓量'
    ,settle                   BIGINT                  COMMENT '结算价'
    ,UNIQUE KEY unique_ymd_stock_code (ymd, stock_code)
) COMMENT='内盘主要期货数据日K(全量表)';

--1.8   凌晨跑
------------------  ods_shareholder_num   个股的股东数
CREATE TABLE `ods_shareholder_num` (
  `stock_code`      varchar(100)    CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci DEFAULT NULL COMMENT '股票代码',
  `stock_name`      varchar(50)     DEFAULT NULL            COMMENT '股票名称',
  `ymd`             date            DEFAULT NULL            COMMENT '交易日期',
  `total_sh`        double          DEFAULT NULL            COMMENT '总股东数',
  `avg_share`       double(10,4)    DEFAULT NULL            COMMENT '每个股东平均持股数',
  `pct_of_total_sh` double(10,4)    DEFAULT NULL            COMMENT '股东数较上期环比波动百分比',
  `pct_of_avg_sh`   double(10,4)    DEFAULT NULL            COMMENT '每个股东平均持股数较上期环比波动百分比',
  `load_time`       date            DEFAULT (curdate())     COMMENT '记录首次写入日期(yyyy-mm-dd)',
  `ann_date`        date            DEFAULT NULL            COMMENT '公告日期/可知日',
  UNIQUE KEY `unique_ymd_stock_code` (`ymd`,`stock_code`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci COMMENT='个股的股东数(历史表)';



-------------------------------------------   akshare 数据  ---------------------------------
--2.1   下午跑
------------------  ods_akshare_stock_yjkb_em   股票基本面数据_业绩快报数据    全量的每日切片数据 可选定日期
CREATE TABLE quant.ods_akshare_stock_yjkb_em (
     ymd                      DATE                    COMMENT '公告日期（核心日期维度）'
    ,serial_num               varchar(50)             COMMENT '序号'
    ,stock_code               varchar(50)             COMMENT '股票代码'
    ,stock_name               varchar(50)             COMMENT '股票简称'
    ,eps                      double                  COMMENT '每股收益'
    ,income                   double                  COMMENT '营业收入-营业收入'
    ,income_last_year         double                  COMMENT '营业收入-去年同期'
    ,income_yoy               varchar(50)             COMMENT '营业收入-同比增长'
    ,income_qoq               double                  COMMENT '营业收入-季度环比增长'
    ,profit                   double                  COMMENT '净利润-净利润'
    ,profit_last_year         double                  COMMENT '净利润-去年同期'
    ,profit_yoy               varchar(50)             COMMENT '净利润-同比增长'
    ,profit_qoq               double                  COMMENT '净利润-季度环比增长'
    ,asset_per_share          float                   COMMENT '每股净资产'
    ,roe                      double                  COMMENT '净资产收益率'
    ,industry                 varchar(100)            COMMENT '所处行业'
    ,UNIQUE KEY unique_ymd_stock_code (ymd, stock_code)
) COMMENT='股票基本面数据_业绩快报数据';

--2.2   下午跑
------------------  ods_akshare_stock_yjyg_em   股票基本面数据_业绩预告数据    全量的每日切片数据  可选定日期
CREATE TABLE quant.ods_akshare_stock_yjyg_em (
     ymd                      DATE                    COMMENT '公告日期'
    ,serial_num               varchar(50)             COMMENT '序号'
    ,stock_code               varchar(50)             COMMENT '股票代码'
    ,stock_name               varchar(50)             COMMENT '股票简称'
    ,forecast_index           varchar(200)            COMMENT '预测指标'
    ,performance_change       text                    COMMENT '业绩变动'
    ,forecast_value           double                  COMMENT '预测数值(元)'
    ,change_pct               double                  COMMENT '业绩变动幅度(%)'
    ,change_reason            text            COMMENT '业绩变动原因'
    ,forecast_type            varchar(50)             COMMENT '预告类型'
    ,last_year_value          double                  COMMENT '上年同期值(元)'
    ,UNIQUE KEY unique_ymd_stock_code (ymd, stock_code)
) COMMENT='股票基本面数据_业绩预告数据';

--2.3   下午跑
------------------  ods_akshare_stock_a_high_low_statistics   大盘情绪数据_大盘区间内的新低新高股票数  全量的每日切片数据 不可指定日期
CREATE TABLE quant.ods_akshare_stock_a_high_low_statistics (
     ymd                      DATE                    COMMENT '交易日'
    ,market                   varchar(50)             COMMENT '市场类型'
    ,close                    float                   COMMENT '相关指数收盘价'
    ,high20                   int                     COMMENT '20日新高'
    ,low20                    int                     COMMENT '20日新低'
    ,high60                   int                     COMMENT '60日新高'
    ,low60                    int                     COMMENT '60日新低'
    ,high120                  int                     COMMENT '120日新高'
    ,low120                   int                     COMMENT '120日新低'
    ,UNIQUE KEY unique_ymd_market (ymd, market)
) COMMENT='大盘情绪数据_大盘区间内的新低新高股票数';


--2.4   周末跑
------------------  ods_akshare_stock_value_em   股票基本面数据_估值数据              个股的全量历史数据   不可选定日期
CREATE TABLE `ods_akshare_stock_value_em` (
  `ymd`               date          DEFAULT NULL            COMMENT '数据日期',
  `stock_code`        varchar(50)   DEFAULT NULL            COMMENT '股票代码',
  `close`             float         DEFAULT NULL            COMMENT '当日收盘价(元)',
  `change_pct`        float         DEFAULT NULL            COMMENT '当日涨跌幅(%)',
  `total_market`      double        DEFAULT NULL            COMMENT '总市值(元)',
  `circulation_market` double       DEFAULT NULL            COMMENT '流通市值(元)',
  `total_shares`      double        DEFAULT NULL            COMMENT '总股本(股)',
  `circulation_shares` double       DEFAULT NULL            COMMENT '流通股本(股)',
  `pe_ttm`            float(12,2)   DEFAULT NULL            COMMENT 'PE(TTM)',
  `pe_static`         float(12,2)   DEFAULT NULL            COMMENT 'PE(静)',
  `pb`                float(12,2)   DEFAULT NULL            COMMENT '市净率',
  `peg`               float(12,2)   DEFAULT NULL            COMMENT 'PEG值',
  `pcf`               float(12,2)   DEFAULT NULL            COMMENT '市现率',
  `ps`                float(12,2)   DEFAULT NULL            COMMENT '市销率',
  `stock_code_pure`   varchar(50)   GENERATED ALWAYS AS (`stock_code`) VIRTUAL,
  UNIQUE KEY `unique_ymd_stock_code` (`ymd`,`stock_code`),
  KEY `idx_ymd` (`ymd`),
  KEY `idx_ymd_pure` (`ymd`,`stock_code_pure`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci COMMENT='股票基本面数据_估值数据'


--2.5   周末跑
------------------  ods_akshare_stock_zh_a_gdhs_detail_em   股票基本面数据_股东数据    个股的全量历史数据   不可选定日期
CREATE TABLE quant.ods_akshare_stock_zh_a_gdhs_detail_em (
     ymd                      DATE                    COMMENT '股东户数统计截止日（对应核心日期维度）'
    ,stock_code               varchar(50)             COMMENT '股票代码'
    ,stock_name               varchar(50)             COMMENT '股票名称'
    ,range_change_pct         float                   COMMENT '区间涨跌幅(%)'
    ,holder_num_current       bigint                  COMMENT '股东户数-本次'
    ,holder_num_last          bigint                  COMMENT '股东户数-上次'
    ,holder_num_change        bigint                  COMMENT '股东户数-增减'
    ,holder_num_change_pct    float                   COMMENT '股东户数-增减比例(%)'
    ,avg_holder_market        double                  COMMENT '户均持股市值'
    ,avg_holder_share_num     float                   COMMENT '户均持股数量'
    ,total_market             double                  COMMENT '总市值'
    ,total_shares             bigint                  COMMENT '总股本'
    ,share_change             bigint                  COMMENT '股本变动'
    ,share_change_reason      varchar(255)            COMMENT '股本变动原因'
    ,holder_num_announce_date DATE                    COMMENT '股东户数公告日期'
    ,UNIQUE KEY unique_ymd_stock_code (ymd, stock_code)
) COMMENT='股票基本面数据_股东数据';


-------------------------------------------   tushare 数据  ---------------------------------
--3.1   下午跑
------------------  ods_stock_kline_daily_ts   行情数据_A股历史日K线的tushare数据
CREATE TABLE `ods_stock_kline_daily_ts` (
  `stock_code`      varchar(50)     NOT NULL                COMMENT '股票代码',
  `ymd`             date            NOT NULL                COMMENT '交易日期',
  `open`            float           DEFAULT NULL            COMMENT '开盘价',
  `close`           float           DEFAULT NULL            COMMENT '收盘价',
  `high`            float           DEFAULT NULL            COMMENT '最高价',
  `low`             float           DEFAULT NULL            COMMENT '最低价',
  `change_pct`      float           DEFAULT NULL            COMMENT '当日涨跌幅(%)',
  `volume`          bigint          DEFAULT NULL            COMMENT '成交量',
  `trading_amount`  double          DEFAULT NULL            COMMENT '成交额',
  `today_pct`       decimal(10,2)   DEFAULT NULL            COMMENT '当日涨跌幅(%), (close-open)/open*100',
  `is_down`         tinyint(1)      DEFAULT NULL            COMMENT '是否阴线, 1:是(close<open), 0:否(close>=open)',
  `stock_code_pure` varchar(50)     GENERATED ALWAYS AS (substring_index(`stock_code`,_utf8mb4'.',1)) VIRTUAL,
  UNIQUE KEY `unique_ymd_stock_code` (`ymd`,`stock_code`),
  KEY `idx_ymd_pure` (`ymd`,`stock_code_pure`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci COMMENT='行情数据_A股历史日K线的tushare数据'


--3.2   下午跑
------------------  ods_tushare_board_concept_name_ths   同花顺板块指数列表 (ths_index 接口)
CREATE TABLE quant.ods_tushare_board_concept_name_ths (
     ymd                      DATE                    COMMENT '数据日期（核心日期维度，适配量化数据统一归档）'
    ,board_name               varchar(100)            COMMENT '板块名称'
    ,board_code               varchar(50)             COMMENT '板块代码'
    ,component_count          int                     COMMENT '成分个数'
    ,market                   varchar(10)             COMMENT '交易所（A-沪深 HK-港股 US-美股）'
    ,list_date                DATE                    COMMENT '上市日期YYYYMMDD'
    ,index_type               varchar(10)             COMMENT '指数类型（N-概念 I-行业 R-地域 S-特色 ST-风格 TH-主题 BB-宽基）'
    ,UNIQUE KEY unique_ymd_board_code (ymd, board_code)
) COMMENT='板块数据_同花顺板块码值_Tushare';

--3.3   下午跑
------------------  ods_tushare_stock_board_concept_index_ths   同花顺板块历史行情数据 (ths_daily 接口)
CREATE TABLE quant.ods_tushare_stock_board_concept_index_ths (
     ymd                      DATE                    COMMENT '日期（行情交易日）'
    ,board_name               varchar(100)            COMMENT '板块名称'
    ,board_code               varchar(50)             COMMENT '板块代码'
    ,open                     float                   COMMENT '开盘点位'
    ,high                     float                   COMMENT '最高点位'
    ,low                      float                   COMMENT '最低点位'
    ,close                    float                   COMMENT '收盘点位'
    ,trading_volume           float                   COMMENT '成交量'
    ,trading_amount           float                   COMMENT '成交额'
    ,UNIQUE KEY unique_ymd_board_code (ymd, board_code)
) COMMENT='行情数据_同花顺板块历史行情_Tushare';

--3.4   下午跑
------------------  ods_tushare_stock_board_concept_maps_ths   同花顺板块内含股票 (ths_member 接口)
CREATE TABLE quant.ods_tushare_stock_board_concept_maps_ths (
     ymd                      DATE                    COMMENT '数据日期（核心日期维度，用于归档和跨表关联）'
    ,board_name               varchar(100)            COMMENT '板块名称'
    ,board_code               varchar(50)             COMMENT '板块代码'
    ,stock_code               varchar(50)             COMMENT '股票代码'
    ,stock_name               varchar(100)            COMMENT '股票名称'
    ,weight                   float                   COMMENT '权重'
    ,in_date                  varchar(8)              COMMENT '纳入日期YYYYMMDD'
    ,out_date                 varchar(8)              COMMENT '剔除日期YYYYMMDD'
    ,is_new                   varchar(1)              COMMENT '是否最新(Y/N)'
    ,UNIQUE KEY unique_ymd_board_stock (ymd, board_code, stock_code)
) COMMENT='行情数据_同花顺板块内含股票_Tushare';


-------------------------------------------   vantage 数据  ---------------------------------
--5.1
------------------  ods_us_stock_daily_vantage   美股 日K
CREATE TABLE quant.ods_us_stock_daily_vantage (
     stock_name               VARCHAR(50) NOT NULL    COMMENT '股票名称'
    ,ymd                      DATE        NOT NULL    COMMENT '交易日期'
    ,open                     FLOAT                   COMMENT '开盘价'
    ,high                     FLOAT                   COMMENT '最高价'
    ,low                      FLOAT                   COMMENT '最低价'
    ,close                    FLOAT                   COMMENT '收盘价'
    ,volume                   BIGINT                  COMMENT '成交量'
    ,UNIQUE KEY unique_ymd_name (ymd, stock_name)
) COMMENT='美股 日K';

--5.2
------------------  ods_exchange_rate_vantage_detail   汇率&美元指数 日K
CREATE TABLE quant.ods_exchange_rate_vantage_detail (
     stock_name               VARCHAR(50) NOT NULL    COMMENT '货币对'
    ,ymd                      DATE        NOT NULL    COMMENT '交易日期'
    ,open                     FLOAT                   COMMENT '开盘价'
    ,high                     FLOAT                   COMMENT '最高价'
    ,low                      FLOAT                   COMMENT '最低价'
    ,close                    FLOAT                   COMMENT '收盘价'
    ,UNIQUE KEY unique_ymd_name (ymd, stock_name)
) COMMENT='汇率&美元指数 日K';

--5.3
------------------  ods_exchange_dxy_vantage   美元指数 日K
CREATE TABLE quant.ods_exchange_dxy_vantage (
    ymd                       DATE        NOT NULL    COMMENT '交易日期'
   ,stock_name                VARCHAR(50) NOT NULL    COMMENT '货币对'
   ,UNIQUE KEY unique_ymd_name (ymd, stock_name)
) COMMENT='美元指数 日K';


-------------------------------------------   其他  ---------------------------------
--6.1        多渠道板块数据 -- 小红书
------------------  ods_stock_plate_redbook
CREATE TABLE quant.ods_stock_plate_redbook (
     ymd                      DATE        NOT NULL    COMMENT '日期'
    ,board_name               VARCHAR(50) NOT NULL    COMMENT '板块名称'
    ,stock_code               VARCHAR(50)             COMMENT '标的代码'
    ,stock_name               VARCHAR(50)             COMMENT '标的名称'
    ,remark                   VARCHAR(50)             COMMENT '备注'
) COMMENT='多渠道板块数据 -- 小红书';

--6.2        股票基本面数据_所属交易所，主板/创业板/科创板/北证
------------------  ods_stock_exchange_market
CREATE TABLE quant.ods_stock_exchange_market (
     ymd                      DATE        NOT NULL    COMMENT '日期'
    ,stock_code               VARCHAR(50)             COMMENT '标的代码'
    ,stock_name               VARCHAR(50)             COMMENT '标的名称'
    ,market                   VARCHAR(50)             COMMENT '市场特征主板创业板等'
    ,UNIQUE KEY unique_ymd_stock_code (ymd, stock_code)
) COMMENT='股票基本面数据_所属交易所，主板/创业板/科创板/北证';


```

--------------------------------------------------------------------------------
## datas_prepare\C00_SQL\__init__.py

```python

```

--------------------------------------------------------------------------------
## strategy\__init__.py

```python
from .factor_library import FactorLibrary

__all__ = ['FactorLibrary']
```

--------------------------------------------------------------------------------
## strategy\factor_library.py

```python
# strategy/factor_library.py
import pandas as pd
import numpy as np
import logging
import math
from CommonProperties import Mysql_Utils
from CommonProperties.DateUtility import DateUtility
from CommonProperties.Base_utils import timing_decorator, script_run
from CommonProperties import set_config
logger = logging.getLogger(__name__)
set_config.setup_logging_config()

class FactorLibrary:
    """因子计算库：改为百分制输出"""

    def __init__(self):
        self.user = Mysql_Utils.origin_user
        self.password = Mysql_Utils.origin_password
        self.host = Mysql_Utils.origin_host
        self.database = Mysql_Utils.origin_database
        # 简单缓存，不强制依赖
        self.cached_factors = {}
        # 获取全量股票列表（stock_code, stock_name）
        self.stocks_df = Mysql_Utils.get_stock_codes_latest()


    def get_trading_days(self, start_date, end_date):
        """获取交易日列表"""
        try:
            trading_days_df = Mysql_Utils.data_from_mysql_to_dataframe(
                user=self.user,
                password=self.password,
                host=self.host,
                database=self.database,
                table_name='ods_trading_days_insight',
                start_date=start_date,
                end_date=end_date,
                cols=['ymd']
            )

            if trading_days_df.empty:
                return []

            # 直接返回排序后的日期列表
            trading_days = sorted(trading_days_df['ymd'].unique())
            return [d.strftime('%Y%m%d') for d in trading_days]

        except Exception as e:
            logger.error(f"获取交易日失败：{str(e)}")
            return []


    @timing_decorator
    def pb_factor_score(self, start_date, end_date, reverse=True, save_to_cache=True):
        """
        计算 PB 因子百分制评分
        source_table: dwd_ashare_stock_base_info
        简化版：对于 pb >0 的股票，根据每日截面计算 pb 排名。
        reverse为 True（默认）, pb升序排名, pb越小, 排名值越小, 打分越高（有效数据得分[1, 100]，无效数据得分0）
        reverse为 False, pb降序排名, pb越大, 排名越小, 打分越高（有效数据得分[1, 100]，无效数据得分0）
        """
        try:
            # 获取PB数据
            pb_df = Mysql_Utils.data_from_mysql_to_dataframe(
                user=self.user,
                password=self.password,
                host=self.host,
                database=self.database,
                table_name='dwd_ashare_stock_base_info',
                start_date=start_date,
                end_date=end_date,
                cols=['ymd', 'stock_code', 'pb']
            )

            if pb_df.empty:
                return pd.DataFrame(columns=['ymd', 'stock_code', 'pb_score'])

            # 转换数值，无效值变为NaN
            pb_df['pb'] = pd.to_numeric(pb_df['pb'], errors='coerce')
            # 新增：pb<=0 视为无效（0 是 dwd 层缺失填充值，负 PB 是净资产为负的困难企业）
            # 之后Pe也要这么处理，pe<=视为无效
            pb_df['pb'] = pb_df['pb'].mask(pb_df['pb'] <= 0)

            # 按日期分组计算排名
            result_dfs = []

            for date, date_df in pb_df.groupby('ymd'):
                date_df = date_df.copy()

                # 获取有效数据的数量: valid_count
                valid_mask = date_df['pb'].notna()
                valid_count = valid_mask.sum()

                if valid_count > 0:
                    # 对有效数据计算百分制得分
                    valid_data = date_df.loc[valid_mask].copy()

                    if reverse:
                        # PB越低分越高（价值因子）
                        valid_data['pb_rank'] = valid_data['pb'].rank(method='min', ascending=True)
                    else:
                        valid_data['pb_rank'] = valid_data['pb'].rank(method='min', ascending=False)

                    max_rank = valid_data['pb_rank'].max()
                    valid_data['pb_score'] = (100.0 if max_rank == 1 else (
                                1 + 99 * (max_rank - valid_data['pb_rank']) / (max_rank - 1)).round(2))
                    # 将得分合并回原数据框
                    for idx in valid_data.index:
                        date_df.loc[idx, 'pb_score'] = valid_data.loc[idx, 'pb_score']

                # 无效数据给0分（简单处理）
                date_df.loc[~valid_mask, 'pb_score'] = 0.0

                result_dfs.append(date_df[['ymd', 'stock_code', 'pb_score']])

            result_df = pd.concat(result_dfs, ignore_index=True)
            logger.info(f"PB因子百分制计算完成：共{len(result_df)}条记录")

            # 保存到缓存
            if save_to_cache:
                self.cached_factors['pb'] = result_df.copy()

            return result_df

        except Exception as e:
            logger.error(f"计算PB因子失败：{str(e)}")
            return pd.DataFrame(columns=['ymd', 'stock_code', 'pb_score'])


    @timing_decorator
    def zt_factor_score(self, start_date, end_date, lookback_days=10,
                        type_weight=None, decay='exp', half_life=5,
                        streak_bonus=5, max_streak=5,
                        reverse=False, save_to_cache=True):
        """
        计算涨停因子百分制评分
        source_table: dwd_stock_zt_list
        逻辑：统计每只股票过去 lookback_days 个交易日内的涨停强度 raw_score，由三部分构成：
            1) 【涨停类型】：一字板 yizi=1.0 / T字板 tzi=0.9 / 实体板 shiti=0.8 / 炸板 zha=0.2
               （封板质量越高分越高；炸板为弱势涨停，大幅弱化但仍保留少量动量信息）；
            2) 【时间衰减】：距今越近权重越高。decay='exp' 指数衰减 exp(-age/half_life)，
               其中，age 指某次涨停事件距离"当前打分日"隔了几个交易日,
               half_life 半衰期,影响力衰减到原来一半所需要的交易日数。
               decay='linear' 线性衰减 (lookback_days-age)/lookback_days，
               decay='none' 不衰减；
            3) 【连板加成】：截至当日连续封板天数 streak（炸板打断连板），首板无加成，每多一板加 streak_bonus 分，封顶 max_streak 板
               （连板高度是短线最强信号，边际递增）。
                连板加成 = (连板高度 - 1) × streak_bonus，封顶 max_streak
                streak_bonus：每一板值多少分
                max_streak：连板加成的封顶板数（防止极端值垄断）
        raw_score(t) = Σ [ 涨停类型权重 × 时间衰减权重 ]   ← 对窗口内每次涨停求和
                       + 连板加成                       ← 独立相加
        每日截面上对 raw_score 排名映射到 [1,100]：
            reverse=False（默认）：raw_score 越大，排名越靠前，得分越高；
            reverse=True：反向打分（用于观察涨停后的反转/回落效应）。
        异常值处理：
            - zt_type 缺失的涨停记录按实体板 shiti=0.8 计；
            - 区间内无任何涨停记录的股票得分 0（与 pb_score 的无效=0 口径一致）；
            - 仅炸板(zha)无封板的股票 raw_score 很低，截面排名自然靠后。
        股票池：以 ods_stock_code_daily_insight（每日已上市股票码表）为全量底表，
            窗口内无涨停记录的股票 raw_score 补 0，保证输出覆盖全市场，
            与 pb_score 等因子的股票池口径一致，便于合成。
        """
        try:
            if type_weight is None:
                type_weight = {'yizi': 1.0, 'tzi': 0.9, 'shiti': 0.8, 'zha': 0.2}

            # ========== 1. 交易日与目标区间 ==========
            query_start = (pd.to_datetime(start_date)
                           - pd.Timedelta(days=lookback_days * 2 + 20)).strftime('%Y%m%d')
            all_days = self.get_trading_days(query_start, end_date)
            if not all_days:
                return pd.DataFrame(columns=['ymd', 'stock_code', 'zt_score'])

            start_idx = next((i for i, d in enumerate(all_days) if d >= start_date), None)
            end_idx = next((i for i in range(len(all_days) - 1, -1, -1) if all_days[i] <= end_date), None)
            if start_idx is None or end_idx is None:
                return pd.DataFrame(columns=['ymd', 'stock_code', 'zt_score'])

            target_days = all_days[start_idx:end_idx + 1]
            # 连板计算需要额外回溯（连板最长可能超过 lookback_days）
            earliest_idx = max(0, start_idx - lookback_days - max_streak)
            query_start_zt = all_days[earliest_idx]

            # ========== 2. 读取涨停数据 ==========
            zt_df = Mysql_Utils.data_from_mysql_to_dataframe(
                user=self.user, password=self.password, host=self.host, database=self.database,
                table_name='dwd_stock_zt_list',
                start_date=query_start_zt, end_date=end_date,
                cols=['ymd', 'stock_code', 'zt_type']
            )
            if zt_df.empty:
                return pd.DataFrame(columns=['ymd', 'stock_code', 'zt_score'])

            zt_df['ymd'] = pd.to_datetime(zt_df['ymd']).dt.strftime('%Y%m%d')
            # zt_type 缺失按实体板计
            zt_df['type_w'] = zt_df['zt_type'].map(type_weight).fillna(type_weight['shiti'])
            # 封板标记：炸板不算封板（用于连板计算，炸板会打断连板）
            zt_df['is_sealed'] = (zt_df['zt_type'] != 'zha').astype(int)

            # ========== 3. 构建事件矩阵（行=交易日，列=股票） ==========
            event = zt_df.pivot_table(index='ymd', columns='stock_code',
                                      values='type_w', aggfunc='max')
            event = event.reindex(index=all_days).fillna(0)

            sealed = zt_df.pivot_table(index='ymd', columns='stock_code',
                                       values='is_sealed', aggfunc='max')
            sealed = sealed.reindex(index=all_days).fillna(0)

            # ========== 4. 时间衰减加权的涨停强度 ==========
            lags = np.arange(lookback_days)
            if decay == 'exp':
                decay_w = np.exp(-lags / half_life)
            elif decay == 'linear':
                decay_w = (lookback_days - lags) / lookback_days
            else:  # 'none'
                decay_w = np.ones(lookback_days)

            # raw = Σ_lag 事件权重(lag天前) × 衰减(lag)，向量化实现
            raw = pd.DataFrame(0.0, index=all_days, columns=event.columns)
            for lag in range(lookback_days):
                raw = raw.add(event.shift(lag).fillna(0) * decay_w[lag], fill_value=0)

            # ========== 5. 连板加成 ==========
            def _streak(s):
                # 截至当日的连续封板天数：0/1 序列上的连续1计数
                return s * s.groupby((s == 0).cumsum()).cumsum()

            streak = sealed.apply(_streak, axis=0)
            # 首板不加成，每多一板 +streak_bonus，封顶 max_streak 板
            raw = raw + (streak - 1).clip(lower=0).clip(upper=max_streak - 1) * streak_bonus

            # ========== 6. 截取目标区间并转长格式 ==========
            target_raw = raw.loc[target_days]
            long_df = (target_raw.rename_axis('ymd')
                       .reset_index()
                       .melt(id_vars='ymd', var_name='stock_code', value_name='raw_score'))

            # ========== 6.1 取全量股票底表（每日已上市股票） ==========
            universe_df = Mysql_Utils.data_from_mysql_to_dataframe(
                user=self.user, password=self.password, host=self.host, database=self.database,
                table_name='ods_stock_code_daily_insight',
                start_date=target_days[0], end_date=target_days[-1],
                cols=['ymd', 'stock_code']
            )
            if universe_df.empty:
                # 码表不可用时退化为只输出有涨停记录的股票
                logger.warning("ods_stock_code_daily_insight 无数据，因子仅覆盖窗口内有涨停记录的股票")
            else:
                universe_df['ymd'] = pd.to_datetime(universe_df['ymd']).dt.strftime('%Y%m%d')
                universe_df = universe_df[['ymd', 'stock_code']].drop_duplicates()
                # 左连接到底表：窗口内无涨停记录的股票 raw_score 补 0
                long_df = universe_df.merge(long_df, on=['ymd', 'stock_code'], how='left')
                long_df['raw_score'] = long_df['raw_score'].fillna(0)

            # ========== 7. 每日截面排名 → [1,100]，无涨停记录=0 ==========
            result_dfs = []
            for date, date_df in long_df.groupby('ymd'):
                date_df = date_df.copy()
                valid_mask = date_df['raw_score'] > 0
                valid_count = valid_mask.sum()

                if valid_count > 0:
                    valid = date_df.loc[valid_mask].copy()
                    # rank 升序：raw 越大 rank 越大
                    valid['raw_rank'] = valid['raw_score'].rank(method='min', ascending=True)
                    max_rank = valid['raw_rank'].max()

                    if max_rank == 1:
                        valid['zt_score'] = 100.0
                    elif reverse:
                        # 反向：raw 越大得分越低
                        valid['zt_score'] = (1 + 99 * (max_rank - valid['raw_rank'])
                                             / (max_rank - 1)).round(2)
                    else:
                        # 正向：raw 越大得分越高
                        valid['zt_score'] = (1 + 99 * (valid['raw_rank'] - 1)
                                             / (max_rank - 1)).round(2)

                    date_df.loc[valid.index, 'zt_score'] = valid['zt_score']

                date_df.loc[~valid_mask, 'zt_score'] = 0.0
                result_dfs.append(date_df[['ymd', 'stock_code', 'zt_score']])

            result_df = pd.concat(result_dfs, ignore_index=True)
            logger.info(f"涨停因子百分制计算完成：共{len(result_df)}条记录")

            if save_to_cache:
                self.cached_factors['zt'] = result_df.copy()

            return result_df

        except Exception as e:
            logger.error(f"计算涨停因子失败：{str(e)}")
            import traceback
            logger.error(traceback.format_exc())
            return pd.DataFrame(columns=['ymd', 'stock_code', 'zt_score'])


    @timing_decorator
    def shareholder_factor_score(self, start_date, end_date, save_to_cache=True):
        """
        计算股东户数因子百分制评分（0-100分）
        source_table: dwd_shareholder_num_daily
        评分逻辑（三阶段）：
        1) 【截面分位基础分】：每日在全部股票内，对 latest_qoq_sh
           （季度对齐环比%）升序排名——股东数降幅越大排名越靠前——映射到 [1,100]。
           截面排名而非固定 sigmoid，保证不同交易日的得分分布可比（与 pb_score 口径一致）；
        2) 【新鲜度衰减】：score × (0.5 + 0.5 × fresh_weight)
           fresh_weight = exp(-days_since_load/10)，刚披露权重 1.0，之后指数衰减，
           陈旧数据保底 50%（季度数据本身时效性有限，不清零）；
        3) 【边际加速】：latest_mom_sh < 0（季内最新一次披露仍在减少）→ 得分 × 1.1，
           体现"筹码持续集中"的确认信号。
        shareholder_score(t) = ( 1 + 99 × (max_rank − qoq_rank) / (max_rank − 1) )  ← 第1阶段：截面分位基础分
                               × ( 0.5 + 0.5 × exp(−days_since_load / 10) )         ← 第2阶段：新鲜度衰减（保底0.5）
                               × 1.1（若 latest_mom_sh < 0，否则 × 1.0）              ← 第3阶段：边际加速
                               全程封顶 100
        其中：qoq_rank = 当日截面内 latest_qoq_sh 升序排名（降幅最大者 rank=1）；
              max_rank = 当日参与排名的股票总数。
        股票池（重要）：全市场覆盖。dwd_shareholder_num_daily 是每日对全量股票的
           前向填充快照（5000+ 只，绝大多数股票的值为最近一次披露，陈旧度由
           days_since_load / fresh_weight 刻画）；仅过滤 latest_qoq_sh 为 NULL 的股票
           （首次披露、无季度基期，align_flag='first'），这类股票当日不参与打分。
           与其他因子（pb/zt）输出同为全市场口径，可直接按 (ymd, stock_code) 合成。
        数据特性提示：股东数披露随财报季聚集（4月底/8月底/10月底为高峰），
           因子信号强度呈脉冲式，回测时建议按月份分层观察 IC。
        异常值处理：
           - latest_qoq_sh 缺失的股票跳过，不出现在输出中；
           - 加速系数作用后封顶 100 分。
        """
        try:
            # 获取股东数据（新宽表：每日前向填充 + 新鲜度特征）
            shareholder_df = Mysql_Utils.data_from_mysql_to_dataframe(
                user=self.user,
                password=self.password,
                host=self.host,
                database=self.database,
                table_name='dwd_shareholder_num_daily',
                start_date=start_date,
                end_date=end_date,
                cols=['ymd', 'stock_code', 'latest_qoq_sh', 'latest_mom_sh',
                      'days_since_load', 'fresh_weight']
            )

            if shareholder_df.empty:
                result_df = pd.DataFrame(columns=['ymd', 'stock_code', 'shareholder_score'])
            else:
                # 转换数值
                for col in ['latest_qoq_sh', 'latest_mom_sh', 'days_since_load', 'fresh_weight']:
                    shareholder_df[col] = pd.to_numeric(shareholder_df[col], errors='coerce')

                # 仅过滤无有效环比的股票（首次披露、无季度基期），其余全量参与
                shareholder_df = shareholder_df[shareholder_df['latest_qoq_sh'].notna()].copy()

                # ========== 1. 截面分位基础分 [1,100] ==========
                # qoq 越小（户数降幅越大）→ rank 越靠前 → 得分越高
                shareholder_df['qoq_rank'] = shareholder_df.groupby('ymd')['latest_qoq_sh'].rank(
                    method='min', ascending=True)
                shareholder_df['max_rank'] = shareholder_df.groupby('ymd')['qoq_rank'].transform('max')

                shareholder_df['base_score'] = 100.0
                mask_multi = shareholder_df['max_rank'] > 1
                # rank=1（户数降幅最大）→ 100 分；rank=max（户数增加最多）→ 1 分
                shareholder_df.loc[mask_multi, 'base_score'] = (
                        1 + 99 * (shareholder_df.loc[mask_multi, 'max_rank']
                                  - shareholder_df.loc[mask_multi, 'qoq_rank'])
                        / (shareholder_df.loc[mask_multi, 'max_rank'] - 1)
                ).round(2)

                # ========== 2. 新鲜度衰减系数：0.5 ~ 1.0 ==========
                shareholder_df['fresh_coef'] = (
                        0.5 + 0.5 * shareholder_df['fresh_weight'].fillna(0)
                ).clip(upper=1.0)

                shareholder_df['shareholder_score'] = (
                        shareholder_df['base_score'] * shareholder_df['fresh_coef']
                )

                # ========== 3. 边际加速：季内最新披露仍在减少 → ×1.1 ==========
                mask_accel = shareholder_df['latest_mom_sh'] < 0
                shareholder_df.loc[mask_accel, 'shareholder_score'] = (
                        shareholder_df.loc[mask_accel, 'shareholder_score'] * 1.1
                ).clip(upper=100)

                shareholder_df['shareholder_score'] = shareholder_df['shareholder_score'].round(2)

                result_df = shareholder_df[['ymd', 'stock_code', 'shareholder_score']]

            logger.info(f"股东人数因子计算完成：共{len(result_df)}条记录")

            if save_to_cache:
                self.cached_factors['shareholder'] = result_df.copy()

            return result_df

        except Exception as e:
            logger.error(f"计算股东数因子失败：{str(e)}")
            return pd.DataFrame(columns=['ymd', 'stock_code', 'shareholder_score'])


    @timing_decorator
    def volume_shrink_factor(self, start_date, end_date, baseline='ma20',
                             min_streak=3, max_streak=5,
                             reverse=False, save_to_cache=True):
        """
        计算连续缩量因子百分制评分（0-100分）
        source_table: dwd_stock_technical_indicators
        定义：连续缩量 = 截至今日，连续 min_streak 个交易日以上，每日成交量均低于均量基准线；
              窗口内一旦某天放量（超均线），连续计数立刻清零重新积累。
        计算公式：
            shortfall(t) = max(0, -volume_vs_maXX(t))        ← 当日低于均量的幅度(%)，未低于记 0
            S(t)         = 截至今日连续低于均量的天数（放量日清零，封顶 max_streak）
            raw(t)       = Σ_{lag=0}^{S(t)-1} shortfall(t-lag)  ← 只累计"当前连续段"的缩量深度
            raw(t)       = 0，若 S(t) < min_streak               ← 新一段不足 min_streak 天判 0
            raw(t) 的物理含义：当前连缩段内"累计低于均量的百分比总和"，单位是"百分点·天"。
            raw(t) 把两个维度压成一个数：连得久（S 大）和缩得狠（shortfall 大）都会推高 raw
        参数：
            baseline: 均量基准线，'ma5'（最松）/ 'ma20'（推荐）。
                      注意：偏离度只有 ma5/ma20 预计算，ma60 不可用；
            min_streak: 连续缩量的最少天数，默认 3（放量打断后重新积累不足此天数 → 0 分）；
            max_streak: 连续缩量累计封顶天数，默认 5（更早的缩量不再额外加分；
                        想不清封顶可传大值如 20）；
        每日截面上对 raw_score 排名映射到 [1,100]：
            raw=0（未达 min_streak）得 0 分；raw 越大（缩量越深、连续越久）得分越高；
            reverse=True 反向。
        示例（每天缩量幅度 5%，min_streak=3）：
            5天全缩        → S=5, raw=25，高分
            缩缩放缩缩     → S=2, raw=0（第3天放量清零，新段不足3天）
            5天全缩后又缩2天 → S=5(封顶), raw=最近5天累计，高分
        异常值处理：
            - 基准均线为 NULL（上市初期）视为未低于均量（不打断也不加分）；
            - 停牌日无数据，视为段外（打断连续），属合理处理。
        """
        try:
            vs_col = {'ma5': 'volume_vs_ma5', 'ma20': 'volume_vs_ma20'}.get(baseline)
            if vs_col is None:
                raise ValueError(f"baseline 只支持 'ma5'/'ma20'（偏离度未预计算 ma60），收到: {baseline}")

            # ========== 1. 交易日与目标区间 ==========
            query_start = (pd.to_datetime(start_date)
                           - pd.Timedelta(days=max_streak * 2 + 20)).strftime('%Y%m%d')
            all_days = self.get_trading_days(query_start, end_date)
            if not all_days:
                return pd.DataFrame(columns=['ymd', 'stock_code', 'volume_shrink_score'])

            start_idx = next((i for i, d in enumerate(all_days) if d >= start_date), None)
            end_idx = next((i for i in range(len(all_days) - 1, -1, -1) if all_days[i] <= end_date), None)
            if start_idx is None or end_idx is None:
                return pd.DataFrame(columns=['ymd', 'stock_code', 'volume_shrink_score'])

            target_days = all_days[start_idx:end_idx + 1]
            # 深度累计最多回溯 max_streak 天
            earliest_idx = max(0, start_idx - max_streak)
            query_start_data = all_days[earliest_idx]

            # ========== 2. 读取偏离度数据 ==========
            tech_df = Mysql_Utils.data_from_mysql_to_dataframe(
                user=self.user, password=self.password, host=self.host, database=self.database,
                table_name='dwd_stock_technical_indicators',
                start_date=query_start_data, end_date=end_date,
                cols=['ymd', 'stock_code', vs_col]
            )
            if tech_df.empty:
                return pd.DataFrame(columns=['ymd', 'stock_code', 'volume_shrink_score'])

            tech_df['ymd'] = pd.to_datetime(tech_df['ymd']).dt.strftime('%Y%m%d')
            # 低于均量的幅度：vs<0 时取 -vs（正数），否则 0；NULL 视为 0
            tech_df['shortfall'] = (-tech_df[vs_col]).clip(lower=0).fillna(0)
            # 当日是否在均量下方（NULL 计为否，不打断连续也不加分）
            tech_df['is_below'] = (tech_df[vs_col] < 0).fillna(False).astype(float)

            # ========== 3. 矩阵化：连续计数 S + 分段深度累计 ==========
            short_m = tech_df.pivot_table(index='ymd', columns='stock_code',
                                          values='shortfall', aggfunc='max')
            short_m = short_m.reindex(index=all_days).fillna(0)
            below_m = tech_df.pivot_table(index='ymd', columns='stock_code',
                                          values='is_below', aggfunc='max')
            below_m = below_m.reindex(index=all_days).fillna(0)

            # S：连续低于均量的天数（放量日 below=0 → 连续计数清零），封顶 max_streak
            S = below_m.apply(lambda s: s * s.groupby((s == 0).cumsum()).cumsum(), axis=0)
            S = S.clip(upper=max_streak)

            # 分段深度：只累加"当前连续段"内每天的 shortfall
            # shortfall(t-lag) 计入 raw(t) 当且仅当 lag < S(t)（t-lag 还在当前段内）
            raw = pd.DataFrame(0.0, index=all_days, columns=short_m.columns)
            for lag in range(max_streak):
                raw = raw.add(short_m.shift(lag).fillna(0) * (S > lag), fill_value=0)

            # 连续段不足 min_streak 天 → 0 分
            raw = raw.where(S >= min_streak, 0.0)

            # ========== 4. 截目标区间、转长格式、补全市场 ==========
            target_raw = raw.loc[target_days]
            long_df = (target_raw.rename_axis('ymd')
                       .reset_index()
                       .melt(id_vars='ymd', var_name='stock_code', value_name='raw_score'))

            universe_df = Mysql_Utils.data_from_mysql_to_dataframe(
                user=self.user, password=self.password, host=self.host, database=self.database,
                table_name='ods_stock_code_daily_insight',
                start_date=target_days[0], end_date=target_days[-1],
                cols=['ymd', 'stock_code']
            )
            if universe_df.empty:
                logger.warning("ods_stock_code_daily_insight 无数据，因子仅覆盖窗口内有记录的股票")
            else:
                universe_df['ymd'] = pd.to_datetime(universe_df['ymd']).dt.strftime('%Y%m%d')
                universe_df = universe_df[['ymd', 'stock_code']].drop_duplicates()
                long_df = universe_df.merge(long_df, on=['ymd', 'stock_code'], how='left')
                long_df['raw_score'] = long_df['raw_score'].fillna(0)

            # ========== 5. 每日截面排名 → [1,100]，raw=0 得 0 分 ==========
            result_dfs = []
            for date, date_df in long_df.groupby('ymd'):
                date_df = date_df.copy()
                valid_mask = date_df['raw_score'] > 0
                if valid_mask.sum() > 0:
                    valid = date_df.loc[valid_mask].copy()
                    valid['raw_rank'] = valid['raw_score'].rank(method='min', ascending=True)
                    max_rank = valid['raw_rank'].max()
                    if max_rank == 1:
                        valid['volume_shrink_score'] = 100.0
                    elif reverse:
                        valid['volume_shrink_score'] = (1 + 99 * (max_rank - valid['raw_rank'])
                                                        / (max_rank - 1)).round(2)
                    else:
                        valid['volume_shrink_score'] = (1 + 99 * (valid['raw_rank'] - 1)
                                                        / (max_rank - 1)).round(2)
                    date_df.loc[valid.index, 'volume_shrink_score'] = valid['volume_shrink_score']
                date_df.loc[~valid_mask, 'volume_shrink_score'] = 0.0
                result_dfs.append(date_df[['ymd', 'stock_code', 'volume_shrink_score']])

            result_df = pd.concat(result_dfs, ignore_index=True)
            logger.info(f"连续缩量因子计算完成：共{len(result_df)}条记录")

            if save_to_cache:
                self.cached_factors['volume_shrink'] = result_df.copy()
            return result_df

        except Exception as e:
            logger.error(f"计算连续缩量因子失败：{str(e)}")
            import traceback
            logger.error(traceback.format_exc())
            return pd.DataFrame(columns=['ymd', 'stock_code', 'volume_shrink_score'])


    @timing_decorator
    def price_shrink_factor(self, start_date, end_date,
                            min_streak=3, max_streak=4,
                            reverse=False, save_to_cache=True):
        """
        计算连续阴线因子百分制评分（0-100分）
        source_table: ods_stock_kline_daily_ts（is_down + today_pct）
        定义：连续阴线 = 截至今日，连续 min_streak 个交易日以上，每日均为阴线（close<open）；
              一旦某天收阳，连续计数立刻清零重新积累。
        计算公式：
            down_depth(t) = max(0, -today_pct(t))            ← 当日阴线下跌幅度(%)，阳线记 0
                            （today_pct = (close-open)/open*100，阴线日自带负值）
            S(t)          = 截至今日连续阴线天数（阳线日清零，封顶 max_streak）
            raw(t)        = Σ_{lag=0}^{S(t)-1} down_depth(t-lag)  ← 只累计"当前连阴段"的累计跌幅
            raw(t)        = 0，若 S(t) < min_streak                  ← 新一段不足 min_streak 天判 0
        参数：
            min_streak: 连续阴线的最少天数，默认 3（3~4 天为经验最优区；
                        1~2 天太常见无区分度，5 天以上多为阴跌庄股）；
            max_streak: 连阴累计封顶天数，默认 4（更早的阴线不再额外加分）；
        每日截面上对 raw_score 排名映射到 [1,100]：
            raw=0（未达 min_streak）得 0 分；raw 越大（跌得越深、连阴越久）得分越高；
            reverse=True 反向（用于观察超跌反转）。
        与旧版（按连阴天数封顶打分）的区别：
            raw 为连续值（累计跌幅），消除"4连阴与8连阴同分"的并列；
            且一根阳线即清零，避免"连阴中夹阳线"的票冒充纯连阴。
        注意：
            - 阴线判定用 K 线颜色（close<open），不是涨跌幅：低开高走的票
              change_pct<0 但收阳，本因子不计入也不打断连阴；
            - 停牌日无数据，视为段外（打断连续），属合理处理。
        """
        try:
            # ========== 1. 交易日与目标区间 ==========
            query_start = (pd.to_datetime(start_date)
                           - pd.Timedelta(days=max_streak * 2 + 20)).strftime('%Y%m%d')
            all_days = self.get_trading_days(query_start, end_date)
            if not all_days:
                return pd.DataFrame(columns=['ymd', 'stock_code', 'price_shrink_score'])

            start_idx = next((i for i, d in enumerate(all_days) if d >= start_date), None)
            end_idx = next((i for i in range(len(all_days) - 1, -1, -1) if all_days[i] <= end_date), None)
            if start_idx is None or end_idx is None:
                return pd.DataFrame(columns=['ymd', 'stock_code', 'price_shrink_score'])

            target_days = all_days[start_idx:end_idx + 1]
            # 深度累计最多回溯 max_streak 天
            earliest_idx = max(0, start_idx - max_streak)
            query_start_data = all_days[earliest_idx]

            # ========== 2. 读取阴线数据 ==========
            kline_df = Mysql_Utils.data_from_mysql_to_dataframe(
                user=self.user, password=self.password, host=self.host, database=self.database,
                table_name='ods_stock_kline_daily_ts',
                start_date=query_start_data, end_date=end_date,
                cols=['ymd', 'stock_code', 'is_down', 'today_pct']
            )
            if kline_df.empty:
                return pd.DataFrame(columns=['ymd', 'stock_code', 'price_shrink_score'])

            kline_df['ymd'] = pd.to_datetime(kline_df['ymd']).dt.strftime('%Y%m%d')
            # 阴线下跌幅度：阴线日 today_pct<0 取 -today_pct（正数），阳线记 0；NULL 视为 0
            kline_df['down_depth'] = (-kline_df['today_pct']).clip(lower=0).fillna(0)
            # 阴线标记（NULL 计为否）
            kline_df['is_down_f'] = kline_df['is_down'].fillna(0).astype(float)

            # ========== 3. 矩阵化：连续计数 S + 分段深度累计 ==========
            depth_m = kline_df.pivot_table(index='ymd', columns='stock_code',
                                           values='down_depth', aggfunc='max')
            depth_m = depth_m.reindex(index=all_days).fillna(0)
            down_m = kline_df.pivot_table(index='ymd', columns='stock_code',
                                          values='is_down_f', aggfunc='max')
            down_m = down_m.reindex(index=all_days).fillna(0)

            # S：连续阴线天数（阳线日 down=0 → 清零），封顶 max_streak
            S = down_m.apply(lambda s: s * s.groupby((s == 0).cumsum()).cumsum(), axis=0)
            S = S.clip(upper=max_streak)

            # 分段深度：只累加"当前连阴段"内每天的跌幅
            raw = pd.DataFrame(0.0, index=all_days, columns=depth_m.columns)
            for lag in range(max_streak):
                raw = raw.add(depth_m.shift(lag).fillna(0) * (S > lag), fill_value=0)

            # 连阴段不足 min_streak 天 → 0 分
            raw = raw.where(S >= min_streak, 0.0)

            # ========== 4. 截目标区间、转长格式、补全市场 ==========
            target_raw = raw.loc[target_days]
            long_df = (target_raw.rename_axis('ymd')
                       .reset_index()
                       .melt(id_vars='ymd', var_name='stock_code', value_name='raw_score'))

            universe_df = Mysql_Utils.data_from_mysql_to_dataframe(
                user=self.user, password=self.password, host=self.host, database=self.database,
                table_name='ods_stock_code_daily_insight',
                start_date=target_days[0], end_date=target_days[-1],
                cols=['ymd', 'stock_code']
            )
            if universe_df.empty:
                logger.warning("ods_stock_code_daily_insight 无数据，因子仅覆盖窗口内有记录的股票")
            else:
                universe_df['ymd'] = pd.to_datetime(universe_df['ymd']).dt.strftime('%Y%m%d')
                universe_df = universe_df[['ymd', 'stock_code']].drop_duplicates()
                long_df = universe_df.merge(long_df, on=['ymd', 'stock_code'], how='left')
                long_df['raw_score'] = long_df['raw_score'].fillna(0)

            # ========== 5. 每日截面排名 → [1,100]，raw=0 得 0 分 ==========
            result_dfs = []
            for date, date_df in long_df.groupby('ymd'):
                date_df = date_df.copy()
                valid_mask = date_df['raw_score'] > 0
                if valid_mask.sum() > 0:
                    valid = date_df.loc[valid_mask].copy()
                    valid['raw_rank'] = valid['raw_score'].rank(method='min', ascending=True)
                    max_rank = valid['raw_rank'].max()
                    if max_rank == 1:
                        valid['price_shrink_score'] = 100.0
                    elif reverse:
                        valid['price_shrink_score'] = (1 + 99 * (max_rank - valid['raw_rank'])
                                                       / (max_rank - 1)).round(2)
                    else:
                        valid['price_shrink_score'] = (1 + 99 * (valid['raw_rank'] - 1)
                                                       / (max_rank - 1)).round(2)
                    date_df.loc[valid.index, 'price_shrink_score'] = valid['price_shrink_score']
                date_df.loc[~valid_mask, 'price_shrink_score'] = 0.0
                result_dfs.append(date_df[['ymd', 'stock_code', 'price_shrink_score']])

            result_df = pd.concat(result_dfs, ignore_index=True)
            logger.info(f"连续阴线因子计算完成：共{len(result_df)}条记录")

            if save_to_cache:
                self.cached_factors['price_shrink'] = result_df.copy()
            return result_df

        except Exception as e:
            logger.error(f"计算连续阴线因子失败：{str(e)}")
            import traceback
            logger.error(traceback.format_exc())
            return pd.DataFrame(columns=['ymd', 'stock_code', 'price_shrink_score'])


    @timing_decorator
    def shrink_down_combo_factor(self, start_date, end_date,
                                 baseline='ma20',
                                 min_streak_vol=3, max_streak_vol=5,
                                 min_streak_down=3, max_streak_down=4,
                                 combine='geo',
                                 reverse=False, save_to_cache=True):
        """
        计算"连缩量且连阴线"组合因子百分制评分（0-100分）
        source_table: dwd_stock_technical_indicators（量） + ods_stock_kline_daily_ts（价）
        定义（双条件同时满足才纳入）：
            条件1（量）：截至今日连续 min_streak_vol 天以上，每日成交量低于均量基准线
                        （放量日清零，深度=低于均量的幅度%累计，封顶 max_streak_vol 天）；
            条件2（价）：截至今日连续 min_streak_down 天以上，每日均为阴线（close<open，
                        阳线日清零，深度=阴线实体跌幅%累计，封顶 max_streak_down 天）；
            任一端不满足 → 整体 0 分（不输出）。
        计算公式（两步）：
            第一步（两端各自的段内深度）：
                raw_vol(t)  = Σ_{lag=0}^{S_vol(t)-1}  shortfall(t-lag)，S_vol  < min_streak_vol  记 0
                raw_down(t) = Σ_{lag=0}^{S_down(t)-1} down_depth(t-lag)，S_down < min_streak_down 记 0
            第二步（在双满足的股票内合成，先各自截面排名成 [1,100] 消除量纲）：
                combine='geo'（默认）：combo = √(vol_pct × down_pct)，几何平均——
                    强调"双强"，一边 100 一边 20 → 约 45 分，不会被单边拉高；
                combine='mean'：combo = (vol_pct + down_pct) / 2，算术平均——容忍一端偏强；
                combine='min'：combo = min(vol_pct, down_pct)——最严，取短板。
        每日输出 combo_score ∈ [0,100]；reverse=True 在双满足集合内整体反向。
        注意：
            - 价端为 K 线颜色口径（阴线 is_down），假阳线（收阳但实际下跌）会打断连阴；
            - 停牌日两端均无数据，同时打断两边的连续计数；
            - 双条件都满足的股票通常每天只有几十到几百只（单条件可能上千），
              输出不补全 0 分股票——"未双满足"与"很差"在此因子中同义；
            - 若某交易日双满足的股票只有 1 只，该票 vol_pct/down_pct 直接给 100。
        """
        try:
            import numpy as np

            vs_col = {'ma5': 'volume_vs_ma5', 'ma20': 'volume_vs_ma20'}.get(baseline)
            if vs_col is None:
                raise ValueError(f"baseline 只支持 'ma5'/'ma20'，收到: {baseline}")
            if combine not in ('geo', 'mean', 'min'):
                raise ValueError(f"combine 必须是 'geo'/'mean'/'min'，收到: {combine}")

            backtrack = max(max_streak_vol, max_streak_down)

            # ========== 1. 交易日与目标区间 ==========
            query_start = (pd.to_datetime(start_date)
                           - pd.Timedelta(days=backtrack * 2 + 20)).strftime('%Y%m%d')
            all_days = self.get_trading_days(query_start, end_date)
            if not all_days:
                return pd.DataFrame(columns=['ymd', 'stock_code', 'combo_score'])

            start_idx = next((i for i, d in enumerate(all_days) if d >= start_date), None)
            end_idx = next((i for i in range(len(all_days) - 1, -1, -1) if all_days[i] <= end_date), None)
            if start_idx is None or end_idx is None:
                return pd.DataFrame(columns=['ymd', 'stock_code', 'combo_score'])

            target_days = all_days[start_idx:end_idx + 1]
            earliest_idx = max(0, start_idx - backtrack)
            query_start_data = all_days[earliest_idx]

            # ========== 2a. 量端数据 ==========
            tech_df = Mysql_Utils.data_from_mysql_to_dataframe(
                user=self.user, password=self.password, host=self.host, database=self.database,
                table_name='dwd_stock_technical_indicators',
                start_date=query_start_data, end_date=end_date,
                cols=['ymd', 'stock_code', vs_col]
            )
            if tech_df.empty:
                return pd.DataFrame(columns=['ymd', 'stock_code', 'combo_score'])

            tech_df['ymd'] = pd.to_datetime(tech_df['ymd']).dt.strftime('%Y%m%d')
            tech_df['shortfall'] = (-tech_df[vs_col]).clip(lower=0).fillna(0)
            tech_df['is_below'] = (tech_df[vs_col] < 0).fillna(False).astype(float)

            # ========== 2b. 价端数据（K线颜色口径，含 today_pct 实体跌幅） ==========
            kline_df = Mysql_Utils.data_from_mysql_to_dataframe(
                user=self.user, password=self.password, host=self.host, database=self.database,
                table_name='ods_stock_kline_daily_ts',
                start_date=query_start_data, end_date=end_date,
                cols=['ymd', 'stock_code', 'is_down', 'today_pct']
            )
            if kline_df.empty:
                return pd.DataFrame(columns=['ymd', 'stock_code', 'combo_score'])

            kline_df['ymd'] = pd.to_datetime(kline_df['ymd']).dt.strftime('%Y%m%d')
            kline_df['down_depth'] = (-kline_df['today_pct']).clip(lower=0).fillna(0)
            kline_df['is_down_f'] = kline_df['is_down'].fillna(0).astype(float)

            # ========== 3. 两端矩阵 + 各自的 S / raw ==========
            def _seg_depth(flag_m, depth_m, max_streak, min_streak):
                """S 连续计数（清零+封顶）→ 当前段深度累计 → 不足 min_streak 判 0"""
                S = flag_m.apply(lambda s: s * s.groupby((s == 0).cumsum()).cumsum(), axis=0)
                S = S.clip(upper=max_streak)
                raw = pd.DataFrame(0.0, index=flag_m.index, columns=flag_m.columns)
                for lag in range(max_streak):
                    raw = raw.add(depth_m.shift(lag).fillna(0) * (S > lag), fill_value=0)
                return raw.where(S >= min_streak, 0.0)

            short_m = tech_df.pivot_table(index='ymd', columns='stock_code',
                                          values='shortfall', aggfunc='max')
            short_m = short_m.reindex(index=all_days).fillna(0)
            below_m = tech_df.pivot_table(index='ymd', columns='stock_code',
                                          values='is_below', aggfunc='max')
            below_m = below_m.reindex(index=all_days).fillna(0)

            depth_m = kline_df.pivot_table(index='ymd', columns='stock_code',
                                           values='down_depth', aggfunc='max')
            depth_m = depth_m.reindex(index=all_days).fillna(0)
            down_m = kline_df.pivot_table(index='ymd', columns='stock_code',
                                          values='is_down_f', aggfunc='max')
            down_m = down_m.reindex(index=all_days).fillna(0)

            raw_vol = _seg_depth(below_m, short_m, max_streak_vol, min_streak_vol)
            raw_down = _seg_depth(down_m, depth_m, max_streak_down, min_streak_down)

            # ========== 4. 截目标区间、转长格式、双条件合并 ==========
            vol_long = (raw_vol.loc[target_days].rename_axis('ymd').reset_index()
                        .melt(id_vars='ymd', var_name='stock_code', value_name='raw_vol'))
            down_long = (raw_down.loc[target_days].rename_axis('ymd').reset_index()
                         .melt(id_vars='ymd', var_name='stock_code', value_name='raw_down'))

            combo_long = vol_long.merge(down_long, on=['ymd', 'stock_code'], how='inner')
            # 双条件门槛：任一端 raw=0（未达 min_streak 或无深度）→ 剔除
            combo_long = combo_long[(combo_long['raw_vol'] > 0) & (combo_long['raw_down'] > 0)].copy()

            if combo_long.empty:
                logger.warning("无同时满足连缩与连阴的股票，组合因子输出为空")
                result_df = pd.DataFrame(columns=['ymd', 'stock_code', 'combo_score'])
            else:
                # 在双满足集合内分别截面排名
                combo_long['vol_rank'] = combo_long.groupby('ymd')['raw_vol'].rank(method='min')
                combo_long['down_rank'] = combo_long.groupby('ymd')['raw_down'].rank(method='min')

                # 各自映射到 [1,100]（向量化；当日仅 1 只时给满分，避免除零）
                grp_v = combo_long.groupby('ymd')['vol_rank']
                grp_d = combo_long.groupby('ymd')['down_rank']

                combo_long['vol_pct'] = (1 + 99 * (combo_long['vol_rank'] - 1)
                                         / (grp_v.transform('max') - 1)).round(2)
                combo_long['down_pct'] = (1 + 99 * (combo_long['down_rank'] - 1)
                                          / (grp_d.transform('max') - 1)).round(2)

                combo_long.loc[grp_v.transform('max') == 1, 'vol_pct'] = 100.0
                combo_long.loc[grp_d.transform('max') == 1, 'down_pct'] = 100.0

                # 合成
                if combine == 'geo':
                    combo_long['combo_score'] = np.sqrt(combo_long['vol_pct']
                                                        * combo_long['down_pct']).round(2)
                elif combine == 'mean':
                    combo_long['combo_score'] = ((combo_long['vol_pct']
                                                  + combo_long['down_pct']) / 2).round(2)
                else:  # 'min'
                    combo_long['combo_score'] = combo_long[['vol_pct', 'down_pct']].min(axis=1).round(2)

                if reverse:
                    combo_long['combo_score'] = (101 - combo_long['combo_score']).clip(1, 100).round(2)

                result_df = combo_long[['ymd', 'stock_code', 'combo_score']]

            logger.info(f"连缩量且连阴线组合因子计算完成：共{len(result_df)}条记录")

            if save_to_cache:
                self.cached_factors['shrink_down'] = result_df.copy()
            return result_df

        except Exception as e:
            logger.error(f"计算组合因子失败：{str(e)}")
            import traceback
            logger.error(traceback.format_exc())
            return pd.DataFrame(columns=['ymd', 'stock_code', 'combo_score'])


    @timing_decorator
    def aggregate_factors(self, start_date, end_date, weights=None, factors=None,
                          zero_fill_factors=('shrink_down',), save_to_mysql=True):
        """
        因子汇总：把缓存中的各因子得分合并成全市场宽表，计算多因子加权综合分，写入 dwd_factor_summary。

        处理流程：
        1) 【骨架】从 ods_stock_code_daily_insight（每日已上市股票码表，含 stock_name）取
           start_date~end_date 的每日股票池——避免用单日静态名单（stocks_df）造成的
           次新股幽灵行/退市股缺失；
        2) 【约定式合并】每个因子 DataFrame 遵守"3 列约定"（ymd, stock_code, 唯一得分列），
           逐因子 left join 到骨架，无手工列映射——新增因子无需改动本函数；
           合并后得分列保持 NaN（区分"未满足/无数据"与"0 分"），composite 算完再统一填 0；
        3) 【加权 composite】composite = Σ(w_f × score_f) / Σ(w_f)，两种缺失语义：
            - zero_fill_factors（默认含 'shrink_down'）：门槛型因子，未出现在输出中
              表示"未双满足"，缺失按 0 分计且权重照算（拉低 composite）；
            - 其余因子：缺失表示"数据未知"（如股东数未披露），不计分也不计权重，
              权重在同日有效因子间重新分配（不惩罚信息缺失）；
        4) 【signal_level】由 composite 派生（A≥80/B≥60/C≥40/D≥20/E<20），
           本列唯一写入方，语义为"综合分等级"；
        5) 【落库】写入 dwd_factor_summary（merge_on ymd+stock_code，INSERT IGNORE 幂等）。

        参数：
            weights: dict，如 {'zt': 0.3, 'pb': 0.2, 'shareholder': 0.2,
                                'volume_shrink': 0.15, 'price_shrink': 0.1, 'shrink_down': 0.05}；
                     None 表示对参与因子等权；weights 中未缓存的因子自动剔除并告警；
            factors: None 表示用缓存中全部因子；也可传名单只汇总指定因子；
            zero_fill_factors: 缺失按 0 且权重照算的门槛型因子名单；
            save_to_mysql: False 时仅返回 DataFrame 便于调试，不写库。
        返回：
            summary_df（骨架列 + 各因子得分列 + composite_score + signal_level）
        """
        try:
            # ========== 1. 骨架：每日已上市股票 ==========
            skeleton = Mysql_Utils.data_from_mysql_to_dataframe(
                user=self.user, password=self.password, host=self.host, database=self.database,
                table_name='ods_stock_code_daily_insight',
                start_date=start_date, end_date=end_date,
                cols=['ymd', 'stock_code', 'stock_name']
            )
            if skeleton.empty:
                logger.warning(f"ods_stock_code_daily_insight 无数据: {start_date}~{end_date}")
                return pd.DataFrame()

            skeleton['ymd'] = pd.to_datetime(skeleton['ymd']).dt.strftime('%Y%m%d')
            skeleton = skeleton.drop_duplicates(subset=['ymd', 'stock_code'])

            # ========== 2. 选择参与汇总的因子 ==========
            available = {k: v for k, v in self.cached_factors.items() if v is not None and not v.empty}
            if factors is not None:
                available = {k: v for k, v in available.items() if k in factors}
            if not available:
                logger.warning("缓存中没有可用因子，请先运行各因子函数")
                return pd.DataFrame()

            # ========== 3. 约定式合并各因子（向量化，无逐行循环） ==========
            summary_df = skeleton.copy()
            score_cols = {}  # 因子名 → 得分列名
            for name, df in available.items():
                cols = [c for c in df.columns if c not in ('ymd', 'stock_code')]
                if len(cols) != 1:
                    logger.warning(f"因子 {name} 的得分列不唯一（{cols}），已跳过")
                    continue
                score_col = cols[0]
                d = df[['ymd', 'stock_code', score_col]].copy()
                d['ymd'] = pd.to_datetime(d['ymd']).dt.strftime('%Y%m%d')
                summary_df = summary_df.merge(d, on=['ymd', 'stock_code'], how='left')
                score_cols[name] = score_col

            if not score_cols:
                logger.warning("没有因子成功合并")
                return pd.DataFrame()

            # ========== 4. 加权 composite（区分两种缺失语义） ==========
            if weights is None:
                weights = {name: 1.0 for name in score_cols}  # 等权
            use_weights = {k: w for k, w in weights.items() if k in score_cols}
            skipped = set(weights) - set(use_weights)
            if skipped:
                logger.warning(f"以下因子未在缓存中，已剔除出合成: {skipped}")

            contrib = pd.Series(0.0, index=summary_df.index)
            wsum = pd.Series(0.0, index=summary_df.index)
            for name, w in use_weights.items():
                col = score_cols[name]
                valid = summary_df[col].notna()
                vals = summary_df[col].astype(float).fillna(0)
                if name in zero_fill_factors:
                    # 门槛型：缺失=未满足=0 分，权重照算（拉低总分）
                    contrib += w * vals
                    wsum += w
                else:
                    # 信息型：缺失=未知，权重在同日有效因子间重分配（不惩罚）
                    contrib += w * vals * valid
                    wsum += w * valid

            summary_df['composite_score'] = (contrib / wsum.replace(0, np.nan)).clip(0, 100).round(2)
            summary_df['composite_score'] = summary_df['composite_score'].fillna(0)

            # ========== 5. signal_level：由 composite 派生（唯一写入方） ==========
            summary_df['signal_level'] = pd.cut(
                summary_df['composite_score'],
                bins=[-1, 20, 40, 60, 80, 100],
                labels=['E', 'D', 'C', 'B', 'A']
            ).astype(str)

            # ========== 6. 落库：得分列填 0 + 日期转 YYYY-MM-DD ==========
            for col in score_cols.values():
                summary_df[col] = summary_df[col].fillna(0)
            summary_df['ymd'] = pd.to_datetime(summary_df['ymd'], format='%Y%m%d').dt.strftime('%Y-%m-%d')

            logger.info(f"因子汇总完成：共{len(summary_df)}条，合成因子及权重: {use_weights}")

            if save_to_mysql:
                out_cols = (['ymd', 'stock_code', 'stock_name']
                            + list(score_cols.values())
                            + ['composite_score', 'signal_level'])
                Mysql_Utils.data_from_dataframe_to_mysql(
                    user=self.user, password=self.password, host=self.host, database=self.database,
                    df=summary_df[out_cols],
                    table_name="dwd_factor_summary",
                    merge_on=['ymd', 'stock_code']
                )
                logger.info(f"因子汇总已保存到 dwd_factor_summary")

            return summary_df

        except Exception as e:
            logger.error(f"因子汇总失败：{str(e)}")
            import traceback
            logger.error(traceback.format_exc())
            return pd.DataFrame()


    @script_run(script_name="factor_library.py")
    def setup(self):

        start = DateUtility.next_day(-5)
        end = DateUtility.today()

        #  pb 因子计算
        self.pb_factor_score(start_date=start, end_date=end)

        #  涨停 因子计算
        self.zt_factor_score(start_date=start, end_date=end)

        #  股东数 因子计算
        self.shareholder_factor_score(start_date=start, end_date=end)

        #  成交量缩量 因子计算
        self.volume_shrink_factor(start_date=start, end_date=end)  # 连续缩量

        #  日K连续阴线 因子计算
        self.price_shrink_factor(start_date=start, end_date=end)  # 连续阴线

        #  缩量且连阴线 因子计算
        self.shrink_down_combo_factor(start_date=start, end_date=end)  # 连续阴线

        #  因子汇总
        self.aggregate_factors(start_date=start, end_date=end)


if __name__ == '__main__':
    factorlib = FactorLibrary()
    factorlib.setup()


```

--------------------------------------------------------------------------------
## strategy\strategy_engine.py

```python
# strategy/strategy_engine.py
import pandas as pd
import logging
from CommonProperties.Base_utils import timing_decorator
from factor_library import FactorLibrary

logger = logging.getLogger(__name__)


class StrategyEngine:
    """策略执行引擎:支持多日回测，负责执行多因子策略"""

    def __init__(self, factor_lib):
        self.factor_lib = factor_lib  # 注入因子库实例
        self.strategies = {}          # 存储已注册的策略

    def register_strategy(self, name, func, params=None):
        """注册策略"""
        self.strategies[name] = {
            'func': func,
            'params': params or {}
        }
        logger.info(f"策略[{name}]注册成功")

    # @timing_decorator
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


if __name__ == '__main__':
    f1 = FactorLibrary()
    sn = StrategyEngine(f1)
    res = sn.value_chip_zt_strategy(start_date='20260101', end_date='20260110')



```
