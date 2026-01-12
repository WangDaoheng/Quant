# 量化工程V1.0 代码梳理文档
*生成时间: 2026-01-12 15:30:10*

## 项目统计信息
- 项目根目录: F:\Quant\Backtrader_PJ1
- 总文件数: 45
- Python文件数: 40
- SQL文件数: 4
- Shell文件数: 1
- 有效目录数: 14

# Backtrader_PJ1 项目目录结构
*生成时间: 2026-01-12 15:30:10*

📁 Backtrader_PJ1/
    📄 main-doubao.py
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
        📄 strategy_dashboard.py
    📁 datas_prepare/
        📄 __init__.py
        📄 run_data_prepare.sh
        📄 setup_data_prepare.py
        📁 C00_SQL/
            📄 DW_mysql_tables_nopart.sql
            📄 MART_mysql_tables_nopart.sql
            📄 __init__.py
            📄 create_mysql_tables.sql
            📄 create_mysql_tables_nopart.sql
        📁 C01_data_download_daily/
            📄 __init__.py
            📄 download_insight_data_afternoon.py
            📄 download_insight_data_afternoon_of_history.py
            📄 download_vantage_data_afternoon.py
        📁 C02_data_merge/
            📄 __init__.py
            📄 merge_insight_data_afternoon.py
        📁 C03_data_DWD/
            📄 __init__.py
            📄 calculate_DWD_datas.py
        📁 C04_data_MART/
            📄 __init__.py
            📄 calculate_MART_datas.py
        📁 C06_data_transfer/
            📄 __init__.py
            📄 get_example_tables.py
            📄 put_df_to_mysql.py
            📄 transfer_between_local_and_originMySQL.py
    📁 monitor/
        📄 __init__.py
        📄 alert_system.py
        📄 realtime_monitor.py
    📁 review/
        📄 __init__.py
        📄 daily_review.py
    📁 strategy/
        📄 __init__.py
        📄 factor_library.py
        📄 strategy_engine.py

# 项目代码内容

--------------------------------------------------------------------------------
## main-doubao.py

```python
import logging
from backtest import StockBacktestEngine, PerformanceAnalyzer
from monitor.realtime_monitor import RealtimeMonitor
from monitor.alert_system import AlertSystem
from review.daily_review import DailyReview
from dashboard.strategy_dashboard import StrategyDashboard
from CommonProperties.DateUtility import DateUtility

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('quant_strategy.log', encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

def main():
    # 1. 初始化核心组件
    engine = StockBacktestEngine()
    alert_system = AlertSystem()

    # 2. 回测参数配置
    start_date = DateUtility.first_day_of_month_before_n_months(6)
    end_date = DateUtility.today()
    initial_cash = 100000
    initial_stock_codes = ['600000', '000001', '601318', '002594', '300059']

    # 3. 运行回测（因子驱动策略）
    logger.info("======= 开始因子驱动策略回测 =======")
    factor_perf = engine.run_backtest(
        stock_codes=initial_stock_codes,
        start_date=start_date,
        end_date=end_date,
        initial_cash=initial_cash,
        strategy_type='factor_driven'
    )

    if not factor_perf:
        logger.error("回测失败，终止程序")
        return

    # 4. 生成回测报告
    analyzer = PerformanceAnalyzer()
    factor_report = analyzer.generate_report(factor_perf, "因子驱动策略", start_date, end_date)
    logger.info("\n======= 因子驱动策略回测报告 =======\n" + factor_report)

    # 5. 初始化Cerebro实例（用于监控/复盘）
    # cerebro = engine.run_backtest.__self__.cerebro  # 实际需从回测引擎中获取真实Cerebro实例
    cerebro = engine.cerebro  # 回测引擎中已保存了cerebro实例
    # 6. 实时监控
    monitor = RealtimeMonitor(engine, initial_stock_codes)
    # 单次监控（非循环）
    factor_alerts = monitor.monitor_factor_signals()
    position_alerts = monitor.monitor_position_performance(cerebro)
    price_alerts = monitor.monitor_price_volatility()

    # 触发预警
    if factor_alerts or position_alerts or price_alerts:
        alert_system.trigger_alert('all', {
            'factor': factor_alerts,
            'position': position_alerts,
            'price': price_alerts
        })

    # 7. 每日复盘
    review = DailyReview(engine, cerebro, 'factor_driven')
    review_report = review.generate_daily_review_report()
    logger.info("\n======= 每日复盘报告 =======\n" + review_report)

    # 8. 生成可视化仪表盘
    dashboard = StrategyDashboard(engine, factor_perf, 'factor_driven')
    dashboard_path = dashboard.generate_dashboard(cerebro)
    logger.info(f"可视化仪表盘路径：{dashboard_path}")

    # 9. 启动实时监控（可选，注释掉则只运行一次）
    # monitor.run_monitor(cerebro, interval=3600)  # 1小时监控一次

    logger.info("======= 量化策略分析流程完成 =======")


if __name__ == "__main__":
    main()


```

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
import backtrader as bt
import pandas as pd
import logging
from CommonProperties import Mysql_Utils
from CommonProperties.Base_utils import timing_decorator
from Others.strategy.factor_library import FactorLibrary
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
        查询指定股票/日期的因子信号
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
```

--------------------------------------------------------------------------------
## backtest\factor_driven_strategy.py

```python
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




######################################################################

######################  insight 账号信息  #############################
user = "USER019331L1"
password = "F_Y+.3mtc4tU"


######################     当下数据目录     #############################
dir_insight_base = r'F:\QDatas\insight_A'
dir_vantage_base = r'F:\QDatas\vantage'


######################     历史数据目录     #############################
dir_history_insight_base = r'F:\QDatas\history\insight_A'
dir_history_vantage_base = r'F:\QDatas\history\vantage'


######################     merge数据目录     #############################
dir_merge_insight_base = r'F:\QDatas\merge\insight_A'
dir_merge_vantage_base = r'F:\QDatas\merge\vantage'





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


######################  京东云 日志文件 留存地址  #############################

log_file_linux_path = r"/opt/Logs"
log_file_window_path = r"F:\QDatas\logs"



######################  个人 配置 留存地址  #############################

personal_linux_path = r"/opt/ss_property"
personal_window_path = r"F:\QDatas\ss_property"
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
import platform
import json

from CommonProperties.set_config import setup_logging_config


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
        logging.info(f"文件: {file_name} 函数: {func.__name__} 开始执行...")

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
        logging.info(f"文件: {file_name} 函数: {func.__name__} 执行时间: {execution_time:.2f} 秒")
        return result
    return wrapper


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

import pymysql
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import sessionmaker
import gc
from datetime import datetime, timedelta
from typing import List, Optional
import traceback  # 用于打印详细的错误堆栈

import pandas as pd
import numpy as np
import logging
import platform

import CommonProperties.Base_Properties as base_properties
import CommonProperties.Base_utils as base_utils
from CommonProperties.set_config import setup_logging_config


###################  mysql 配置   ######################
local_user = base_properties.local_mysql_user
local_password = base_properties.local_mysql_password
local_database = base_properties.local_mysql_database
local_host = base_properties.local_mysql_host

origin_user = base_properties.origin_mysql_user
origin_password = base_properties.origin_mysql_password
origin_database = base_properties.origin_mysql_database
origin_host = base_properties.origin_mysql_host



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


def data_from_dataframe_to_mysql(user, password, host, database='quant', df=pd.DataFrame(), table_name='', merge_on=[]):
    """
    把 dataframe 类型数据写入 mysql 表里面, 同时调用了
    Args:
        df:
        table_name:
        database:
    Returns:
    """
    db_url = f'mysql+pymysql://{user}:{password}@{host}:3306/{database}'
    engine = create_engine(db_url)

    # 对输入的df的空值做处理
    df = df.replace({np.nan: None})

    # 确保 df 中的字段列顺序与表中的列顺序一致
    columns = df.columns.tolist()

    # 检查是否存在重复数据，并将其去除
    df.drop_duplicates(subset=merge_on, keep='first', inplace=True)

    total_rows = df.shape[0]
    if total_rows == 0:
        logging.info(f"所有数据已存在，无需插入新的数据到 {host} 的 {table_name} 表中。")
        return

    # 使用 INSERT IGNORE 来去重
    insert_sql = f"""
    INSERT IGNORE INTO {table_name} ({', '.join(columns)})
    VALUES ({', '.join([f':{col}' for col in columns])});
    """

    # 转换 df 为一个可以传递给 executemany 的字典列表
    values = df.to_dict('records')

    with engine.connect() as connection:
        transaction = connection.begin()
        try:
            connection.execute(text(insert_sql), values)
            transaction.commit()
            logging.info(f"成功插入 {total_rows} 行数据到 {host} 的 {table_name} 表中。")
        except Exception as e:
            transaction.rollback()
            logging.error(f"写入 {host} 的表：{table_name} 时发生错误: {e}")
            raise


def data_from_mysql_to_dataframe(user, password, host, database='quant', table_name='', start_date=None, end_date=None, cols=None):
    """
    从 MySQL 表中读取数据到 DataFrame，同时进行最终的数据完整性检查和日志记录
    Args:
        table_name: MySQL 表名
        database: 数据库名称
        start_date: 起始日期
        end_date: 结束日期
        cols: 要选择的字段列表

    Returns:
        df: 读取到的 DataFrame
    """

    db_url = f'mysql+pymysql://{user}:{password}@{host}:3306/{database}'
    engine = create_engine(db_url)

    # 构建 SELECT 语句
    if cols:
        selected_cols = ', '.join(cols)
    else:
        selected_cols = '*'

    # 构建 WHERE 条件
    where_conditions = []
    if start_date:
        where_conditions.append(f"ymd >= '{start_date}'")
    if end_date:
        where_conditions.append(f"ymd <= '{end_date}'")

    where_clause = " AND ".join(where_conditions)

    # 读取 MySQL 表中的记录总数
    query_total = f"SELECT COUNT(*) FROM {table_name}"
    if where_clause:
        query_total += f" WHERE {where_clause}"
    total_rows = pd.read_sql(query_total, engine).iloc[0, 0]

    # 读取数据的批量大小
    chunk_size = 10000
    chunks = []

    try:
        for offset in range(0, total_rows, chunk_size):
            query = f"SELECT {selected_cols} FROM {table_name}"
            if where_clause:
                query += f" WHERE {where_clause}"
            query += f" LIMIT {chunk_size} OFFSET {offset}"
            chunk = pd.read_sql(query, engine)
            chunks.append(chunk)

        df = pd.concat(chunks, ignore_index=True)

        # 最终的数据完整性检查
        if df.shape[0] == total_rows:
            logging.info(f"{host} 的 mysql表：{table_name} 数据读取成功且无遗漏，共 {total_rows} 行。")
        else:
            logging.warning(f"{table_name} 数据读取可能有问题，预期记录数为 {total_rows}，实际读取记录数为 {df.shape[0]}。")

    except Exception as e:
        logging.error(f"从表：{table_name} 读取数据时发生错误: {e}")
        df = pd.DataFrame()  # 返回一个空的 DataFrame 以防出错时没有返回数据

    return df


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

    db_url = f'mysql+pymysql://{user}:{password}@{host}:3306/{database}'
    engine = create_engine(db_url)

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
    db_url = f'mysql+pymysql://{user}:{password}@{host}:3306/{database}'
    engine = create_engine(db_url)

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
    source_db_url = f'mysql+pymysql://{source_user}:{source_password}@{source_host}:3306/{source_database}'
    source_engine = create_engine(source_db_url)

    # 目标服务器连接
    target_db_url = f'mysql+pymysql://{target_user}:{target_password}@{target_host}:3306/{target_database}'
    target_engine = create_engine(target_db_url)

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
    source_db_url = f'mysql+pymysql://{source_user}:{source_password}@{source_host}:3306/{source_database}'
    source_engine = create_engine(source_db_url)

    # 目标服务器连接
    target_db_url = f'mysql+pymysql://{target_user}:{target_password}@{target_host}:3306/{target_database}'
    target_engine = create_engine(target_db_url)

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




def get_stock_codes_latest(df):
    """
    这是为了取最新的 stock_code, 首先默认从类变量里面获取 stock_code(df), 如果df为空，就从mysql里面去取最新的
    Args:
        df:
    Returns:
    """

    if df is None or df.empty:

        if platform.system() == "Windows":
            user = local_user
            password = local_password
            host = local_host
            database = local_database
        else:
            user = origin_user
            password = origin_password
            host = origin_host
            database = origin_database

        stock_code_df = data_from_mysql_to_dataframe_latest(user=user,
                                                            password=password,
                                                            host=host,
                                                            database=database,
                                                            table_name='ods_stock_code_daily_insight')

        mysql_stock_code_list = stock_code_df['htsc_code'].tolist()
        logging.info("    从 本地Mysql库 里读取最新的股票代码")
    else:
        mysql_stock_code_list = df['htsc_code'].tolist()
        logging.info("    从 self.stock_code 里读取最新的股票代码")

    return mysql_stock_code_list


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
from CommonProperties.Base_Properties import log_file_linux_path, log_file_window_path, personal_linux_path, personal_window_path, personal_property_file


def get_platform_is_window():
    """
    判断当前操作系统是window 还是 其他
    Returns: True  是window平台
             Flase 是其他平台
    """
    if platform.system() == "Windows":
        return True
    else:
        return False


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
    if  get_platform_is_window():
        personal_window_file = os.path.join(personal_window_path, personal_property_file)
        personal_property_dict = read_json_file(personal_window_file)

    else:
        personal_linux_file = os.path.join(personal_linux_path, personal_property_file)
        personal_property_dict = read_json_file(personal_linux_file)

    return personal_property_dict


def read_logfile():
    """
    读取日志文件的地址
    Returns:
    """
    # 获取当前日期并生成日志文件名
    current_date = datetime.now().strftime('%Y-%m-%d')

    if get_platform_is_window():
        log_file = f"log_windows_{current_date}.txt"
        log_filedir = os.path.join(log_file_window_path, log_file)

    else:
        log_file = f"log_linux_{current_date}.txt"
        log_filedir = os.path.join(log_file_linux_path, log_file)

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
        if platform.system() == "Windows":
            log_file_window_filename = f'log_windows_{current_date}.txt'
            log_file_window = os.path.join(log_file_window_path, log_file_window_filename)
            log_file_path = log_file_window  # Windows 下的路径
        else:
            log_file_linux_filename = f'log_linux_{current_date}.txt'
            log_file_linux = os.path.join(log_file_linux_path, log_file_linux_filename)
            log_file_path = log_file_linux    # Linux 下的路径

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
## datas_prepare\__init__.py

```python

```

--------------------------------------------------------------------------------
## datas_prepare\run_data_prepare.sh

```bash
#!/bin/bash
# 定义日志文件名（格式：run_log_YYYY-MM-DD.txt）
LOG_FILE="/opt/run_logs/run_log_$(date +%Y-%m-%d).txt"
# 执行Python脚本，并将输出（stdout和stderr）写入日志文件
python3 /opt/Quant/datas_prepare/setup_data_prepare.py >> "$LOG_FILE" 2>&1
```

--------------------------------------------------------------------------------
## datas_prepare\setup_data_prepare.py

```python
# -*- coding: utf-8 -*-
import sys
import os

# 获取当前脚本所在目录（/opt/quants/Quant/datas_prepare）
script_dir = os.path.dirname(os.path.abspath(__file__))
# 获取项目根目录（/opt/quants/Quant/，即 script_dir 的父目录）
project_root = os.path.dirname(script_dir)

# 将项目根目录添加到 Python 搜索路径
if project_root not in sys.path:
    sys.path.insert(0, project_root)


from datas_prepare.C01_data_download_daily.download_insight_data_afternoon import SaveInsightData
from datas_prepare.C01_data_download_daily.download_insight_data_afternoon_of_history import SaveInsightHistoryData
from datas_prepare.C01_data_download_daily.download_vantage_data_afternoon import SaveVantageData

from datas_prepare.C02_data_merge.merge_insight_data_afternoon import MergeInsightData
from datas_prepare.C03_data_DWD.calculate_DWD_datas import CalDWD
from datas_prepare.C04_data_MART.calculate_MART_datas import CalDMART


import CommonProperties.set_config as set_config

# ************************************************************************
# 本代码的作用是   运行整个 DataPrepare 工作
# 主要功能模块：
#   1. 数据下载        01_data_download
#      当日数据下载
#         download_insight_data_afternoon.py
#         download_vantage_data_afternoon.py
#      历史数据下载
#         download_insight_data_afternoon_of_history
#
#   2. 数据merge      C02_data_merge
#         merge_insight_data_afternoon.py
#
# ************************************************************************



class RunDataPrepare:

    def __init__(self):
        self.save_insight_now = SaveInsightData()
        self.save_insight_history = SaveInsightHistoryData()
        self.save_vantage_now = SaveVantageData()
        self.merge_insight = MergeInsightData()
        self.dwd_cal = CalDWD()
        self.dmart_cal = CalDMART()


    def send_logfile_email(self):
        """
        聚合后发送邮件的服务
        Returns:

        """
        set_config.send_log_via_email()


    def setup(self):

        #  下载 insight 当日数据
        self.save_insight_now.setup()

        #  合并 insight 当日跑批的数据至历史数据中
        self.merge_insight.setup()

        #  执行 DWD层逻辑
        self.dwd_cal.setup()

        #  执行 MART层逻辑
        self.dmart_cal.setup()

        #  下载 vantage 当日数据
        # self.save_vantage_now.setup()

        #  下载历史数据
        # self.save_insight_history.setup()

        #  发送邮件
        self.send_logfile_email()


if __name__ == '__main__':
    run_data_prepare = RunDataPrepare()
    run_data_prepare.setup()


```

--------------------------------------------------------------------------------
## datas_prepare\C00_SQL\DW_mysql_tables_nopart.sql

```sql

--1.1
------------------  dwd_ashare_stock_base_info   股票基本信息大宽表
create table quant.dwd_ashare_stock_base_info (
     ymd              DATE               --日期
    ,stock_code       varchar(50)        --代码
    ,stock_name       varchar(50)        --名称
    ,close            double             --最新收盘价
    ,market_value     double             --流通市值(亿)
    ,total_value      double             --总市值(亿)
    ,total_asset      double             --总资产(亿)
    ,net_asset        double             --净资产(亿)
    ,total_capital    double             --总股本(亿)
    ,float_capital    double             --流通股(亿)
    ,shareholder_num  bigint             --股东人数
    ,pb               varchar(50)        --市净率
    ,pe               varchar(50)        --市盈(动)
    ,market           VARCHAR(50)        --市场特征主板创业板等
    ,plate_names      VARCHAR(500)       --板块名称
    ,UNIQUE KEY unique_ymd_stock_code (ymd, stock_code)
) ;


--1.2
------------------  dwd_stock_zt_list   涨停股票清单
CREATE TABLE quant.dwd_stock_zt_list (
     ymd                DATE          NOT NULL   --交易日期
    ,stock_code         VARCHAR(50)   NOT NULL   --股票代码
    ,stock_name         VARCHAR(50)   NOT NULL   --股票名称
    ,last_close         FLOAT                    --昨日收盘价
    ,close              FLOAT                    --收盘价
    ,rate               FLOAT                    --涨幅
    ,market_value       double                   --流通市值(亿)
    ,total_value        double                   --总市值(亿)
    ,total_asset        double                   --总资产(亿)
    ,net_asset          double                   --净资产(亿)
    ,total_capital      double                   --总股本(亿)
    ,float_capital      double                   --流通股(亿)
    ,shareholder_num    bigint                   --股东人数
    ,pb                 varchar(50)              --市净率
    ,pe                 varchar(50)              --市盈(动)
	,market             VARCHAR(50)              --市场特征主板创业板等
    ,plate_names        VARCHAR(500)             --板块名称
    ,concept_plate      VARCHAR(500)             --概念板块
    ,index_plate        VARCHAR(500)             --指数板块
    ,industry_plate     VARCHAR(500)             --行业板块
    ,style_plate        VARCHAR(500)             --风格板块
    ,out_plate          VARCHAR(500)             --外部数据板块
    ,UNIQUE KEY unique_ymd_stock_code (ymd, stock_code)
);



--1.3
------------------  dwd_stock_dt_list   跌停股票清单
CREATE TABLE quant.dwd_stock_dt_list (
     ymd                DATE          NOT NULL   --交易日期
    ,stock_code         VARCHAR(50)   NOT NULL   --股票代码
    ,stock_name         VARCHAR(50)   NOT NULL   --股票名称
    ,last_close         FLOAT                    --昨日收盘价
    ,close              FLOAT                    --收盘价
    ,rate               FLOAT                    --涨幅
    ,market_value       double                   --流通市值(亿)
    ,total_value        double                   --总市值(亿)
    ,total_asset        double                   --总资产(亿)
    ,net_asset          double                   --净资产(亿)
    ,total_capital      double                   --总股本(亿)
    ,float_capital      double                   --流通股(亿)
    ,shareholder_num    bigint                   --股东人数
    ,pb                 varchar(50)              --市净率
    ,pe                 varchar(50)              --市盈(动)
	,market             VARCHAR(50)              --市场特征主板创业板等
    ,plate_names        VARCHAR(500)             --板块名称
    ,UNIQUE KEY unique_ymd_stock_code (ymd, stock_code)
);


--4.2        多渠道板块数据 -- 多渠道汇总
------------------  dwd_stock_a_total_plate
CREATE TABLE quant.dwd_stock_a_total_plate (
     ymd          DATE        NOT NULL      --日期
    ,plate_name   VARCHAR(50) NOT NULL      --板块名称
    ,stock_code   VARCHAR(50)               --标的代码
    ,stock_name   VARCHAR(50)               --标的名称
    ,source_table VARCHAR(50)               --来源表
    ,remark       VARCHAR(50)               --备注
    ,UNIQUE KEY unique_ymd_plate_code (ymd, plate_name, stock_code)
) ;


```

--------------------------------------------------------------------------------
## datas_prepare\C00_SQL\MART_mysql_tables_nopart.sql

```sql

--1.1
------------------  dmart_stock_zt_details   股票涨停明细
create table quant.dmart_stock_zt_details (
     ymd                DATE                     --日期
    ,stock_code         varchar(50)              --代码
    ,stock_name         varchar(50)              --名称
    ,concept_plate      VARCHAR(500)             --概念板块
    ,index_plate        VARCHAR(500)             --指数板块
    ,industry_plate     VARCHAR(500)             --行业板块
    ,style_plate        VARCHAR(500)             --风格板块
    ,out_plate          VARCHAR(500)             --外部数据板块
    ,UNIQUE KEY unique_ymd_stock_code (ymd, stock_code)
) ;


------------------  dmart_stock_zt_details   股票涨停明细拆分
CREATE TABLE quant.dmart_stock_zt_details_expanded (
    ymd DATE,
    stock_code VARCHAR(20),
    stock_name VARCHAR(50),
    concept_plate VARCHAR(500),
    index_plate VARCHAR(500),
    industry_plate VARCHAR(500),
    style_plate VARCHAR(500),
    out_plate VARCHAR(500)
);


```

--------------------------------------------------------------------------------
## datas_prepare\C00_SQL\__init__.py

```python

```

--------------------------------------------------------------------------------
## datas_prepare\C00_SQL\create_mysql_tables.sql

```sql

--1.1
------------------  ods_stock_code_daily_insight   当日已上市股票码表
CREATE TABLE quant.ods_stock_code_daily_insight (
    ymd DATE NOT NULL,
    htsc_code VARCHAR(50) NOT NULL,
    name VARCHAR(50),
    exchange VARCHAR(50),
    UNIQUE KEY unique_ymd_stock_code (ymd, htsc_code)
);


--1.2
------------------  stock_kline_daily_insight   当日已上市股票的历史日K
CREATE TABLE quant.ods_stock_kline_daily_insight_now (
    htsc_code VARCHAR(50) NOT NULL,
    ymd DATE NOT NULL,
    open FLOAT,
    close FLOAT,
    high FLOAT,
    low FLOAT,
    num_trades BIGINT,
    volume BIGINT,
    UNIQUE KEY unique_ymd_stock_code (ymd, htsc_code)
) ;


CREATE TABLE quant.ods_stock_kline_daily_insight (
    htsc_code VARCHAR(50) NOT NULL,
    ymd DATE NOT NULL,
    open FLOAT,
    close FLOAT,
    high FLOAT,
    low FLOAT,
    num_trades BIGINT,
    volume BIGINT,
    UNIQUE KEY unique_ymd_stock_code (ymd, htsc_code)
) PARTITION BY RANGE (YEAR(ymd) * 100 + MONTH(ymd)) (
    PARTITION p202112 VALUES LESS THAN (202201),
    PARTITION p202212 VALUES LESS THAN (202301),
    PARTITION p202312 VALUES LESS THAN (202401),
    PARTITION p202401 VALUES LESS THAN (202402),
    PARTITION p202402 VALUES LESS THAN (202403),
    PARTITION p202403 VALUES LESS THAN (202404),
    PARTITION p202404 VALUES LESS THAN (202405),
    PARTITION p202405 VALUES LESS THAN (202406),
    PARTITION p202406 VALUES LESS THAN (202407),
    PARTITION p202407 VALUES LESS THAN (202408),
    PARTITION p202408 VALUES LESS THAN (202409),
    -- 添加其他月份的分区
    PARTITION pmax VALUES LESS THAN MAXVALUE
);


--1.3
------------------  index_a_share_insight   大A的主要指数日K
CREATE TABLE quant.ods_index_a_share_insight_now (
    htsc_code VARCHAR(50) NOT NULL,
    name VARCHAR(50) NOT NULL,
    ymd DATE NOT NULL,
    open FLOAT,
    close FLOAT,
    high FLOAT,
    low FLOAT,
    volume BIGINT,
    UNIQUE KEY unique_ymd_stock_code (ymd, htsc_code)
) ;


CREATE TABLE quant.ods_index_a_share_insight (
    htsc_code VARCHAR(50) NOT NULL,
    name VARCHAR(50) NOT NULL,
    ymd DATE NOT NULL,
    open FLOAT,
    close FLOAT,
    high FLOAT,
    low FLOAT,
    volume BIGINT,
    UNIQUE KEY unique_ymd_stock_code (ymd, htsc_code)
) PARTITION BY RANGE (YEAR(ymd) * 100 + MONTH(ymd)) (
    PARTITION p202112 VALUES LESS THAN (202201),
    PARTITION p202212 VALUES LESS THAN (202301),
    PARTITION p202312 VALUES LESS THAN (202401),
    PARTITION p202401 VALUES LESS THAN (202402),
    PARTITION p202402 VALUES LESS THAN (202403),
    PARTITION p202403 VALUES LESS THAN (202404),
    PARTITION p202404 VALUES LESS THAN (202405),
    PARTITION p202405 VALUES LESS THAN (202406),
    PARTITION p202406 VALUES LESS THAN (202407),
    PARTITION p202407 VALUES LESS THAN (202408),
    PARTITION p202408 VALUES LESS THAN (202409),
    -- 添加其他月份的分区
    PARTITION pmax VALUES LESS THAN MAXVALUE
);


--1.4
------------------  stock_limit_summary_insight   当日大A行情温度
CREATE TABLE quant.ods_stock_limit_summary_insight_now (
    ymd DATE NOT NULL,
    name VARCHAR(50) NOT NULL,
    today_ZT INT,
    today_DT INT,
    yesterday_ZT INT,
    yesterday_DT INT,
    yesterday_ZT_rate FLOAT,
    UNIQUE KEY unique_ymd_name (ymd, name)
) ;


CREATE TABLE quant.ods_stock_limit_summary_insight (
    ymd DATE NOT NULL,
    name VARCHAR(50) NOT NULL,
    today_ZT INT,
    today_DT INT,
    yesterday_ZT INT,
    yesterday_DT INT,
    yesterday_ZT_rate FLOAT,
    UNIQUE KEY unique_ymd_name (ymd, name)
) PARTITION BY RANGE (YEAR(ymd) * 100 + MONTH(ymd)) (
    PARTITION p202112 VALUES LESS THAN (202201),
    PARTITION p202212 VALUES LESS THAN (202301),
    PARTITION p202312 VALUES LESS THAN (202401),
    PARTITION p202401 VALUES LESS THAN (202402),
    PARTITION p202402 VALUES LESS THAN (202403),
    PARTITION p202403 VALUES LESS THAN (202404),
    PARTITION p202404 VALUES LESS THAN (202405),
    PARTITION p202405 VALUES LESS THAN (202406),
    PARTITION p202406 VALUES LESS THAN (202407),
    PARTITION p202407 VALUES LESS THAN (202408),
    PARTITION p202408 VALUES LESS THAN (202409),
    -- 添加其他月份的分区
    PARTITION pmax VALUES LESS THAN MAXVALUE
);



--1.5
------------------  future_inside_insight   内盘主要期货数据日K
CREATE TABLE quant.ods_future_inside_insight_now (
    htsc_code VARCHAR(50) NOT NULL,
    ymd DATE NOT NULL,
    open FLOAT,
    close FLOAT,
    high FLOAT,
    low FLOAT,
    volume BIGINT,
    open_interest BIGINT,
    settle BIGINT,
    UNIQUE KEY unique_ymd_stock_code (ymd, htsc_code)
) ;


CREATE TABLE quant.ods_future_inside_insight (
    htsc_code VARCHAR(50) NOT NULL,
    ymd DATE NOT NULL,
    open FLOAT,
    close FLOAT,
    high FLOAT,
    low FLOAT,
    volume BIGINT,
    open_interest BIGINT,
    settle BIGINT,
    UNIQUE KEY unique_ymd_stock_code (ymd, htsc_code)
) PARTITION BY RANGE (YEAR(ymd) * 100 + MONTH(ymd)) (
    PARTITION p202112 VALUES LESS THAN (202201),
    PARTITION p202212 VALUES LESS THAN (202301),
    PARTITION p202312 VALUES LESS THAN (202401),
    PARTITION p202401 VALUES LESS THAN (202402),
    PARTITION p202402 VALUES LESS THAN (202403),
    PARTITION p202403 VALUES LESS THAN (202404),
    PARTITION p202404 VALUES LESS THAN (202405),
    PARTITION p202405 VALUES LESS THAN (202406),
    PARTITION p202406 VALUES LESS THAN (202407),
    PARTITION p202407 VALUES LESS THAN (202408),
    PARTITION p202408 VALUES LESS THAN (202409),
    -- 添加其他月份的分区
    PARTITION pmax VALUES LESS THAN MAXVALUE
);



--1.6
------------------  stock_chouma_insight   A股的筹码分布数据

CREATE TABLE quant.ods_stock_chouma_insight (
    htsc_code                                VARCHAR(50) NOT NULL
   ,ymd                                      DATE NOT NULL
   ,exchange                                 VARCHAR(50)
   ,last                                     FLOAT
   ,prev_close                               FLOAT
   ,total_share                              BIGINT
   ,a_total_share                            BIGINT
   ,a_listed_share                           BIGINT
   ,listed_share                             BIGINT
   ,restricted_share                         BIGINT
   ,cost_5pct                                FLOAT
   ,cost_15pct                               FLOAT
   ,cost_50pct                               FLOAT
   ,cost_85pct                               FLOAT
   ,cost_95pct                               FLOAT
   ,avg_cost                                 FLOAT
   ,max_cost                                 FLOAT
   ,min_cost                                 FLOAT
   ,winner_rate                              FLOAT
   ,diversity                                FLOAT
   ,pre_winner_rate                          FLOAT
   ,restricted_avg_cost                      FLOAT
   ,restricted_max_cost                      FLOAT
   ,restricted_min_cost                      FLOAT
   ,large_shareholders_avg_cost              FLOAT
   ,large_shareholders_total_share           FLOAT
   ,large_shareholders_total_share_pct       FLOAT
   ,UNIQUE KEY unique_ymd_stock_code (ymd, htsc_code)
 ) PARTITION BY RANGE (YEAR(ymd) * 100 + MONTH(ymd)) (
    PARTITION p202112 VALUES LESS THAN (202201),
    PARTITION p202212 VALUES LESS THAN (202301),
    PARTITION p202312 VALUES LESS THAN (202401),
    PARTITION p202401 VALUES LESS THAN (202402),
    PARTITION p202402 VALUES LESS THAN (202403),
    PARTITION p202403 VALUES LESS THAN (202404),
    PARTITION p202404 VALUES LESS THAN (202405),
    PARTITION p202405 VALUES LESS THAN (202406),
    PARTITION p202406 VALUES LESS THAN (202407),
    PARTITION p202407 VALUES LESS THAN (202408),
    PARTITION p202408 VALUES LESS THAN (202409),
    -- 添加其他月份的分区
    PARTITION pmax VALUES LESS THAN MAXVALUE
);



--1.7
------------------  astock_industry_overview   行业分类，申万三级分类
CREATE TABLE quant.ods_astock_industry_overview (
    ymd                  DATE
   ,classified           varchar(100)
   ,industry_name        varchar(100)
   ,industry_code        varchar(100)
   ,l1_code              varchar(100)
   ,l1_name              varchar(100)
   ,l2_code              varchar(100)
   ,l2_name              varchar(100)
   ,l3_code              varchar(100)
   ,l3_name              varchar(100)
   ,UNIQUE KEY unique_industry_code (ymd, industry_code)
 );


--1.8
------------------  astock_industry_detail   股票&行业的关联
CREATE TABLE quant.ods_astock_industry_detail (
    ymd              DATE
   ,htsc_code        varchar(100)
   ,name             varchar(100)
   ,industry_name    varchar(100)
   ,industry_code    varchar(100)
   ,l1_code          varchar(100)
   ,l1_name          varchar(100)
   ,l2_code          varchar(100)
   ,l2_name          varchar(100)
   ,l3_code          varchar(100)
   ,l3_name          varchar(100)
   ,UNIQUE KEY unique_industry_code (ymd, htsc_code)
);


--1.9
------------------  shareholder_num   个股的股东数
CREATE TABLE quant.ods_shareholder_num_now (
      htsc_code              varchar(100)
     ,name                   varchar(100)
     ,ymd                    DATE
     ,total_sh               DOUBLE
     ,avg_share              DOUBLE
     ,pct_of_total_sh        DOUBLE
     ,pct_of_avg_sh          DOUBLE
   ,UNIQUE KEY unique_industry_code (ymd, htsc_code)
 );


CREATE TABLE quant.ods_shareholder_num (
      htsc_code              varchar(100)
     ,name                   varchar(100)
     ,ymd                    DATE
     ,total_sh               DOUBLE
     ,avg_share              DOUBLE
     ,pct_of_total_sh        DOUBLE
     ,pct_of_avg_sh          DOUBLE
   ,UNIQUE KEY unique_industry_code (ymd, htsc_code)
 );


--1.10
------------------  north_bound   北向持仓数据
CREATE TABLE quant.ods_north_bound_daily_now (
      htsc_code            varchar(100)
     ,ymd                  DATE
     ,sh_hkshare_hold      BIGINT
     ,pct_total_share      FLOAT
   ,UNIQUE KEY unique_industry_code (ymd, htsc_code)
 );


CREATE TABLE quant.ods_north_bound_daily (
      htsc_code            varchar(100)
     ,ymd                  DATE
     ,sh_hkshare_hold      BIGINT
     ,pct_total_share      FLOAT
   ,UNIQUE KEY unique_industry_code (ymd, htsc_code)
 );


--2.1
------------------  us_stock_daily_vantage   美股 日K
CREATE TABLE quant.ods_us_stock_daily_vantage (
    name VARCHAR(50) NOT NULL,
    ymd DATE NOT NULL,
    open FLOAT,
    high FLOAT,
    low FLOAT,
    close FLOAT,
    volume BIGINT,
    UNIQUE KEY unique_ymd_stock_code (ymd, name)
) ;



--2.2
------------------  exchange_rate_vantage_detail   汇率&美元指数 日K
CREATE TABLE quant.ods_exchange_rate_vantage_detail (
    name VARCHAR(50) NOT NULL,
    ymd DATE NOT NULL,
    open FLOAT,
    high FLOAT,
    low FLOAT,
    close FLOAT,
    UNIQUE KEY unique_ymd_stock_code (ymd, name)
) ;


CREATE TABLE quant.ods_exchange_dxy_vantage (
    ymd DATE NOT NULL,
    name VARCHAR(50) NOT NULL,
    UNIQUE KEY unique_ymd_stock_code (ymd, name)
) ;



```

--------------------------------------------------------------------------------
## datas_prepare\C00_SQL\create_mysql_tables_nopart.sql

```sql

--1.1
------------------  ods_stock_code_daily_insight   当日已上市股票码表
CREATE TABLE quant.ods_stock_code_daily_insight (
     ymd          DATE NOT NULL            --交易日期
    ,htsc_code    VARCHAR(50) NOT NULL     --股票代码
    ,name         VARCHAR(50)              --股票名
    ,exchange     VARCHAR(50)              --交易所名称
    ,UNIQUE KEY unique_ymd_stock_code (ymd, htsc_code)
);


--1.2
------------------  ods_stock_kline_daily_insight   当日已上市股票的历史日K
CREATE TABLE quant.ods_stock_kline_daily_insight_now (
     htsc_code    VARCHAR(50) NOT NULL    --股票代码
    ,ymd          DATE NOT NULL           --交易日期
    ,open         FLOAT                   --开盘价
    ,close        FLOAT                   --收盘价
    ,high         FLOAT                   --最高价
    ,low          FLOAT                   --最低价
    ,num_trades   BIGINT                  --交易笔数
    ,volume       BIGINT                  --成交量
    ,UNIQUE KEY unique_ymd_stock_code (ymd, htsc_code)
) ;


CREATE TABLE quant.ods_stock_kline_daily_insight (
     htsc_code    VARCHAR(50) NOT NULL    --股票代码
    ,ymd          DATE NOT NULL           --交易日期
    ,open         FLOAT                   --开盘价
    ,close        FLOAT                   --收盘价
    ,high         FLOAT                   --最高价
    ,low          FLOAT                   --最低价
    ,num_trades   BIGINT                  --交易笔数
    ,volume       BIGINT                  --成交量
    ,UNIQUE KEY unique_ymd_stock_code (ymd, htsc_code)
);


--1.3
------------------  ods_index_a_share_insight   大A的主要指数日K
CREATE TABLE quant.ods_index_a_share_insight_now (
     htsc_code    VARCHAR(50) NOT NULL    --指数代码
    ,name         VARCHAR(50) NOT NULL    --指数名称
    ,ymd          DATE NOT NULL           --交易日期
    ,open         FLOAT                   --开盘价
    ,close        FLOAT                   --收盘价
    ,high         FLOAT                   --最高价
    ,low          FLOAT                   --最低价
    ,volume       BIGINT                  --成交量
    ,UNIQUE KEY unique_ymd_stock_code (ymd, htsc_code)
) ;


CREATE TABLE quant.ods_index_a_share_insight (
     htsc_code    VARCHAR(50) NOT NULL    --指数代码
    ,name         VARCHAR(50) NOT NULL    --指数名称
    ,ymd          DATE NOT NULL           --交易日期
    ,open         FLOAT                   --开盘价
    ,close        FLOAT                   --收盘价
    ,high         FLOAT                   --最高价
    ,low          FLOAT                   --最低价
    ,volume       BIGINT                  --成交量
    ,UNIQUE KEY unique_ymd_stock_code (ymd, htsc_code)
);


--1.4
------------------  ods_stock_limit_summary_insight   当日大A行情温度
CREATE TABLE quant.ods_stock_limit_summary_insight_now (
     ymd          DATE NOT NULL           --日期
    ,name         VARCHAR(50) NOT NULL    --市场名称
    ,today_ZT     INT                     --今日涨停股票数
    ,today_DT     INT                     --今日跌停股票数
    ,yesterday_ZT INT                     --昨日涨停股票数
    ,yesterday_DT INT                     --昨日跌停股票数
    ,yesterday_ZT_rate FLOAT              --昨日涨停股票的今日平均涨幅
    ,UNIQUE KEY unique_ymd_name (ymd, name)
) ;


CREATE TABLE quant.ods_stock_limit_summary_insight (
     ymd          DATE NOT NULL           --日期
    ,name         VARCHAR(50) NOT NULL    --市场名称
    ,today_ZT     INT                     --今日涨停股票数
    ,today_DT     INT                     --今日跌停股票数
    ,yesterday_ZT INT                     --昨日涨停股票数
    ,yesterday_DT INT                     --昨日跌停股票数
    ,yesterday_ZT_rate FLOAT              --昨日涨停股票的今日平均涨幅
    ,UNIQUE KEY unique_ymd_name (ymd, name)
) ;


--1.5
------------------  ods_future_inside_insight   内盘主要期货数据日K
CREATE TABLE quant.ods_future_inside_insight_now (
     htsc_code      VARCHAR(50) NOT NULL  --期货标的代码
    ,ymd            DATE NOT NULL         --交易日期
    ,open           FLOAT                 --开盘价
    ,close          FLOAT                 --收盘价
    ,high           FLOAT                 --最高价
    ,low            FLOAT                 --最低价
    ,volume         BIGINT                --成交量
    ,open_interest  BIGINT
    ,settle         BIGINT
    ,UNIQUE KEY unique_ymd_stock_code (ymd, htsc_code)
) ;


CREATE TABLE quant.ods_future_inside_insight (
     htsc_code      VARCHAR(50) NOT NULL  --期货标的代码
    ,ymd            DATE NOT NULL         --交易日期
    ,open           FLOAT                 --开盘价
    ,close          FLOAT                 --收盘价
    ,high           FLOAT                 --最高价
    ,low            FLOAT                 --最低价
    ,volume         BIGINT                --成交量
    ,open_interest  BIGINT
    ,settle         BIGINT
    ,UNIQUE KEY unique_ymd_stock_code (ymd, htsc_code)
) ;


--1.6
------------------  ods_stock_chouma_insight   A股的筹码分布数据
CREATE TABLE quant.ods_stock_chouma_insight (
    htsc_code                                VARCHAR(50) NOT NULL     --证券代码
   ,ymd                                      DATE NOT NULL            --交易日
   ,exchange                                 VARCHAR(50)              --交易所
   ,last                                     FLOAT                    --最新价格
   ,prev_close                               FLOAT                    --昨收价格
   ,total_share                              BIGINT                   --总股本（股）
   ,a_total_share                            BIGINT                   --A股总数(股)
   ,a_listed_share                           BIGINT                   --流通a股（万股）
   ,listed_share                             BIGINT                   --流通股总数
   ,restricted_share                         BIGINT                   --限售股总数
   ,cost_5pct                                FLOAT                    --5分位持仓成本（持仓成本最低的 5%的持仓成本）
   ,cost_15pct                               FLOAT                    --15分位持仓成本
   ,cost_50pct                               FLOAT                    --50分位持仓成本
   ,cost_85pct                               FLOAT                    --85分位持仓成本
   ,cost_95pct                               FLOAT                    --95分位持仓成本
   ,avg_cost                                 FLOAT                    --流通股加权平均持仓成本
   ,max_cost                                 FLOAT                    --流通股最大持仓成本
   ,min_cost                                 FLOAT                    --流通股最小持仓成本
   ,winner_rate                              FLOAT                    --流通股获利胜率
   ,diversity                                FLOAT                    --流通股筹码分散程度百分比
   ,pre_winner_rate                          FLOAT                    --流通股昨日获利胜率
   ,restricted_avg_cost                      FLOAT                    --限售股平均持仓成本
   ,restricted_max_cost                      FLOAT                    --限售股最大持仓成本
   ,restricted_min_cost                      FLOAT                    --限售股最小持仓成本
   ,large_shareholders_avg_cost              FLOAT                    --大流通股股东持股平均持仓成本
   ,large_shareholders_total_share           FLOAT                    --大流通股股东持股总数
   ,large_shareholders_total_share_pct       FLOAT                    --大流通股股东持股占总股本的比例
   ,UNIQUE KEY unique_ymd_stock_code (ymd, htsc_code)
 );



--1.7
------------------  ods_astock_industry_overview   行业分类，申万三级分类
CREATE TABLE quant.ods_astock_industry_overview (
    ymd                  DATE                  --交易日期
   ,classified           varchar(100)          --行业分类
   ,industry_name        varchar(100)          --行业名称
   ,industry_code        varchar(100)          --行业代码
   ,l1_code              varchar(100)          --一级行业代码
   ,l1_name              varchar(100)          --一级行业名称
   ,l2_code              varchar(100)          --二级行业代码
   ,l2_name              varchar(100)          --二级行业名称
   ,l3_code              varchar(100)          --三级行业代码
   ,l3_name              varchar(100)          --三级行业名称
   ,UNIQUE KEY unique_industry_code (ymd, industry_code)
 );


--1.8
------------------  ods_astock_industry_detail   股票&行业的关联
CREATE TABLE quant.ods_astock_industry_detail (
    ymd              DATE                      --交易日期
   ,htsc_code        varchar(100)              --股票代码
   ,name             varchar(100)              --股票名称
   ,industry_name    varchar(100)              --行业名称
   ,industry_code    varchar(100)              --行业代码
   ,l1_code          varchar(100)              --一级行业代码
   ,l1_name          varchar(100)              --一级行业名称
   ,l2_code          varchar(100)              --二级行业代码
   ,l2_name          varchar(100)              --二级行业名称
   ,l3_code          varchar(100)              --三级行业代码
   ,l3_name          varchar(100)              --三级行业名称
   ,UNIQUE KEY unique_industry_code (ymd, htsc_code)
);


--1.9
------------------  ods_shareholder_num   个股的股东数
CREATE TABLE quant.ods_shareholder_num_now (
       htsc_code              varchar(100)            --股票代码
      ,name                   varchar(100)            --股票名称
      ,ymd                    DATE                    --交易日期
      ,total_sh               DOUBLE                  --总股东数
      ,avg_share              DOUBLE(10, 4)           --每个股东平均持股数
      ,pct_of_total_sh        DOUBLE(10, 4)           --股东数较上期环比波动百分比
      ,pct_of_avg_sh          DOUBLE(10, 4)           --每个股东平均持股数较上期环比波动百分比
      ,UNIQUE KEY unique_ymd_stock_code (ymd, htsc_code)
);

CREATE TABLE quant.ods_shareholder_num (
       htsc_code              varchar(100)            --股票代码
      ,name                   varchar(100)            --股票名称
      ,ymd                    DATE                    --交易日期
      ,total_sh               DOUBLE                  --总股东数
      ,avg_share              DOUBLE(10, 4)           --每个股东平均持股数
      ,pct_of_total_sh        DOUBLE(10, 4)           --股东数较上期环比波动百分比
      ,pct_of_avg_sh          DOUBLE(10, 4)           --每个股东平均持股数较上期环比波动百分比
      ,UNIQUE KEY unique_ymd_stock_code (ymd, htsc_code)
);


--1.10
------------------  ods_north_bound_daily   北向持仓数据
CREATE TABLE quant.ods_north_bound_daily_now (
      htsc_code            varchar(100)
     ,ymd                  DATE
     ,sh_hkshare_hold      BIGINT
     ,pct_total_share      FLOAT
     ,UNIQUE KEY unique_ymd_stock_code (ymd, htsc_code)
 );


CREATE TABLE quant.ods_north_bound_daily (
      htsc_code            varchar(100)
     ,ymd                  DATE
     ,sh_hkshare_hold      BIGINT
     ,pct_total_share      FLOAT
     ,UNIQUE KEY unique_ymd_stock_code (ymd, htsc_code)
 );


--2.1
------------------  ods_us_stock_daily_vantage   美股 日K
CREATE TABLE quant.ods_us_stock_daily_vantage (
     name     VARCHAR(50) NOT NULL          --股票名称
    ,ymd      DATE        NOT NULL          --交易日期
    ,open     FLOAT                         --开盘价
    ,high     FLOAT                         --最高价
    ,low      FLOAT                         --最低价
    ,close    FLOAT                         --收盘价
    ,volume   BIGINT                        --成交量
    ,UNIQUE KEY unique_ymd_name (ymd, name)
) ;


--2.2
------------------  ods_exchange_rate_vantage_detail   汇率&美元指数 日K
CREATE TABLE quant.ods_exchange_rate_vantage_detail (
     name      VARCHAR(50) NOT NULL         --货币对
    ,ymd       DATE        NOT NULL         --交易日期
    ,open      FLOAT                        --开盘价
    ,high      FLOAT                        --最高价
    ,low       FLOAT                        --最低价
    ,close     FLOAT                        --收盘价
    ,UNIQUE KEY unique_ymd_name (ymd, name)
) ;


--2.3
------------------  ods_exchange_dxy_vantage   美元指数 日K
CREATE TABLE quant.ods_exchange_dxy_vantage (
    ymd DATE NOT NULL,
    name VARCHAR(50) NOT NULL,
    UNIQUE KEY unique_ymd_name (ymd, name)
) ;


--3.1        通达信数据
------------------  ods_tdx_stock_concept_plate   通达信概念板块数据
CREATE TABLE quant.ods_tdx_stock_concept_plate (
     ymd DATE NOT NULL                    --日期
    ,concept_code VARCHAR(50) NOT NULL    --概念板块代码
    ,concept_name VARCHAR(50)             --概念板块名称
    ,stock_code VARCHAR(50)               --股票代码
    ,stock_name VARCHAR(50)               --股票名称
) ;


--3.2        通达信数据
------------------  ods_tdx_stock_style_plate   通达信风格板块数据
CREATE TABLE quant.ods_tdx_stock_style_plate (
     ymd DATE NOT NULL                    --日期
    ,style_code VARCHAR(50) NOT NULL    --概念板块代码
    ,style_name VARCHAR(50)             --概念板块名称
    ,stock_code VARCHAR(50)               --股票代码
    ,stock_name VARCHAR(50)               --股票名称
) ;


--3.3        通达信数据
------------------  ods_tdx_stock_industry_plate   通达信行业板块数据
CREATE TABLE quant.ods_tdx_stock_industry_plate (
     ymd DATE NOT NULL                    --日期
    ,industry_code VARCHAR(50) NOT NULL   --行业板块代码
    ,industry_name VARCHAR(50)            --行业板块名称
    ,stock_code VARCHAR(50)               --股票代码
    ,stock_name VARCHAR(50)               --股票名称
) ;


--3.4        通达信数据
------------------  ods_tdx_stock_region_plate   通达信地区板块数据
CREATE TABLE quant.ods_tdx_stock_region_plate (
     ymd         DATE NOT NULL            --日期
    ,region_code VARCHAR(50) NOT NULL     --地区板块代码
    ,region_name VARCHAR(50)              --地区板块名称
    ,stock_code  VARCHAR(50)              --股票代码
    ,stock_name  VARCHAR(50)              --股票名称
) ;


--3.5        通达信数据
------------------  ods_tdx_stock_index_plate   通达信指数板块数据
CREATE TABLE quant.ods_tdx_stock_index_plate (
     ymd         DATE NOT NULL            --日期
    ,index_code  VARCHAR(50) NOT NULL     --指数板块代码
    ,index_name  VARCHAR(50)              --指数板块名称
    ,stock_code  VARCHAR(50)              --股票代码
    ,stock_name  VARCHAR(50)              --股票名称
) ;


--3.6        通达信数据
------------------  ods_tdx_stock_pepb_info   股票基本面数据_资产数据
CREATE TABLE quant.ods_tdx_stock_pepb_info (
     ymd              DATE               --日期
    ,stock_code       varchar(50)        --代码
    ,stock_name       varchar(50)        --名称
    ,market_value     double             --流通市值(亿)
    ,total_asset      double             --总资产(亿)
    ,net_asset        double             --净资产(亿)
    ,total_capital    double             --总股本(亿)
    ,float_capital    double             --流通股(亿)
    ,shareholder_num  bigint             --股东人数
    ,pb               double             --市净率
    ,pe               double             --市盈(动)
    ,industry         varchar(50)        --细分行业
    ,UNIQUE KEY unique_ymd_stock_code (ymd, stock_code)
) ;


--4.1        多渠道板块数据 -- 小红书
------------------  ods_stock_plate_redbook
CREATE TABLE quant.ods_stock_plate_redbook (
     ymd          DATE        NOT NULL      --日期
    ,plate_name   VARCHAR(50) NOT NULL      --板块名称
    ,stock_code   VARCHAR(50)               --标的代码
    ,stock_name   VARCHAR(50)               --标的名称
    ,remark       VARCHAR(50)               --备注
) ;


--4.2        多渠道板块数据 -- 多渠道汇总
------------------  dwd_stock_a_total_plate
CREATE TABLE quant.dwd_stock_a_total_plate (
     ymd          DATE        NOT NULL      --日期
    ,plate_name   VARCHAR(50) NOT NULL      --板块名称
    ,stock_code   VARCHAR(50)               --标的代码
    ,stock_name   VARCHAR(50)               --标的名称
    ,source_table VARCHAR(50)               --来源表
    ,remark       VARCHAR(50)               --备注
) ;


--5.1        股票基本面数据_所属交易所，主板/创业板/科创板/北证
------------------  ods_stock_exchange_market
CREATE TABLE quant.ods_stock_exchange_market (
     ymd          DATE        NOT NULL      --日期
    ,stock_code   VARCHAR(50)               --标的代码
    ,stock_name   VARCHAR(50)               --标的名称
    ,market       VARCHAR(50)               --市场特征主板创业板等
    ,UNIQUE KEY unique_ymd_stock_code (ymd, stock_code)
) ;



```

--------------------------------------------------------------------------------
## datas_prepare\C01_data_download_daily\__init__.py

```python

```

--------------------------------------------------------------------------------
## datas_prepare\C01_data_download_daily\download_insight_data_afternoon.py

```python
# -*- coding: utf-8 -*-

import os
import sys
import io
import numpy as np
from insight_python.com.insight import common
from insight_python.com.insight.query import *
from insight_python.com.insight.market_service import market_service
from datetime import datetime
import contextlib

import time
import logging
import platform


import CommonProperties.Base_Properties as base_properties
import CommonProperties.Base_utils as base_utils
import CommonProperties.Mysql_Utils as mysql_utils
from CommonProperties.DateUtility import DateUtility
from CommonProperties.Base_utils import timing_decorator
from CommonProperties import set_config

# ************************************************************************
# 本代码的作用是下午收盘后下载 insight 行情源数据, 本地保存,用于后续分析
# 需要下载的数据:
# 1.上市股票代码   get_all_stocks()
# 2.筹码分布数据   get_chouma_datas()


# ************************************************************************


# ************************************************************************
#  调用日志配置
set_config.setup_logging_config()
#  调用mysql日志配置
local_user = base_properties.local_mysql_user
local_password = base_properties.local_mysql_password
local_database = base_properties.local_mysql_database
local_host = base_properties.local_mysql_host

origin_user = base_properties.origin_mysql_user
origin_password = base_properties.origin_mysql_password
origin_database = base_properties.origin_mysql_database
origin_host = base_properties.origin_mysql_host
# ************************************************************************


class SaveInsightData:

    def __init__(self):

        self.init_dirs()

        self.init_variant()

    def init_dirs(self):
        """
        关键路径初始化
        """
        #  文件路径_____insight文件基础路径
        self.dir_insight_base = base_properties.dir_insight_base

        #  文件路径_____上市交易股票codes
        self.dir_stock_codes_base = os.path.join(self.dir_insight_base, 'stock_codes')

        #  文件路径_____上市交易股票的日k线数据
        self.dir_stock_kline_base = os.path.join(self.dir_insight_base, 'stock_kline')

        #  文件路径_____关键大盘指数
        self.dir_index_a_share_base = os.path.join(self.dir_insight_base, 'index_a_share')

        #  文件路径_____涨跌停数量
        self.dir_limit_summary_base = os.path.join(self.dir_insight_base, 'limit_summary')

        #  文件路径_____内盘期货
        self.dir_future_inside_base = os.path.join(self.dir_insight_base, 'future_inside')

        #  文件路径_____筹码数据
        self.dir_chouma_base = os.path.join(self.dir_insight_base, 'chouma')

        #  文件路径_____行业分类数据_概览
        self.dir_industry_overview_base = os.path.join(self.dir_insight_base, 'industry_overview')

        #  文件路径_____行业分类数据_明细
        self.dir_industry_detail_base = os.path.join(self.dir_insight_base, 'industry_detail')

        #  文件路径_____个股的股东数_明细
        self.dir_shareholder_num_base = os.path.join(self.dir_insight_base, 'shareholder_num')

        #  文件路径_____北向持仓数据_明细
        self.dir_north_bound_base = os.path.join(self.dir_insight_base, 'north_bound')


    def init_variant(self):
        """
        结果变量初始化
        """
        #  除去 ST|退|B 的五要素   [ymd	htsc_code	name	exchange]
        self.stock_code_df = pd.DataFrame()

        #  上述stock_code 对应的日K
        self.stock_kline_df = pd.DataFrame()

        #  获得A股市场的股指 [htsc_code 	time	frequency	open	close	high	low	volume	value]
        self.index_a_share = pd.DataFrame()

        #  大盘涨跌停数量          [time	name	今日涨停	今日跌停	昨日涨停	昨日跌停	昨日涨停表现]
        self.limit_summary_df = pd.DataFrame()

        #  期货市场数据    原油  贵金属  有色
        self.future_index = pd.DataFrame()

        #  可以获取筹码的股票数据
        self.stock_chouma_available = pd.DataFrame()

        #  可以获取insight中的 行业分类 数据概览
        self.industry_overview = pd.DataFrame()

        #  可以获取insight中的 行业分类 数据明细
        self.industry_detail = pd.DataFrame()

        #  获取insight 中个股的 股东数
        self.shareholder_num_df = pd.DataFrame()

        #  获取insight 中北向的 持仓数据
        self.north_bound_df = pd.DataFrame()


    # @timing_decorator
    def login(self):
        # 登陆前 初始化，没有密码可以访问进行自动化注册
        # https://findata-insight.htsc.com:9151/terminalWeb/#/signup
        user = base_properties.user
        password = base_properties.password
        common.login(market_service, user, password)


    # @timing_decorator
    def get_stock_codes(self):
        """
        获取当日的stock代码合集
        :return:
         stock_code_df  [ymd	htsc_code	name	exchange]
        """

        #  1.获取日期
        formatted_date = DateUtility.today()
        # formatted_date = '20240930'

        #  2.请求insight数据   get_all_stocks_info
        stock_all_df = get_all_stocks_info(listing_state="上市交易")
        print(stock_all_df.shape)
        #  3.日期格式转换
        stock_all_df.insert(0, 'ymd', formatted_date)

        #  4.声明所有的列名，去除多余列
        stock_all_df = stock_all_df[['ymd', 'htsc_code', 'name', 'exchange']]
        filtered_df = stock_all_df[~stock_all_df['name'].str.contains('ST|退|B')]

        #  5.删除重复记录，只保留每组 (ymd, stock_code) 中的第一个记录
        filtered_df = filtered_df.drop_duplicates(subset=['ymd', 'htsc_code'], keep='first')

        #  6.更新dataframe ymd  htsc_code  name  exchange
        self.stock_code_df = filtered_df

        ############################   文件输出模块     ############################
        if platform.system() == "Windows":
            #  7.本地csv文件的落盘保存
            filehead = 'stocks_codes_all'
            stock_codes_listed_filename = base_utils.save_out_filename(filehead=filehead, file_type='csv')
            stock_codes_listed_dir = os.path.join(self.dir_stock_codes_base, stock_codes_listed_filename)
            filtered_df.to_csv(stock_codes_listed_dir, index=False)

            #  8.结果数据保存到 本地 mysql中
            mysql_utils.data_from_dataframe_to_mysql(user=local_user,
                                                     password=local_password,
                                                     host=local_host,
                                                     database=local_database,
                                                     df=filtered_df,
                                                     table_name="ods_stock_code_daily_insight",
                                                     merge_on=['ymd', 'htsc_code'])

            #  9.结果数据保存到 远端 mysql中
            mysql_utils.data_from_dataframe_to_mysql(user=origin_user,
                                                     password=origin_password,
                                                     host=origin_host,
                                                     database=origin_database,
                                                     df=filtered_df,
                                                     table_name="ods_stock_code_daily_insight",
                                                     merge_on=['ymd', 'htsc_code'])
        else:
            #  9.结果数据保存到 远端 mysql中
            mysql_utils.data_from_dataframe_to_mysql(user=origin_user,
                                                     password=origin_password,
                                                     host=origin_host,
                                                     database=origin_database,
                                                     df=filtered_df,
                                                     table_name="ods_stock_code_daily_insight",
                                                     merge_on=['ymd', 'htsc_code'])


    @timing_decorator
    def get_stock_kline(self):
        """
        根据当日上市的stock_codes，来获得全部(去除ST|退|B)股票的历史数据
        :return:
         stock_kline_df  [ymd	htsc_code	name	exchange]
        """

        # 1. 获取今天日期
        today = DateUtility.today()

        # 2. 计算当月15天前的日期
        if int(today[6:8]) > 15:
            time_start_date = DateUtility.next_day(-15)  # 15天前
        else:
            time_start_date = DateUtility.first_day_of_month()  # 当月1号

        # 3. 设置结束日期为今天
        time_end_date = today

        # time_start_date = '20241026'
        # time_end_date = '20241026'

        time_start_date = datetime.strptime(time_start_date, '%Y%m%d')
        time_end_date = datetime.strptime(time_end_date, '%Y%m%d')

        #  2.每个批次取 100 个元素
        batch_size = 100

        #  3.这是一个切分批次的内部函数
        def get_batches(lst, batch_size):
            for start in range(0, len(lst), batch_size):
                yield lst[start:start + batch_size]

        #  4.获取最新的stock_code 的list
        stock_code_list = mysql_utils.get_stock_codes_latest(self.stock_code_df)

        #  5.计算总批次数
        total_batches = (len(stock_code_list) + batch_size - 1) // batch_size

        #  6.kline的总和dataframe
        kline_total_df = pd.DataFrame()

        #  7.请求insight数据  get_kline
        for i, batch_list in enumerate(get_batches(stock_code_list, batch_size), start=1):
            #  一种非常巧妙的循环打印日志的方式
            sys.stdout.write(f"\r当前执行get_stock_kline的 第 {i} 次循环，总共 {total_batches} 个批次")
            sys.stdout.flush()
            time.sleep(0.01)

            res = get_kline(htsc_code=batch_list, time=[time_start_date, time_end_date], frequency="daily", fq="pre")
            kline_total_df = pd.concat([kline_total_df, res], ignore_index=True)

        sys.stdout.write("\n")

        ##  insight 返回值的非空判断
        if not kline_total_df.empty:

            #  8.日期格式转换
            kline_total_df['time'] = pd.to_datetime(kline_total_df['time']).dt.strftime('%Y%m%d')
            kline_total_df.rename(columns={'time': 'ymd'}, inplace=True)

            #  9.声明所有的列名，去除多余列
            kline_total_df = kline_total_df[
                ['htsc_code', 'ymd', 'open', 'close', 'high', 'low', 'num_trades', 'volume']]

            #  10.删除重复记录，只保留每组 (ymd, stock_code) 中的第一个记录
            kline_total_df = kline_total_df.drop_duplicates(subset=['ymd', 'htsc_code'], keep='first')

            #  11.更新dataframe
            self.stock_kline_df = kline_total_df

            ############################   文件输出模块     ############################
            if platform.system() == "Windows":
                #  12.本地csv文件的落盘保存
                stock_kline_filename = base_utils.save_out_filename(filehead='stock_kline', file_type='csv')
                stcok_kline_filedir = os.path.join(self.dir_stock_kline_base, stock_kline_filename)
                kline_total_df.to_csv(stcok_kline_filedir, index=False)

                #  13.结果数据保存到 本地 mysql中
                mysql_utils.data_from_dataframe_to_mysql(user=local_user,
                                                         password=local_password,
                                                         host=local_host,
                                                         database=local_database,
                                                         df=kline_total_df,
                                                         table_name="ods_stock_kline_daily_insight_now",
                                                         merge_on=['ymd', 'htsc_code'])

                #  14.结果数据保存到 远端 mysql中
                mysql_utils.data_from_dataframe_to_mysql(user=origin_user,
                                                         password=origin_password,
                                                         host=origin_host,
                                                         database=origin_database,
                                                         df=kline_total_df,
                                                         table_name="ods_stock_kline_daily_insight_now",
                                                         merge_on=['ymd', 'htsc_code'])
            else:
                #  14.结果数据保存到 远端 mysql中
                mysql_utils.data_from_dataframe_to_mysql(user=origin_user,
                                                         password=origin_password,
                                                         host=origin_host,
                                                         database=origin_database,
                                                         df=kline_total_df,
                                                         table_name="ods_stock_kline_daily_insight_now",
                                                         merge_on=['ymd', 'htsc_code'])
        else:
            ## insight 返回为空值
            logging.info('    get_stock_kline 的返回值为空值')


    @timing_decorator
    def get_index_a_share(self):
        """
        000001.SH    上证指数
        399002.SZ    深成指
        399006.SZ	 创业板指
        000016.SH    上证50
        000300.SH    沪深300
        000849.SH    沪深300非银行金融指数
        000905.SH	 中证500
        399852.SZ    中证1000
        000688.SH    科创50
        当月至今的指数
        Returns:
             index_a_share   [htsc_code 	time	frequency	open	close	high	low	volume	value]
        """

        #  1.当月数据的起止时间
        start_date = DateUtility.first_day_of_month()
        end_date = DateUtility.today()

        # start_date = '20240901'
        # end_date = '20240930'

        start_date = datetime.strptime(start_date, '%Y%m%d')
        end_date = datetime.strptime(end_date, '%Y%m%d')

        #  2.查询标的
        index_dict = {"000001.SH": "上证指数"
            , "399002.SZ": "深成指"
            , "399006.SZ": "创业板指"
            , "000016.SH": "上证50"
            , "000300.SH": "沪深300"
            , "000849.SH": "300非银"
            , "000905.SH": "中证500"
            , "399852.SZ": "中证1000"
            , "000688.SH": "科创50"}
        index_list = list(index_dict.keys())

        #  3.index_a_share 的总和dataframe
        index_df = pd.DataFrame()

        #  4.请求insight数据   get_kline
        res = get_kline(htsc_code=index_list, time=[start_date, end_date],
                        frequency="daily", fq="pre")
        index_df = pd.concat([index_df, res], ignore_index=True)

        ##  insight 返回值的非空判断
        if not index_df.empty:

            #  5.日期格式转换
            index_df['time'] = pd.to_datetime(index_df['time']).dt.strftime('%Y%m%d')
            index_df.rename(columns={'time': 'ymd'}, inplace=True)

            #  6.根据映射关系，添加stock_name
            index_df['name'] = index_df['htsc_code'].map(index_dict)

            #  7.声明所有的列名，去除多余列
            index_df = index_df[['htsc_code', 'name', 'ymd', 'open', 'close', 'high', 'low', 'volume']]

            #  8.删除重复记录，只保留每组 (ymd, stock_code) 中的第一个记录
            index_df = index_df.drop_duplicates(subset=['ymd', 'htsc_code'], keep='first')

            ############################   文件输出模块     ############################
            #  9.更新dataframe
            self.index_a_share = index_df

            if platform.system() == "Windows":
                #  10.本地csv文件的落盘保存
                index_filename = base_utils.save_out_filename(filehead='index_a_share', file_type='csv')
                index_filedir = os.path.join(self.dir_index_a_share_base, index_filename)
                index_df.to_csv(index_filedir, index=False)

                #  11.结果数据保存到 本地 mysql中
                mysql_utils.data_from_dataframe_to_mysql(user=local_user,
                                                         password=local_password,
                                                         host=local_host,
                                                         database=local_database,
                                                         df=index_df,
                                                         table_name="ods_index_a_share_insight_now",
                                                         merge_on=['ymd', 'htsc_code'])

                #  12.结果数据保存到 远端 mysql中
                mysql_utils.data_from_dataframe_to_mysql(user=origin_user,
                                                         password=origin_password,
                                                         host=origin_host,
                                                         database=origin_database,
                                                         df=index_df,
                                                         table_name="ods_index_a_share_insight_now",
                                                         merge_on=['ymd', 'htsc_code'])
            else:
                #  12.结果数据保存到 远端 mysql中
                mysql_utils.data_from_dataframe_to_mysql(user=origin_user,
                                                         password=origin_password,
                                                         host=origin_host,
                                                         database=origin_database,
                                                         df=index_df,
                                                         table_name="ods_index_a_share_insight_now",
                                                         merge_on=['ymd', 'htsc_code'])
        else:
            ## insight 返回为空值
            logging.info('    get_index_a_share 的返回值为空值')


    @timing_decorator
    def get_limit_summary(self):
        """
        大盘涨跌停分析数据
        Args:
            market:
                1	sh_a_share	上海A股
                2	sz_a_share	深圳A股
                3	a_share	A股
                4	a_share	B股
                5	gem	创业
                6	sme	中小板
                7	star	科创板
            trading_day: List<datetime>	交易日期范围，[start_date, end_date]

        Returns: ups_downs_limit_count_up_limits
                 ups_downs_limit_count_down_limits
                 ups_downs_limit_count_pre_up_limits
                 ups_downs_limit_count_pre_down_limits
                 ups_downs_limit_count_pre_up_limits_average_change_percent

                 [time	name	今日涨停	今日跌停	昨日涨停	昨日跌停	昨日涨停表现]

        """

        #  1.当月数据的起止时间
        start_date = DateUtility.first_day_of_month()
        end_date = DateUtility.today()

        # start_date = '20240901'
        # end_date = '20240930'

        start_date = datetime.strptime(start_date, '%Y%m%d')
        end_date = datetime.strptime(end_date, '%Y%m%d')

        #  2.请求insight数据   get_kline
        res = get_change_summary(market=["a_share"], trading_day=[start_date, end_date])

        #  3.limit_summary 的总和dataframe
        limit_summary_df = pd.DataFrame()
        limit_summary_df = pd.concat([limit_summary_df, res], ignore_index=True)

        ##  insight 返回值的非空判断
        if not limit_summary_df.empty:
            #  4.声明所有的列名，去除多余列
            limit_summary_df = limit_summary_df[['time',
                                                 'name',
                                                 'ups_downs_limit_count_up_limits',
                                                 'ups_downs_limit_count_down_limits',
                                                 'ups_downs_limit_count_pre_up_limits',
                                                 'ups_downs_limit_count_pre_down_limits',
                                                 'ups_downs_limit_count_pre_up_limits_average_change_percent']]
            limit_summary_df.columns = ['ymd', 'name', 'today_ZT', 'today_DT', 'yesterday_ZT', 'yesterday_DT',
                                        'yesterday_ZT_rate']

            #  5.日期格式转换
            limit_summary_df['ymd'] = pd.to_datetime(limit_summary_df['ymd']).dt.strftime('%Y%m%d')

            #  6.删除重复记录，只保留每组 (ymd, stock_code) 中的第一个记录
            limit_summary_df = limit_summary_df.drop_duplicates(subset=['ymd', 'name'], keep='first')

            ############################   文件输出模块     ############################
            #  7.更新dataframe
            self.limit_summary_df = limit_summary_df

            if platform.system() == "Windows":
                #  8.本地csv文件的落盘保存
                test_summary_filename = base_utils.save_out_filename(filehead='stock_limit_summary', file_type='csv')
                test_summary_dir = os.path.join(self.dir_limit_summary_base, test_summary_filename)
                limit_summary_df.to_csv(test_summary_dir, index=False)

                #  9.结果数据保存到 本地 mysql中
                mysql_utils.data_from_dataframe_to_mysql(user=local_user,
                                                         password=local_password,
                                                         host=local_host,
                                                         database=local_database,
                                                         df=limit_summary_df,
                                                         table_name="ods_stock_limit_summary_insight_now",
                                                         merge_on=['ymd', 'name'])

                #  10.结果数据保存到 远端 mysql中
                mysql_utils.data_from_dataframe_to_mysql(user=origin_user,
                                                         password=origin_password,
                                                         host=origin_host,
                                                         database=origin_database,
                                                         df=limit_summary_df,
                                                         table_name="ods_stock_limit_summary_insight_now",
                                                         merge_on=['ymd', 'name'])
            else:
                #  10.结果数据保存到 远端 mysql中
                mysql_utils.data_from_dataframe_to_mysql(user=origin_user,
                                                         password=origin_password,
                                                         host=origin_host,
                                                         database=origin_database,
                                                         df=limit_summary_df,
                                                         table_name="ods_stock_limit_summary_insight_now",
                                                         merge_on=['ymd', 'name'])
        else:
            ## insight 返回为空值
            logging.info('    get_limit_summary 的返回值为空值')


    @timing_decorator
    def get_future_inside(self):
        """
        期货市场数据
        贵金属,  有色数据
        国际市场  国内市场
        AU9999.SHF    沪金主连
        AU2409.SHF	  沪金
        AG9999.SHF    沪银主连
        AG2409.SHF    沪银
        CU9999.SHF    沪铜主连
        CU2409.SHF    沪铜

        EC9999.INE    欧线集运主连
        EC2410.INE    欧线集运
        SC9999.INE    原油主连
        SC2410.INE    原油

        V9999.DCE     PVC主连
        V2409.DCE     PVC
        MA9999.ZCE    甲醇主连      (找不到)
        MA2409.ZCE    甲醇         (找不到)
        目前主连找不到数据，只有月份的，暂时用 t+2 月去代替主连吧

        Returns:
        """
        #  1.起止时间 查询起始时间写2月前的月初第1天
        time_start_date = DateUtility.first_day_of_month(-2)
        time_end_date = DateUtility.today()

        time_start_date = datetime.strptime(time_start_date, '%Y%m%d')
        time_end_date = datetime.strptime(time_end_date, '%Y%m%d')

        #  2.查询标的
        index_list = ["AU{}.SHF", "AG{}.SHF", "CU{}.SHF", "EC{}.INE", "SC{}.INE", "V{}.DCE"]
        replacement = DateUtility.first_day_of_month(2)[2:6]

        future_index_list = [index.format(replacement) for index in index_list]

        #  3.future_inside 的总和dataframe
        future_inside_df = pd.DataFrame()

        #  4.请求insight数据   get_kline
        res = get_kline(htsc_code=future_index_list, time=[time_start_date, time_end_date],
                        frequency="daily", fq="pre")
        future_inside_df = pd.concat([future_inside_df, res], ignore_index=True)

        ##  insight 返回值的非空判断
        if not future_inside_df.empty:

            #  5.日期格式转换
            future_inside_df['time'] = pd.to_datetime(future_inside_df['time']).dt.strftime('%Y%m%d')
            future_inside_df.rename(columns={'time': 'ymd'}, inplace=True)

            #  6.声明所有的列名，去除多余列
            future_inside_df = future_inside_df[
                ['htsc_code', 'ymd', 'open', 'close', 'high', 'low', 'volume', 'open_interest', 'settle']]

            #  7.删除重复记录，只保留每组 (ymd, stock_code) 中的第一个记录
            future_inside_df = future_inside_df.drop_duplicates(subset=['ymd', 'htsc_code'], keep='first')

            ############################   文件输出模块     ############################
            #  8.更新dataframe
            self.future_index = future_inside_df

            if platform.system() == "Windows":
                #  9.本地csv文件的落盘保存
                # future_inside_df = future_inside_df.fillna(value=None)
                future_inside_df_filename = base_utils.save_out_filename(filehead='future_inside', file_type='csv')
                future_inside_df_filedir = os.path.join(self.dir_future_inside_base, future_inside_df_filename)
                future_inside_df.to_csv(future_inside_df_filedir, index=False)

                #  10.结果数据保存到 本地 mysql中
                mysql_utils.data_from_dataframe_to_mysql(user=local_user,
                                                         password=local_password,
                                                         host=local_host,
                                                         database=local_database,
                                                         df=future_inside_df,
                                                         table_name="ods_future_inside_insight_now",
                                                         merge_on=['ymd', 'htsc_code'])

                #  11.结果数据保存到 远端 mysql中
                mysql_utils.data_from_dataframe_to_mysql(user=origin_user,
                                                         password=origin_password,
                                                         host=origin_host,
                                                         database=origin_database,
                                                         df=future_inside_df,
                                                         table_name="ods_future_inside_insight_now",
                                                         merge_on=['ymd', 'htsc_code'])
            else:
                #  11.结果数据保存到 远端 mysql中
                mysql_utils.data_from_dataframe_to_mysql(user=origin_user,
                                                         password=origin_password,
                                                         host=origin_host,
                                                         database=origin_database,
                                                         df=future_inside_df,
                                                         table_name="ods_future_inside_insight_now",
                                                         merge_on=['ymd', 'htsc_code'])
        else:
            ## insight 返回为空值
            logging.info('    get_future_inside 的返回值为空值')


    @timing_decorator
    def get_chouma_datas(self):
        """
        1.获取每日的筹码分布数据
        2.找到那些当日能够拿到筹码数据的codes
        :return:
        """
        #  1.起止时间 查询起始时间写本月月初
        time_start_date = DateUtility.first_day_of_month()
        #  结束时间必须大于等于当日，这里取明天的日期
        time_end_date = DateUtility.next_day(1)

        time_start_date = datetime.strptime(time_start_date, '%Y%m%d')
        time_end_date = datetime.strptime(time_end_date, '%Y%m%d')

        #  2.每个批次取 100 个元素（原代码是1，保留你的配置）
        batch_size = 1

        #  3.这是一个切分批次的内部函数
        def get_batches(lst, batch_size):
            for start in range(0, len(lst), batch_size):
                yield lst[start:start + batch_size]

        #  4.获取最新的stock_code_list
        stock_code_list = mysql_utils.get_stock_codes_latest(self.stock_code_df)

        #  5.计算总批次数
        total_batches = (len(stock_code_list) + batch_size - 1) // batch_size

        #  6.chouma 的总和dataframe
        chouma_total_df = pd.DataFrame()

        #  7.调用insight数据  get_chip_distribution
        for i, code_list in enumerate(get_batches(stock_code_list, batch_size), start=1):
            #  一种非常巧妙的循环打印日志的方式
            valid_num = chouma_total_df.shape[0]
            sys.stdout.write(
                f"\r当前执行 get_chouma_datas  第 {i} 次循环，总共 {total_batches} 个批次, {valid_num}个有效筹码数据")
            sys.stdout.flush()
            time.sleep(0.01)

            try:
                res = get_chip_distribution(htsc_code=code_list, trading_day=[time_start_date, time_end_date])
                chouma_total_df = pd.concat([chouma_total_df, res], ignore_index=True)
            except Exception as e:
                continue
            time.sleep(0.1)

        sys.stdout.write("\n")

        ##  insight 返回值的非空判断
        if not chouma_total_df.empty:
            #  8.日期格式转换
            chouma_total_df['time'] = pd.to_datetime(chouma_total_df['time']).dt.strftime('%Y%m%d')
            chouma_total_df.rename(columns={'time': 'ymd'}, inplace=True)

            #  9.数据格式调整
            cols_to_clean = ['last', 'prev_close', 'avg_cost', 'max_cost', 'min_cost', 'winner_rate', 'diversity',
                             'pre_winner_rate', 'restricted_avg_cost', 'restricted_max_cost', 'restricted_min_cost',
                             'large_shareholders_avg_cost', 'large_shareholders_total_share_pct']

            for col in cols_to_clean:
                # ========== 核心修改1：修复inplace=True警告，合并冗余步骤 ==========
                # 原代码：先转字符串→replace(inplace)→to_numeric→fillna(inplace)→apply
                # 优化后：链式调用，一次遍历完成所有操作，去掉inplace=True
                chouma_total_df[col] = (
                    chouma_total_df[col]
                    # 转为字符串（保留原逻辑）
                    .astype(str)
                    # 替换空字符串和'nan'为NaN（去掉inplace，直接赋值）
                    .replace({'': np.nan, 'nan': np.nan})
                    # 转换为float，错误返回NaN
                    .pipe(lambda s: pd.to_numeric(s, errors='coerce'))
                    # 填充NaN为0（去掉inplace，直接赋值）
                    .fillna(0)
                    # 价格转换逻辑（保留原逻辑）
                    .apply(lambda x: round(x * 10000, 2) if x < 1 else x)
                )

            # ========== 核心修改2：修复applymap已弃用警告 ==========
            # 原代码：chouma_total_df[cols_to_clean] = chouma_total_df[cols_to_clean].applymap(lambda x: f"{x:.2f}")
            # 修复方案1：如果是多列，用apply + map（兼容所有pandas版本）
            chouma_total_df[cols_to_clean] = chouma_total_df[cols_to_clean].apply(
                lambda s: s.map(lambda x: f"{x:.2f}")
            )
            # 修复方案2：如果想保留数值类型（推荐，避免后续数据库插入的类型问题），可替换为：
            # chouma_total_df[cols_to_clean] = chouma_total_df[cols_to_clean].round(2)

            ############################   文件输出模块     ############################
            #  9.更新dataframe
            self.stock_chouma_available = chouma_total_df

            if platform.system() == "Windows":
                #  10.本地csv文件的落盘保存
                chouma_filename = base_utils.save_out_filename(filehead=f"stock_chouma", file_type='csv')
                chouma_data_filedir = os.path.join(self.dir_chouma_base, 'chouma_data', chouma_filename)
                chouma_total_df.to_csv(chouma_data_filedir, index=False)

                #  11.结果数据保存到 本地 mysql中
                mysql_utils.data_from_dataframe_to_mysql(user=local_user,
                                                         password=local_password,
                                                         host=local_host,
                                                         database=local_database,
                                                         df=chouma_total_df,
                                                         table_name="ods_stock_chouma_insight",
                                                         merge_on=['ymd', 'htsc_code'])

                #  12.结果数据保存到 远端 mysql中
                mysql_utils.data_from_dataframe_to_mysql(user=origin_user,
                                                         password=origin_password,
                                                         host=origin_host,
                                                         database=origin_database,
                                                         df=chouma_total_df,
                                                         table_name="ods_stock_chouma_insight",
                                                         merge_on=['ymd', 'htsc_code'])
            else:
                #  12.结果数据保存到 远端 mysql中
                mysql_utils.data_from_dataframe_to_mysql(user=origin_user,
                                                         password=origin_password,
                                                         host=origin_host,
                                                         database=origin_database,
                                                         df=chouma_total_df,
                                                         table_name="ods_stock_chouma_insight",
                                                         merge_on=['ymd', 'htsc_code'])
        else:
            ## insight 返回为空值
            logging.info('    get_chouma_datas 的返回值为空值')



    @timing_decorator
    def get_Ashare_industry_overview(self):
        """
        获取行业信息 申万三级 的行业信息
        :return:
         industry_overview  ['ymd', 'classified', 'industry_name', 'industry_code', 'l1_code', 'l1_name', 'l2_code', 'l2_name', 'l3_code', 'l3_name']
        """

        #  1.当月数据的起止时间
        time_today = DateUtility.today()
        # time_today = '20240930'

        time_today = datetime.strptime(time_today, '%Y%m%d')

        #  2.行业信息的总和dataframe
        industry_df = pd.DataFrame()

        #  3.请求insight 上的申万三级行业 数据
        res = get_industries(classified='sw_l3')
        industry_df = pd.concat([industry_df, res], ignore_index=True)

        ##  insight 返回值的非空判断
        if not industry_df.empty:
            #  4.日期格式转换
            industry_df.insert(0, 'ymd', time_today)
            industry_df['ymd'] = pd.to_datetime(industry_df['ymd']).dt.strftime('%Y%m%d')

            #  5.声明所有的列名，去除多余列
            industry_df = industry_df[
                ['ymd', 'classified', 'industry_name', 'industry_code', 'l1_code', 'l1_name', 'l2_code', 'l2_name',
                 'l3_code', 'l3_name']]

            #  6.删除重复记录，只保留每组 (ymd, stock_code) 中的第一个记录
            industry_df = industry_df.drop_duplicates(subset=['ymd', 'industry_code'], keep='first')

            ############################   文件输出模块     ############################
            #  7.更新dataframe
            self.industry_overview = industry_df

            if platform.system() == "Windows":
                #  8.本地csv文件的落盘保存
                sw_industry_filename = base_utils.save_out_filename(filehead='sw_industry', file_type='csv')
                sw_industry_filedir = os.path.join(self.dir_industry_overview_base, sw_industry_filename)
                industry_df.to_csv(sw_industry_filedir, index=False)

                #  9.结果数据保存到 本地 mysql中
                mysql_utils.data_from_dataframe_to_mysql(user=local_user,
                                                         password=local_password,
                                                         host=local_host,
                                                         database=local_database,
                                                         df=industry_df,
                                                         table_name="ods_astock_industry_overview",
                                                         merge_on=['ymd', 'industry_code'])

                #  10.结果数据保存到 远端 mysql中
                mysql_utils.data_from_dataframe_to_mysql(user=origin_user,
                                                         password=origin_password,
                                                         host=origin_host,
                                                         database=origin_database,
                                                         df=industry_df,
                                                         table_name="ods_astock_industry_overview",
                                                         merge_on=['ymd', 'industry_code'])
            else:
                #  10.结果数据保存到 远端 mysql中
                mysql_utils.data_from_dataframe_to_mysql(user=origin_user,
                                                         password=origin_password,
                                                         host=origin_host,
                                                         database=origin_database,
                                                         df=industry_df,
                                                         table_name="ods_astock_industry_overview",
                                                         merge_on=['ymd', 'industry_code'])
        else:
            ## insight 返回为空值
            logging.info('    get_Ashare_industry_overview 的返回值为空值')


    @timing_decorator
    def get_Ashare_industry_detail(self):
        """
        获取股票的行业信息 申万三级 的行业信息
        :return:
         industry_detail  ['ymd', 'htsc_code', 'name', 'industry_name', 'industry_code', 'l1_code', 'l1_name', 'l2_code', 'l2_name', 'l3_code', 'l3_name']
        """

        # 如果今天不是周五，跳过逻辑
        if not DateUtility.is_friday():
            logging.info("今天不是周五，跳过行业信息获取逻辑。")
            return


        #  1.当月数据的起止时间
        time_today = DateUtility.today()
        # time_today = '20240930'

        time_today = datetime.strptime(time_today, '%Y%m%d')

        #  2.行业信息的总和dataframe
        stock_in_industry_df = pd.DataFrame()

        #  3.获取最新的 stock_code 数据
        index_list = mysql_utils.get_stock_codes_latest(self.stock_code_df)

        #  4.请求insight 上的申万三级行业 数据
        i = 1                                     # 总第 i个 循环标记
        total_batches = len(index_list)           # 总批次数量

        for stock_code in index_list:

            valid_num = stock_in_industry_df.shape[0]
            sys.stdout.write(f"\r当前执行 get_Ashare_industry_detail  第 {i} 次循环，总共 {total_batches} 个批次, {valid_num}个有效股票行业数据")
            sys.stdout.flush()
            time.sleep(0.03)

            res = get_industry(htsc_code=stock_code, classified='sw')
            stock_in_industry_df = pd.concat([stock_in_industry_df, res], ignore_index=True)

            i += 1

        sys.stdout.write("\n")

        ##  insight 返回值的非空判断
        if not stock_in_industry_df.empty:
            #  5.日期格式转换
            stock_in_industry_df.insert(0, 'ymd', time_today)
            stock_in_industry_df['ymd'] = pd.to_datetime(stock_in_industry_df['ymd']).dt.strftime('%Y%m%d')

            #  6.声明所有的列名，去除多余列
            stock_in_industry_df = stock_in_industry_df[
                ['ymd', 'htsc_code', 'name', 'industry_name', 'industry_code', 'l1_code', 'l1_name', 'l2_code',
                 'l2_name', 'l3_code', 'l3_name']]

            #  7.删除重复记录，只保留每组 (ymd, stock_code) 中的第一个记录
            stock_in_industry_df = stock_in_industry_df.drop_duplicates(subset=['ymd', 'htsc_code'], keep='first')

            ############################   文件输出模块     ############################
            #  8.更新dataframe
            self.industry_detail = stock_in_industry_df

            if platform.system() == "Windows":
                #  9.本地csv文件的落盘保存
                sw_industry_filename = base_utils.save_out_filename(filehead='sw_industry', file_type='csv')
                sw_industry_filedir = os.path.join(self.dir_industry_detail_base, sw_industry_filename)
                stock_in_industry_df.to_csv(sw_industry_filedir, index=False)

                #  10.结果数据保存到 本地 mysql中
                mysql_utils.data_from_dataframe_to_mysql(user=local_user,
                                                         password=local_password,
                                                         host=local_host,
                                                         database=local_database,
                                                         df=stock_in_industry_df,
                                                         table_name="ods_astock_industry_detail",
                                                         merge_on=['ymd', 'htsc_code'])

                #  11.结果数据保存到 远端 mysql中
                mysql_utils.data_from_dataframe_to_mysql(user=origin_user,
                                                         password=origin_password,
                                                         host=origin_host,
                                                         database=origin_database,
                                                         df=stock_in_industry_df,
                                                         table_name="ods_astock_industry_detail",
                                                         merge_on=['ymd', 'htsc_code'])
            else:
                #  11.结果数据保存到 远端 mysql中
                mysql_utils.data_from_dataframe_to_mysql(user=origin_user,
                                                         password=origin_password,
                                                         host=origin_host,
                                                         database=origin_database,
                                                         df=stock_in_industry_df,
                                                         table_name="ods_astock_industry_detail",
                                                         merge_on=['ymd', 'htsc_code'])
        else:
            ## insight 返回为空值
            logging.info('    get_Ashare_industry_detail 的返回值为空值')



    @timing_decorator
    def get_shareholder_north_bound_num(self):
        """
        获取 股东数 & 北向资金情况
        Returns:
        """

        #  1.起止时间 查询起始时间写 2月前的月初
        time_start_date = DateUtility.first_day_of_month(-2)
        #  结束时间必须大于等于当日，这里取明天的日期
        time_end_date = DateUtility.next_day(1)

        time_start_date = datetime.strptime(time_start_date, '%Y%m%d')
        time_end_date = datetime.strptime(time_end_date, '%Y%m%d')

        #  2.行业信息的总和dataframe
        shareholder_num_df = pd.DataFrame()
        #  北向资金的总和dataframe
        # north_bound_df = pd.DataFrame()

        #  3.获取最新的stock_codes 数据
        code_list = mysql_utils.get_stock_codes_latest(self.stock_code_df)

        #  4.请求insight  个股股东数   数据
        #    请求insight  北向资金持仓  数据
        total_xunhuan = len(code_list)
        i = 1                       # 总循环标记

        for stock_code in code_list:
            # 屏蔽 stdout 和 stderr
            with contextlib.redirect_stdout(io.StringIO()), contextlib.redirect_stderr(io.StringIO()):
                res_shareholder = get_shareholder_num(htsc_code=stock_code, end_date=[time_start_date, time_end_date])
                # res_north_bound =get_north_bound(htsc_code=stock_code, trading_day=[time_start_date, time_end_date])

                valid_shareholder = shareholder_num_df.shape[0]
                # valid_north_bound = north_bound_df.shape[0]

            if res_shareholder is not None:
                shareholder_num_df = pd.concat([shareholder_num_df, res_shareholder], ignore_index=True)
                sys.stdout.write(f"\r当前执行 get_shareholder_num  第 {i} 次循环，总共 {total_xunhuan} 个批次, {valid_shareholder}个有效股东数据")
                sys.stdout.flush()

            time.sleep(0.03)

            i += 1

        sys.stdout.write("\n")

        ##  insight 返回值的非空判断
        if not shareholder_num_df.empty:

            #  5.日期格式转换
            shareholder_num_df.rename(columns={'end_date': 'ymd'}, inplace=True)
            shareholder_num_df['ymd'] = pd.to_datetime(shareholder_num_df['ymd']).dt.strftime('%Y%m%d')

            # north_bound_df.rename(columns={'trading_day': 'ymd'}, inplace=True)
            # north_bound_df['ymd'] = pd.to_datetime(shareholder_num_df['ymd']).dt.strftime('%Y%m%d')

            #  6.声明所有的列名，去除多余列
            shareholder_num_df = shareholder_num_df[
                ['htsc_code', 'name', 'ymd', 'total_sh', 'avg_share', 'pct_of_total_sh', 'pct_of_avg_sh']]
            # north_bound_df = north_bound_df[['htsc_code', 'ymd', 'sh_hkshare_hold', 'pct_total_share']]

            #  7.删除重复记录，只保留每组 (ymd, stock_code) 中的第一个记录
            shareholder_num_df = shareholder_num_df.drop_duplicates(subset=['ymd', 'htsc_code'], keep='first')
            # north_bound_df = north_bound_df.drop_duplicates(subset=['ymd', 'htsc_code'], keep='first')

            ############################   文件输出模块     ############################
            #  8.更新dataframe
            self.shareholder_num_df = shareholder_num_df
            # self.north_bound_df = north_bound_df

            if platform.system() == "Windows":
                #  9.本地csv文件的落盘保存
                shareholder_num_filename = base_utils.save_out_filename(filehead='shareholder_num', file_type='csv')
                shareholder_num_filedir = os.path.join(self.dir_shareholder_num_base, shareholder_num_filename)
                shareholder_num_df.to_csv(shareholder_num_filedir, index=False)

                # north_bound_filename = base_utils.save_out_filename(filehead='north_bound', file_type='csv')
                # north_bound_filedir = os.path.join(self.dir_north_bound_base, north_bound_filename)
                # north_bound_df.to_csv(north_bound_filedir, index=False)

                #  10.结果数据保存到 本地 mysql中
                mysql_utils.data_from_dataframe_to_mysql(user=local_user,
                                                         password=local_password,
                                                         host=local_host,
                                                         database=local_database,
                                                         df=shareholder_num_df,
                                                         table_name="ods_shareholder_num_now",
                                                         merge_on=['ymd', 'htsc_code'])

                # mysql_utils.data_from_dataframe_to_mysql(user=local_user,
                #                                          password=local_password,
                #                                          host=local_host,
                #                                          database=local_database,
                #                                          df=north_bound_df,
                #                                          table_name="north_bound_daily_now",
                #                                          merge_on=['ymd', 'htsc_code'])

                #  11.结果数据保存到 远端 mysql中
                mysql_utils.data_from_dataframe_to_mysql(user=origin_user,
                                                         password=origin_password,
                                                         host=origin_host,
                                                         database=origin_database,
                                                         df=shareholder_num_df,
                                                         table_name="ods_shareholder_num_now",
                                                         merge_on=['ymd', 'htsc_code'])

                # mysql_utils.data_from_dataframe_to_mysql(user=origin_user,
                #                                          password=origin_password,
                #                                          host=origin_host,
                #                                          database=origin_database,
                #                                          df=north_bound_df,
                #                                          table_name="north_bound_daily_now",
                #                                          merge_on=['ymd', 'htsc_code'])
            else:
                #  11.结果数据保存到 远端 mysql中
                mysql_utils.data_from_dataframe_to_mysql(user=origin_user,
                                                         password=origin_password,
                                                         host=origin_host,
                                                         database=origin_database,
                                                         df=shareholder_num_df,
                                                         table_name="ods_shareholder_num_now",
                                                         merge_on=['ymd', 'htsc_code'])
        else:
            ## insight 返回为空值
            logging.info('    get_shareholder_north_bound_num 的返回值为空值')



    # @timing_decorator
    def setup(self):
        #  登陆insight数据源
        self.login()

        #  除去 ST |  退  | B 的股票集合
        self.get_stock_codes()

        #  获取上述股票的当月日K
        self.get_stock_kline()

        #  获取主要股指
        self.get_index_a_share()

        #  大盘涨跌概览
        self.get_limit_summary()

        #  期货__内盘
        self.get_future_inside()

        # 筹码概览
        self.get_chouma_datas()

        # 获取A股的行业分类数据, 是行业数据
        self.get_Ashare_industry_overview()

        # 获取A股的行业分类数据, 是stock_code & industry 关联后的大表数据
        self.get_Ashare_industry_detail()

        #  个股股东数
        self.get_shareholder_north_bound_num()



if __name__ == '__main__':
    save_insight_data = SaveInsightData()
    save_insight_data.setup()

```

--------------------------------------------------------------------------------
## datas_prepare\C01_data_download_daily\download_insight_data_afternoon_of_history.py

```python
# -*- coding: utf-8 -*-

import os
import sys
import contextlib
import io
from insight_python.com.insight import common
from insight_python.com.insight.query import *
from insight_python.com.insight.market_service import market_service
from datetime import datetime
import time
import platform


import CommonProperties.Base_Properties as base_properties
import CommonProperties.Base_utils as base_utils
import CommonProperties.Mysql_Utils as mysql_utils
from CommonProperties.DateUtility import DateUtility
from CommonProperties.Base_utils import timing_decorator
from CommonProperties.set_config import setup_logging_config

# ************************************************************************
# 本代码的作用是下午收盘后下载 insight 行情源数据, 本地保存,用于后续分析
# 需要下载的数据:
# 1.上市股票代码   get_all_stocks()
# 2.筹码分布数据   get_chouma_datas()

# 调用日志配置
setup_logging_config()

# ************************************************************************

######################  mysql 配置信息  本地和远端服务器  ####################
local_user = base_properties.local_mysql_user
local_password = base_properties.local_mysql_password
local_database = base_properties.local_mysql_database
local_host = base_properties.local_mysql_host

origin_user = base_properties.origin_mysql_user
origin_password = base_properties.origin_mysql_password
origin_database = base_properties.origin_mysql_database
origin_host = base_properties.origin_mysql_host



class SaveInsightHistoryData:

    def __init__(self):

        self.init_dirs()

        self.init_variant()

    def init_dirs(self):
        """
        关键路径初始化
        """
        #  文件路径_____insight文件基础路径
        self.dir_history_insight_base = base_properties.dir_history_insight_base

        #  文件路径_____上市交易股票codes
        self.dir_history_stock_codes_base = os.path.join(self.dir_history_insight_base, 'stock_codes')

        #  文件路径_____上市交易股票的日k线数据
        self.dir_history_stock_kline_base = os.path.join(self.dir_history_insight_base, 'stock_kline')

        #  文件路径_____关键大盘指数
        self.dir_history_index_a_share_base = os.path.join(self.dir_history_insight_base, 'index_a_share')

        #  文件路径_____涨跌停数量
        self.dir_history_limit_summary_base = os.path.join(self.dir_history_insight_base, 'limit_summary')

        #  文件路径_____内盘期货
        self.dir_history_future_inside_base = os.path.join(self.dir_history_insight_base, 'future_inside')

        #  文件路径_____筹码数据
        self.dir_history_chouma_base = os.path.join(self.dir_history_insight_base, 'chouma')

        #  文件路径_____个股的股东数_明细
        self.dir_history_shareholder_num_base = os.path.join(self.dir_history_insight_base, 'shareholder_num')

        #  文件路径_____北向持仓数据_明细
        self.dir_history_north_bound_base = os.path.join(self.dir_history_insight_base, 'north_bound')


    def init_variant(self):
        """
        结果变量初始化
        """
        #  除去 ST|退|B 的五要素   [ymd	htsc_code	name	exchange]
        self.stock_code_df = pd.DataFrame()

        #  获取上述股票的历史数据   日K级别
        self.kline_total_history = pd.DataFrame()

        #  获得A股市场的股指 [htsc_code 	time	frequency	open	close	high	low	volume	value]
        self.index_a_share = pd.DataFrame()

        #  大盘涨跌停数量          [time	name	今日涨停	今日跌停	昨日涨停	昨日跌停	昨日涨停表现]
        self.limit_summary_df = pd.DataFrame()

        #  期货市场数据    原油  贵金属  有色
        self.future_index = pd.DataFrame()

        #  可以获取筹码的股票数据
        self.stock_chouma_available = ""


    @timing_decorator
    def login(self):
        # 登陆前 初始化，没有密码可以访问进行自动化注册
        # https://findata-insight.htsc.com:9151/terminalWeb/#/signup
        user = base_properties.user
        password = base_properties.password
        common.login(market_service, user, password)


    @timing_decorator
    def get_stock_codes(self):
        """
        获取当日的stock代码合集   剔除掉ST  退  B
        :return:
         stock_code_df  [ymd	htsc_code	name	exchange]
        """

        #  1.获取日期
        formatted_date = DateUtility.today()

        #  2.请求insight数据   get_all_stocks_info
        stock_all_df = get_all_stocks_info(listing_state="上市交易")

        #  3.日期格式转换
        stock_all_df.insert(0, 'ymd', formatted_date)

        #  4.声明所有的列名，去除多余列
        stock_all_df = stock_all_df[['ymd', 'htsc_code', 'name', 'exchange']]
        filtered_df = stock_all_df[~stock_all_df['name'].str.contains('ST|退|B')]

        #  5.删除重复记录，只保留每组 (ymd, stock_code) 中的第一个记录
        filtered_df = filtered_df.drop_duplicates(subset=['ymd', 'htsc_code'], keep='first')

        #  6.已上市状态stock_codes
        self.stock_code_df = filtered_df


    @timing_decorator
    def get_stock_kline(self):
        """
        根据当日上市的stock_codes，来获得全部(去除ST|退|B)股票的历史数据
        :return:
         stock_kline_df  [ymd	htsc_code	name	exchange]
        """

        #  1.历史数据的起止时间
        time_start_date = DateUtility.first_day_of_year(-3)
        time_end_date = DateUtility.today()

        time_start_date = datetime.strptime(time_start_date, '%Y%m%d')
        time_end_date = datetime.strptime(time_end_date, '%Y%m%d')

        #  2.每个批次取 40 个元素
        batch_size = 40

        #  3.这是一个切分批次的内部函数
        def get_batches(lst, batch_size):
            for start in range(0, len(lst), batch_size):
                yield lst[start:start + batch_size]

        #  4.获取最新 stock_code 的list
        stock_code_list = mysql_utils.get_stock_codes_latest(self.stock_code_df)

        #  5.计算总批次数
        total_batches = (len(stock_code_list) + batch_size - 1) // batch_size

        #  6.kline的总和dataframe
        kline_total_df = pd.DataFrame()

        #  7.请求insight数据
        for i, batch_list in enumerate(get_batches(stock_code_list, batch_size), start=1):
            #  一种非常巧妙的循环打印日志的方式
            sys.stdout.write(f"\r当前执行get_stock_kline的 第 {i} 次循环，总共 {total_batches} 个批次")
            sys.stdout.flush()
            time.sleep(0.01)

            res = get_kline(htsc_code=batch_list, time=[time_start_date, time_end_date], frequency="daily", fq="pre")
            kline_total_df = pd.concat([kline_total_df, res], ignore_index=True)

        #  8.循环结束后打印换行符，以确保后续输出在新行开始
        sys.stdout.write("\n")

        #  9.日期格式转换
        kline_total_df['time'] = pd.to_datetime(kline_total_df['time']).dt.strftime('%Y%m%d')
        kline_total_df.rename(columns={'time': 'ymd'}, inplace=True)

        #  10.声明所有的列名，去除value列
        kline_total_df = kline_total_df[['htsc_code', 'ymd', 'open', 'close', 'high', 'low', 'num_trades', 'volume']]

        #  11.删除重复记录，只保留每组 (ymd, stock_code) 中的第一个记录
        # kline_total_df = kline_total_df.drop_duplicates(subset=['ymd', 'htsc_code'], keep='first')

        #  12.文件输出模块
        self.kline_total_history = kline_total_df

        ############################   文件输出模块     ############################

        if platform.system() == "Windows":
            #  13.本地csv文件的落盘保存
            kline_total_filename = base_utils.save_out_filename(filehead='stock_kline_history', file_type='csv')
            kline_total_filedir = os.path.join(self.dir_history_stock_kline_base, kline_total_filename)
            kline_total_df.to_csv(kline_total_filedir, index=False)

            #  14.结果数据保存到 本地 mysql中
            mysql_utils.data_from_dataframe_to_mysql(user=local_user,
                                                     password=local_password,
                                                     host=local_host,
                                                     database=local_database,
                                                     df=kline_total_df,
                                                     table_name="ods_stock_kline_daily_insight",
                                                     merge_on=['ymd', 'htsc_code'])

            #  15.结果数据保存到 远端 mysql中
            mysql_utils.data_from_dataframe_to_mysql(user=origin_user,
                                                     password=origin_password,
                                                     host=origin_host,
                                                     database=origin_database,
                                                     df=kline_total_df,
                                                     table_name="ods_stock_kline_daily_insight",
                                                     merge_on=['ymd', 'htsc_code'])
        else:
            #  15.结果数据保存到 远端 mysql中
            mysql_utils.data_from_dataframe_to_mysql(user=origin_user,
                                                     password=origin_password,
                                                     host=origin_host,
                                                     database=origin_database,
                                                     df=kline_total_df,
                                                     table_name="ods_stock_kline_daily_insight",
                                                     merge_on=['ymd', 'htsc_code'])



    @timing_decorator
    def get_index_a_share(self):
        """
        000001.SH    上证指数
        399006.SZ	 创业板指
        000016.SH    上证50
        000300.SH    沪深300
        000849.SH    沪深300非银行金融指数
        000905.SH	 中证500
        399852.SZ    中证1000
        000688.SH    科创50
        899050.BJ    北证50

        Returns:
             index_a_share   [htsc_code 	time	frequency	open	close	high	low	volume	value]
        """

        #  1.当月数据的起止时间
        time_start_date = DateUtility.first_day_of_year(-3)
        time_end_date = DateUtility.today()

        time_start_date = datetime.strptime(time_start_date, '%Y%m%d')
        time_end_date = datetime.strptime(time_end_date, '%Y%m%d')

        #  2.查询标的
        index_dict = {"000001.SH": "上证指数"
            , "399002.SZ": "深成指"
            , "399006.SZ": "创业板指"
            , "000016.SH": "上证50"
            , "000300.SH": "沪深300"
            , "000849.SH": "300非银"
            , "000905.SH": "中证500"
            , "399852.SZ": "中证1000"
            , "000688.SH": "科创50"
            , "899050.BJ": "北证50"}
        index_list = list(index_dict.keys())

        #  3.index_a_share 的总和dataframe
        index_df = pd.DataFrame()

        #  4.请求insight数据   get_kline
        res = get_kline(htsc_code=index_list, time=[time_start_date, time_end_date],
                        frequency="daily", fq="pre")
        index_df = pd.concat([index_df, res], ignore_index=True)

        #  5.日期格式转换
        index_df['time'] = pd.to_datetime(index_df['time']).dt.strftime('%Y%m%d')
        index_df.rename(columns={'time': 'ymd'}, inplace=True)

        #  6.根据映射关系，添加stock_name
        index_df['name'] = index_df['htsc_code'].map(index_dict)

        #  7.声明所有的列名，去除多余列
        index_df = index_df[['htsc_code', 'name', 'ymd', 'open', 'close', 'high', 'low', 'volume']]

        #  8.删除重复记录，只保留每组 (ymd, stock_code) 中的第一个记录
        index_df = index_df.drop_duplicates(subset=['ymd', 'htsc_code'], keep='first')

        ############################   文件输出模块     ############################
        #  9.更新dataframe
        self.index_a_share = index_df

        if platform.system() == "Windows":
            #  10.本地csv文件的落盘保存
            index_filename = base_utils.save_out_filename(filehead='index_a_share_history', file_type='csv')
            index_filedir = os.path.join(self.dir_history_index_a_share_base, index_filename)
            index_df.to_csv(index_filedir, index=False)

            #  11.结果数据保存到 本地 mysql中
            mysql_utils.data_from_dataframe_to_mysql(user=local_user,
                                                     password=local_password,
                                                     host=local_host,
                                                     database=local_database,
                                                     df=index_df,
                                                     table_name="ods_index_a_share_insight",
                                                     merge_on=['ymd', 'htsc_code'])

            #  12.结果数据保存到 远端 mysql中
            mysql_utils.data_from_dataframe_to_mysql(user=origin_user,
                                                     password=origin_password,
                                                     host=origin_host,
                                                     database=origin_database,
                                                     df=index_df,
                                                     table_name="ods_index_a_share_insight",
                                                     merge_on=['ymd', 'htsc_code'])
        else:
            #  12.结果数据保存到 远端 mysql中
            mysql_utils.data_from_dataframe_to_mysql(user=origin_user,
                                                     password=origin_password,
                                                     host=origin_host,
                                                     database=origin_database,
                                                     df=index_df,
                                                     table_name="ods_index_a_share_insight",
                                                     merge_on=['ymd', 'htsc_code'])


    @timing_decorator
    def get_limit_summary(self):
        """
        大盘涨跌停分析数据
        Args:
            market:
                1	sh_a_share	上海A股
                2	sz_a_share	深圳A股
                3	a_share	A股
                4	a_share	B股
                5	gem	创业
                6	sme	中小板
                7	star	科创板
            trading_day: List<datetime>	交易日期范围，[start_date, end_date]

        Returns: ups_downs_limit_count_up_limits
                 ups_downs_limit_count_down_limits
                 ups_downs_limit_count_pre_up_limits
                 ups_downs_limit_count_pre_down_limits
                 ups_downs_limit_count_pre_up_limits_average_change_percent

                 [time	name	今日涨停	今日跌停	昨日涨停	昨日跌停	昨日涨停表现]

        """

        #  1.当月数据的起止时间
        start_date = DateUtility.first_day_of_year(-3)
        end_date = DateUtility.today()

        start_date = datetime.strptime(start_date, '%Y%m%d')
        end_date = datetime.strptime(end_date, '%Y%m%d')

        #  2.请求insight数据   get_kline
        res = get_change_summary(market=["a_share"], trading_day=[start_date, end_date])

        #  3.limit_summary 的总和dataframe
        filter_limit_df = pd.DataFrame()
        filter_limit_df = pd.concat([filter_limit_df, res], ignore_index=True)

        #  4.声明所有的列名，去除多余列
        filter_limit_df = filter_limit_df[['time',
                                     'name',
                                     'ups_downs_limit_count_up_limits',
                                     'ups_downs_limit_count_down_limits',
                                     'ups_downs_limit_count_pre_up_limits',
                                     'ups_downs_limit_count_pre_down_limits',
                                     'ups_downs_limit_count_pre_up_limits_average_change_percent']]
        filter_limit_df.columns = ['ymd', 'name', 'today_ZT', 'today_DT', 'yesterday_ZT', 'yesterday_DT',
                                   'yesterday_ZT_rate']

        #  5.日期格式转换
        filter_limit_df['ymd'] = pd.to_datetime(filter_limit_df['ymd']).dt.strftime('%Y%m%d')

        #  6.删除重复记录，只保留每组 (ymd, stock_code) 中的第一个记录
        filter_limit_df = filter_limit_df.drop_duplicates(subset=['ymd', 'name'], keep='first')

        ############################   文件输出模块     ############################
        #  7.更新dataframe
        self.limit_summary_df = filter_limit_df

        if platform.system() == "Windows":
            #  8.本地csv文件的落盘保存
            summary_filename = base_utils.save_out_filename(filehead='stock_limit_summary', file_type='csv')
            summary_dir = os.path.join(self.dir_history_limit_summary_base, summary_filename)
            filter_limit_df.to_csv(summary_dir, index=False)

            #  9.结果数据保存到 本地 mysql中
            mysql_utils.data_from_dataframe_to_mysql(user=local_user,
                                                     password=local_password,
                                                     host=local_host,
                                                     database=local_database,
                                                     df=filter_limit_df,
                                                     table_name="ods_stock_limit_summary_insight",
                                                     merge_on=['ymd', 'name'])

            #  10.结果数据保存到 远端 mysql中
            mysql_utils.data_from_dataframe_to_mysql(user=origin_user,
                                                     password=origin_password,
                                                     host=origin_host,
                                                     database=origin_database,
                                                     df=filter_limit_df,
                                                     table_name="ods_stock_limit_summary_insight",
                                                     merge_on=['ymd', 'name'])
        else:
            #  10.结果数据保存到 远端 mysql中
            mysql_utils.data_from_dataframe_to_mysql(user=origin_user,
                                                     password=origin_password,
                                                     host=origin_host,
                                                     database=origin_database,
                                                     df=filter_limit_df,
                                                     table_name="ods_stock_limit_summary_insight",
                                                     merge_on=['ymd', 'name'])

    @timing_decorator
    def get_future_inside(self):
        """
        期货市场数据
        贵金属,  有色数据
        国际市场  国内市场
        AU9999.SHF    沪金主连
        AU2409.SHF	  沪金
        AG9999.SHF    沪银主连
        AG2409.SHF    沪银
        CU9999.SHF    沪铜主连
        CU2409.SHF    沪铜

        EC9999.INE    欧线集运主连
        EC2410.INE    欧线集运
        SC9999.INE    原油主连
        SC2410.INE    原油

        V9999.DCE     PVC主连
        V2409.DCE     PVC
        MA9999.ZCE    甲醇主连      (找不到)
        MA2409.ZCE    甲醇         (找不到)
        目前主连找不到数据，只有月份的，暂时用 t+2 月去代替主连吧

        Returns:
        """

        #  1.起止时间 查询起始时间写2月前的月初第1天
        #  查询起始时间写36月前的月初第1天
        time_start_date = DateUtility.first_day_of_month(-36)
        time_end_date = DateUtility.today()

        time_start_date = datetime.strptime(time_start_date, '%Y%m%d')
        time_end_date = datetime.strptime(time_end_date, '%Y%m%d')

        #  2.查询标的
        index_list = ["AU{}.SHF", "AG{}.SHF", "CU{}.SHF", "EC{}.INE", "SC{}.INE", "V{}.DCE"]
        replacement = DateUtility.first_day_of_month(2)[2:6]

        future_index_list = [index.format(replacement) for index in index_list]

        #  3.future_inside 的总和dataframe
        future_inside_df = pd.DataFrame()

        #  4.请求insight数据   get_kline
        res = get_kline(htsc_code=future_index_list, time=[time_start_date, time_end_date],
                        frequency="daily", fq="pre")
        future_inside_df = pd.concat([future_inside_df, res], ignore_index=True)

        #  5.日期格式转换
        future_inside_df['time'] = pd.to_datetime(future_inside_df['time']).dt.strftime('%Y%m%d')
        future_inside_df.rename(columns={'time': 'ymd'}, inplace=True)

        #  6.声明所有的列名，去除多余列
        future_inside_df = future_inside_df[
            ['htsc_code', 'ymd', 'open', 'close', 'high', 'low', 'volume', 'open_interest', 'settle']]

        #  7.删除重复记录，只保留每组 (ymd, stock_code) 中的第一个记录
        future_inside_df = future_inside_df.drop_duplicates(subset=['ymd', 'htsc_code'], keep='first')

        ############################   文件输出模块     ############################
        #  8.更新dataframe
        self.future_index = future_inside_df

        if platform.system() == "Windows":
            #  9.本地csv文件的落盘保存
            future_inside_filename = base_utils.save_out_filename(filehead='future_inside', file_type='csv')
            future_inside_filedir = os.path.join(self.dir_history_future_inside_base, future_inside_filename)
            future_inside_df.to_csv(future_inside_filedir, index=False)

            #  10.结果数据保存到 本地 mysql中
            mysql_utils.data_from_dataframe_to_mysql(user=local_user,
                                                     password=local_password,
                                                     host=local_host,
                                                     database=local_database,
                                                     df=future_inside_df,
                                                     table_name="ods_future_inside_insight",
                                                     merge_on=['ymd', 'htsc_code'])

            #  11.结果数据保存到 远端 mysql中
            mysql_utils.data_from_dataframe_to_mysql(user=origin_user,
                                                     password=origin_password,
                                                     host=origin_host,
                                                     database=origin_database,
                                                     df=future_inside_df,
                                                     table_name="ods_future_inside_insight",
                                                     merge_on=['ymd', 'htsc_code'])
        else:
            #  11.结果数据保存到 远端 mysql中
            mysql_utils.data_from_dataframe_to_mysql(user=origin_user,
                                                     password=origin_password,
                                                     host=origin_host,
                                                     database=origin_database,
                                                     df=future_inside_df,
                                                     table_name="ods_future_inside_insight",
                                                     merge_on=['ymd', 'htsc_code'])


    @timing_decorator
    def get_shareholder_north_bound_num(self):
        """
        获取 股东数 & 北向资金情况
        Returns:
        """

        #  1.起止时间 查询起始时间写 36月前的月初
        time_start_date = DateUtility.first_day_of_month(-36)
        #  结束时间必须大于等于当日，这里取明天的日期
        time_end_date = DateUtility.next_day(1)

        time_start_date = datetime.strptime(time_start_date, '%Y%m%d')
        time_end_date = datetime.strptime(time_end_date, '%Y%m%d')

        #  2.行业信息的总和dataframe
        shareholder_num_df = pd.DataFrame()
        #  北向资金的总和dataframe
        north_bound_df = pd.DataFrame()

        #  3.获取最新的stock_codes 数据
        code_list = mysql_utils.get_stock_codes_latest(self.stock_code_df)

        #  4.请求insight  个股股东数   数据
        #    请求insight  北向资金持仓  数据
        total_xunhuan = len(code_list)
        i = 1                       # 总循环标记
        valid_shareholder = 1       # 个股股东数有效标记
        valid_north_bound = 1       # 北向资金持仓有效标记

        for stock_code in code_list:
            # 屏蔽 stdout 和 stderr
            with contextlib.redirect_stdout(io.StringIO()), contextlib.redirect_stderr(io.StringIO()):
                res_shareholder = get_shareholder_num(htsc_code=stock_code, end_date=[time_start_date, time_end_date])
                res_north_bound =get_north_bound(htsc_code=stock_code, trading_day=[time_start_date, time_end_date])

            if res_shareholder is not None:
                shareholder_num_df = pd.concat([shareholder_num_df, res_shareholder], ignore_index=True)
                sys.stdout.write(f"\r当前执行 get_shareholder_num  第 {i} 次循环，总共 {total_xunhuan} 个批次, {valid_shareholder}个有效股东数据")
                sys.stdout.flush()
                valid_shareholder += 1

            if res_north_bound is not None:
                north_bound_df = pd.concat([north_bound_df, res_north_bound], ignore_index=True)
                sys.stdout.write(f"\r当前执行 get_north_bound  第 {i} 次循环，总共 {total_xunhuan} 个批次, {valid_north_bound}个有效北向持仓数据")
                sys.stdout.flush()
                valid_north_bound += 1

            i += 1

        sys.stdout.write("\n")

        #  5.日期格式转换
        shareholder_num_df.rename(columns={'end_date': 'ymd'}, inplace=True)
        shareholder_num_df['ymd'] = pd.to_datetime(shareholder_num_df['ymd']).dt.strftime('%Y%m%d')

        north_bound_df.rename(columns={'trading_day': 'ymd'}, inplace=True)
        north_bound_df['ymd'] = pd.to_datetime(shareholder_num_df['ymd']).dt.strftime('%Y%m%d')

        #  6.声明所有的列名，去除多余列
        shareholder_num_df = shareholder_num_df[['htsc_code', 'name', 'ymd', 'total_sh', 'avg_share', 'pct_of_total_sh', 'pct_of_avg_sh']]
        north_bound_df = north_bound_df[['htsc_code', 'ymd', 'sh_hkshare_hold', 'pct_total_share']]

        #  7.删除重复记录，只保留每组 (ymd, stock_code) 中的第一个记录
        shareholder_num_df = shareholder_num_df.drop_duplicates(subset=['ymd', 'htsc_code'], keep='first')
        north_bound_df = north_bound_df.drop_duplicates(subset=['ymd', 'htsc_code'], keep='first')

        ############################   文件输出模块     ############################
        #  8.更新dataframe
        self.shareholder_num_df = shareholder_num_df
        self.north_bound_df = north_bound_df

        if platform.system() == "Windows":
            #  9.本地csv文件的落盘保存
            shareholder_num_filename = base_utils.save_out_filename(filehead='shareholder_num', file_type='csv')
            shareholder_num_filedir = os.path.join(self.dir_history_north_bound_base, shareholder_num_filename)
            shareholder_num_df.to_csv(shareholder_num_filedir, index=False)

            north_bound_filename = base_utils.save_out_filename(filehead='north_bound', file_type='csv')
            north_bound_filedir = os.path.join(self.dir_history_north_bound_base, north_bound_filename)
            north_bound_df.to_csv(north_bound_filedir, index=False)

            #  10.结果数据保存到 本地 mysql中
            mysql_utils.data_from_dataframe_to_mysql(user=local_user,
                                                     password=local_password,
                                                     host=local_host,
                                                     database=local_database,
                                                     df=shareholder_num_df,
                                                     table_name="ods_shareholder_num",
                                                     merge_on=['ymd', 'htsc_code'])

            mysql_utils.data_from_dataframe_to_mysql(user=local_user,
                                                     password=local_password,
                                                     host=local_host,
                                                     database=local_database,
                                                     df=north_bound_df,
                                                     table_name="ods_north_bound_daily",
                                                     merge_on=['ymd', 'htsc_code'])

            #  11.结果数据保存到 远端 mysql中
            mysql_utils.data_from_dataframe_to_mysql(user=origin_user,
                                                     password=origin_password,
                                                     host=origin_host,
                                                     database=origin_database,
                                                     df=shareholder_num_df,
                                                     table_name="ods_shareholder_num",
                                                     merge_on=['ymd', 'htsc_code'])

            mysql_utils.data_from_dataframe_to_mysql(user=origin_user,
                                                     password=origin_password,
                                                     host=origin_host,
                                                     database=origin_database,
                                                     df=north_bound_df,
                                                     table_name="ods_north_bound_daily",
                                                     merge_on=['ymd', 'htsc_code'])
        else:
            #  11.结果数据保存到 远端 mysql中
            mysql_utils.data_from_dataframe_to_mysql(user=origin_user,
                                                     password=origin_password,
                                                     host=origin_host,
                                                     database=origin_database,
                                                     df=shareholder_num_df,
                                                     table_name="ods_shareholder_num",
                                                     merge_on=['ymd', 'htsc_code'])

            mysql_utils.data_from_dataframe_to_mysql(user=origin_user,
                                                     password=origin_password,
                                                     host=origin_host,
                                                     database=origin_database,
                                                     df=north_bound_df,
                                                     table_name="ods_north_bound_daily",
                                                     merge_on=['ymd', 'htsc_code'])



    @timing_decorator
    def setup(self):
        #  登陆insight数据源
        self.login()

        #  除去 ST |  退  | B 的股票集合
        self.get_stock_codes()

        #  获取当前已上市股票过去3年到今天的历史kline
        self.get_stock_kline()

        #  获取主要股指
        self.get_index_a_share()

        #  大盘涨跌概览
        self.get_limit_summary()

        #  期货__内盘
        self.get_future_inside()

        #  个股股东数
        self.get_shareholder_north_bound_num()


if __name__ == '__main__':
    save_insight_data = SaveInsightHistoryData()
    save_insight_data.setup()

```

--------------------------------------------------------------------------------
## datas_prepare\C01_data_download_daily\download_vantage_data_afternoon.py

```python
# -*- coding: utf-8 -*-

import pandas as pd
import requests
import platform
# from yahoo_fin.stock_info import *
from io import StringIO
import os
import logging


from CommonProperties.DateUtility import DateUtility
import CommonProperties.Base_Properties as base_properties
import CommonProperties.Base_utils as base_utils
import CommonProperties.Mysql_Utils as mysql_utils
from CommonProperties.Base_utils import timing_decorator
from CommonProperties.set_config import setup_logging_config


# 配置日志处理器
# 调用日志配置
setup_logging_config()

#  vantage  测试环境文件保存目录
vantage_test_dir = os.path.join(base_properties.dir_vantage_base, 'test')


api_key = 'ICTN 9 P9 ES 00 EADUF'
# api_key = 'BI8JFEOOP3C563PO'
key_US_stock = ['TSLA', 'AAPL', 'NVDA', 'MSFT', 'META']

# 构建 API 请求 URL
base_url = 'https://www.alphavantage.co/query'
headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3'
}



######################  mysql 配置信息  本地和远端服务器  ####################
local_user = base_properties.local_mysql_user
local_password = base_properties.local_mysql_password
local_database = base_properties.local_mysql_database
local_host = base_properties.local_mysql_host

origin_user = base_properties.origin_mysql_user
origin_password = base_properties.origin_mysql_password
origin_database = base_properties.origin_mysql_database
origin_host = base_properties.origin_mysql_host



class SaveVantageData:
    def __init__(self):
        self.init_dirs()
        self.init_variant()

    def init_dirs(self):
        """
        关键路径初始化
        """
        #  文件路径_____vantage 文件基础路径
        self.dir_vantage_base = base_properties.dir_vantage_base

        #  文件路径_____US 的 stock
        self.dir_US_stock_base = os.path.join(self.dir_vantage_base, 'US_stock')

        #  文件路径_____USD 的 汇率明细
        self.dir_USD_FX_detail_base = os.path.join(self.dir_vantage_base, 'USD_FX_detail')

        #  文件路径_____USD 的 美元指数
        self.dir_USD_FX_base = os.path.join(self.dir_vantage_base, 'USD_FX')


    def init_variant(self):
        """
        结果变量初始化
        """
        #  关键的stock_code
        self.key_US_stock = ['TSLA', 'AAPL', 'NVDA', 'MSFT', 'META']

        #  获得US核心stock  [name, timestamp  open  high  low   close   volume]
        self.vantage_US_stock = pd.DataFrame()


    @timing_decorator
    def get_US_stock_from_vantage(self):
        """
        关键 US stcok
        Returns:
            [name, timestamp  open  high  low   close   volume]
        """

        function = 'TIME_SERIES_DAILY'
        res_df = pd.DataFrame()

        for symbol in self.key_US_stock:
            url = f'{base_url}?function={function}&symbol={symbol}&apikey={api_key}&outputsize=full&datatype=csv'
            # 发送 GET 请求
            response = requests.get(url, headers=headers, timeout=10)

            # 处理响应数据
            if response.status_code == 200:
                # 返回csv字符串
                csv_string = response.text
                csv_file = StringIO(csv_string)
                vantage_df = pd.read_csv(csv_file)
                vantage_df.insert(0, 'name', symbol)

                res_df = pd.concat([res_df, vantage_df], ignore_index=True)
            else:
                print(f'Error fetching {symbol} data: {response.status_code} - {response.text}')

        #  8.日期格式转换
        res_df['timestamp'] = pd.to_datetime(res_df['timestamp']).dt.strftime('%Y%m%d')
        res_df.rename(columns={'timestamp': 'ymd'}, inplace=True)

        ############################   文件输出模块     ############################
        if platform.system() == "Windows":
            US_stock_filename = base_utils.save_out_filename(filehead='US_stock', file_type='csv')
            US_stock_filedir = os.path.join(self.dir_US_stock_base, US_stock_filename)
            res_df.to_csv(US_stock_filedir, index=False)

            #  结果数据保存到 本地 mysql中
            mysql_utils.data_from_dataframe_to_mysql(user=local_user,
                                                     password=local_password,
                                                     host=local_host,
                                                     database=local_database,
                                                     df=res_df,
                                                     table_name="ods_us_stock_daily_vantage",
                                                     merge_on=['ymd', 'name'])

            #  结果数据保存到 远端 mysql中
            mysql_utils.data_from_dataframe_to_mysql(user=origin_user,
                                                     password=origin_password,
                                                     host=origin_host,
                                                     database=origin_database,
                                                     df=res_df,
                                                     table_name="ods_us_stock_daily_vantage",
                                                     merge_on=['ymd', 'name'])
        else:
            #  结果数据保存到 远端 mysql中
            mysql_utils.data_from_dataframe_to_mysql(user=origin_user,
                                                     password=origin_password,
                                                     host=origin_host,
                                                     database=origin_database,
                                                     df=res_df,
                                                     table_name="ods_us_stock_daily_vantage",
                                                     merge_on=['ymd', 'name'])




    def get_USD_FX_core(self, url, flag):
        """
        Args:
            url: 请求的URL地址
            flag: 数据标识符
        Returns:
            返回包含汇率数据的DataFrame
        """
        # 存放汇率结果数据
        res_df = pd.DataFrame()

        # 使用带重试功能的请求
        response = base_utils.get_with_retries(url, headers=headers, timeout=10)

        # 处理响应数据
        if response is not None and response.status_code == 200:
            # 返回csv字符串
            csv_string = response.text
            csv_file = StringIO(csv_string)
            vantage_df = pd.read_csv(csv_file)
            vantage_df.insert(0, 'name', flag)

            res_df = pd.concat([res_df, vantage_df], ignore_index=True)
        else:
            logging.error(f'Error fetching {flag} data: 请求失败或无效的响应')

        logging.info(f"get_USD_FX_core 完成 {flag} 汇率查询")

        return res_df


    @timing_decorator
    def get_USD_FX_from_vantage(self):
        """
        计算美元指数, 从主流货币去计算美元指数
        Returns:
            [name, timestamp  open  high  low   close   volume]
        """
        function = 'FX_DAILY'

        #  存放汇率数据
        res_df = pd.DataFrame()

        # 定义权重
        weights = {
            'EUR_USD': -0.576,
            'USD_JPY': 0.136,
            'GBP_USD': -0.119,
            'USD_CAD': 0.091,
            'USD_SEK': 0.042,
            'USD_CHF': 0.036
        }

        # 定义初始常数
        constant = 50.14348112

        #  --------------------------  开始计算美元指数  ------------------------------
        #  欧元兑美元
        url_EUR_USD = f'{base_url}?function={function}&from_symbol=EUR&to_symbol=USD&apikey={api_key}&datatype=csv'
        df_EUR_USD = self.get_USD_FX_core(url=url_EUR_USD, flag='EUR_USD')

        #  美元兑日元
        url_USD_JPY = f'{base_url}?function={function}&from_symbol=USD&to_symbol=JPY&apikey={api_key}&datatype=csv'
        df_USD_JPY = self.get_USD_FX_core(url=url_USD_JPY, flag='USD_JPY')

        #  英镑兑美元
        url_GBP_USD = f'{base_url}?function={function}&from_symbol=GBP&to_symbol=USD&apikey={api_key}&datatype=csv'
        df_GBP_USD = self.get_USD_FX_core(url=url_GBP_USD, flag='GBP_USD')

        #  美元兑加拿大元
        url_USD_CAD = f'{base_url}?function={function}&from_symbol=USD&to_symbol=CAD&apikey={api_key}&datatype=csv'
        df_USD_CAD = self.get_USD_FX_core(url=url_USD_CAD, flag='USD_CAD')

        #  美元兑瑞典克朗
        url_USD_SEK = f'{base_url}?function={function}&from_symbol=USD&to_symbol=SEK&apikey={api_key}&datatype=csv'
        df_USD_SEK = self.get_USD_FX_core(url=url_USD_SEK, flag='USD_SEK')

        #  美元兑瑞士法郎
        url_USD_CHF = f'{base_url}?function={function}&from_symbol=USD&to_symbol=CHF&apikey={api_key}&datatype=csv'
        df_USD_CHF = self.get_USD_FX_core(url=url_USD_CHF, flag='USD_CHF')

        #  汇总得到美元指数的主要成分
        res_df = pd.concat([res_df, df_EUR_USD, df_USD_JPY, df_GBP_USD, df_USD_CAD, df_USD_SEK, df_USD_CHF], ignore_index=True)

        #  日期格式转换
        res_df['timestamp'] = pd.to_datetime(res_df['timestamp']).dt.strftime('%Y%m%d')
        res_df.rename(columns={'timestamp': 'ymd'}, inplace=True)

        if platform.system() == "Windows":
            ##  文件输出模块     输出汇率明细
            USD_FX_detail_filename = base_utils.save_out_filename(filehead='USD_FX_detail', file_type='csv')
            USD_FX_detail_filedir = os.path.join(self.dir_USD_FX_detail_base, USD_FX_detail_filename)
            res_df.to_csv(USD_FX_detail_filedir, index=False)

            #  将汇率明细写入 本地 mysql
            mysql_utils.data_from_dataframe_to_mysql(user=local_user,
                                                     password=local_password,
                                                     host=local_host,
                                                     database=local_database,
                                                     df=res_df,
                                                     table_name="ods_exchange_rate_vantage_detail",
                                                     merge_on=["ymd", "name"])

            #  将汇率明细写入 远端 mysql
            mysql_utils.data_from_dataframe_to_mysql(user=origin_user,
                                                     password=origin_password,
                                                     host=origin_host,
                                                     database=origin_database,
                                                     df=res_df,
                                                     table_name="ods_exchange_rate_vantage_detail",
                                                     merge_on=["ymd", "name"])
        else:
            #  将汇率明细写入 远端 mysql
            mysql_utils.data_from_dataframe_to_mysql(user=origin_user,
                                                     password=origin_password,
                                                     host=origin_host,
                                                     database=origin_database,
                                                     df=res_df,
                                                     table_name="ods_exchange_rate_vantage_detail",
                                                     merge_on=["ymd", "name"])


        #  --------------------------  开始计算美元指数  ------------------------------
        # 获取唯一的时间戳
        timestamps = res_df['ymd'].unique()

        # 创建一个空列表来存储结果
        results = []

        for timestamp in timestamps:
            # 获取当前时间戳的所有汇率数据
            current_data = res_df[res_df['ymd'] == timestamp]
            if current_data.shape[0] != 6:
                break

            # 初始化DXY值
            dxy = constant
            # 计算DXY
            for name, weight in weights.items():
                rate = current_data[current_data['name'] == name]['close'].values[0]
                dxy *= rate ** weight
            # 将结果添加到列表中
            results.append([timestamp, dxy])

        # 将结果转换为DataFrame
        dxy_df = pd.DataFrame(results, columns=['ymd', 'DXY'])

        if platform.system() == "Windows":
            ##  文件输出模块     输出美元指数
            USD_FX_filename = base_utils.save_out_filename(filehead='USD_FX', file_type='csv')
            USD_FX_filedir = os.path.join(self.dir_USD_FX_base, USD_FX_filename)
            dxy_df.to_csv(USD_FX_filedir, index=False)

            #  将汇率明细写入 本地 mysql
            mysql_utils.data_from_dataframe_to_mysql(user=local_user,
                                                     password=local_password,
                                                     host=local_host,
                                                     database=local_database,
                                                     df=dxy_df,
                                                     table_name="ods_exchange_dxy_vantage",
                                                     merge_on=["ymd", "name"])

            #  将汇率明细写入 远端 mysql
            mysql_utils.data_from_dataframe_to_mysql(user=origin_user,
                                                     password=origin_password,
                                                     host=origin_host,
                                                     database=origin_database,
                                                     df=dxy_df,
                                                     table_name="ods_exchange_dxy_vantage",
                                                     merge_on=["ymd", "name"])
        else:
            #  将汇率明细写入 远端 mysql
            mysql_utils.data_from_dataframe_to_mysql(user=origin_user,
                                                     password=origin_password,
                                                     host=origin_host,
                                                     database=origin_database,
                                                     df=dxy_df,
                                                     table_name="ods_exchange_dxy_vantage",
                                                     merge_on=["ymd", "name"])


    @timing_decorator
    def setup(self):

        #  获取 US 主要stock 的全部数据
        # self.get_US_stock_from_vantage()
        self.get_USD_FX_from_vantage()


if __name__ == '__main__':
    save_vantage_data = SaveVantageData()
    save_vantage_data.setup()



```

--------------------------------------------------------------------------------
## datas_prepare\C02_data_merge\__init__.py

```python

```

--------------------------------------------------------------------------------
## datas_prepare\C02_data_merge\merge_insight_data_afternoon.py

```python
# -*- coding: utf-8 -*-

import os
from datetime import datetime

import pandas as pd
from sqlalchemy import create_engine, text
import time
import platform

# import dataprepare_properties
# import dataprepare_utils
from CommonProperties import Base_Properties
import CommonProperties.Base_utils as base_utils
from CommonProperties.DateUtility import DateUtility
from CommonProperties.Base_utils import timing_decorator
import CommonProperties.Mysql_Utils as mysql_utils
from CommonProperties import set_config

# ************************************************************************
# 本代码的作用是下午收盘后针对 insight 行情源数据的本地保存部分开展merge
# 需要下载的数据:
# 1.上市股票代码   get_all_stocks()
# 2.筹码分布数据   get_chouma_datas()


# ************************************************************************
#  调用日志配置
set_config.setup_logging_config()

######################  mysql 配置信息  本地和远端服务器  ####################
local_user = Base_Properties.local_mysql_user
local_password = Base_Properties.local_mysql_password
local_database = Base_Properties.local_mysql_database
local_host = Base_Properties.local_mysql_host

origin_user = Base_Properties.origin_mysql_user
origin_password = Base_Properties.origin_mysql_password
origin_database = Base_Properties.origin_mysql_database
origin_host = Base_Properties.origin_mysql_host





class MergeInsightData:

    def __init__(self):
        pass


    @timing_decorator
    def merge_stock_kline(self):
        """
        将 stock_kline 的历史数据和当月数据做merge
        :return:
         stock_kline_df  [ymd	htsc_code	name	exchange]
        """
        source_table = 'ods_stock_kline_daily_insight_now'
        target_table = 'ods_stock_kline_daily_insight'
        columns = ['htsc_code', 'ymd', 'open', 'close', 'high', 'low', 'num_trades', 'volume']
        ############################   文件输出模块     ############################
        if platform.system() == "Windows":
            # 对本地 Mysql 做数据聚合
            mysql_utils.upsert_table(user=local_user,
                                     password=local_password,
                                     host=local_host,
                                     database=local_database,
                                     source_table=source_table,
                                     target_table=target_table,
                                     columns=columns)

            # 对远端 Mysql 做数据聚合
            mysql_utils.upsert_table(user=origin_user,
                                     password=origin_password,
                                     host=origin_host,
                                     database=origin_database,
                                     source_table=source_table,
                                     target_table=target_table,
                                     columns=columns)
        else:
            # 对远端 Mysql 做数据聚合
            mysql_utils.upsert_table(user=origin_user,
                                     password=origin_password,
                                     host=origin_host,
                                     database=origin_database,
                                     source_table=source_table,
                                     target_table=target_table,
                                     columns=columns)


    @timing_decorator
    def merge_index_a_share(self):
        """
        000001.SH    上证指数
        399006.SZ	 创业板指
        000016.SH    上证50
        000300.SH    沪深300
        000849.SH    沪深300非银行金融指数
        000905.SH	 中证500
        399852.SZ    中证1000
        000688.SH    科创50

        Returns:
             index_a_share   [htsc_code 	time	frequency	open	close	high	low	volume	value]
        """
        source_table = 'ods_index_a_share_insight_now'
        target_table = 'ods_index_a_share_insight'
        columns = ['htsc_code', 'name', 'ymd', 'open', 'close', 'high', 'low', 'volume']
        ############################   文件输出模块     ############################
        if platform.system() == "Windows":
            # 对本地 Mysql 做数据聚合
            mysql_utils.upsert_table(user=local_user,
                                     password=local_password,
                                     host=local_host,
                                     database=local_database,
                                     source_table=source_table,
                                     target_table=target_table,
                                     columns=columns)

            # 对远端 Mysql 做数据聚合
            mysql_utils.upsert_table(user=origin_user,
                                     password=origin_password,
                                     host=origin_host,
                                     database=origin_database,
                                     source_table=source_table,
                                     target_table=target_table,
                                     columns=columns)
        else:
            # 对远端 Mysql 做数据聚合
            mysql_utils.upsert_table(user=origin_user,
                                     password=origin_password,
                                     host=origin_host,
                                     database=origin_database,
                                     source_table=source_table,
                                     target_table=target_table,
                                     columns=columns)


    @timing_decorator
    def merge_limit_summary(self):
        """
        大盘涨跌停分析数据
        Args:
            market:
                1	sh_a_share	上海A股
                2	sz_a_share	深圳A股
                3	a_share	A股
                4	a_share	B股
                5	gem	创业
                6	sme	中小板
                7	star	科创板
            trading_day: List<datetime>	交易日期范围，[start_date, end_date]

        Returns: ups_downs_limit_count_up_limits
                 ups_downs_limit_count_down_limits
                 ups_downs_limit_count_pre_up_limits
                 ups_downs_limit_count_pre_down_limits
                 ups_downs_limit_count_pre_up_limits_average_change_percent

                 [time	name	今日涨停	今日跌停	昨日涨停	昨日跌停	昨日涨停表现]
        """
        source_table = 'ods_stock_limit_summary_insight_now'
        target_table = 'ods_stock_limit_summary_insight'
        columns = ['ymd', 'name', 'today_ZT', 'today_DT', 'yesterday_ZT', 'yesterday_DT', 'yesterday_ZT_rate']
        ############################   文件输出模块     ############################
        if platform.system() == "Windows":
            # 对本地 Mysql 做数据聚合
            mysql_utils.upsert_table(user=local_user,
                                     password=local_password,
                                     host=local_host,
                                     database=local_database,
                                     source_table=source_table,
                                     target_table=target_table,
                                     columns=columns)

            # 对远端 Mysql 做数据聚合
            mysql_utils.upsert_table(user=origin_user,
                                     password=origin_password,
                                     host=origin_host,
                                     database=origin_database,
                                     source_table=source_table,
                                     target_table=target_table,
                                     columns=columns)
        else:
            # 对远端 Mysql 做数据聚合
            mysql_utils.upsert_table(user=origin_user,
                                     password=origin_password,
                                     host=origin_host,
                                     database=origin_database,
                                     source_table=source_table,
                                     target_table=target_table,
                                     columns=columns)


    @timing_decorator
    def merge_future_inside(self):
        """
        期货市场数据
        贵金属,  有色数据
        国际市场  国内市场
        AU9999.SHF    沪金主连
        AU2409.SHF	  沪金
        AG9999.SHF    沪银主连
        AG2409.SHF    沪银
        CU9999.SHF    沪铜主连
        CU2409.SHF    沪铜

        EC9999.INE    欧线集运主连
        EC2410.INE    欧线集运
        SC9999.INE    原油主连
        SC2410.INE    原油

        V9999.DCE     PVC主连
        V2409.DCE     PVC
        MA9999.ZCE    甲醇主连      (找不到)
        MA2409.ZCE    甲醇         (找不到)
        目前主连找不到数据，只有月份的，暂时用 t+2 月去代替主连吧

        Returns:
        """
        source_table = 'ods_future_inside_insight_now'
        target_table = 'ods_future_inside_insight'
        columns = ['htsc_code', 'ymd', 'open', 'close', 'high', 'low', 'volume', 'open_interest', 'settle']
        ############################   文件输出模块     ############################
        if platform.system() == "Windows":
            # 对本地 Mysql 做数据聚合
            mysql_utils.upsert_table(user=local_user,
                                     password=local_password,
                                     host=local_host,
                                     database=local_database,
                                     source_table=source_table,
                                     target_table=target_table,
                                     columns=columns)

            # 对远端 Mysql 做数据聚合
            mysql_utils.upsert_table(user=origin_user,
                                     password=origin_password,
                                     host=origin_host,
                                     database=origin_database,
                                     source_table=source_table,
                                     target_table=target_table,
                                     columns=columns)
        else:
            # 对远端 Mysql 做数据聚合
            mysql_utils.upsert_table(user=origin_user,
                                     password=origin_password,
                                     host=origin_host,
                                     database=origin_database,
                                     source_table=source_table,
                                     target_table=target_table,
                                     columns=columns)


    @timing_decorator
    def merge_shareholder_num(self):
        """
        A股市场的股东数
        Returns:
        """
        source_table = 'ods_shareholder_num_now'
        target_table = 'ods_shareholder_num'
        columns = ['htsc_code', 'name', 'ymd', 'total_sh', 'avg_share', 'pct_of_total_sh', 'pct_of_avg_sh']
        ############################   文件输出模块     ############################
        if platform.system() == "Windows":
            # 对本地 Mysql 做数据聚合
            mysql_utils.upsert_table(user=local_user,
                                     password=local_password,
                                     host=local_host,
                                     database=local_database,
                                     source_table=source_table,
                                     target_table=target_table,
                                     columns=columns)

            # 对远端 Mysql 做数据聚合
            mysql_utils.upsert_table(user=origin_user,
                                     password=origin_password,
                                     host=origin_host,
                                     database=origin_database,
                                     source_table=source_table,
                                     target_table=target_table,
                                     columns=columns)
        else:
            # 对远端 Mysql 做数据聚合
            mysql_utils.upsert_table(user=origin_user,
                                     password=origin_password,
                                     host=origin_host,
                                     database=origin_database,
                                     source_table=source_table,
                                     target_table=target_table,
                                     columns=columns)


    @timing_decorator
    def merge_north_bound(self):
        """
        A股市场的北向资金数据
        Returns:
        """
        source_table = 'ods_north_bound_daily_now'
        target_table = 'ods_north_bound_daily'
        columns = ['htsc_code', 'ymd', 'sh_hkshare_hold', 'pct_total_share']
        ############################   文件输出模块     ############################
        if platform.system() == "Windows":
            # 对本地 Mysql 做数据聚合
            mysql_utils.upsert_table(user=local_user,
                                     password=local_password,
                                     host=local_host,
                                     database=local_database,
                                     source_table=source_table,
                                     target_table=target_table,
                                     columns=columns)

            # 对远端 Mysql 做数据聚合
            mysql_utils.upsert_table(user=origin_user,
                                     password=origin_password,
                                     host=origin_host,
                                     database=origin_database,
                                     source_table=source_table,
                                     target_table=target_table,
                                     columns=columns)
        else:
            # 对远端 Mysql 做数据聚合
            mysql_utils.upsert_table(user=origin_user,
                                     password=origin_password,
                                     host=origin_host,
                                     database=origin_database,
                                     source_table=source_table,
                                     target_table=target_table,
                                     columns=columns)


    def setup(self):

        #  获取当前已上市股票过去3年到今天的历史kline
        self.merge_stock_kline()

        #  获取主要股指
        self.merge_index_a_share()

        #  大盘涨跌概览
        self.merge_limit_summary()

        #  期货__内盘
        self.merge_future_inside()

        #  股东数
        self.merge_shareholder_num()

        #  北向
        #self.merge_north_bound()



if __name__ == '__main__':
    save_insight_data = MergeInsightData()
    save_insight_data.setup()

```

--------------------------------------------------------------------------------
## datas_prepare\C03_data_DWD\__init__.py

```python

```

--------------------------------------------------------------------------------
## datas_prepare\C03_data_DWD\calculate_DWD_datas.py

```python
# -*- coding: utf-8 -*-

import os
from datetime import datetime

import pandas as pd
from sqlalchemy import create_engine, text
import time
import platform
import logging


# import dataprepare_properties
# import dataprepare_utils
from CommonProperties import Base_Properties
import CommonProperties.Base_utils as base_utils
from CommonProperties.DateUtility import DateUtility
from CommonProperties.Base_utils import timing_decorator

import CommonProperties.Mysql_Utils as mysql_utils

from CommonProperties import set_config

# ************************************************************************
# 本代码的作用是下午收盘后针对 insight 行情源数据的本地保存部分开展merge
# 需要下载的数据:
# 1.上市股票代码
# 2.筹码分布数据   get_chouma_datas()


# ************************************************************************
#  调用日志配置
set_config.setup_logging_config()

######################  mysql 配置信息  本地和远端服务器  ####################
local_user = Base_Properties.local_mysql_user
local_password = Base_Properties.local_mysql_password
local_database = Base_Properties.local_mysql_database
local_host = Base_Properties.local_mysql_host

origin_user = Base_Properties.origin_mysql_user
origin_password = Base_Properties.origin_mysql_password
origin_database = Base_Properties.origin_mysql_database
origin_host = Base_Properties.origin_mysql_host


class CalDWD:

    def __init__(self):



        pass

    @timing_decorator
    def cal_ashare_plate(self):
        """
        聚合股票的板块，把各个板块数据聚合在一起
        Returns:
        """

        #  1.获取日期
        ymd = DateUtility.today()
        # ymd = "20241004"

        # 2.定义 SQL 模板
        sql_statements_template = [
            """
            DELETE FROM quant.dwd_stock_a_total_plate WHERE ymd='{ymd}';
            """,
            """
            INSERT INTO quant.dwd_stock_a_total_plate
            SELECT 
                ymd, 
                concept_name AS plate_name,
                stock_code,
                stock_name,
                'ods_tdx_stock_concept_plate' AS source_table,
                '' AS remark
            FROM quant.ods_tdx_stock_concept_plate
            WHERE ymd='{ymd}'
            UNION ALL
            SELECT 
                ymd,
                style_name AS plate_name,
                stock_code,
                stock_name,
                'ods_tdx_stock_style_plate' AS source_table,
                '' AS remark
            FROM quant.ods_tdx_stock_style_plate
            WHERE ymd='{ymd}'
            UNION ALL
            SELECT 
                ymd,
                industry_name AS plate_name,
                stock_code,
                stock_name,
                'ods_tdx_stock_industry_plate' AS source_table,
                '' AS remark
            FROM quant.ods_tdx_stock_industry_plate
            WHERE ymd='{ymd}'
            UNION ALL
            SELECT 
                ymd,
                region_name AS plate_name,
                stock_code,
                stock_name,
                'ods_tdx_stock_region_plate' AS source_table,
                '' AS remark
            FROM quant.ods_tdx_stock_region_plate
            WHERE ymd='{ymd}'
            UNION ALL
            SELECT 
                ymd,
                index_name AS plate_name,
                stock_code,
                stock_name,
                'ods_tdx_stock_index_plate' AS source_table,
                '' AS remark
            FROM quant.ods_tdx_stock_index_plate
            WHERE ymd='{ymd}'
            UNION ALL
            SELECT 
                ymd,
                plate_name,
                stock_code,
                stock_name,
                'ods_stock_plate_redbook' AS source_table,
                remark
            FROM quant.ods_stock_plate_redbook
            WHERE ymd='{ymd}';
            """
        ]

        # 3.主程序替换 {ymd} 占位符
        sql_statements = [stmt.format(ymd=ymd) for stmt in sql_statements_template]

        # 4.执行 SQL
        if platform.system() == "Windows":

            mysql_utils.execute_sql_statements(
                user=local_user,
                password=local_password,
                host=local_host,
                database=local_database,
                sql_statements=sql_statements)

            mysql_utils.execute_sql_statements(
                user=origin_user,
                password=origin_password,
                host=origin_host,
                database=origin_database,
                sql_statements=sql_statements)
        else:
            mysql_utils.execute_sql_statements(
                user=origin_user,
                password=origin_password,
                host=origin_host,
                database=origin_database,
                sql_statements=sql_statements)


    @timing_decorator
    def cal_stock_exchange(self):
        """
        计算股票所归属的交易所，判断其是主办、创业板、科创板、北交所等等
        Returns:
        """

        #  1.获取日期
        ymd = DateUtility.today()
        # ymd = "20241122"

        # 2.定义 SQL 模板
        sql_statements_template = [
            """
            DELETE  FROM quant.ods_stock_exchange_market WHERE  ymd = '{ymd}';
            """,
            """
            INSERT INTO quant.ods_stock_exchange_market (ymd, stock_code, stock_name, market)
            SELECT 
                t1.ymd
               ,t1.htsc_code AS stock_code
               ,t1.name      AS stock_name
               ,CASE
               WHEN t1.htsc_code LIKE '300%' OR t1.htsc_code LIKE '301%' THEN '创业板' 
               WHEN t1.htsc_code LIKE '8%'   OR t1.htsc_code LIKE '4%'   THEN '北交所'  
               WHEN t1.htsc_code LIKE '000%' OR t1.htsc_code LIKE '001%' OR t1.htsc_code LIKE '002%' OR t1.htsc_code LIKE '003%' THEN '深圳主板' 
               WHEN t1.htsc_code LIKE '688%' OR t1.htsc_code LIKE '689%' THEN '科创板'  
               WHEN t1.htsc_code LIKE '600%' OR t1.htsc_code LIKE '601%' OR t1.htsc_code LIKE '603%' OR t1.htsc_code LIKE '605%' THEN '上海主板' 
               ELSE '未知类型' 
               END AS market
            FROM quant.ods_stock_code_daily_insight     t1
            WHERE  t1.ymd = '{ymd}';
            """
        ]

        # 3.主程序替换 {ymd} 占位符
        sql_statements = [stmt.format(ymd=ymd) for stmt in sql_statements_template]

        # 4.执行 SQL
        if platform.system() == "Windows":

            mysql_utils.execute_sql_statements(
                user=local_user,
                password=local_password,
                host=local_host,
                database=local_database,
                sql_statements=sql_statements)

            mysql_utils.execute_sql_statements(
                user=origin_user,
                password=origin_password,
                host=origin_host,
                database=origin_database,
                sql_statements=sql_statements)
        else:
            mysql_utils.execute_sql_statements(
                user=origin_user,
                password=origin_password,
                host=origin_host,
                database=origin_database,
                sql_statements=sql_statements)


    @timing_decorator
    def cal_stock_base_info(self):
        """
        计算股票基础信息，汇总表，名称、编码、板块、股本、市值、净资产
        Returns:
        """

        #  1.获取日期
        ymd = DateUtility.today()
        # ymd = "20241122"

        # 2.定义 SQL 模板
        sql_statements_template = [
            """
            DELETE  FROM quant.dwd_ashare_stock_base_info WHERE  ymd = '{ymd}';
            """,
            """
            insert IGNORE  into quant.dwd_ashare_stock_base_info 
            select 
                  tkline.ymd                                         
                 ,tpbe.stock_code                                    
                 ,tpbe.stock_name                                    
                 ,tkline.close                                       
                 ,tpbe.market_value                                  
                 ,tpbe.total_capital*tkline.close   as  total_value  
                 ,tpbe.total_asset                                   
                 ,tpbe.net_asset                                     
                 ,tpbe.total_capital                                 
                 ,tpbe.float_capital                                 
                 ,tpbe.shareholder_num                               
                 ,tpbe.pb                                            
                 ,tpbe.pe                                            
                 ,texchange.market                                   
                 ,tplate.plate_names         
                 ,tconcept.plate_names             as concept_plate
                 ,tindex.plate_names               as index_plate
                 ,tindustry.plate_names            as industry_plate
                 ,tstyle.plate_names               as style_plate
                 ,tout.plate_names                 as out_plate
            from  
             ( select
                  htsc_code                                         
                 ,ymd                                               
                 ,open                                              
                 ,close                                             
                 ,high                                              
                 ,low                                               
                 ,num_trades                                        
                 ,volume                                            
              from  quant.ods_stock_kline_daily_insight   
              where ymd = (SELECT MAX(ymd) FROM quant.ods_stock_kline_daily_insight)
            ) tkline
            left join 
            ( select 
                  ymd                                                
                 ,stock_code                                         
                 ,stock_name                                         
                 ,market_value                                       
                 ,total_asset                                        
                 ,net_asset                                          
                 ,total_capital                                      
                 ,float_capital                                      
                 ,shareholder_num                                    
                 ,pb                                                 
                 ,pe                                                 
                 ,industry                                           
              from  quant.ods_tdx_stock_pepb_info 
              WHERE ymd = (SELECT MAX(ymd) FROM quant.ods_tdx_stock_pepb_info)
            ) tpbe
            ON SUBSTRING_INDEX(tkline.htsc_code, '.', 1) = tpbe.stock_code
            left join 
            ( select 
                  ymd                                               
                 ,stock_code                                        
                 ,stock_name                                        
                 ,market                                            
              from  quant.ods_stock_exchange_market 
              where ymd = (SELECT MAX(ymd) FROM quant.ods_stock_exchange_market)
            ) texchange 
            on tkline.htsc_code = texchange.stock_code
            left join 
            (
              select 
                  ymd                                              
                 ,stock_code                                       
                 ,stock_name                                       
                 ,GROUP_CONCAT(plate_name ORDER BY plate_name SEPARATOR ',') AS plate_names   
              from  quant.dwd_stock_a_total_plate  
              where ymd = (SELECT MAX(ymd) FROM quant.dwd_stock_a_total_plate)
              group by ymd, stock_code, stock_name 
            ) tplate
            ON SUBSTRING_INDEX(tkline.htsc_code, '.', 1) = tplate.stock_code
            LEFT JOIN 
                (
                    SELECT 
                        ymd,                                              
                        stock_code,                                       
                        GROUP_CONCAT(plate_name ORDER BY plate_name SEPARATOR ',') AS plate_names   
                    FROM quant.dwd_stock_a_total_plate  
                    WHERE ymd = (SELECT MAX(ymd) FROM quant.dwd_stock_a_total_plate)
                      AND source_table = 'ods_tdx_stock_concept_plate'
                    GROUP BY ymd, stock_code
                ) tconcept
            ON SUBSTRING_INDEX(tkline.htsc_code, '.', 1) = tconcept.stock_code
            LEFT JOIN 
                (
                    SELECT 
                        ymd,                                              
                        stock_code,                                       
                        GROUP_CONCAT(plate_name ORDER BY plate_name SEPARATOR ',') AS plate_names   
                    FROM quant.dwd_stock_a_total_plate  
                    WHERE ymd = (SELECT MAX(ymd) FROM quant.dwd_stock_a_total_plate)
                      AND source_table = 'ods_tdx_stock_index_plate'
                    GROUP BY ymd, stock_code
                ) tindex
            ON SUBSTRING_INDEX(tkline.htsc_code, '.', 1) = tindex.stock_code
            LEFT JOIN 
                (
                    SELECT 
                        ymd,                                              
                        stock_code,                                       
                        GROUP_CONCAT(plate_name ORDER BY plate_name SEPARATOR ',') AS plate_names   
                    FROM quant.dwd_stock_a_total_plate  
                    WHERE ymd = (SELECT MAX(ymd) FROM quant.dwd_stock_a_total_plate)
                      AND source_table = 'ods_tdx_stock_industry_plate'
                    GROUP BY ymd, stock_code
                ) tindustry
            ON SUBSTRING_INDEX(tkline.htsc_code, '.', 1) = tindustry.stock_code
            LEFT JOIN 
                (
                    SELECT 
                        ymd,                                              
                        stock_code,                                       
                        GROUP_CONCAT(plate_name ORDER BY plate_name SEPARATOR ',') AS plate_names   
                    FROM quant.dwd_stock_a_total_plate  
                    WHERE ymd = (SELECT MAX(ymd) FROM quant.dwd_stock_a_total_plate)
                      AND source_table = 'ods_tdx_stock_style_plate'
                    GROUP BY ymd, stock_code
                ) tstyle
            ON SUBSTRING_INDEX(tkline.htsc_code, '.', 1) = tstyle.stock_code
            LEFT JOIN 
                (
                    SELECT 
                        ymd,                                              
                        stock_code,                                       
                        GROUP_CONCAT(plate_name ORDER BY plate_name SEPARATOR ',') AS plate_names   
                    FROM quant.dwd_stock_a_total_plate  
                    WHERE ymd = (SELECT MAX(ymd) FROM quant.dwd_stock_a_total_plate)
                      AND source_table = 'ods_stock_plate_redbook'
                    GROUP BY ymd, stock_code
                ) tout
            ON SUBSTRING_INDEX(tkline.htsc_code, '.', 1) = tout.stock_code;
            """]

        # 3.主程序替换 {ymd} 占位符
        sql_statements = [stmt.format(ymd=ymd) for stmt in sql_statements_template]

        # 4.执行 SQL
        if platform.system() == "Windows":

            mysql_utils.execute_sql_statements(
                user=local_user,
                password=local_password,
                host=local_host,
                database=local_database,
                sql_statements=sql_statements)

            mysql_utils.execute_sql_statements(
                user=origin_user,
                password=origin_password,
                host=origin_host,
                database=origin_database,
                sql_statements=sql_statements)
        else:
            mysql_utils.execute_sql_statements(
                user=origin_user,
                password=origin_password,
                host=origin_host,
                database=origin_database,
                sql_statements=sql_statements)


    @timing_decorator
    def cal_ZT_DT(self):
        """
        计算一只股票是否 涨停 / 跌停
        Returns:
        """

        # 1.确定起止日期
        time_start_date = DateUtility.next_day(-7)
        time_end_date = DateUtility.next_day(0)

        # 2.获取起止日期范围内的日K线数据
        df = mysql_utils.data_from_mysql_to_dataframe(user=origin_user, password=origin_password, host=origin_host,
                                                      database=origin_database,
                                                      table_name='ods_stock_kline_daily_insight',
                                                      start_date=time_start_date, end_date=time_end_date)

        if df.empty:
            # print(f"{time_start_date} - {time_end_date}日期的K线数据为空，终止 cal_ZT_DT 运行！")
            logging.info(f"{time_start_date} - {time_end_date}日期的K线数据为空，终止 cal_ZT_DT 运行！")
            return

        df = df.rename(columns={'htsc_code': 'stock_code'})

        # 按照 ymd 排序，确保数据是按日期排列的
        latest_15_days = df.sort_values(by=['stock_code', 'ymd'])

        # 按股票代码分组，然后对每个分组进行 shift(1) 操作, 计算昨日close
        latest_15_days['last_close'] = latest_15_days.groupby('stock_code')['close'].shift(1)

        # 过滤掉没有昨日数据的行
        latest_15_days = latest_15_days.dropna(subset=['last_close'])

        if latest_15_days.empty:
            # print(f"{time_start_date} - {time_end_date}日期的日期差值时间为空，终止 cal_ZT_DT 运行！")
            logging.info(f"{time_start_date} - {time_end_date}日期的日期差值时间为空，终止 cal_ZT_DT 运行！")
            return

        # 获取市场特征
        stock_market_init = mysql_utils.data_from_mysql_to_dataframe_latest(
            user=origin_user, password=origin_password, host=origin_host,
            database=origin_database, table_name='dwd_ashare_stock_base_info')

        stock_base_info = stock_market_init[['stock_code', 'stock_name', 'market_value', 'total_value',
                                           'total_asset', 'net_asset', 'total_capital', 'float_capital',
                                           'shareholder_num', 'pb', 'pe', 'market', 'plate_names']]


        # 合并市场信息到最新的15天数据
        latest_15_days['stock_code'] = latest_15_days['stock_code'].str.split('.').str[0]

        latest_15_days = latest_15_days[['ymd', 'stock_code', 'close', 'last_close']]

        latest_15_days = pd.merge(latest_15_days, stock_base_info, on='stock_code', how='left', suffixes=('_latest', '_base'))

        def calculate_ZT_DT(row):
            if row['market'] in ['创业板', '科创板']:
                up_limit = row['last_close'] * 1.20
                down_limit = row['last_close'] * 0.80
            else:  # 上海主板、深圳主板
                up_limit = row['last_close'] * 1.10
                down_limit = row['last_close'] * 0.90
            return pd.Series([up_limit, down_limit])  # 确保返回两个值

        # 应用计算
        latest_15_days[['昨日ZT价', '昨日DT价']] = latest_15_days.apply(calculate_ZT_DT, axis=1, result_type='expand')

        def ZT_DT_orz(price, target_price):
            # 如果 price 和 target_price 之间的差距小于等于0.01，才进一步计算
            if abs(target_price - price) <= 0.01:
                # 计算 price 周围 0.01 范围内的最接近的2个价格
                left_price = price - 0.01
                right_price = price + 0.01

                # 算价差
                left_delta = abs(left_price - target_price)
                mid_delta = abs(price - target_price)
                right_delta = abs(right_price - target_price)
                min_delta = min(left_delta, mid_delta, right_delta)

                # 判断为ZT or DT
                if mid_delta == min_delta:
                    return True

            # 不可能 ZT or DT
            return False

        # 3. 判断每日的涨停或跌停
        latest_15_days['是否涨停'] = latest_15_days.apply(
            lambda row: ZT_DT_orz(row['close'], row['昨日ZT价']), axis=1)
        latest_15_days['是否跌停'] = latest_15_days.apply(
            lambda row: ZT_DT_orz(row['close'], row['昨日DT价']), axis=1)

        # 4. 筛选出涨停和跌停的记录，分别存入两个 DataFrame
        zt_records = latest_15_days[latest_15_days['是否涨停'] == True].copy()
        zt_records['rate'] = ((zt_records['close'] - zt_records['last_close']) / zt_records['last_close'] * 100).round(2)
        zt_df = zt_records[
            ['ymd', 'stock_code', 'stock_name', 'last_close', 'close', 'rate', 'market_value', 'total_value',
             'total_asset', 'net_asset', 'total_capital', 'float_capital', 'shareholder_num', 'pb', 'pe',
             'market', 'plate_names']]
        zt_df = zt_df.sort_values(by=['ymd', 'stock_code'])

        dt_records = latest_15_days[latest_15_days['是否跌停'] == True].copy()
        dt_records['rate'] = ((dt_records['close'] - dt_records['last_close']) / dt_records['last_close'] * 100).round(2)
        dt_df = dt_records[
            ['ymd', 'stock_code', 'stock_name', 'last_close', 'close', 'rate', 'market_value', 'total_value',
             'total_asset', 'net_asset', 'total_capital', 'float_capital', 'shareholder_num', 'pb', 'pe',
             'market', 'plate_names']]
        dt_df = dt_df.sort_values(by=['ymd', 'stock_code'])

        ############################   文件输出模块     ############################
        if platform.system() == "Windows":
            #  涨停数据保存到 本地 mysql中
            mysql_utils.data_from_dataframe_to_mysql(user=local_user,
                                                     password=local_password,
                                                     host=local_host,
                                                     database=local_database,
                                                     df=zt_df,
                                                     table_name="dwd_stock_zt_list",
                                                     merge_on=['ymd', 'stock_code'])

            #  涨停数据保存到 远端 mysql中
            mysql_utils.data_from_dataframe_to_mysql(user=origin_user,
                                                     password=origin_password,
                                                     host=origin_host,
                                                     database=origin_database,
                                                     df=zt_df,
                                                     table_name="dwd_stock_zt_list",
                                                     merge_on=['ymd', 'stock_code'])

            #  跌停数据保存到 本地 mysql中
            mysql_utils.data_from_dataframe_to_mysql(user=local_user,
                                                     password=local_password,
                                                     host=local_host,
                                                     database=local_database,
                                                     df=dt_df,
                                                     table_name="dwd_stock_dt_list",
                                                     merge_on=['ymd', 'stock_code'])

            #  跌停数据保存到 远端 mysql中
            mysql_utils.data_from_dataframe_to_mysql(user=origin_user,
                                                     password=origin_password,
                                                     host=origin_host,
                                                     database=origin_database,
                                                     df=dt_df,
                                                     table_name="dwd_stock_dt_list",
                                                     merge_on=['ymd', 'stock_code'])
        else:
            #  涨停数据保存到 远端 mysql中
            mysql_utils.data_from_dataframe_to_mysql(user=origin_user,
                                                     password=origin_password,
                                                     host=origin_host,
                                                     database=origin_database,
                                                     df=zt_df,
                                                     table_name="dwd_stock_zt_list",
                                                     merge_on=['ymd', 'stock_code'])

            #  跌停数据保存到 远端 mysql中
            mysql_utils.data_from_dataframe_to_mysql(user=origin_user,
                                                     password=origin_password,
                                                     host=origin_host,
                                                     database=origin_database,
                                                     df=dt_df,
                                                     table_name="dwd_stock_dt_list",
                                                     merge_on=['ymd', 'stock_code'])


    def setup(self):

        # 聚合股票的板块，把各个板块数据聚合在一起
        self.cal_ashare_plate()

        # 计算股票所归属的交易所，判断其是主办、创业板、科创板、北交所等等
        self.cal_stock_exchange()

        # 计算股票基础信息，汇总表，名称、编码、板块、股本、市值、净资产
        self.cal_stock_base_info()

        # 计算一只股票是否 涨停 / 跌停
        self.cal_ZT_DT()


if __name__ == '__main__':
    save_insight_data = CalDWD()
    save_insight_data.setup()
```

--------------------------------------------------------------------------------
## datas_prepare\C04_data_MART\__init__.py

```python

```

--------------------------------------------------------------------------------
## datas_prepare\C04_data_MART\calculate_MART_datas.py

```python
# -*- coding: utf-8 -*-

import os
from datetime import datetime

import pandas as pd
from sqlalchemy import create_engine, text
import time
import platform
import logging


# import dataprepare_properties
# import dataprepare_utils
from CommonProperties import Base_Properties
import CommonProperties.Base_utils as base_utils
from CommonProperties.DateUtility import DateUtility
from CommonProperties.Base_utils import timing_decorator

import CommonProperties.Mysql_Utils as mysql_utils

from CommonProperties import set_config

# ************************************************************************
# 本代码的作用是下午收盘后针对 insight 行情源数据的本地保存部分开展merge
# 需要下载的数据:
# 1.上市股票代码
# 2.筹码分布数据   get_chouma_datas()


# ************************************************************************
#  调用日志配置
set_config.setup_logging_config()

######################  mysql 配置信息  本地和远端服务器  ####################
local_user = Base_Properties.local_mysql_user
local_password = Base_Properties.local_mysql_password
local_database = Base_Properties.local_mysql_database
local_host = Base_Properties.local_mysql_host

origin_user = Base_Properties.origin_mysql_user
origin_password = Base_Properties.origin_mysql_password
origin_database = Base_Properties.origin_mysql_database
origin_host = Base_Properties.origin_mysql_host


class CalDMART:

    def __init__(self):
        pass


    # @timing_decorator
    def cal_zt_details(self):
        """
        涨停股票的明细
        Returns:
        """
        #  1.获取日期
        # ymd = DateUtility.today()
        time_start_date = DateUtility.next_day(-2)
        time_start_date = '20250215'

        time_end_date = DateUtility.next_day(0)
        time_end_date = '20250319'

        # 2.定义 SQL 模板
        sql_statements_template = [
            """
            DELETE FROM quant.dmart_stock_zt_details WHERE ymd>= '{time_start_date}' and ymd <= '{time_end_date}';
            """,
            """
            INSERT IGNORE INTO quant.dmart_stock_zt_details
            SELECT 
                tzt.ymd
               ,tzt.stock_code
               ,tzt.stock_name
               ,tbase.concept_plate
               ,tbase.index_plate
               ,tbase.industry_plate
               ,tbase.style_plate
               ,tbase.out_plate
            FROM 
                quant.dwd_stock_zt_list                     tzt
            LEFT JOIN 
                (
                    SELECT 
                        t2.ymd
                       ,t2.stock_code
                       ,t2.concept_plate
                       ,t2.index_plate
                       ,t2.industry_plate
                       ,t2.style_plate
                       ,t2.out_plate
                    FROM 
                        quant.dwd_ashare_stock_base_info    t2
                    INNER JOIN 
                        (
                            SELECT 
                                MAX(ymd) AS latest_ymd
                            FROM 
                                quant.dwd_ashare_stock_base_info
                        ) latest 
                    ON t2.ymd = latest.latest_ymd
                ) tbase
            ON     tzt.stock_code = tbase.stock_code
            WHERE  tzt.ymd >= '{time_start_date}'  AND  tzt.ymd <= '{time_end_date}'  ;
            """
        ]

        # 3.主程序替换 {ymd} 占位符
        sql_statements = [stmt.format(time_start_date=time_start_date, time_end_date=time_end_date) for stmt in sql_statements_template]

        # 4.执行 SQL
        if platform.system() == "Windows":

            # mysql_utils.execute_sql_statements(
            #     user=local_user,
            #     password=local_password,
            #     host=local_host,
            #     database=local_database,
            #     sql_statements=sql_statements)

            mysql_utils.execute_sql_statements(
                user=origin_user,
                password=origin_password,
                host=origin_host,
                database=origin_database,
                sql_statements=sql_statements)
        else:
            mysql_utils.execute_sql_statements(
                user=origin_user,
                password=origin_password,
                host=origin_host,
                database=origin_database,
                sql_statements=sql_statements)

    def cal_zt_details_explode(self):
        """
        涨停股票的明细的拆分
        Returns:
        """
        # 1. 获取日期范围
        time_start_date = DateUtility.next_day(-2)  # 2天前的日期
        time_start_date = '20241126'

        time_end_date = DateUtility.next_day(0)  # 当前日期
        time_end_date = '20250318'

        logging.info(f"开始处理涨停股票明细数据，日期范围：{time_start_date} 至 {time_end_date}")

        # 2. 从 MySQL 获取起止日期范围内的数据
        df = mysql_utils.data_from_mysql_to_dataframe(
            user=origin_user,
            password=origin_password,
            host=origin_host,
            database=origin_database,
            table_name='dmart_stock_zt_details',
            start_date=time_start_date,
            end_date=time_end_date,
            cols=['ymd', 'stock_code', 'stock_name', 'concept_plate', 'index_plate', 'industry_plate', 'style_plate',
                  'out_plate']
        )

        if df.empty:
            logging.warning("未获取到数据，可能日期范围内没有数据或表为空。")
            return

        logging.info(f"成功获取到 {len(df)} 条数据，开始拆解处理...")

        # 3. 定义 unpack_plates 函数
        def unpack_plates(df):
            result = []
            for _, row in df.iterrows():
                ymd = row['ymd']
                stock_code = row['stock_code']
                stock_name = row['stock_name']

                # 获取每个字段的分隔值
                fields = {
                    'concept_plate': row['concept_plate'].split(',') if pd.notna(row['concept_plate']) else [],
                    'index_plate': row['index_plate'].split(',') if pd.notna(row['index_plate']) else [],
                    'industry_plate': row['industry_plate'].split(',') if pd.notna(row['industry_plate']) else [],
                    'style_plate': row['style_plate'].split(',') if pd.notna(row['style_plate']) else [],
                    'out_plate': row['out_plate'].split(',') if pd.notna(row['out_plate']) else []
                }

                # 找到分隔值最多的字段
                max_length = max(len(fields[field]) for field in fields)

                # 按最大长度填充数据
                for i in range(max_length):
                    result_row = {
                        'ymd': ymd.strftime('%Y%m%d'),
                        'stock_code': stock_code,
                        'stock_name': stock_name,
                        'concept_plate': fields['concept_plate'][i].strip() if i < len(
                            fields['concept_plate']) else None,
                        'index_plate': fields['index_plate'][i].strip() if i < len(fields['index_plate']) else None,
                        'industry_plate': fields['industry_plate'][i].strip() if i < len(
                            fields['industry_plate']) else None,
                        'style_plate': fields['style_plate'][i].strip() if i < len(fields['style_plate']) else None,
                        'out_plate': fields['out_plate'][i].strip() if i < len(fields['out_plate']) else None
                    }
                    result.append(result_row)

            return pd.DataFrame(result)

        # 4. 调用 unpack_plates 函数处理数据
        output_df = unpack_plates(df)

        # 5. 将处理后的数据保存到 MySQL
        if platform.system() == "Windows":
            mysql_utils.data_from_dataframe_to_mysql(
                user=local_user,
                password=local_password,
                host=local_host,
                database=local_database,
                df=output_df,
                table_name="dmart_stock_zt_details_expanded",
                merge_on=['ymd', 'stock_code', 'concept_plate', 'index_plate', 'industry_plate', 'style_plate',
                          'out_plate']
            )
            logging.info(
                f"数据处理完成，已将结果保存到 {local_host} 的 {local_database}.dmart_stock_zt_details_expanded 表中。")

            mysql_utils.data_from_dataframe_to_mysql(
                user=origin_user,
                password=origin_password,
                host=origin_host,
                database=origin_database,
                df=output_df,
                table_name="dmart_stock_zt_details_expanded",
                merge_on=['ymd', 'stock_code', 'concept_plate', 'index_plate', 'industry_plate', 'style_plate',
                          'out_plate']
            )
            logging.info(
                f"数据处理完成，已将结果保存到 {origin_host} 的 {origin_database}.dmart_stock_zt_details_expanded 表中。")


        else:
            mysql_utils.data_from_dataframe_to_mysql(
                user=origin_user,
                password=origin_password,
                host=origin_host,
                database=origin_database,
                df=output_df,
                table_name="dmart_stock_zt_details_expanded",
                merge_on=['ymd', 'stock_code', 'concept_plate', 'index_plate', 'industry_plate', 'style_plate',
                          'out_plate']
            )
            logging.info(
                f"数据处理完成，已将结果保存到 {origin_host} 的 {origin_database}.dmart_stock_zt_details_expanded 表中。")



    def setup(self):

        # 涨停股票的明细
        self.cal_zt_details()

        self.cal_zt_details_explode()


if __name__ == '__main__':
    cal_dmart_data = CalDMART()
    cal_dmart_data.setup()




```

--------------------------------------------------------------------------------
## datas_prepare\C06_data_transfer\__init__.py

```python

```

--------------------------------------------------------------------------------
## datas_prepare\C06_data_transfer\get_example_tables.py

```python
import os
import pandas as pd
import logging
from datetime import datetime
from sqlalchemy import create_engine, text
from pathlib import Path  # 新增：导入Path用于路径处理
import CommonProperties.Base_Properties as Base_Properties
from CommonProperties.set_config import setup_logging_config

# 配置日志
setup_logging_config()
logger = logging.getLogger(__name__)


class TableDataExporterFull:
    """导出数据库表数据样例到单个文件 - 显示完整数据"""

    def __init__(self):
        # 使用您的MySQL配置
        self.user = Base_Properties.origin_mysql_user
        self.password = Base_Properties.origin_mysql_password
        self.host = Base_Properties.origin_mysql_host
        self.database = Base_Properties.origin_mysql_database

        # ====================== 核心优化：精准推导 Quant/Others/output 路径 ======================
        # 1. 获取当前脚本（export_table_samples_full.py）的绝对路径
        current_script_path = Path(__file__).resolve()

        # 2. 向上追溯找到项目根目录 Quant/（关键：基于 CommonProperties 目录反向定位，更稳定）
        # 方案1：通过 CommonProperties 目录（项目中固定存在）定位 Quant/（推荐，兼容性更强）
        current_dir = current_script_path.parent
        project_root = None
        # 向上遍历目录，直到找到包含 CommonProperties 的目录（即 Quant/）
        while current_dir != current_dir.parent:
            if (current_dir / "CommonProperties").exists():
                project_root = current_dir
                break
            current_dir = current_dir.parent

        # 方案2：如果脚本目录结构固定，可直接向上追溯（备用，简洁但依赖目录结构）
        # project_root = current_script_path.parent.parent  # 若脚本在 Quant/xxx/ 下，直接向上两级到 Quant/

        # 校验项目根目录是否找到
        if not project_root or not (project_root / "CommonProperties").exists():
            raise FileNotFoundError("❌ 未找到项目根目录 Quant/（缺少 CommonProperties 目录）")

        # 3. 构造 Quant/Others 目录路径
        others_dir = project_root / "Others"

        # 4. 构造 Quant/Others/output 目录路径
        self.output_dir = others_dir / "output"

        # 5. 自动创建 Others 和 output 目录（若不存在）
        self.output_dir.mkdir(parents=True, exist_ok=True)
        print(f"📁 自动创建/确认输出目录: {self.output_dir}")

        # 6. 构造完整的输出文件路径（放入 Quant/Others/output 目录）
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_filename = f"quant_tables_full_{timestamp}.txt"
        self.output_file = self.output_dir / output_filename  # Path对象，支持后续直接操作
        # ======================================================================================

        print(f"数据库配置:")
        print(f"  主机: {self.host}")
        print(f"  数据库: {self.database}")
        print(f"  用户: {self.user}")
        print(f"  输出文件将保存到: {self.output_file}")  # 新增：提示输出文件路径
        print("-" * 50)

    def test_connection(self):
        """测试数据库连接"""
        try:
            db_url = f'mysql+pymysql://{self.user}:{self.password}@{self.host}:3306/{self.database}'
            engine = create_engine(db_url)
            with engine.connect() as connection:
                result = connection.execute(text("SELECT 1"))
                print("✓ 数据库连接成功")
                return True
        except Exception as e:
            print(f"✗ 数据库连接失败: {str(e)}")
            return False

    def get_all_tables(self):
        """获取数据库中的所有表名"""
        try:
            db_url = f'mysql+pymysql://{self.user}:{self.password}@{self.host}:3306/{self.database}'
            engine = create_engine(db_url)

            print("正在获取表列表...")

            # 使用SHOW TABLES
            with engine.connect() as connection:
                result = connection.execute(text("SHOW TABLES"))
                tables = [row[0] for row in result]

            print(f"✓ 找到 {len(tables)} 张表")
            return tables

        except Exception as e:
            print(f"✗ 获取表列表失败: {str(e)}")
            return []

    def get_table_info(self, table_name):
        """获取表的完整信息"""
        try:
            db_url = f'mysql+pymysql://{self.user}:{self.password}@{self.host}:3306/{self.database}'
            engine = create_engine(db_url)

            info = {
                'table_name': table_name,
                'structure': None,
                'sample_data': None,
                'row_count': 0,
                'column_count': 0
            }

            with engine.connect() as connection:
                # 1. 获取表结构
                try:
                    result = connection.execute(text(f"SHOW CREATE TABLE `{table_name}`"))
                    create_table_sql = result.fetchone()[1]
                    info['create_sql'] = create_table_sql
                except:
                    info['create_sql'] = None

                # 2. 获取表描述
                try:
                    result = connection.execute(text(f"DESCRIBE `{table_name}`"))
                    columns_info = []
                    for row in result:
                        col_info = {
                            'Field': row[0],
                            'Type': row[1],
                            'Null': row[2],
                            'Key': row[3],
                            'Default': row[4],
                            'Extra': row[5] if len(row) > 5 else ''
                        }
                        columns_info.append(col_info)
                    info['structure'] = columns_info
                    info['column_count'] = len(columns_info)
                except:
                    pass

                # 3. 获取行数
                try:
                    result = connection.execute(text(f"SELECT COUNT(*) FROM `{table_name}`"))
                    info['row_count'] = result.fetchone()[0]
                except:
                    pass

                # 4. 获取样例数据（最多5行）
                if info['row_count'] > 0:
                    try:
                        limit = min(5, info['row_count'])
                        query = text(f"SELECT * FROM `{table_name}` LIMIT {limit}")
                        df = pd.read_sql(query, connection)
                        info['sample_data'] = df
                    except:
                        pass

            return info

        except Exception as e:
            print(f"  表 {table_name} 信息获取失败: {str(e)[:50]}...")
            return None

    def write_table_info(self, f, table_info, table_num, total_tables):
        """写入单个表的完整信息到文件"""
        if not table_info:
            return

        table_name = table_info['table_name']

        f.write(f"\n【表 {table_num}/{total_tables}】{table_name}\n")
        f.write("=" * 100 + "\n")

        # 1. 基本信息
        f.write(f"基本信息:\n")
        f.write(f"  行数: {table_info.get('row_count', '未知')}\n")
        f.write(f"  列数: {table_info.get('column_count', '未知')}\n")
        f.write("\n")

        # 2. 表结构（完整）
        if table_info.get('structure'):
            f.write("表结构（完整）:\n")
            f.write("-" * 80 + "\n")
            f.write(f"{'字段名':<20} {'类型':<20} {'可空':<5} {'键':<5} {'默认值':<15} {'额外':<10}\n")
            f.write("-" * 80 + "\n")
            for col in table_info['structure']:
                field = col.get('Field', '')
                type_ = col.get('Type', '')
                null = col.get('Null', '')
                key = col.get('Key', '')
                default = str(col.get('Default', '')) if col.get('Default') is not None else 'NULL'
                extra = col.get('Extra', '')

                f.write(f"{field:<20} {type_:<20} {null:<5} {key:<5} {default:<15} {extra:<10}\n")
        f.write("\n")

        # 3. 样例数据（完整显示所有列）
        if table_info.get('sample_data') is not None and not table_info['sample_data'].empty:
            df = table_info['sample_data']
            f.write(f"数据样例（前{len(df)}行，完整列）:\n")
            f.write("-" * 80 + "\n")

            # 显示所有列名
            columns = df.columns.tolist()
            f.write(f"所有列({len(columns)}个):\n")
            for i, col in enumerate(columns, 1):
                f.write(f"  {i:2d}. {col}\n")
            f.write("\n")

            # 显示数据（表格格式）
            # 设置pandas显示选项
            pd.set_option('display.max_columns', None)
            pd.set_option('display.width', None)
            pd.set_option('display.max_colwidth', 50)

            # 转换为字符串
            data_str = df.to_string(index=False)

            # 如果数据太长，分块显示
            if len(data_str) > 5000:
                f.write("数据预览（前5000字符）:\n")
                f.write(data_str[:5000])
                f.write(f"\n... (数据过长，已截断，原始{len(data_str)}字符)\n")
            else:
                f.write(data_str)
        else:
            f.write("数据样例: 表为空或无法读取数据\n")

        f.write("\n" * 2)

    def export_important_tables(self):
        """导出重要的表（按前缀筛选）"""
        print("开始导出数据库表信息...")

        # 测试连接
        if not self.test_connection():
            return

        # 获取所有表
        tables = self.get_all_tables()
        if not tables:
            print("错误：数据库中没有找到任何表")
            return

        # 按重要性筛选表（先导出关键表）
        important_prefixes = ['ods_', 'dwd_', 'dmart_', 'dwt_']
        important_tables = []
        other_tables = []

        for table in tables:
            is_important = False
            for prefix in important_prefixes:
                if table.startswith(prefix):
                    important_tables.append(table)
                    is_important = True
                    break
            if not is_important:
                other_tables.append(table)

        print(f"找到 {len(tables)} 张表，其中:")
        print(f"  重要表（ods/dwd/dmart）: {len(important_tables)} 张")
        print(f"  其他表: {len(other_tables)} 张")

        # 询问用户要导出哪些表
        print("\n导出选项:")
        print("1. 只导出重要表（ods/dwd/dmart开头）")
        print("2. 导出所有表")
        print("3. 导出指定前缀的表")

        choice = input("请选择 (1/2/3, 默认1): ").strip()

        if choice == '2':
            tables_to_export = important_tables + other_tables
        elif choice == '3':
            prefix = input("请输入表前缀 (如 ods_): ").strip()
            tables_to_export = [t for t in tables if t.startswith(prefix)]
            if not tables_to_export:
                print(f"没有以 {prefix} 开头的表")
                return
        else:  # 默认选择1
            tables_to_export = important_tables

        print(f"\n开始导出 {len(tables_to_export)} 张表...")

        # 注意：self.output_file 是Path对象，open时会自动转换为字符串路径，兼容Python内置open函数
        with open(self.output_file, 'w', encoding='utf-8') as f:
            # 写入文件头
            f.write("QUANT数据库表结构及数据样例报告（完整版）\n")
            f.write("=" * 100 + "\n")
            f.write(f"生成时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
            f.write(f"数据库: {self.database} @ {self.host}\n")
            f.write(f"总表数: {len(tables)}\n")
            f.write(f"本次导出表数: {len(tables_to_export)}\n")
            f.write("=" * 100 + "\n\n")

            # 表目录
            f.write("导出表目录:\n")
            for i, table in enumerate(tables_to_export, 1):
                f.write(f"{i:3d}. {table}\n")
            f.write("\n" + "=" * 100 + "\n\n")

            # 按前缀分组导出
            table_groups = {}
            for table in tables_to_export:
                if '_' in table:
                    prefix = table.split('_')[0]
                else:
                    prefix = '其他'
                if prefix not in table_groups:
                    table_groups[prefix] = []
                table_groups[prefix].append(table)

            # 导出每个表
            total_exported = 0
            for prefix in sorted(table_groups.keys()):
                f.write(f"\n【{prefix.upper()}层】({len(table_groups[prefix])}张表)\n")
                f.write("=" * 80 + "\n\n")

                group_tables = sorted(table_groups[prefix])
                for i, table in enumerate(group_tables, 1):
                    print(f"处理: {table} ({total_exported + 1}/{len(tables_to_export)})")

                    try:
                        # 获取表信息
                        table_info = self.get_table_info(table)

                        if table_info:
                            # 写入文件
                            self.write_table_info(f, table_info, total_exported + 1, len(tables_to_export))
                            total_exported += 1

                    except Exception as e:
                        f.write(f"处理表 {table} 时出错: {str(e)[:100]}...\n\n")
                    print(f"  完成")

        # 完成提示（优化：显示完整的输出文件路径）
        if self.output_file.exists():  # Path对象直接调用exists()，比os.path.exists更优雅
            file_size = self.output_file.stat().st_size / 1024  # KB，Path对象直接获取文件信息
            print("\n" + "=" * 60)
            print("导出完成！")
            print("=" * 60)
            print(f"输出文件: {self.output_file}")
            print(f"文件大小: {file_size:.1f} KB")
            print(f"导出表数: {total_exported}/{len(tables_to_export)}")
            print("=" * 60)

            # 显示文件内容建议
            print("\n文件内容包含:")
            print("1. 完整的表结构（所有字段、类型、可空、默认值等）")
            print("2. 完整的数据样例（所有列，最多5行）")
            print("3. 每个表的基本信息（行数、列数）")

            if file_size > 200:
                print(f"\n⚠️  文件较大 ({file_size:.1f}KB)，建议:")
                print("1. 用Notepad++或VSCode打开查看")
                print("2. 可以分多次发送内容")
                print("3. 或压缩后发送文件")
            else:
                print(f"\n✓ 文件大小合适 ({file_size:.1f}KB)，可直接复制粘贴")

            # 显示文件头
            print("\n文件开头预览:")
            print("-" * 60)
            try:
                with open(self.output_file, 'r', encoding='utf-8') as f:
                    lines = []
                    for i in range(50):  # 显示前50行
                        line = f.readline()
                        if not line:
                            break
                        lines.append(line.rstrip())

                    for line in lines[:30]:  # 只显示前30行避免太长
                        if len(line) > 100:
                            print(line[:97] + "...")
                        else:
                            print(line)

                    if len(lines) > 30:
                        print("... (还有更多内容)")
            except Exception as e:
                print(f"预览失败: {str(e)}")

            print("\n" + "=" * 60)
            print("操作说明:")
            print("1. 打开文件，复制需要的内容发送给我")
            print("2. 重要表优先：ods_*, dwd_*, dmart_*")
            print("=" * 60)
        else:
            print("错误：文件未生成")


def main():
    """主函数"""
    print("QUANT数据库表结构导出工具（完整版）")
    print("=" * 60)
    print("本工具将导出完整的表结构和数据")
    print("=" * 60)

    # 创建导出器
    try:
        exporter = TableDataExporterFull()
        # 导出表
        exporter.export_important_tables()
    except Exception as e:
        print(f"\n❌ 程序运行失败: {str(e)}")


if __name__ == "__main__":
    main()
```

--------------------------------------------------------------------------------
## datas_prepare\C06_data_transfer\put_df_to_mysql.py

```python

import pandas as pd
from yahoo_fin.stock_info import *

from CommonProperties.DateUtility import DateUtility
import CommonProperties.Base_Properties as base_properties
import CommonProperties.Mysql_Utils as mysql_utils
from CommonProperties.Base_utils import timing_decorator


def put_csv_to_mysql():

    #  读取csv
    # file_dir = r'F:\QDatas\vantage\USD_FX\USD_FX_2024081114.csv'
    # table_name = r'exchange_dxy_vantage'

    file_dir = r'F:\QDatas\vantage\USD_FX_detail\USD_FX_detail_2024081114.csv'
    table_name = r'exchange_rate_vantage_detail'

    df = pd.read_csv(file_dir)
    df.columns = ['name', 'ymd', 'open', 'high', 'low', 'close']

    mysql_utils.data_from_dataframe_to_mysql(df=df, table_name=table_name, database='quant')


if __name__ == "__main__":
    put_csv_to_mysql()


```

--------------------------------------------------------------------------------
## datas_prepare\C06_data_transfer\transfer_between_local_and_originMySQL.py

```python
# -*- coding: utf-8 -*-

import pandas as pd
from sqlalchemy import create_engine
import gc
from CommonProperties import Base_Properties
from CommonProperties.Base_utils import timing_decorator
import CommonProperties.Mysql_Utils as mysql_utils

local_user = Base_Properties.local_mysql_user
local_password = Base_Properties.local_mysql_password
local_database = Base_Properties.local_mysql_database
local_host = Base_Properties.local_mysql_host

origin_user = Base_Properties.origin_mysql_user
origin_password = Base_Properties.origin_mysql_password
origin_database = Base_Properties.origin_mysql_database
origin_host = Base_Properties.origin_mysql_host


@timing_decorator
def transfer_local_to_origin_mysql():
    """
    从 本地 向 远端 服务器刷新 mysql 数据   全删全插
    Returns:
    """

    local_db_url = f'mysql+pymysql://{local_user}:{local_password}@{local_host}:3306/{local_database}'
    origin_db_url = f'mysql+pymysql://{origin_user}:{origin_password}@{origin_host}:3306/{origin_database}'

    # 'stock_kline_daily_insight',

    table_all_list = ['ods_stock_code_daily_insight',
                      'ods_index_a_share_insight',
                      'ods_astock_industry_detail',
                      'ods_astock_industry_overview',
                      'ods_stock_limit_summary_insight',
                      'ods_future_inside_insight',
                      'ods_north_bound_daily',
                      'ods_shareholder_num',
                      'ods_stock_chouma_insight',
                      'ods_us_stock_daily_vantage',
                      'ods_exchange_rate_vantage_detail',
                      'ods_exchange_dxy_vantage',
                      'ods_tdx_stock_concept_plate',
                      'ods_tdx_stock_index_plate',
                      'ods_tdx_stock_industry_plate',
                      'ods_tdx_stock_region_plate',
                      'ods_tdx_stock_style_plate',
                      'ods_tdx_stock_pepb_info',
                      'ods_stock_kline_daily_insight',
                      'ods_stock_exchange_market',
                      'ods_stock_plate_redbook',
                      'dwd_stock_zt_list',
                      'dwd_stock_dt_list',
                      'dwd_stock_a_total_plate',
                      'dwd_ashare_stock_base_info'
                      ]

    table_temp_list = ['stock_chouma_insight']

    for tableName in table_temp_list:
        mysql_utils.full_replace_migrate(source_host=local_host,
                                         source_db_url=local_db_url,
                                         target_host=origin_host,
                                         target_db_url=origin_db_url,
                                         table_name=tableName)


# @timing_decorator
def transfer_origin_to_local_mysql():
    """
    从 远端 向 本地 主机刷新 mysql 数据   全删全插
    Returns:

    """

    local_db_url = f'mysql+pymysql://{local_user}:{local_password}@{local_host}:3306/{local_database}'
    origin_db_url = f'mysql+pymysql://{origin_user}:{origin_password}@{origin_host}:3306/{origin_database}'

    table_all_list = [
        'ods_stock_code_daily_insight',
        'ods_stock_chouma_insight',
        'ods_shareholder_num',
        'ods_north_bound_daily',
        'ods_stock_exchange_market',
        'ods_tdx_stock_pepb_info',
        'ods_stock_kline_daily_insight',
        'ods_index_a_share_insight',
        'ods_future_inside_insight',
        'ods_us_stock_daily_vantage',
        'ods_exchange_rate_vantage_detail',
        'ods_exchange_dxy_vantage',
        'ods_stock_limit_summary_insight',
        'ods_astock_industry_overview',
        'ods_astock_industry_detail',
        'ods_tdx_stock_concept_plate',
        'ods_tdx_stock_region_plate',
        'ods_tdx_stock_industry_plate',
        'ods_tdx_stock_style_plate',
        'ods_tdx_stock_index_plate',
        'ods_stock_plate_redbook',
        'dwd_stock_a_total_plate',
        'dwd_ashare_stock_base_info',
        'dwd_stock_zt_list',
        'dwd_stock_dt_list',
        'dmart_stock_zt_details',
        'dmart_stock_zt_details_expanded'
    ]

    for tableName in table_all_list:
        mysql_utils.full_replace_migrate(source_host=origin_host,
                                         source_db_url=origin_db_url,
                                         target_host=local_host,
                                         target_db_url=local_db_url,
                                         table_name=tableName)

@timing_decorator
def append_origin_to_local_mysql():
    """
    从 远端 向 本地 服务器刷新 mysql 数据   追加形式
    Returns:
    """

    table_all_list = ['ods_stock_code_daily_insight',
                      'ods_index_a_share_insight',
                      'ods_astock_industry_detail',
                      'ods_astock_industry_overview',
                      'ods_stock_limit_summary_insight',
                      'ods_future_inside_insight',
                      'ods_north_bound_daily',
                      'ods_shareholder_num',
                      'ods_stock_chouma_insight',
                      'ods_us_stock_daily_vantage',
                      'ods_exchange_rate_vantage_detail',
                      'ods_exchange_dxy_vantage',
                      'ods_tdx_stock_concept_plate',
                      'ods_tdx_stock_index_plate',
                      'ods_tdx_stock_industry_plate',
                      'ods_tdx_stock_region_plate',
                      'ods_tdx_stock_style_plate',
                      'ods_tdx_stock_pepb_info',
                      'ods_stock_kline_daily_insight',
                      'ods_stock_exchange_market',
                      'ods_stock_plate_redbook',
                      'dwd_stock_zt_list',
                      'dwd_stock_dt_list',
                      'dwd_stock_a_total_plate',
                      'dwd_ashare_stock_base_info',
                      'dmart_stock_zt_details'
                      ]

    #  设置起止时间，从source_table 中拉取数据
    # start_date = '2024-12-11'
    # end_date = '2025-01-02'

    start_date = '2025-01-03'
    end_date = '2025-02-22'

    for tableName in table_all_list:
        sourceTable = tableName
        targetTable = tableName

        mysql_utils.cross_server_upsert_ymd(source_user=origin_user,
                                            source_password=origin_password,
                                            source_host=origin_host,
                                            source_database=origin_database,
                                            target_user=local_user,
                                            target_password=local_password,
                                            target_host=local_host,
                                            target_database=local_database,
                                            source_table=sourceTable,
                                            target_table=targetTable,
                                            start_date=start_date,
                                            end_date=end_date)

        # mysql_utils.cross_server_upsert_all(source_user=origin_user,
        #                                     source_password=origin_password,
        #                                     source_host=origin_host,
        #                                     source_database=origin_database,
        #                                     target_user=local_user,
        #                                     target_password=local_password,
        #                                     target_host=local_host,
        #                                     target_database=local_database,
        #                                     source_table=sourceTable,
        #                                     target_table=targetTable)


@timing_decorator
def append_local_to_origin_mysql():
    """
    从 本地 向 远端 服务器刷新 mysql 数据   追加形式
    Returns:
    """

    table_all_list = ['ods_stock_code_daily_insight',
                      'ods_index_a_share_insight',
                      'ods_astock_industry_detail',
                      'ods_astock_industry_overview',
                      'ods_stock_limit_summary_insight',
                      'ods_future_inside_insight',
                      'ods_north_bound_daily',
                      'ods_shareholder_num',
                      'ods_stock_chouma_insight',
                      'ods_us_stock_daily_vantage',
                      'ods_exchange_rate_vantage_detail',
                      'ods_exchange_dxy_vantage',
                      'ods_tdx_stock_concept_plate',
                      'ods_tdx_stock_index_plate',
                      'ods_tdx_stock_industry_plate',
                      'ods_tdx_stock_region_plate',
                      'ods_tdx_stock_style_plate',
                      'ods_tdx_stock_pepb_info',
                      'ods_stock_kline_daily_insight',
                      'ods_stock_exchange_market',
                      'ods_stock_plate_redbook',
                      'dwd_stock_zt_list',
                      'dwd_stock_dt_list',
                      'dwd_stock_a_total_plate',
                      'dwd_ashare_stock_base_info'
                      ]

    #  设置起止时间，从source_table 中拉取数据
    start_date = '2024-10-01'
    end_date = '2024-11-04'

    for tableName in table_all_list:
        sourceTable = tableName
        targetTable = tableName

        mysql_utils.cross_server_upsert_ymd(source_user=local_user,
                                            source_password=local_password,
                                            source_host=local_host,
                                            source_database=local_database,
                                            target_user=origin_user,
                                            target_password=origin_password,
                                            target_host=origin_host,
                                            target_database=origin_database,
                                            source_table=sourceTable,
                                            target_table=targetTable,
                                            start_date=start_date,
                                            end_date=end_date)

        # mysql_utils.cross_server_upsert_all(source_user=local_user,
        #                                     source_password=local_password,
        #                                     source_host=local_host,
        #                                     source_database=local_database,
        #                                     target_user=origin_user,
        #                                     target_password=origin_password,
        #                                     target_host=origin_host,
        #                                     target_database=origin_database,
        #                                     source_table=sourceTable,
        #                                     target_table=targetTable)


if __name__ == "__main__":
    #  从 本地 往 远端  msyql迁移数据          全删全插   慎重使用
    # transfer_local_to_origin_mysql()

    #  从 远端 往 本地  msyql迁移数据          全删全插   慎重使用
    transfer_origin_to_local_mysql()

    #  从 远端 向 本地 服务器刷新 mysql 数据    追加形式
    # append_origin_to_local_mysql()

    #  从 本地 向 远端 服务器刷新 mysql 数据    追加形式
    # append_local_to_origin_mysql()

```

--------------------------------------------------------------------------------
## monitor\__init__.py

```python
from .realtime_monitor import RealtimeMonitor
from .alert_system import AlertSystem

__all__ = [
    'RealtimeMonitor',
    'AlertSystem'
]
```

--------------------------------------------------------------------------------
## monitor\alert_system.py

```python
import logging
import smtplib
from email.mime.text import MIMEText
from email.header import Header
from CommonProperties.Base_Properties import (
    smtp_host, smtp_port, smtp_user, smtp_password, alert_receivers
)
from CommonProperties.Base_utils import timing_decorator

logger = logging.getLogger(__name__)

# 补充邮件配置（如果你的Base_Properties没有）
if not 'smtp_host' in locals():
    smtp_host = "smtp.qq.com"
    smtp_port = 465
    smtp_user = "your_email@qq.com"
    smtp_password = "your_auth_code"
    alert_receivers = ["your_receiver@qq.com"]


class AlertSystem:
    """
    预警通知系统：
    1. 邮件预警
    2. 日志预警
    3. 预警记录持久化
    """

    def __init__(self):
        # 邮件配置（从Base_Properties读取）
        self.smtp_host = smtp_host
        self.smtp_port = smtp_port
        self.smtp_user = smtp_user
        self.smtp_password = smtp_password
        self.receivers = alert_receivers  # 预警接收人邮箱列表

    @timing_decorator
    def send_email_alert(self, alert_title, alert_content):
        """发送邮件预警"""
        try:
            # 构建邮件内容
            msg = MIMEText(alert_content, 'plain', 'utf-8')
            msg['From'] = Header("量化策略监控系统", 'utf-8')
            msg['To'] = Header(",".join(self.receivers), 'utf-8')
            msg['Subject'] = Header(alert_title, 'utf-8')

            # 发送邮件
            smtp_obj = smtplib.SMTP_SSL(self.smtp_host, self.smtp_port)
            smtp_obj.login(self.smtp_user, self.smtp_password)
            smtp_obj.sendmail(self.smtp_user, self.receivers, msg.as_string())
            smtp_obj.quit()

            logger.info(f"✅ 预警邮件发送成功 | 标题：{alert_title} | 接收人：{self.receivers}")
            return True
        except Exception as e:
            logger.error(f"❌ 预警邮件发送失败：{str(e)}")
            return False

    @timing_decorator
    def generate_alert_report(self, factor_alerts, position_alerts, price_alerts):
        """生成预警汇总报告"""
        report = f"""
# 🚨 量化策略预警报告
## 🕒 生成时间：{logging.Formatter('%(asctime)s').formatTime(logging.LogRecord('', 0, '', 0, '', (), ()))}

### 🔍 因子信号变化预警
{self._format_factor_alerts(factor_alerts)}

### 📉 持仓回撤预警
{self._format_position_alerts(position_alerts)}

### 📈 价格波动预警
{self._format_price_alerts(price_alerts)}

## ⚠️ 处理建议
1. 因子信号变化：检查因子计算逻辑是否异常
2. 持仓回撤超限：考虑止损或减仓
3. 价格波动超限：核实市场消息，确认是否调仓
        """
        return report

    def _format_factor_alerts(self, factor_alerts):
        """格式化因子预警"""
        if not factor_alerts:
            return "无因子信号变化预警"

        alert_text = []
        for alert in factor_alerts:
            alert_text.append(
                f"- {alert['stock_code']} | {alert['factor_type']}因子 | "
                f"信号变化：{alert['prev_signal']} → {alert['curr_signal']} | "
                f"时间：{alert['change_time']}"
            )
        return "\n".join(alert_text)

    def _format_position_alerts(self, position_alerts):
        """格式化持仓预警"""
        if not position_alerts:
            return "无持仓回撤预警"

        alert_text = []
        for alert in position_alerts:
            alert_text.append(
                f"- {alert['stock_code']} | 回撤：{alert['drawdown_rate']}% | "
                f"成本：{alert['cost_price']} | 当前：{alert['current_price']}"
            )
        return "\n".join(alert_text)

    def _format_price_alerts(self, price_alerts):
        """格式化价格预警"""
        if not price_alerts:
            return "无价格波动预警"

        alert_text = []
        for alert in price_alerts:
            alert_text.append(
                f"- {alert['stock_code']} | 价格变化：{alert['price_change']}% | "
                f"昨日：{alert['prev_price']} | 今日：{alert['curr_price']}"
            )
        return "\n".join(alert_text)

    @timing_decorator
    def trigger_alert(self, alert_type, alert_data):
        """
        触发预警
        :param alert_type: 预警类型（factor/position/price/all）
        :param alert_data: 预警数据（字典/列表）
        """
        if alert_type == 'factor':
            title = "【量化策略】因子信号变化预警"
            content = self._format_factor_alerts(alert_data)
        elif alert_type == 'position':
            title = "【量化策略】持仓回撤超限预警"
            content = self._format_position_alerts(alert_data)
        elif alert_type == 'price':
            title = "【量化策略】价格波动超限预警"
            content = self._format_price_alerts(alert_data)
        elif alert_type == 'all':
            title = "【量化策略】预警汇总报告"
            content = self.generate_alert_report(
                alert_data.get('factor', []),
                alert_data.get('position', []),
                alert_data.get('price', [])
            )
        else:
            logger.warning(f"未知预警类型：{alert_type}")
            return False

        # 日志预警 + 邮件预警
        logger.warning(f"\n{title}\n{content}")
        return self.send_email_alert(title, content)

```

--------------------------------------------------------------------------------
## monitor\realtime_monitor.py

```python
import time
import logging
from datetime import datetime, timedelta
from CommonProperties import Mysql_Utils
from CommonProperties.Base_utils import timing_decorator
from Others.strategy.factor_library import FactorLibrary

logger = logging.getLogger(__name__)


class RealtimeMonitor:
    """
    实时监控模块：
    1. 监控因子信号变化
    2. 监控持仓收益/回撤
    3. 监控股票池价格波动
    """

    def __init__(self, backtest_engine, stock_codes):
        self.engine = backtest_engine  # 回测引擎实例
        self.stock_codes = stock_codes  # 监控股票池
        self.factor_lib = FactorLibrary()
        self.alert_thresholds = {
            'max_drawdown': 0.1,  # 最大回撤预警阈值（10%）
            'pb_change': 0.2,  # PB因子变化预警（20%）
            'price_change': 0.05  # 价格波动预警（5%）
        }

    @timing_decorator
    def monitor_factor_signals(self):
        """监控因子信号实时变化"""
        logger.info("======= 开始监控因子信号 =======")
        current_date = datetime.now().strftime('%Y%m%d')
        factor_changes = []

        for code in self.stock_codes:
            # 查询当日因子信号
            pb_signal = self.engine.get_factor_value(code, datetime.now().date(), 'pb')
            zt_signal = self.engine.get_factor_value(code, datetime.now().date(), 'zt')
            shareholder_signal = self.engine.get_factor_value(code, datetime.now().date(), 'shareholder')

            # 查询昨日因子信号（对比变化）
            yesterday = (datetime.now() - timedelta(days=1)).strftime('%Y%m%d')
            pb_signal_prev = self.engine.get_factor_value(code, (datetime.now() - timedelta(days=1)).date(), 'pb')

            # 因子信号变化判断
            pb_changed = pb_signal != pb_signal_prev
            if pb_changed:
                factor_changes.append({
                    'stock_code': code,
                    'factor_type': 'PB',
                    'prev_signal': pb_signal_prev,
                    'curr_signal': pb_signal,
                    'change_time': current_date
                })
                logger.warning(f"⚠️ {code} PB因子信号变化：{pb_signal_prev} → {pb_signal}")

            # 输出当前因子状态
            logger.info(
                f"{code} 因子状态 | PB：{pb_signal} | 涨停：{zt_signal} | 筹码：{shareholder_signal}"
            )

        return factor_changes

    @timing_decorator
    def monitor_position_performance(self, cerebro):
        """监控持仓收益/回撤"""
        logger.info("======= 开始监控持仓绩效 =======")
        position_alerts = []

        for data in cerebro.datas:
            code = data._name
            position = cerebro.broker.getposition(data)
            if position.size == 0:
                continue

            # 计算持仓收益/回撤
            cost_price = position.price
            current_price = data.close[0]
            profit_rate = (current_price - cost_price) / cost_price
            drawdown_rate = (cost_price - current_price) / cost_price if current_price < cost_price else 0

            # 回撤预警
            if drawdown_rate > self.alert_thresholds['max_drawdown']:
                alert = {
                    'stock_code': code,
                    'alert_type': 'max_drawdown',
                    'drawdown_rate': round(drawdown_rate * 100, 2),
                    'cost_price': cost_price,
                    'current_price': current_price
                }
                position_alerts.append(alert)
                logger.error(
                    f"🚨 {code} 回撤超限 | 成本：{cost_price} | 当前：{current_price} | 回撤：{drawdown_rate * 100:.2f}%"
                )

            # 输出持仓状态
            logger.info(
                f"{code} 持仓状态 | 成本：{cost_price:.2f} | 当前：{current_price:.2f} | "
                f"收益：{profit_rate * 100:.2f}% | 回撤：{drawdown_rate * 100:.2f}%"
            )

        return position_alerts

    @timing_decorator
    def monitor_price_volatility(self):
        """监控股票价格波动"""
        logger.info("======= 开始监控价格波动 =======")
        price_alerts = []
        current_date = datetime.now().strftime('%Y%m%d')

        for code in self.stock_codes:
            # 读取当日/昨日价格
            kline_df = Mysql_Utils.data_from_mysql_to_dataframe(
                user=self.engine.user,
                password=self.engine.password,
                host=self.engine.host,
                database=self.engine.database,
                table_name='ods_stock_kline_daily_insight',
                start_date=(datetime.now() - timedelta(days=1)).strftime('%Y%m%d'),
                end_date=current_date,
                cols=['htsc_code', 'ymd', 'close']
            )

            if len(kline_df) < 2:
                continue

            # 计算价格变化率
            price_prev = kline_df.iloc[0]['close']
            price_curr = kline_df.iloc[1]['close']
            price_change = (price_curr - price_prev) / price_prev

            # 价格波动预警
            if abs(price_change) > self.alert_thresholds['price_change']:
                alert = {
                    'stock_code': code,
                    'alert_type': 'price_volatility',
                    'price_change': round(price_change * 100, 2),
                    'prev_price': price_prev,
                    'curr_price': price_curr
                }
                price_alerts.append(alert)
                logger.warning(
                    f"⚠️ {code} 价格波动超限 | 昨日：{price_prev:.2f} | 今日：{price_curr:.2f} | "
                    f"变化：{price_change * 100:.2f}%"
                )

        return price_alerts

    def run_monitor(self, cerebro, interval=3600):
        """
        启动实时监控
        :param cerebro: Backtrader Cerebro实例
        :param interval: 监控间隔（秒），默认1小时
        """
        logger.info(f"启动实时监控 | 监控股票池：{self.stock_codes} | 间隔：{interval / 3600}小时")

        while True:
            try:
                # 执行监控
                self.monitor_factor_signals()
                self.monitor_position_performance(cerebro)
                self.monitor_price_volatility()

                # 等待下一次监控
                logger.info(f"监控完成，等待{interval / 3600}小时后继续...\n")
                time.sleep(interval)

            except KeyboardInterrupt:
                logger.info("用户终止监控")
                break
            except Exception as e:
                logger.error(f"监控异常：{str(e)}")
                time.sleep(interval)


```

--------------------------------------------------------------------------------
## review\__init__.py

```python

```

--------------------------------------------------------------------------------
## review\daily_review.py

```python
import logging
import pandas as pd
from datetime import datetime, timedelta
from CommonProperties.Mysql_Utils import data_from_mysql_to_dataframe
from backtest.performance_analysis import PerformanceAnalyzer

logger = logging.getLogger(__name__)


class DailyReview:
    """
    每日复盘模块：
    1. 当日交易复盘
    2. 因子效果复盘
    3. 收益/风险复盘
    4. 生成复盘报告
    """

    def __init__(self, backtest_engine, cerebro, strategy_type):
        self.engine = backtest_engine
        self.cerebro = cerebro
        self.strategy_type = strategy_type
        self.analyzer = PerformanceAnalyzer()
        self.review_date = datetime.now().date()

    def review_daily_trades(self):
        """复盘当日交易"""
        logger.info("======= 开始复盘当日交易 =======")
        trade_data = []

        # 获取策略交易记录
        strat = self.cerebro.runstrats[0][0] if self.cerebro.runstrats else None
        if not strat or not hasattr(strat, 'analyzers'):
            return "当日无交易记录"

        trade_ana = strat.analyzers.trade_analyzer.get_analysis()
        if not hasattr(trade_ana, 'total') or trade_ana.total.closed == 0:
            return "当日无完成交易"

        # 提取当日交易
        for trade in strat._trades:
            trade_date = trade.dtclose.date() if trade.dtclose else None
            if trade_date != self.review_date:
                continue

            trade_data.append({
                'stock_code': trade.data._name,
                'trade_type': '买入' if trade.size > 0 else '卖出',
                'price': trade.price,
                'size': abs(trade.size),
                'pnl': trade.pnl,
                'pnl_rate': (trade.pnl / (trade.price * abs(trade.size))) * 100,
                'trade_time': trade.dtclose.strftime('%Y-%m-%d %H:%M:%S')
            })

        if not trade_data:
            return "当日无交易记录"

        # 格式化交易复盘
        trade_df = pd.DataFrame(trade_data)
        review_text = f"""
### 当日交易汇总
- 交易次数：{len(trade_data)}
- 盈利交易：{len(trade_df[trade_df['pnl'] > 0])}
- 亏损交易：{len(trade_df[trade_df['pnl'] < 0])}
- 总盈亏：{trade_df['pnl'].sum():.2f}元

#### 交易明细
{trade_df.to_string(index=False)}
        """
        return review_text

    def review_factor_effectiveness(self):
        """复盘因子效果"""
        logger.info("======= 开始复盘因子效果 =======")
        if self.strategy_type != 'factor_driven':
            return "非因子驱动策略，跳过因子复盘"

        factor_review = []
        review_date_str = self.review_date.strftime('%Y%m%d')

        for data in self.cerebro.datas:
            code = data._name
            # 查询当日因子信号
            pb_signal = self.engine.get_factor_value(code, self.review_date, 'pb')
            zt_signal = self.engine.get_factor_value(code, self.review_date, 'zt')
            shareholder_signal = self.engine.get_factor_value(code, self.review_date, 'shareholder')

            # 查询当日收益
            position = self.cerebro.broker.getposition(data)
            if position.size == 0:
                profit_rate = 0
            else:
                profit_rate = (data.close[0] - position.price) / position.price * 100

            factor_review.append({
                'stock_code': code,
                'pb_signal': pb_signal,
                'zt_signal': zt_signal,
                'shareholder_signal': shareholder_signal,
                'profit_rate': round(profit_rate, 2),
                'is_profitable': profit_rate > 0
            })

        # 因子效果统计
        factor_df = pd.DataFrame(factor_review)
        if len(factor_df) == 0:
            return "无因子数据可复盘"

        # 计算各因子盈利胜率
        pb_profit_win = len(factor_df[(factor_df['pb_signal'] == True) & (factor_df['is_profitable'] == True)])
        pb_total = len(factor_df[factor_df['pb_signal'] == True])
        pb_win_rate = (pb_profit_win / pb_total * 100) if pb_total > 0 else 0

        zt_profit_win = len(factor_df[(factor_df['zt_signal'] == True) & (factor_df['is_profitable'] == True)])
        zt_total = len(factor_df[factor_df['zt_signal'] == True])
        zt_win_rate = (zt_profit_win / zt_total * 100) if zt_total > 0 else 0

        # 组合因子效果
        combo_profit_win = len(factor_df[
                                   (factor_df['pb_signal'] == True) &
                                   (factor_df['zt_signal'] == True) &
                                   (factor_df['shareholder_signal'] == True) &
                                   (factor_df['is_profitable'] == True)
                                   ])
        combo_total = len(factor_df[
                              (factor_df['pb_signal'] == True) &
                              (factor_df['zt_signal'] == True) &
                              (factor_df['shareholder_signal'] == True)
                              ])
        combo_win_rate = (combo_profit_win / combo_total * 100) if combo_total > 0 else 0

        review_text = f"""
### 当日因子效果复盘
#### 单因子盈利胜率
- PB因子：{pb_win_rate:.2f}%（{pb_profit_win}/{pb_total}）
- 涨停因子：{zt_win_rate:.2f}%（{zt_profit_win}/{zt_total}）

#### 组合因子盈利胜率
- PB+涨停+筹码：{combo_win_rate:.2f}%（{combo_profit_win}/{combo_total}）

#### 因子信号与收益明细
{factor_df.to_string(index=False)}
        """
        return review_text

    def review_risk_return(self):
        """复盘当日收益/风险"""
        logger.info("======= 开始复盘收益风险 =======")
        # 获取账户整体状态
        total_cash = self.cerebro.broker.getcash()
        total_value = self.cerebro.broker.getvalue()
        total_position = total_value - total_cash

        # 计算当日收益
        prev_date = self.review_date - timedelta(days=1)
        prev_value = self._get_historical_portfolio_value(prev_date)
        daily_return = (total_value - prev_value) / prev_value * 100 if prev_value > 0 else 0

        # 计算最大回撤
        strat = self.cerebro.runstrats[0][0] if self.cerebro.runstrats else None
        max_drawdown = 0
        if strat and hasattr(strat.analyzers, 'drawdown'):
            max_drawdown = strat.analyzers.drawdown.get_analysis().get('max', {}).get('drawdown', 0)

        review_text = f"""
### 当日收益/风险复盘
- 账户总资产：{total_value:.2f}元
- 持仓市值：{total_position:.2f}元
- 可用现金：{total_cash:.2f}元
- 当日收益：{daily_return:.2f}%
- 累计最大回撤：{max_drawdown:.2f}%

#### 风险提示
{self._generate_risk_tips(daily_return, max_drawdown)}
        """
        return review_text

    def _get_historical_portfolio_value(self, date):
        """获取历史账户价值（模拟，实际需从数据库读取）"""
        # 此处为模拟逻辑，实际需将每日账户价值持久化到数据库
        date_str = date.strftime('%Y%m%d')
        try:
            # 读取当日收盘后账户价值
            value_df = data_from_mysql_to_dataframe(
                user=self.engine.user,
                password=self.engine.password,
                host=self.engine.host,
                database=self.engine.database,
                table_name='ods_portfolio_daily_value',
                start_date=date_str,
                end_date=date_str,
                cols=['date', 'total_value']
            )
            return value_df['total_value'].iloc[0] if not value_df.empty else self.cerebro.broker.getvalue()
        except Exception:
            return self.cerebro.broker.getvalue()

    def _generate_risk_tips(self, daily_return, max_drawdown):
        """生成风险提示"""
        tips = []
        if daily_return < -5:
            tips.append("⚠️ 当日亏损超过5%，建议检查策略逻辑或暂时减仓")
        if max_drawdown > 20:
            tips.append("⚠️ 累计最大回撤超过20%，策略风险过高，需优化")
        if daily_return > 5:
            tips.append("✅ 当日收益超过5%，策略表现优秀，注意止盈")
        if not tips:
            tips.append("📌 当日收益/风险处于正常范围，继续观察")
        return "\n".join(tips)

    def generate_daily_review_report(self):
        """生成完整的每日复盘报告"""
        logger.info(f"======= 生成{self.review_date}复盘报告 =======")

        # 整合各部分复盘内容
        report = f"""
# 📊 量化策略每日复盘报告
## 🕒 复盘日期：{self.review_date}
## 🎯 策略类型：{self.strategy_type}

### 1. 当日交易复盘
{self.review_daily_trades()}

### 2. 因子效果复盘
{self.review_factor_effectiveness()}

### 3. 收益/风险复盘
{self.review_risk_return()}

### 4. 策略优化建议
{self.analyzer._generate_strategy_suggestion({
            '年化收益率': daily_return * 252,  # 年化当日收益
            '最大回撤': max_drawdown,
            '胜率': self._get_daily_win_rate(),
            '盈亏比': self._get_daily_pl_ratio()
        })}

### 5. 明日操作建议
{self._generate_tomorrow_suggestion()}
        """
        # 保存复盘报告到文件
        self._save_report(report)
        logger.info("✅ 每日复盘报告生成完成")
        return report

    def _get_daily_win_rate(self):
        """获取当日胜率"""
        strat = self.cerebro.runstrats[0][0] if self.cerebro.runstrats else None
        if not strat or not hasattr(strat.analyzers, 'trade_analyzer'):
            return 0
        trade_ana = strat.analyzers.trade_analyzer.get_analysis()
        total = trade_ana.total.closed
        won = trade_ana.won.total if hasattr(trade_ana, 'won') else 0
        return (won / total * 100) if total > 0 else 0

    def _get_daily_pl_ratio(self):
        """获取当日盈亏比"""
        strat = self.cerebro.runstrats[0][0] if self.cerebro.runstrats else None
        if not strat or not hasattr(strat.analyzers, 'trade_analyzer'):
            return 0
        trade_ana = strat.analyzers.trade_analyzer.get_analysis()
        avg_win = trade_ana.won.pnl.average if hasattr(trade_ana, 'won') else 0
        avg_loss = abs(trade_ana.lost.pnl.average) if hasattr(trade_ana, 'lost') else 1
        return avg_win / avg_loss

    def _generate_tomorrow_suggestion(self):
        """生成明日操作建议"""
        suggestions = []
        # 基于当日因子信号
        for data in self.cerebro.datas:
            code = data._name
            pb_signal = self.engine.get_factor_value(code, self.review_date, 'pb')
            zt_signal = self.engine.get_factor_value(code, self.review_date, 'zt')
            shareholder_signal = self.engine.get_factor_value(code, self.review_date, 'shareholder')

            if pb_signal and zt_signal and shareholder_signal:
                suggestions.append(f"✅ {code} 因子信号全部满足，建议继续持有")
            elif not pb_signal:
                suggestions.append(f"⚠️ {code} PB因子信号失效，建议关注估值变化")
            elif not zt_signal:
                suggestions.append(f"⚠️ {code} 涨停因子信号失效，建议关注资金动向")

        if not suggestions:
            suggestions.append("📌 无明确操作建议，建议维持当前仓位")
        return "\n".join(suggestions)

    def _save_report(self, report):
        """保存复盘报告到文件"""
        report_path = f"review_reports/daily_review_{self.review_date}.md"
        try:
            import os
            os.makedirs(os.path.dirname(report_path), exist_ok=True)
            with open(report_path, 'w', encoding='utf-8') as f:
                f.write(report)
            logger.info(f"复盘报告已保存至：{report_path}")
        except Exception as e:
            logger.error(f"保存复盘报告失败：{str(e)}")


```

--------------------------------------------------------------------------------
## strategy\__init__.py

```python
from Others.strategy.factor_library import FactorLibrary

__all__ = ['FactorLibrary']
```

--------------------------------------------------------------------------------
## strategy\factor_library.py

```python
# strategy/factor_library.py
import pandas as pd
import logging
from CommonProperties import Mysql_Utils
from CommonProperties.Base_utils import timing_decorator, convert_ymd_format

logger = logging.getLogger(__name__)


class FactorLibrary:
    """因子计算库：基于现有MySQL数据计算PB/涨停/筹码等因子（支持多日回测）"""

    def __init__(self):
        # 复用MySQL配置
        self.user = Mysql_Utils.origin_user
        self.password = Mysql_Utils.origin_password
        self.host = Mysql_Utils.origin_host
        self.database = Mysql_Utils.origin_database

    @timing_decorator
    def pb_factor(self, start_date, end_date, pb_percentile=0.3):
        """
        计算PB因子：为日期范围内的每一天计算PB信号

        返回:
            DataFrame: ymd, stock_code, pb, pb_signal
        """
        try:
            # 从DWD层读取PB数据
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
                logger.warning(f"PB因子数据为空: {start_date}~{end_date}")
                return pd.DataFrame(columns=['ymd', 'stock_code', 'pb', 'pb_signal'])

            # 数据预处理
            pb_df = convert_ymd_format(pb_df, 'ymd')
            pb_df = pb_df.dropna(subset=['pb'])

            # 转换pb列为数值类型
            try:
                pb_df['pb'] = pd.to_numeric(pb_df['pb'], errors='coerce')
            except:
                pb_df['pb'] = pb_df['pb'].astype(str).str.extract(r'([\d\.]+)')[0].astype(float)

            pb_df = pb_df.dropna(subset=['pb'])

            # 按日计算分位数，标记低PB股票
            # 每个元素是一个dataframe
            result_dfs = []

            # 按日期分组处理
            pb_df['ymd_dt'] = pd.to_datetime(pb_df['ymd'])
            unique_dates = pb_df['ymd_dt'].unique()

            for date in unique_dates:
                date_str = date.strftime('%Y%m%d')
                date_df = pb_df[pb_df['ymd_dt'] == date].copy()

                if len(date_df) > 0:
                    pb_threshold = date_df['pb'].quantile(pb_percentile)
                    date_df['pb_signal'] = date_df['pb'] < pb_threshold
                    date_df['ymd'] = date_str

                    result_dfs.append(date_df[['ymd', 'stock_code', 'pb', 'pb_signal']])

            if result_dfs:
                result_df = pd.concat(result_dfs, ignore_index=True)
                logger.info(f"PB因子计算完成：共{len(result_df)}条记录，日期范围{start_date}~{end_date}")
                return result_df
            else:
                return pd.DataFrame(columns=['ymd', 'stock_code', 'pb', 'pb_signal'])

        except Exception as e:
            logger.error(f"计算PB因子失败：{str(e)}")
            return pd.DataFrame(columns=['ymd', 'stock_code', 'pb', 'pb_signal'])

    # @timing_decorator
    def zt_factor(self, start_date, end_date, lookback_days=5):
        """
        计算涨停因子：为日期范围内的每一天计算涨停信号
        返回:
            DataFrame: ymd, stock_code, zt_signal, latest_zt_date
        """
        try:
            # 1. 读取日期范围内的所有涨停记录
            zt_df = Mysql_Utils.data_from_mysql_to_dataframe(
                user=self.user,
                password=self.password,
                host=self.host,
                database=self.database,
                table_name='dwd_stock_zt_list',
                start_date=start_date,
                end_date=end_date,
                cols=['ymd', 'stock_code']
            )

            if zt_df.empty:
                logger.warning(f"涨停因子数据为空: {start_date}~{end_date}")
                # 返回空DataFrame，但包含正确的列结构
                return pd.DataFrame(columns=['ymd', 'stock_code', 'zt_signal'])

            # 2. 数据预处理
            zt_df = convert_ymd_format(zt_df, 'ymd')
            zt_df['ymd_dt'] = pd.to_datetime(zt_df['ymd'])

            # 3. 获取需要计算的所有日期
            start_dt = pd.to_datetime(start_date, format='%Y%m%d')
            end_dt = pd.to_datetime(end_date, format='%Y%m%d')

            # 从PB数据或K线数据获取实际交易日
            # 简化版：先生成所有日期，后续可以优化
            all_dates = pd.date_range(start=start_dt, end=end_dt, freq='D')

            # 4. 获取所有有涨停记录的股票
            all_zt_stocks = zt_df['stock_code'].unique()

            # 5. 为每只股票构建涨停日期列表
            stock_zt_dates = {}
            for stock in all_zt_stocks:
                stock_dates = zt_df[zt_df['stock_code'] == stock]['ymd_dt'].tolist()
                stock_zt_dates[stock] = sorted(stock_dates)

            # 6. 计算每日涨停信号
            result_data = []

            for current_date in all_dates:
                date_str = current_date.strftime('%Y%m%d')

                for stock in all_zt_stocks:
                    if stock in stock_zt_dates and stock_zt_dates[stock]:
                        # 找到小于等于当前日期的涨停记录
                        zt_dates = [d for d in stock_zt_dates[stock] if d <= current_date]

                        if zt_dates:
                            latest_zt_date = max(zt_dates)
                            days_since_zt = (current_date - latest_zt_date).days

                            # 判断是否在lookback_days窗口内
                            zt_signal = 0 <= days_since_zt <= lookback_days

                            result_data.append({
                                'ymd': date_str,
                                'stock_code': stock,
                                'zt_signal': zt_signal,
                                'latest_zt_date': latest_zt_date.strftime('%Y%m%d')
                            })

            # 7. 转换为DataFrame
            result_df = pd.DataFrame(result_data) if result_data else pd.DataFrame(
                columns=['ymd', 'stock_code', 'zt_signal', 'latest_zt_date']
            )

            # 8. 按日期和股票代码排序
            result_df = result_df.sort_values(['ymd', 'stock_code']).reset_index(drop=True)

            logger.info(
                f"涨停因子计算完成：日期范围 {start_date}~{end_date}，"
                f"共{len(all_dates)}天，{len(all_zt_stocks)}只股票有涨停记录，"
                f"总记录数：{len(result_df)}，"
                f"涨停信号True占比：{result_df['zt_signal'].mean() * 100:.2f}%"
            )

            return result_df[['ymd', 'stock_code', 'zt_signal']]

        except Exception as e:
            logger.error(f"计算涨停因子失败：{str(e)}")
            return pd.DataFrame(columns=['ymd', 'stock_code', 'zt_signal'])

    @timing_decorator
    def shareholder_factor(self, start_date, end_date):
        """
        计算筹码因子：为日期范围内的每一天计算股东数信号

        返回:
            DataFrame: ymd, stock_code, shareholder_signal, total_sh, pct_of_total_sh
        """
        try:
            # 从ODS层读取股东数据
            shareholder_df = Mysql_Utils.data_from_mysql_to_dataframe(
                user=self.user,
                password=self.password,
                host=self.host,
                database=self.database,
                table_name='ods_shareholder_num',
                start_date=start_date,
                end_date=end_date,
                cols=['htsc_code', 'ymd', 'total_sh', 'pct_of_total_sh']
            )

            if shareholder_df.empty:
                logger.warning(f"股东因子数据为空: {start_date}~{end_date}")
                return pd.DataFrame(columns=['ymd', 'stock_code', 'shareholder_signal'])

            # 数据预处理
            shareholder_df = convert_ymd_format(shareholder_df, 'ymd')
            shareholder_df.rename(columns={'htsc_code': 'stock_code'}, inplace=True)

            # 清理股票代码格式（移除后缀）
            shareholder_df['stock_code'] = shareholder_df['stock_code'].astype(str)
            shareholder_df['stock_code'] = shareholder_df['stock_code'].str.split('.').str[0]

            # 转换数据为数值类型
            shareholder_df['total_sh'] = pd.to_numeric(shareholder_df['total_sh'], errors='coerce')
            shareholder_df['pct_of_total_sh'] = pd.to_numeric(shareholder_df['pct_of_total_sh'], errors='coerce')
            shareholder_df = shareholder_df.dropna(subset=['total_sh', 'pct_of_total_sh'])

            # 股东数环比下降标记为True
            shareholder_df['shareholder_signal'] = shareholder_df['pct_of_total_sh'] < 0

            # 按日期排序
            shareholder_df = shareholder_df.sort_values(['ymd', 'stock_code'])

            logger.info(
                f"筹码因子计算完成：共{len(shareholder_df)}条记录，"
                f"股东数下降占比：{shareholder_df['shareholder_signal'].mean() * 100:.2f}%"
            )

            return shareholder_df[['ymd', 'stock_code', 'shareholder_signal', 'total_sh', 'pct_of_total_sh']]

        except Exception as e:
            logger.error(f"计算筹码因子失败：{str(e)}")
            return pd.DataFrame(columns=['ymd', 'stock_code', 'shareholder_signal'])

    @timing_decorator
    def get_stock_kline_data(self, stock_code, start_date, end_date):
        """
        获取股票K线数据（用于回测）
        使用 ods_stock_kline_daily_insight 表
        """
        try:
            # 处理股票代码格式
            stock_code_clean = stock_code.split('.')[0] if '.' in stock_code else stock_code

            # 读取K线数据
            kline_df = Mysql_Utils.data_from_mysql_to_dataframe(
                user=self.user,
                password=self.password,
                host=self.host,
                database=self.database,
                table_name='ods_stock_kline_daily_insight',
                start_date=start_date,
                end_date=end_date,
                cols=['htsc_code', 'ymd', 'open', 'high', 'low', 'close', 'volume']
            )

            if kline_df.empty:
                return pd.DataFrame()

            # 过滤指定股票代码
            kline_df = kline_df[kline_df['htsc_code'].str.contains(stock_code_clean)]

            # 数据预处理
            kline_df = convert_ymd_format(kline_df, 'ymd')
            kline_df.rename(columns={'htsc_code': 'stock_code'}, inplace=True)

            return kline_df

        except Exception as e:
            logger.error(f"获取K线数据失败 {stock_code}: {str(e)}")
            return pd.DataFrame()

    @timing_decorator
    def get_trading_days(self, start_date, end_date):
        """
        获取交易日列表（优化版）
        """
        try:
            # 从K线数据中获取实际的交易日
            kline_dates = Mysql_Utils.data_from_mysql_to_dataframe(
                user=self.user,
                password=self.password,
                host=self.host,
                database=self.database,
                table_name='ods_stock_kline_daily_insight',
                cols=['ymd']
            )['ymd'].unique()

            # 转换为日期格式
            kline_dates = pd.to_datetime(kline_dates, format='%Y%m%d')

            # 筛选日期范围
            start_dt = pd.to_datetime(start_date, format='%Y%m%d')
            end_dt = pd.to_datetime(end_date, format='%Y%m%d')

            trading_days = sorted([d for d in kline_dates if start_dt <= d <= end_dt])

            # 转换为字符串格式
            trading_days_str = [d.strftime('%Y%m%d') for d in trading_days]

            logger.info(f"获取交易日：{len(trading_days_str)}天，从{trading_days_str[0]}到{trading_days_str[-1]}")
            return trading_days_str

        except Exception as e:
            logger.error(f"获取交易日失败：{str(e)}")
            # 返回所有日期作为后备
            start_dt = pd.to_datetime(start_date, format='%Y%m%d')
            end_dt = pd.to_datetime(end_date, format='%Y%m%d')
            all_dates = pd.date_range(start=start_dt, end=end_dt, freq='D')
            return [d.strftime('%Y%m%d') for d in all_dates]

if __name__=='__main__':
    factorlib = FactorLibrary()
    res = factorlib.zt_factor(start_date='20260101', end_date='20260109')



```

--------------------------------------------------------------------------------
## strategy\strategy_engine.py

```python
# strategy/strategy_engine.py
import pandas as pd
import logging
from CommonProperties.Base_utils import timing_decorator

logger = logging.getLogger(__name__)


class StrategyEngine:
    """策略引擎：支持多日回测的策略执行器"""

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
        低PB+筹码集中+涨停 组合因子策略（支持多日回测）

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
```
