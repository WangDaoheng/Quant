# 量化工程V1.0 代码梳理文档
*生成时间: 2026-03-16 10:39:41*
*选中的目录: backtest, CommonProperties, dashboard, monitor, review, strategy*
*特殊包含: datas_prepare/C00_SQL 目录（无论是否选中）*

## 项目统计信息
- 项目根目录: F:\Quant\Backtrader_PJ1
- 总文件数: 28 (其中包含 0 个 C00_SQL 特殊文件)
- Python文件数: 25
- SQL文件数: 3
- Shell文件数: 0
- 有效目录数: 8

# Backtrader_PJ1 项目目录结构
*生成时间: 2026-03-16 10:39:41*
*选中的目录: backtest, CommonProperties, dashboard, monitor, review, strategy*
*特殊包含: datas_prepare/C00_SQL 目录（无论是否选中）*

📁 Backtrader_PJ1/
    📄 main-doubao.py
    📄 main.py
    📄 unlock.py
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
        📁 C00_SQL 🔸
            📄 DWD_mysql_tables.sql 🔸
            📄 MART_mysql_tables.sql 🔸
            📄 ODS_mysql_tables.sql 🔸
            📄 __init__.py 🔸
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
*选中的目录: backtest, CommonProperties, dashboard, monitor, review, strategy*
*特殊包含: datas_prepare/C00_SQL 目录（无论是否选中）*

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
## unlock.py

```python

import mysql.connector

host = '117.72.162.13'
user = 'root'
password = 'WZHwzh123!!!'
database = 'quant'

print("=" * 60)
print("开始解锁")
print("=" * 60)

conn = mysql.connector.connect(
    host=host,
    user=user,
    password=password,
    database=database
)

cursor = conn.cursor()

print("查看当前进程:")
cursor.execute("SHOW FULL PROCESSLIST")
processes = cursor.fetchall()
for p in processes:
    print(f"ID: {p[0]}, 命令: {p[4]}, 状态: {p[6]}, 信息: {p[7]}")

print("\n正在杀死相关进程...")
for p in processes:
    pid = p[0]
    info = p[7]
    if info and ('DELETE' in str(info) or 'UPDATE' in str(info) or 'ALTER' in str(info) or 'INSERT' in str(info)):
        try:
            print(f"  杀死进程 {pid}...")
            cursor.execute(f"KILL {pid}")
            print(f"    ✓ 成功")
        except Exception as e:
            print(f"    ✗ 失败: {e}")

cursor.close()
conn.close()

print("\n解锁完成！")
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


######################  tushare   #############################
ts_token = "300919ac6f3f72efe445092de7643f7e40f8458096149c315c0e467a"
# ts_token = "ab0fc47587ec2b284dd7a604befdd4fecb5f324554545abe0dc1cdf8abd2"


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


# def data_from_dataframe_to_mysql(user, password, host, database='quant', df=pd.DataFrame(), table_name='', merge_on=[]):
#     """
#     把 dataframe 类型数据写入 mysql 表里面, 同时调用了
#     Args:
#         df:
#         table_name:
#         database:
#     Returns:
#     """
#     db_url = f'mysql+pymysql://{user}:{password}@{host}:3306/{database}'
#     engine = create_engine(db_url)
#
#     # 对输入的df的空值做处理
#     df = df.replace({np.nan: None})
#
#     # 确保 df 中的字段列顺序与表中的列顺序一致
#     columns = df.columns.tolist()
#
#     # 检查是否存在重复数据，并将其去除
#     if merge_on:
#         df.drop_duplicates(subset=merge_on, keep='first', inplace=True)
#
#     total_rows = df.shape[0]
#     if total_rows == 0:
#         logging.info(f"所有数据已存在，无需插入新的数据到 {host} 的 {table_name} 表中。")
#         return
#
#     # 使用 INSERT IGNORE 来去重
#     insert_sql = f"""
#     INSERT IGNORE INTO {table_name} ({', '.join(columns)})
#     VALUES ({', '.join([f':{col}' for col in columns])});
#     """
#
#     # 转换 df 为一个可以传递给 executemany 的字典列表
#     values = df.to_dict('records')
#
#     with engine.connect() as connection:
#         transaction = connection.begin()
#         try:
#             connection.execute(text(insert_sql), values)
#             transaction.commit()
#             logging.info(f"成功插入 {total_rows} 行数据到 {host} 的 {table_name} 表中。")
#         except Exception as e:
#             transaction.rollback()
#             logging.error(f"写入 {host} 的表：{table_name} 时发生错误: {e}")
#             raise


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

    # 分批插入
    db_url = f'mysql+pymysql://{user}:{password}@{host}:3306/{database}'
    engine = create_engine(db_url)
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

        logging.info(f"已插入 {inserted}/{total_rows} 条")
        time.sleep(0.5)

    engine.dispose()
    logging.info(f"完成：共插入 {inserted} 条到 {table_name}")
    return inserted


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
    logging.info("    获取最新的<股票代码, 股票名称>")

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
## datas_prepare\C00_SQL\DWD_mysql_tables.sql

```sql
--1.1
------------------  dwd_ashare_stock_base_info   股票基本信息大宽表
CREATE TABLE quant.dwd_ashare_stock_base_info (
       ymd                DATE                     COMMENT '日期'
      ,stock_code         varchar(50)              COMMENT '代码'
      ,stock_name         varchar(50)              COMMENT '名称'
      ,close              double(12,2)             COMMENT '最新收盘价'
      ,change_pct         float                    COMMENT '当日涨跌幅(%)'
      ,volume             BIGINT                   COMMENT '成交量'
      ,trading_amount     double                   COMMENT '成交额'
      ,market_value       double                   COMMENT '流通市值(亿)'
      ,total_value        double                   COMMENT '总市值(亿)'
      ,total_asset        double                   COMMENT '总资产(亿)'
      ,net_asset          double                   COMMENT '净资产(亿)'
      ,total_capital      double                   COMMENT '总股本(亿)'
      ,float_capital      double                   COMMENT '流通股(亿)'
      ,shareholder_num    bigint                   COMMENT '股东人数'
      ,pb                 float(12,2)              COMMENT '市净率'
      ,pe                 float(12,2)              COMMENT '市盈(动)'
      ,market             VARCHAR(50)              COMMENT '市场特征主板创业板等'
      ,plate_names        VARCHAR(500)             COMMENT '板块名称'
      ,UNIQUE KEY unique_ymd_stock_code (ymd, stock_code)
) COMMENT='股票基本信息大宽表';


--1.2
------------------  dwd_stock_zt_list   涨停股票清单
CREATE TABLE quant.dwd_stock_zt_list (
       ymd                DATE          NOT NULL   COMMENT '交易日期'
      ,stock_code         VARCHAR(50)   NOT NULL   COMMENT '股票代码'
      ,stock_name         VARCHAR(50)              COMMENT '股票名称'
      ,last_close         FLOAT                    COMMENT '昨日收盘价'
      ,close              FLOAT                    COMMENT '收盘价'
      ,rate               FLOAT                    COMMENT '涨幅'
      ,market_value       double                   COMMENT '流通市值(亿)'
      ,total_value        double                   COMMENT '总市值(亿)'
      ,total_asset        double                   COMMENT '总资产(亿)'
      ,net_asset          double                   COMMENT '净资产(亿)'
      ,total_capital      double                   COMMENT '总股本(亿)'
      ,float_capital      double                   COMMENT '流通股(亿)'
      ,shareholder_num    bigint                   COMMENT '股东人数'
      ,pb                 varchar(50)              COMMENT '市净率'
      ,pe                 varchar(50)              COMMENT '市盈(动)'
      ,market             VARCHAR(50)              COMMENT '市场特征主板创业板等'
      ,plate_names        VARCHAR(500)             COMMENT '板块名称'
      ,UNIQUE KEY unique_ymd_stock_code (ymd, stock_code)
) COMMENT='涨停股票清单';


--1.3
------------------  dwd_stock_dt_list   跌停股票清单
CREATE TABLE quant.dwd_stock_dt_list (
       ymd                DATE          NOT NULL   COMMENT '交易日期'
      ,stock_code         VARCHAR(50)   NOT NULL   COMMENT '股票代码'
      ,stock_name         VARCHAR(50)              COMMENT '股票名称'
      ,last_close         FLOAT                    COMMENT '昨日收盘价'
      ,close              FLOAT                    COMMENT '收盘价'
      ,rate               FLOAT                    COMMENT '涨幅'
      ,market_value       double                   COMMENT '流通市值(亿)'
      ,total_value        double                   COMMENT '总市值(亿)'
      ,total_asset        double                   COMMENT '总资产(亿)'
      ,net_asset          double                   COMMENT '净资产(亿)'
      ,total_capital      double                   COMMENT '总股本(亿)'
      ,float_capital      double                   COMMENT '流通股(亿)'
      ,shareholder_num    bigint                   COMMENT '股东人数'
      ,pct_of_total_sh    DOUBLE(10, 4)            COMMENT '股东数较上期环比波动百分比'
      ,pb                 varchar(50)              COMMENT '市净率'
      ,pe                 varchar(50)              COMMENT '市盈(动)'
      ,market             VARCHAR(50)              COMMENT '市场特征主板创业板等'
      ,plate_names        VARCHAR(500)             COMMENT '板块名称'
      ,UNIQUE KEY unique_ymd_stock_code (ymd, stock_code)
) COMMENT='跌停股票清单';



--1.4
------------------  dwd_shareholder_num_latest   个股的股东数(全量最新数据表)
CREATE TABLE quant.dwd_shareholder_num_latest (
       ymd                    DATE                    COMMENT '交易日期'
      ,stock_code             varchar(100)            COMMENT '股票代码'
      ,stock_name             varchar(50)             COMMENT '股票名称'
      ,total_sh               DOUBLE                  COMMENT '总股东数'
      ,avg_share              DOUBLE(10, 4)           COMMENT '每个股东平均持股数'
      ,pct_of_total_sh        DOUBLE(10, 4)           COMMENT '股东数较上期环比波动百分比'
      ,pct_of_avg_sh          DOUBLE(10, 4)           COMMENT '每个股东平均持股数较上期环比波动百分比'
      ,UNIQUE KEY unique_ymd_stock_code (ymd, stock_code)
) COMMENT='个股的股东数(全量最新数据表)';




--1.5
------------------  dwd_stock_technical_indicators   股票技术指标预计算表（均线等）
CREATE TABLE quant.dwd_stock_technical_indicators (
     ymd                      DATE        NOT NULL    COMMENT '交易日期'
    ,stock_code               VARCHAR(50) NOT NULL    COMMENT '股票代码'
    ,stock_name               varchar(50)             COMMENT '股票名称'
    ,close                    FLOAT                   COMMENT '收盘价'
    ,volume                   BIGINT                  COMMENT '成交量'
    -- 成交量均线
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







--2.1
------------------  dwd_factor_volume_shrinkage  缩量下跌因子明细表
CREATE TABLE IF NOT EXISTS quant.dwd_factor_volume_shrinkage (
    ymd                     DATE        NOT NULL COMMENT '交易日期',
    stock_code              VARCHAR(50) NOT NULL COMMENT '股票代码',
    stock_name              VARCHAR(50)          COMMENT '股票名称',
    -- 原始数据             
    close                   FLOAT                COMMENT '收盘价',
    volume                  BIGINT               COMMENT '成交量',
    -- 成交量指标           
    vol_ma5                 FLOAT                COMMENT '5日均量',
    vol_ma60                FLOAT                COMMENT '60日均量',
    volume_vs_ma5           DECIMAL(10,2)        COMMENT '成交量/5日均量-1, 单位%',
    -- 缩量判断             
    is_shrink_today         TINYINT(1)           COMMENT '当日是否缩量(1:是,0:否)',
    consecutive_shrink_days INT                  COMMENT '连续缩量天数',
    -- 阴线判断
    is_down                 TINYINT(1)           COMMENT '当日是否阴线(1:是,0:否)',
    consecutive_down_days   INT                  COMMENT '连续阴线天数',
    -- 因子得分
    volume_score            DECIMAL(5,2)         COMMENT '缩量因子得分(0-60)',
    price_score             DECIMAL(5,2)         COMMENT '下跌因子得分(0-40)',
    composite_score         DECIMAL(5,2)         COMMENT '缩量下跌综合得分(0-100)',
    signal_level            CHAR(1)              COMMENT '信号等级(A/B/C/D/E)',
    UNIQUE KEY unique_ymd_stock (ymd, stock_code)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci
COMMENT='缩量下跌因子明细表';




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
CREATE TABLE quant.dwd_stock_a_total_plate (
       ymd                DATE          NOT NULL   COMMENT '日期'
      ,board_code         VARCHAR(50)   NOT NULL   COMMENT '板块代码'
      ,board_name         VARCHAR(50)   NOT NULL   COMMENT '板块名称'
      ,stock_code         VARCHAR(50)              COMMENT '标的代码'
      ,stock_name         VARCHAR(50)              COMMENT '标的名称'
      ,source_table       VARCHAR(50)              COMMENT '来源表'
      ,remark             VARCHAR(50)              COMMENT '备注'
      ,UNIQUE KEY unique_ymd_plate_code (ymd, plate_name, stock_code)
) COMMENT='多渠道板块数据 -- 多渠道汇总';



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


--1.1
------------------  ods_stock_code_daily_insight   当日已上市股票码表
CREATE TABLE quant.ods_stock_code_daily_insight (
     ymd                      DATE        NOT NULL    COMMENT '交易日期'
    ,stock_code               VARCHAR(50) NOT NULL    COMMENT '股票代码'
    ,stock_name               VARCHAR(50)             COMMENT '股票名'
    ,exchange                 VARCHAR(50)             COMMENT '交易所名称'
    ,UNIQUE KEY unique_ymd_stock_code (ymd, stock_code)
) COMMENT='当日已上市股票码表';


--1.2
------------------  ods_stock_kline_daily_insight   当日已上市股票的历史日K
CREATE TABLE quant.ods_stock_kline_daily_insight_now (
     stock_code               VARCHAR(50) NOT NULL    COMMENT '股票代码'
    ,ymd                      DATE        NOT NULL    COMMENT '交易日期'
    ,open                     FLOAT                   COMMENT '开盘价'
    ,close                    FLOAT                   COMMENT '收盘价'
    ,high                     FLOAT                   COMMENT '最高价'
    ,low                      FLOAT                   COMMENT '最低价'
    ,num_trades               BIGINT                  COMMENT '交易笔数'
    ,volume                   BIGINT                  COMMENT '成交量'
    ,UNIQUE KEY unique_ymd_stock_code (ymd, stock_code)
) COMMENT='当日已上市股票的历史日K(日增量表)';


CREATE TABLE quant.ods_stock_kline_daily_insight (
     stock_code               VARCHAR(50) NOT NULL    COMMENT '股票代码'
    ,ymd                      DATE        NOT NULL    COMMENT '交易日期'
    ,open                     FLOAT                   COMMENT '开盘价'
    ,close                    FLOAT                   COMMENT '收盘价'
    ,high                     FLOAT                   COMMENT '最高价'
    ,low                      FLOAT                   COMMENT '最低价'
    ,num_trades               BIGINT                  COMMENT '交易笔数'
    ,volume                   BIGINT                  COMMENT '成交量'
    ,UNIQUE KEY unique_ymd_stock_code (ymd, stock_code)
) COMMENT='当日已上市股票的历史日K(全量表)';


--1.3
------------------  ods_index_a_share_insight   大A的主要指数日K
CREATE TABLE quant.ods_index_a_share_insight_now (
     index_code               VARCHAR(50) NOT NULL    COMMENT '指数代码'
    ,index_name               VARCHAR(50) NOT NULL    COMMENT '指数名称'
    ,ymd                      DATE        NOT NULL    COMMENT '交易日期'
    ,open                     FLOAT                   COMMENT '开盘价'
    ,close                    FLOAT                   COMMENT '收盘价'
    ,high                     FLOAT                   COMMENT '最高价'
    ,low                      FLOAT                   COMMENT '最低价'
    ,volume                   BIGINT                  COMMENT '成交量'
    ,UNIQUE KEY unique_ymd_stock_code (ymd, index_code)
) COMMENT='大A的主要指数日K(日增量表)';


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


--1.4
------------------  ods_stock_limit_summary_insight   当日大A行情温度
CREATE TABLE quant.ods_stock_limit_summary_insight_now (
     ymd                      DATE        NOT NULL    COMMENT '日期'
    ,name                     VARCHAR(50) NOT NULL    COMMENT '市场名称'
    ,today_ZT                 INT                     COMMENT '今日涨停股票数'
    ,today_DT                 INT                     COMMENT '今日跌停股票数'
    ,yesterday_ZT             INT                     COMMENT '昨日涨停股票数'
    ,yesterday_DT             INT                     COMMENT '昨日跌停股票数'
    ,yesterday_ZT_rate        FLOAT                   COMMENT '昨日涨停股票的今日平均涨幅'
    ,UNIQUE KEY unique_ymd_name (ymd, name)
) COMMENT='当日大A行情温度(日增量表)';


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


--1.5
------------------  ods_future_inside_insight   内盘主要期货数据日K
CREATE TABLE quant.ods_future_inside_insight_now (
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
) COMMENT='内盘主要期货数据日K(日增量表)';


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


--1.6
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


--1.7
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


--1.8
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


--1.9
------------------  ods_shareholder_num   个股的股东数
CREATE TABLE quant.ods_shareholder_num_now (
       stock_code             varchar(100)            COMMENT '股票代码'
      ,stock_name             varchar(50)             COMMENT '股票名称'
      ,ymd                    DATE                    COMMENT '交易日期'
      ,total_sh               DOUBLE                  COMMENT '总股东数'
      ,avg_share              DOUBLE(10, 4)           COMMENT '每个股东平均持股数'
      ,pct_of_total_sh        DOUBLE(10, 4)           COMMENT '股东数较上期环比波动百分比'
      ,pct_of_avg_sh          DOUBLE(10, 4)           COMMENT '每个股东平均持股数较上期环比波动百分比'
      ,UNIQUE KEY unique_ymd_stock_code (ymd, stock_code)
) COMMENT='个股的股东数(日增量表)';

CREATE TABLE quant.ods_shareholder_num (
       stock_code             varchar(100)            COMMENT '股票代码'
      ,stock_name             varchar(50)             COMMENT '股票名称'
      ,ymd                    DATE                    COMMENT '交易日期'
      ,total_sh               DOUBLE                  COMMENT '总股东数'
      ,avg_share              DOUBLE(10, 4)           COMMENT '每个股东平均持股数'
      ,pct_of_total_sh        DOUBLE(10, 4)           COMMENT '股东数较上期环比波动百分比'
      ,pct_of_avg_sh          DOUBLE(10, 4)           COMMENT '每个股东平均持股数较上期环比波动百分比'
      ,UNIQUE KEY unique_ymd_stock_code (ymd, stock_code)
) COMMENT='个股的股东数(全量表)';


--1.10
------------------  ods_north_bound_daily   北向持仓数据
CREATE TABLE quant.ods_north_bound_daily_now (
      stock_code              varchar(100)            COMMENT '股票代码'
     ,ymd                     DATE                    COMMENT '交易日期'
     ,sh_hkshare_hold         BIGINT                  COMMENT '持股数量'
     ,pct_total_share         FLOAT                   COMMENT '持股占总股本比例'
     ,UNIQUE KEY unique_ymd_stock_code (ymd, stock_code)
) COMMENT='北向持仓数据(日增量表)';


CREATE TABLE quant.ods_north_bound_daily (
      stock_code              varchar(100)            COMMENT '股票代码'
     ,ymd                     DATE                    COMMENT '交易日期'
     ,sh_hkshare_hold         BIGINT                  COMMENT '持股数量'
     ,pct_total_share         FLOAT                   COMMENT '持股占总股本比例'
     ,UNIQUE KEY unique_ymd_stock_code (ymd, stock_code)
) COMMENT='北向持仓数据(全量表)';


--2.1
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


--2.2
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


--2.3
------------------  ods_exchange_dxy_vantage   美元指数 日K
CREATE TABLE quant.ods_exchange_dxy_vantage (
    ymd                       DATE        NOT NULL    COMMENT '交易日期'
   ,stock_name                VARCHAR(50) NOT NULL    COMMENT '货币对'
   ,UNIQUE KEY unique_ymd_name (ymd, stock_name)
) COMMENT='美元指数 日K';


-------------------------------------------   通达信数据  ---------------------------------
--3.1        
------------------  ods_tdx_stock_concept_plate   通达信概念板块数据
CREATE TABLE quant.ods_tdx_stock_concept_plate (
     ymd                      DATE        NOT NULL    COMMENT '日期'
    ,concept_code             VARCHAR(50) NOT NULL    COMMENT '概念板块代码'
    ,concept_name             VARCHAR(50)             COMMENT '概念板块名称'
    ,stock_code               VARCHAR(50)             COMMENT '股票代码'
    ,stock_name               VARCHAR(50)             COMMENT '股票名称'
) COMMENT='通达信概念板块数据';


--3.2        
------------------  ods_tdx_stock_style_plate   通达信风格板块数据
CREATE TABLE quant.ods_tdx_stock_style_plate (
     ymd                      DATE        NOT NULL    COMMENT '日期'
    ,style_code               VARCHAR(50) NOT NULL    COMMENT '风格板块代码'
    ,style_name               VARCHAR(50)             COMMENT '风格板块名称'
    ,stock_code               VARCHAR(50)             COMMENT '股票代码'
    ,stock_name               VARCHAR(50)             COMMENT '股票名称'
) COMMENT='通达信风格板块数据';


--3.3        
------------------  ods_tdx_stock_industry_plate   通达信行业板块数据
CREATE TABLE quant.ods_tdx_stock_industry_plate (
     ymd                      DATE        NOT NULL    COMMENT '日期'
    ,industry_code            VARCHAR(50) NOT NULL    COMMENT '行业板块代码'
    ,industry_name            VARCHAR(50)             COMMENT '行业板块名称'
    ,stock_code               VARCHAR(50)             COMMENT '股票代码'
    ,stock_name               VARCHAR(50)             COMMENT '股票名称'
) COMMENT='通达信行业板块数据';


--3.4        
------------------  ods_tdx_stock_region_plate   通达信地区板块数据
CREATE TABLE quant.ods_tdx_stock_region_plate (
     ymd                      DATE        NOT NULL    COMMENT '日期'
    ,region_code              VARCHAR(50) NOT NULL    COMMENT '地区板块代码'
    ,region_name              VARCHAR(50)             COMMENT '地区板块名称'
    ,stock_code               VARCHAR(50)             COMMENT '股票代码'
    ,stock_name               VARCHAR(50)             COMMENT '股票名称'
) COMMENT='通达信地区板块数据';


--3.5        
------------------  ods_tdx_stock_index_plate   通达信指数板块数据
CREATE TABLE quant.ods_tdx_stock_index_plate (
     ymd                      DATE        NOT NULL    COMMENT '日期'
    ,index_code               VARCHAR(50) NOT NULL    COMMENT '指数板块代码'
    ,index_name               VARCHAR(50)             COMMENT '指数板块名称'
    ,stock_code               VARCHAR(50)             COMMENT '股票代码'
    ,stock_name               VARCHAR(50)             COMMENT '股票名称'
) COMMENT='通达信指数板块数据';


--3.6        
------------------  ods_tdx_stock_pepb_info   股票基本面数据_资产数据   需手动下载的
CREATE TABLE quant.ods_tdx_stock_pepb_info (
     ymd                      DATE                    COMMENT '日期'
    ,stock_code               varchar(50)             COMMENT '代码'
    ,stock_name               varchar(50)             COMMENT '名称'
    ,market_value             double                  COMMENT '流通市值(亿)'
    ,total_asset              double                  COMMENT '总资产(亿)'
    ,net_asset                double                  COMMENT '净资产(亿)'
    ,total_capital            double                  COMMENT '总股本(亿)'
    ,float_capital            double                  COMMENT '流通股(亿)'
    ,shareholder_num          bigint                  COMMENT '股东人数'
    ,pb                       double                  COMMENT '市净率'
    ,pe                       double                  COMMENT '市盈(动)'
    ,industry                 varchar(50)             COMMENT '细分行业'
    ,UNIQUE KEY unique_ymd_stock_code (ymd, stock_code)
) COMMENT='股票基本面数据_资产数据';



-------------------------------------------   akshare 数据  ---------------------------------
--4.1        
------------------  ods_akshare_stock_value_em   股票基本面数据_估值数据              个股的全量历史数据   不可选定日期
CREATE TABLE quant.ods_akshare_stock_value_em (
     ymd                      DATE                    COMMENT '数据日期'
    ,stock_code               varchar(50)             COMMENT '股票代码'
    ,close                    float                   COMMENT '当日收盘价(元)'
    ,change_pct               float                   COMMENT '当日涨跌幅(%)'
    ,total_market             double                  COMMENT '总市值(元)'
    ,circulation_market       double                  COMMENT '流通市值(元)'
    ,total_shares             double                  COMMENT '总股本(股)'
    ,circulation_shares       double                  COMMENT '流通股本(股)'
    ,pe_ttm                   float(12,2)             COMMENT 'PE(TTM)'
    ,pe_static                float(12,2)             COMMENT 'PE(静)'
    ,pb                       float(12,2)             COMMENT '市净率'
    ,peg                      float(12,2)             COMMENT 'PEG值'
    ,pcf                      float(12,2)             COMMENT '市现率'
    ,ps                       float(12,2)             COMMENT '市销率'
    ,UNIQUE KEY unique_ymd_stock_code (ymd, stock_code)
) COMMENT='股票基本面数据_估值数据';


--4.2
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


--4.3
------------------  ods_akshare_stock_cyq_em   股票基本面数据_筹码数据                 个股的全量历史数据   不可选定日期
CREATE TABLE quant.ods_akshare_stock_cyq_em (
     ymd                      DATE                    COMMENT '日期'
    ,stock_code               varchar(50)             COMMENT '股票代码'
    ,stock_name               varchar(50)             COMMENT '股票名称'
    ,profit_ratio             float                   COMMENT '获利比例'
    ,avg_cost                 float                   COMMENT '平均成本'
    ,cost_low_90              float                   COMMENT '90成本-低'
    ,cost_high_90             float                   COMMENT '90成本-高'
    ,concentration_90         float                   COMMENT '90集中度'
    ,cost_low_70              float                   COMMENT '70成本-低'
    ,cost_high_70             float                   COMMENT '70成本-高'
    ,concentration_70         float                   COMMENT '70集中度'
    ,UNIQUE KEY unique_ymd_stock_code (ymd, stock_code)
) COMMENT='股票基本面数据_筹码数据';


--4.4
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


--4.5
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


--4.6
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


--4.7
------------------  ods_akshare_stock_zh_a_spot_em            行情数据_个股行情数据  全量的每日切片数据 不可指定日期
CREATE TABLE quant.ods_akshare_stock_zh_a_spot_em (
     ymd                      DATE                    COMMENT '数据日期（行情交易日，统一日期维度）'
    ,serial_num               bigint                  COMMENT '序号'
    ,stock_code               varchar(50)             COMMENT '代码'
    ,stock_name               varchar(50)             COMMENT '名称'
    ,close                    float                   COMMENT '最新价格'
    ,change_pct               float                   COMMENT '涨跌幅(%)'
    ,change_amt               float                   COMMENT '涨跌额'
    ,trading_volume           float                   COMMENT '成交量(手)'
    ,trading_amount           double                  COMMENT '成交额(元)'
    ,amplitude                float                   COMMENT '振幅(%)'
    ,high                     float                   COMMENT '最高'
    ,low                      float                   COMMENT '最低'
    ,open                     float                   COMMENT '今开'
    ,prev_close               float                   COMMENT '昨收'
    ,volume_ratio             float                   COMMENT '量比'
    ,turnover_rate            float                   COMMENT '换手率(%)'
    ,pe_dynamic               float                   COMMENT '市盈率-动态'
    ,pb                       float                   COMMENT '市净率'
    ,total_market             double                  COMMENT '总市值(元)'
    ,circulation_market       double                  COMMENT '流通市值(元)'
    ,price_rise_speed         float                   COMMENT '涨速'
    ,five_min_price_change    float                   COMMENT '5分钟涨跌(%)'
    ,sixty_day_price_change   float                   COMMENT '60日涨跌幅(%)'
    ,ytd_price_change         float                   COMMENT '年初至今涨跌幅(%)'
    ,UNIQUE KEY unique_ymd_stock_code (ymd, stock_code)
) COMMENT='行情数据_个股行情数据';


--4.8
------------------  ods_akshare_stock_board_concept_name_em   行情数据_板块行情数据           全量的每日切片数据 不可指定日期   板块三剑客1 
CREATE TABLE quant.ods_akshare_stock_board_concept_name_em (
     ymd                      DATE                    COMMENT '数据日期（核心日期维度，适配量化数据统一归档）'
    ,ranking                  int                     COMMENT '排名'
    ,board_name               varchar(100)            COMMENT '板块名称'
    ,board_code               varchar(50)             COMMENT '板块代码'
    ,close                    float                   COMMENT '最新价'
    ,change_amt               float                   COMMENT '涨跌额'
    ,change_pct               float                   COMMENT '涨跌幅(%)'
    ,total_market             double                  COMMENT '总市值'
    ,turnover_rate            float                   COMMENT '换手率(%)'
    ,rising_stocks_num        int                     COMMENT '上涨家数'
    ,falling_stocks_num       int                     COMMENT '下跌家数'
    ,leading_stock            varchar(100)            COMMENT '领涨股票'
    ,leading_stock_pct        float                   COMMENT '领涨股票-涨跌幅(%)'
    ,UNIQUE KEY unique_ymd_board_code (ymd, board_code)
) COMMENT='行情数据_板块行情数据';


--4.9
------------------  ods_akshare_stock_board_concept_cons_em   行情数据_板块内个股的行情数据    全量的每日切片数据 不可指定日期   板块三剑客2
CREATE TABLE quant.ods_akshare_stock_board_concept_cons_em (
     ymd                      DATE                    COMMENT '数据日期（核心日期维度，用于归档和跨表关联）'
    ,board_name               varchar(100)            COMMENT '板块名称'
    ,board_code               varchar(50)             COMMENT '板块代码'
    ,serial_num               int                     COMMENT '序号'
    ,stock_code               varchar(50)             COMMENT '代码'
    ,stock_name               varchar(50)             COMMENT '名称'
    ,close                    float                   COMMENT '最新价'
    ,change_pct               float                   COMMENT '涨跌幅(%)'
    ,change_amt               float                   COMMENT '涨跌额'
    ,trading_volume           float                   COMMENT '成交量(手)'
    ,trading_amount           float                   COMMENT '成交额'
    ,amplitude                float                   COMMENT '振幅(%)'
    ,high                     float                   COMMENT '最高'
    ,low                      float                   COMMENT '最低'
    ,open                     float                   COMMENT '今开'
    ,prev_close               float                   COMMENT '昨收'
    ,turnover_rate            float                   COMMENT '换手率(%)'
    ,pe_dynamic               float                   COMMENT '市盈率-动态'
    ,pb                       float                   COMMENT '市净率'
    ,UNIQUE KEY unique_ymd_stock_code (ymd, stock_code)
) COMMENT='行情数据_板块内个股的行情数据';


--4.10
------------------  ods_akshare_stock_board_concept_hist_em   行情数据_板块历史行情数据    可指定日期范围   板块三剑客3
CREATE TABLE quant.ods_akshare_stock_board_concept_hist_em (
     ymd                      DATE                    COMMENT '日期（行情交易日）'
    ,board_code               varchar(50)             COMMENT '板块代码（补充字段，关联板块基础信息，适配量化关联分析）'
    ,open                     float                   COMMENT '开盘'
    ,close                    float                   COMMENT '收盘'
    ,high                     float                   COMMENT '最高'
    ,low                      float                   COMMENT '最低'
    ,change_pct               float                   COMMENT '涨跌幅(%)'
    ,change_amt               float                   COMMENT '涨跌额'
    ,trading_volume           bigint                  COMMENT '成交量'
    ,trading_amount           double                  COMMENT '成交额'
    ,amplitude                float                   COMMENT '振幅(%)'
    ,turnover_rate            float                   COMMENT '换手率(%)'
    ,UNIQUE KEY unique_ymd_board_code (ymd, board_code)
) COMMENT='行情数据_板块历史行情数据';




--4.11
------------------  ods_akshare_board_concept_name_ths          行情数据_同花顺板块码值       全量的每日切片数据 不可指定日期   同花顺板块三剑客1 
CREATE TABLE quant.ods_akshare_board_concept_name_ths (
     ymd                      DATE                    COMMENT '数据日期（核心日期维度，适配量化数据统一归档）'
    ,board_name               varchar(100)            COMMENT '板块名称'
    ,board_code               varchar(50)             COMMENT '板块代码'
    ,UNIQUE KEY unique_ymd_board_code (ymd, board_code)
) COMMENT='板块数据_同花顺板块码值';


--4.12
------------------  ods_akshare_stock_board_concept_index_ths   行情数据_同花顺板块行情数据    全量的每日切片数据 不可指定日期   板块三剑客2
CREATE TABLE quant.ods_akshare_stock_board_concept_index_ths (
     ymd                      DATE                    COMMENT '数据日期（核心日期维度，用于归档和跨表关联）'
    ,board_name               varchar(100)            COMMENT '板块名称'
    ,board_code               varchar(50)             COMMENT '板块代码'
    ,open                     float                   COMMENT '今开'
    ,high                     float                   COMMENT '最高'
    ,low                      float                   COMMENT '最低'
    ,close                    float                   COMMENT '最新价'
    ,trading_volume           float                   COMMENT '成交量(手)'
    ,trading_amount           float                   COMMENT '成交额'
    ,UNIQUE KEY unique_ymd_board_code (ymd, board_code)
) COMMENT='行情数据_板块内个股的行情数据';



--4.13
------------------  ods_akshare_stock_board_concept_index_ths   行情数据_同花顺板块内含股票    手动跑爬虫获取
CREATE TABLE quant.ods_akshare_stock_board_concept_maps_ths (
     ymd                      DATE                    COMMENT '数据日期（核心日期维度，用于归档和跨表关联）'
    ,board_name               varchar(100)            COMMENT '板块名称'
    ,board_code               varchar(50)             COMMENT '板块代码'
    ,stock_code               varchar(50)             COMMENT '代码'
    ,stock_name               varchar(50)             COMMENT '名称'
    ,UNIQUE KEY unique_ymd_board_code (ymd, board_code)
) COMMENT='行情数据_同花顺板块内含股票';



----------------------------------------- tushare 数据  ----------------------------------------------
-- ============ 1. 同花顺板块指数列表 (ths_index) ============
-- ============ 1. 同花顺板块码值（基础信息） ============
-- 对应接口：ths_index
CREATE TABLE quant.ods_tushare_board_concept_name_ths (
     ymd                      DATE                    COMMENT '数据日期（核心日期维度，适配量化数据统一归档）'
    ,board_name               varchar(100)            COMMENT '板块名称'
    ,board_code               varchar(50)             COMMENT '板块代码'
    ,component_count          int                     COMMENT '成分个数'
    ,market                   varchar(10)             COMMENT '交易所（A-沪深 HK-港股 US-美股）'
    ,list_date                varchar(8)              COMMENT '上市日期YYYYMMDD'
    ,index_type               varchar(10)             COMMENT '指数类型（N-概念 I-行业 R-地域 S-特色 ST-风格 TH-主题 BB-宽基）'
    ,UNIQUE KEY unique_ymd_board_code (ymd, board_code)
) COMMENT='板块数据_同花顺板块码值_Tushare';


-- ============ 2. 同花顺板块历史行情数据 ============
-- 对应接口：ths_daily
CREATE TABLE quant.ods_tushare_stock_board_concept_hist_ths (
     ymd                      DATE                    COMMENT '日期（行情交易日）'
    ,board_name               varchar(100)            COMMENT '板块名称'
    ,board_code               varchar(50)             COMMENT '板块代码'
    ,open                     float                   COMMENT '开盘点位'
    ,high                     float                   COMMENT '最高点位'
    ,low                      float                   COMMENT '最低点位'
    ,close                    float                   COMMENT '收盘点位'
    ,prev_close               float                   COMMENT '昨日收盘点'
    ,avg_price                float                   COMMENT '平均价'
    ,change_amt               float                   COMMENT '涨跌点位'
    ,change_pct               float                   COMMENT '涨跌幅(%)'
    ,trading_volume           float                   COMMENT '成交量'
    ,turnover_rate            float                   COMMENT '换手率(%)'
    ,total_market_value       float                   COMMENT '总市值'
    ,float_market_value       float                   COMMENT '流通市值'
    ,UNIQUE KEY unique_ymd_board_code (ymd, board_code)
) COMMENT='行情数据_同花顺板块历史行情_Tushare';


-- ============ 3. 同花顺板块内含股票（核心） ============
-- 对应接口：ths_member
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


-- ============ 4. 东方财富概念板块行情数据 ============
-- 对应接口：dc_index
CREATE TABLE quant.ods_tushare_stock_board_concept_name_em (
     ymd                      DATE                    COMMENT '数据日期（核心日期维度，适配量化数据统一归档）'
    ,board_name               varchar(100)            COMMENT '板块名称'
    ,board_code               varchar(50)             COMMENT '板块代码'
    ,leading_stock            varchar(100)            COMMENT '领涨股票名称'
    ,leading_stock_code       varchar(50)             COMMENT '领涨股票代码'
    ,change_pct               float                   COMMENT '涨跌幅(%)'
    ,leading_stock_pct        float                   COMMENT '领涨股票涨跌幅(%)'
    ,total_market_value       float                   COMMENT '总市值（万元）'
    ,turnover_rate            float                   COMMENT '换手率(%)'
    ,rising_stocks_num        int                     COMMENT '上涨家数'
    ,falling_stocks_num       int                     COMMENT '下降家数'
    ,UNIQUE KEY unique_ymd_board_code (ymd, board_code)
) COMMENT='行情数据_东方财富概念板块行情_Tushare';


-- ============ 5. 东方财富板块历史行情数据 ============
-- 对应接口：dc_daily
CREATE TABLE quant.ods_tushare_stock_board_concept_hist_em (
     ymd                      DATE                    COMMENT '日期（行情交易日）'
    ,board_name               varchar(100)            COMMENT '板块名称'
    ,board_code               varchar(50)             COMMENT '板块代码'
    ,open                     float                   COMMENT '开盘点位'
    ,high                     float                   COMMENT '最高点位'
    ,low                      float                   COMMENT '最低点位'
    ,close                    float                   COMMENT '收盘点位'
    ,change_amt               float                   COMMENT '涨跌点位'
    ,change_pct               float                   COMMENT '涨跌幅(%)'
    ,trading_volume           float                   COMMENT '成交量(股)'
    ,trading_amount           float                   COMMENT '成交额(元)'
    ,amplitude                float                   COMMENT '振幅(%)'
    ,turnover_rate            float                   COMMENT '换手率(%)'
    ,category                 varchar(20)             COMMENT '板块类型（概念板块/行业板块/地域板块）'
    ,UNIQUE KEY unique_ymd_board_code (ymd, board_code)
) COMMENT='行情数据_东方财富板块历史行情_Tushare';


-- ============ 6. 东方财富板块内含股票 ============
-- 对应接口：dc_member
CREATE TABLE quant.ods_tushare_stock_board_concept_maps_em (
     ymd                      DATE                    COMMENT '数据日期（核心日期维度，用于归档和跨表关联）'
    ,board_name               varchar(100)            COMMENT '板块名称'
    ,board_code               varchar(50)             COMMENT '板块代码'
    ,stock_code               varchar(50)             COMMENT '成分股票代码'
    ,stock_name               varchar(100)            COMMENT '成分股名称'
    ,UNIQUE KEY unique_ymd_board_stock (ymd, board_code, stock_code)
) COMMENT='行情数据_东方财富板块内含股票_Tushare';





--6.1
------------------  ods_stock_kline_daily_ts   行情数据_A股历史日K线的tushare数据
CREATE TABLE quant.ods_stock_kline_daily_ts (
     stock_code               VARCHAR(50) NOT NULL    COMMENT '股票代码'
    ,ymd                      DATE        NOT NULL    COMMENT '交易日期'
    ,open                     FLOAT                   COMMENT '开盘价'
    ,close                    FLOAT                   COMMENT '收盘价'
    ,high                     FLOAT                   COMMENT '最高价'
    ,low                      FLOAT                   COMMENT '最低价'
    ,change_pct               FLOAT                   COMMENT '当日涨跌幅(%)'
    ,volume                   BIGINT                  COMMENT '成交量'
    ,trading_amount           double                  COMMENT '成交额'
    ,today_pct                decimal(10,2)           COMMENT '日内涨跌幅'
    ,is_down                  tinyint                 COMMENT '是否阴线'
    ,UNIQUE KEY unique_ymd_stock_code (ymd, stock_code)
) COMMENT='行情数据_A股历史日K线的tushare数据';









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
                cols=['stock_code', 'ymd', 'close']
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

    def pb_factor_score(self, start_date, end_date, reverse=True, save_to_cache=True):
        """
        计算PB因子百分制评分（0-100分）
        简化版：有数据的计算排名，无数据的给0分
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
                    valid_data['pb_score'] = ((max_rank - valid_data['pb_rank']) / max_rank * 100).round(2)

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

    def zt_factor_score(self, start_date, end_date, lookback_days=5, scoring_method='linear', save_to_cache=True):
        """
        计算涨停因子百分制评分（0-100分）

        Args:
            start_date: 开始日期，格式'YYYYMMDD'
            end_date: 结束日期，格式'YYYYMMDD'
            lookback_days: 回溯交易日数量，默认5天
            scoring_method: 评分方法
                - 'linear': 线性得分，每涨停一次得 100/lookback_days 分
                - 'log': 对数得分，涨停越多边际效应递减
                - 'binary': 二元得分，有涨停就得100分
            save_to_cache: 是否保存到缓存

        Returns:
            DataFrame: 包含 ymd, stock_code, zt_score 三列
        """
        try:
            # 1. 获取完整的交易日列表（包含历史数据）
            # 计算合理的起始查询日期：往前推 lookback_days * 2 + 5 天
            start_dt = pd.to_datetime(start_date)
            query_start_dt = start_dt - pd.Timedelta(days=lookback_days * 2 + 5)
            query_start_date = query_start_dt.strftime('%Y%m%d')

            # 获取从查询起始日期到 end_date 的所有交易日
            full_trading_days = self.get_trading_days(query_start_date, end_date)
            if not full_trading_days:
                logger.warning(f"未获取到交易日数据，范围: {query_start_date} - {end_date}")
                result_df = pd.DataFrame(columns=['ymd', 'stock_code', 'zt_score'])
            else:
                logger.info(
                    f"完整交易日范围: {full_trading_days[0]} 到 {full_trading_days[-1]}, 共{len(full_trading_days)}个交易日")

                # 2. 找到 start_date 在完整交易日列表中的位置
                try:
                    start_idx = full_trading_days.index(start_date)
                except ValueError:
                    # 如果 start_date 不是交易日，找第一个大于等于 start_date 的交易日
                    start_idx = None
                    for i, d in enumerate(full_trading_days):
                        if d >= start_date:
                            start_idx = i
                            break
                    if start_idx is None:
                        logger.warning(f"在交易日列表中找不到 {start_date} 及之后的日期")
                        result_df = pd.DataFrame(columns=['ymd', 'stock_code', 'zt_score'])
                    else:
                        # 3. 找到 end_date 在完整交易日列表中的位置
                        try:
                            end_idx = full_trading_days.index(end_date)
                        except ValueError:
                            # 如果 end_date 不是交易日，找最后一个小于等于 end_date 的交易日
                            end_idx = len(full_trading_days) - 1
                            for i in range(len(full_trading_days) - 1, -1, -1):
                                if full_trading_days[i] <= end_date:
                                    end_idx = i
                                    break

                        # 4. 截取需要计算的交易日范围（从 start_date 到 end_date）
                        target_trading_days = full_trading_days[start_idx:end_idx + 1]
                        logger.info(
                            f"目标计算区间: {target_trading_days[0]} 到 {target_trading_days[-1]}, 共{len(target_trading_days)}个交易日")

                        # 5. 计算需要查询涨停记录的起始日期（精确计算回溯期）
                        # 最早需要回溯的交易日索引
                        earliest_needed_idx = max(0, start_idx - lookback_days)
                        actual_query_start = full_trading_days[earliest_needed_idx]
                        logger.info(
                            f"实际查询涨停记录范围: {actual_query_start} 到 {end_date} (回溯{lookback_days}个交易日)")

                        # 6. 获取涨停记录
                        zt_df = Mysql_Utils.data_from_mysql_to_dataframe(
                            user=self.user,
                            password=self.password,
                            host=self.host,
                            database=self.database,
                            table_name='dwd_stock_zt_list',
                            start_date=actual_query_start,
                            end_date=end_date,
                            cols=['ymd', 'stock_code']
                        )

                        if zt_df.empty:
                            logger.info(f"在范围 {actual_query_start} - {end_date} 内无涨停记录，全部返回0分")
                            result_df = self._get_zero_scores(target_trading_days, start_date, end_date, 'zt_score')
                        else:
                            # 7. 确保日期列是字符串格式
                            if not pd.api.types.is_string_dtype(zt_df['ymd']):
                                zt_df['ymd'] = zt_df['ymd'].astype(str)

                            # 8. 获取全量股票列表
                            stock_base_df = Mysql_Utils.data_from_mysql_to_dataframe(
                                user=self.user,
                                password=self.password,
                                host=self.host,
                                database=self.database,
                                table_name='dwd_ashare_stock_base_info',
                                start_date=target_trading_days[-1],
                                end_date=target_trading_days[-1],
                                cols=['stock_code']
                            )

                            if stock_base_df.empty:
                                logger.warning("未获取到股票基础信息")
                                result_df = pd.DataFrame(columns=['ymd', 'stock_code', 'zt_score'])
                            else:
                                all_stocks = stock_base_df['stock_code'].unique()
                                logger.info(f"全量股票数量: {len(all_stocks)}")

                                # 9. 为每只股票构建涨停字典
                                stock_zt_dict = {}
                                for _, row in zt_df.iterrows():
                                    stock = row['stock_code']
                                    date = row['ymd']
                                    if stock not in stock_zt_dict:
                                        stock_zt_dict[stock] = []
                                    stock_zt_dict[stock].append(date)

                                logger.info(f"有涨停记录的股票数量: {len(stock_zt_dict)}")

                                # 10. 为每个目标交易日预先计算回溯起始日期（使用完整交易日列表）
                                lookback_start_dates = {}
                                date_to_idx = {date: idx for idx, date in enumerate(full_trading_days)}

                                for current_date in target_trading_days:
                                    current_idx = date_to_idx[current_date]
                                    # 计算回溯起始索引：取前 lookback_days 个交易日
                                    start_idx = max(0, current_idx - lookback_days + 1)  # +1 是因为要包含当前日
                                    lookback_start_dates[current_date] = full_trading_days[start_idx]

                                # 11. 根据评分方法计算得分函数
                                if scoring_method == 'linear':
                                    # 线性得分：每涨停一次得 100/lookback_days 分
                                    max_possible_zts = lookback_days
                                    score_per_zt = 100 / max_possible_zts if max_possible_zts > 0 else 20
                                    logger.info(f"线性评分: 每涨停一次得 {score_per_zt:.2f} 分")

                                    def calculate_score(zt_count):
                                        return min(zt_count * score_per_zt, 100)

                                elif scoring_method == 'log':
                                    # 对数得分：涨停越多边际效应递减
                                    # 公式：log2(涨停次数+1) * (100/log2(lookback_days+1))
                                    import math
                                    max_score_factor = 100 / math.log2(lookback_days + 1) if lookback_days > 0 else 20

                                    def calculate_score(zt_count):
                                        if zt_count == 0:
                                            return 0
                                        score = math.log2(zt_count + 1) * max_score_factor
                                        return min(round(score, 2), 100)

                                    logger.info(f"对数评分: 最大得分因子 {max_score_factor:.2f}")

                                elif scoring_method == 'binary':
                                    # 二元得分：有涨停就得100分
                                    def calculate_score(zt_count):
                                        return 100 if zt_count > 0 else 0

                                    logger.info("二元评分: 有涨停得100分，无涨停得0分")
                                else:
                                    # 默认线性
                                    score_per_zt = 100 / lookback_days if lookback_days > 0 else 20

                                    def calculate_score(zt_count):
                                        return min(zt_count * score_per_zt, 100)

                                    logger.info(f"默认线性评分: 每涨停一次得 {score_per_zt:.2f} 分")

                                # 12. 计算每个交易日的得分
                                result_data = []
                                total_days = len(target_trading_days)

                                for i, current_date in enumerate(target_trading_days):
                                    if i % 100 == 0 and i > 0:  # 每100个交易日打印一次进度
                                        logger.info(f"处理进度: {i}/{total_days}")

                                    lookback_start = lookback_start_dates[current_date]

                                    for stock in all_stocks:
                                        zt_score = 0

                                        if stock in stock_zt_dict:
                                            # 统计回溯期内涨停次数（使用字符串比较，精确到日）
                                            recent_zts = [d for d in stock_zt_dict[stock]
                                                          if lookback_start <= d <= current_date]

                                            zt_score = calculate_score(len(recent_zts))
                                            # 四舍五入保留2位小数
                                            zt_score = round(zt_score, 2)

                                        result_data.append({
                                            'ymd': current_date,
                                            'stock_code': stock,
                                            'zt_score': zt_score
                                        })

                                result_df = pd.DataFrame(result_data)

                                # 13. 统计得分分布
                                score_distribution = result_df['zt_score'].value_counts().sort_index()
                                logger.info(f"得分分布(前10): {dict(list(score_distribution.head(10).items()))}")

            logger.info(
                f"涨停因子计算完成：共{len(result_df)}条记录，使用{lookback_days}个交易日回溯，评分方法:{scoring_method}")

            # 保存到缓存（只添加这一行，不改变原有逻辑）
            if save_to_cache:
                self.cached_factors['zt'] = result_df.copy()

            return result_df[['ymd', 'stock_code', 'zt_score']]

        except Exception as e:
            logger.error(f"计算涨停因子失败：{str(e)}")
            import traceback
            logger.error(traceback.format_exc())
            return pd.DataFrame(columns=['ymd', 'stock_code', 'zt_score'])

    def shareholder_factor_score(self, start_date, end_date, save_to_cache=True):
        """
        计算筹码因子百分制评分（0-100分）
        使用 dwd_shareholder_num_latest 表

        评分逻辑：股东人数减少得高分，增加得低分
        使用平滑的 sigmoid 函数

        Args:
            start_date: 开始日期 (YYYYMMDD)
            end_date: 结束日期 (YYYYMMDD)
            save_to_cache: 是否保存到缓存

        Returns:
            DataFrame: 包含 ymd, stock_code, stock_name, shareholder_score
        """
        try:
            import math

            # 获取股东数据
            shareholder_df = Mysql_Utils.data_from_mysql_to_dataframe(
                user=self.user,
                password=self.password,
                host=self.host,
                database=self.database,
                table_name='dwd_shareholder_num_latest',
                start_date=start_date,
                end_date=end_date,
                cols=['ymd', 'stock_code', 'stock_name', 'pct_of_total_sh']
            )

            if shareholder_df.empty:
                result_df = pd.DataFrame(columns=['ymd', 'stock_code', 'stock_name', 'shareholder_score'])
            else:
                # 转换数值
                shareholder_df['pct_of_total_sh'] = pd.to_numeric(shareholder_df['pct_of_total_sh'], errors='coerce')

                # 定义平滑得分函数
                def smooth_score(pct):
                    if pd.isna(pct):
                        return 0.0
                    # sigmoid 变换: 股东减少(-) → 高分，股东增加(+) → 低分
                    x = pct * 0.15  # 0.15 控制曲线陡峭程度
                    sigmoid = 1 / (1 + math.exp(-x))
                    return round(100 * (1 - sigmoid), 2)

                # 计算得分
                shareholder_df['shareholder_score'] = shareholder_df['pct_of_total_sh'].apply(smooth_score)
                result_df = shareholder_df[['ymd', 'stock_code', 'stock_name', 'shareholder_score']]

            logger.info(f"股东人数因子计算完成：共{len(result_df)}条记录")

            # 保存到缓存
            if save_to_cache:
                self.cached_factors['shareholder'] = result_df.copy()

            return result_df

        except Exception as e:
            logger.error(f"计算股东数因子失败：{str(e)}")
            return pd.DataFrame(columns=['ymd', 'stock_code', 'stock_name', 'shareholder_score'])


    def _get_zero_scores(self, trading_days, start_date, end_date, score_col):
        """生成全0分数据"""
        try:
            # 获取股票列表
            stock_base_df = Mysql_Utils.data_from_mysql_to_dataframe(
                user=self.user,
                password=self.password,
                host=self.host,
                database=self.database,
                table_name='dwd_ashare_stock_base_info',
                start_date=trading_days[-1] if trading_days else end_date,
                end_date=trading_days[-1] if trading_days else end_date,
                cols=['stock_code']
            )

            if stock_base_df.empty:
                return pd.DataFrame(columns=['ymd', 'stock_code', score_col])

            all_stocks = stock_base_df['stock_code'].unique()

            result_data = []
            for date_str in trading_days:
                for stock in all_stocks:
                    result_data.append({
                        'ymd': date_str,
                        'stock_code': stock,
                        score_col: 0.0
                    })

            return pd.DataFrame(result_data)

        except Exception as e:
            logger.error(f"生成零分数据失败：{str(e)}")
            return pd.DataFrame(columns=['ymd', 'stock_code', score_col])

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

    def volume_shrinkage_factor(self, start_date, end_date,  save_to_cache=True):
        """
        计算缩量下跌因子（0-100分）
        使用预计算的均线表，固定使用60日均量作为长期基准

        评分逻辑：
        1. 成交量条件（60分）：
           - 近5日均量低于60日均量（30分）
           - 近1-3天连续缩量（30分）
        2. 价格条件（40分）：
           - 连续阴线天数越多，得分越高
           - 连续3天阴线得满分40分
        """
        try:
            # 1. 从技术指标表获取数据
            tech_df = Mysql_Utils.data_from_mysql_to_dataframe(
                user=self.user,
                password=self.password,
                host=self.host,
                database=self.database,
                table_name='dwd_stock_technical_indicators',
                start_date=start_date,
                end_date=end_date,
                cols=['ymd', 'stock_code', 'stock_name', 'close', 'volume',
                      'vol_ma5', 'vol_ma60', 'volume_vs_ma5']
            )

            # 定义固定的列顺序（与表结构完全一致）
            fixed_columns = [
                'ymd', 'stock_code', 'stock_name',
                'close', 'volume',
                'vol_ma5', 'vol_ma60', 'volume_vs_ma5',
                'is_shrink_today', 'consecutive_shrink_days',
                'is_down', 'consecutive_down_days',
                'volume_score', 'price_score', 'composite_score', 'signal_level'
            ]

            if tech_df.empty:
                logger.warning(f"技术指标数据为空: {start_date}~{end_date}")
                return pd.DataFrame(columns=fixed_columns)

            # 2. 获取阴线数据   关于引线 todo  最好是连阴但累计跌幅却有限的
            down_df = self.get_down_days(start_date, end_date)

            if down_df.empty:
                logger.warning(f"阴线数据为空: {start_date}~{end_date}")
                return pd.DataFrame(columns=fixed_columns)

            # 3. 合并数据
            merged_df = pd.merge(
                tech_df,
                down_df[['ymd', 'stock_code', 'is_down']],
                on=['ymd', 'stock_code'],
                how='inner'
            )

            # 4. 按股票分组计算连续指标
            result_dfs = []

            for stock, stock_df in merged_df.groupby('stock_code'):
                stock_df = stock_df.copy()
                stock_df = stock_df.sort_values('ymd')

                # 计算连续缩量（volume_vs_ma5 < 0 表示当日成交量低于5日均量）
                stock_df['is_shrink_today'] = stock_df['volume_vs_ma5'] < 0

                # 计算连续缩量天数
                def count_consecutive(series):
                    """计算连续True的天数（从最新往旧统计）"""
                    count = 0
                    for val in series[::-1]:
                        if val:
                            count += 1
                        else:
                            break
                    return count

                # 连续缩量天数（最近5天内）
                stock_df['consecutive_shrink_days'] = 0
                for i in range(len(stock_df)):
                    start_idx = max(0, i - 4)  # 最近5天
                    window = stock_df['is_shrink_today'].iloc[start_idx:i + 1]
                    stock_df.iloc[i, stock_df.columns.get_loc('consecutive_shrink_days')] = \
                        count_consecutive(window.values)

                # 连续阴线天数（最近5天内）
                stock_df['consecutive_down_days'] = 0
                for i in range(len(stock_df)):
                    start_idx = max(0, i - 4)  # 最近5天
                    window = stock_df['is_down'].iloc[start_idx:i + 1]
                    stock_df.iloc[i, stock_df.columns.get_loc('consecutive_down_days')] = \
                        count_consecutive(window.values)

                result_dfs.append(stock_df)

            final_df = pd.concat(result_dfs, ignore_index=True)

            # 5. 计算成交量得分（0-60分）
            def calculate_volume_score(row):
                score = 0

                # 条件1：5日均量 < 60日均量（30分）
                if not pd.isna(row['vol_ma5']) and not pd.isna(row['vol_ma60']):
                    if row['vol_ma5'] < row['vol_ma60'] * 0.8:  # 低于80%
                        score += 30
                    elif row['vol_ma5'] < row['vol_ma60']:  # 低于100%
                        score += 20
                    elif row['vol_ma5'] < row['vol_ma60'] * 1.2:  # 低于120%
                        score += 10

                # 条件2：连续缩量天数（30分）
                if row['consecutive_shrink_days'] >= 3:
                    score += 30
                elif row['consecutive_shrink_days'] == 2:
                    score += 20
                elif row['consecutive_shrink_days'] == 1:
                    score += 10

                return min(score, 60)

            # 6. 计算价格得分（0-40分）
            def calculate_price_score(row):
                if row['consecutive_down_days'] >= 3:
                    return 40
                elif row['consecutive_down_days'] == 2:
                    return 30
                elif row['consecutive_down_days'] == 1:
                    return 20
                else:
                    return 0

            final_df['volume_score'] = final_df.apply(calculate_volume_score, axis=1)
            final_df['price_score'] = final_df.apply(calculate_price_score, axis=1)
            final_df['composite_score'] = (final_df['volume_score'] + final_df['price_score']).round(2)

            # 7. 添加评分等级
            def get_score_level(score):
                if score >= 80:
                    return 'A'  # 强烈信号
                elif score >= 60:
                    return 'B'  # 明显信号
                elif score >= 40:
                    return 'C'  # 一般信号
                elif score >= 20:
                    return 'D'  # 弱信号
                else:
                    return 'E'  # 无信号

            final_df['signal_level'] = final_df['composite_score'].apply(get_score_level)

            # 6. 按固定列顺序选择数据
            result_df = final_df[fixed_columns].copy()
            logger.info(f"缩量下跌因子计算完成：共{len(result_df)}条记录")

            # 7. 保存到缓存
            if save_to_cache:
                self.cached_factors['volume'] = result_df.copy()

            # 8. 保存到数据库
            try:
                Mysql_Utils.data_from_dataframe_to_mysql(
                    user=self.user,
                    password=self.password,
                    host=self.host,
                    database=self.database,
                    df=result_df,
                    table_name="dwd_factor_volume_shrinkage",
                    merge_on=['ymd', 'stock_code']
                )
                logger.info(f"缩量下跌因子已保存到 dwd_factor_volume_shrinkage，共{len(result_df)}条")
            except Exception as e:
                logger.error(f"保存缩量下跌因子失败：{str(e)}")

            return result_df

        except Exception as e:
            logger.error(f"计算缩量下跌因子失败：{str(e)}")
            import traceback
            logger.error(traceback.format_exc())
            return pd.DataFrame(columns=fixed_columns)


    def explain_volume_shrinkage(self, stock_code, date):
        """
        详细解释某只股票某天的缩量下跌因子
        使用 ods_stock_kline_daily_ts 表
        """
        try:
            # 获取该股票的历史数据
            target_date = pd.to_datetime(date)
            start_dt = target_date - pd.Timedelta(days=100)

            df = Mysql_Utils.data_from_mysql_to_dataframe(
                user=self.user,
                password=self.password,
                host=self.host,
                database=self.database,
                table_name='ods_stock_kline_daily_ts',
                start_date=start_dt.strftime('%Y%m%d'),
                end_date=date,
                cols=['stock_code', 'ymd', 'close', 'volume', 'change_pct']
            )

            if df.empty:
                print(f"没有找到 {start_dt} ~ {date} 之间的数据")
                return

            df = df[df['stock_code'] == stock_code].sort_values('ymd')
            df['ymd'] = pd.to_datetime(df['ymd'])

            # 计算指标
            df['is_down'] = df['change_pct'] < 0
            df['volume_ma90'] = df['volume'].rolling(90).mean()
            df['volume_ma5'] = df['volume'].rolling(5).mean()
            df['volume_decrease'] = df['volume'].diff() < 0

            # 计算连续天数
            def count_consecutive(series):
                count = 0
                for val in series[::-1]:
                    if val:
                        count += 1
                    else:
                        break
                return count

            # 获取目标日期的数据
            target_idx = df[df['ymd'] == target_date].index
            if len(target_idx) == 0:
                print(f"没有找到 {stock_code} 在 {date} 的数据")
                return

            target_idx = target_idx[0]
            start_idx = max(0, target_idx - 4)

            recent_down = df.loc[start_idx:target_idx, 'is_down']
            recent_volume = df.loc[start_idx:target_idx, 'volume_decrease']

            consecutive_down = count_consecutive(recent_down.values)
            consecutive_volume = count_consecutive(recent_volume.values)

            target = df.loc[target_idx]

            print("=" * 70)
            print(f"缩量下跌因子详解 - {stock_code} @ {date}")
            print("=" * 70)

            # 显示最近5天的K线
            print("\n【最近5天走势】")
            print(f"{'日期':<10} {'涨跌幅':>8} {'收盘':>8} {'成交量':>12} {'状态':>6}")
            print("-" * 50)

            recent = df.loc[max(0, target_idx - 4):target_idx]
            for _, row in recent.iterrows():
                status = "阴线" if row['is_down'] else "阳线"
                print(f"{row['ymd'].strftime('%m-%d'):<10} "
                      f"{row['change_pct']:>7.2f}% "
                      f"{row['close']:>8.2f} "
                      f"{row['volume']:>12.0f} "
                      f"{status:>6}")

            # 成交量分析
            print("\n【成交量分析】")
            if not pd.isna(target['volume_ma5']) and not pd.isna(target['volume_ma90']):
                ratio = target['volume_ma5'] / target['volume_ma90']
                print(f"  当前成交量: {target['volume']:.0f}")
                print(f"  5日均量: {target['volume_ma5']:.0f}")
                print(f"  90日均量: {target['volume_ma90']:.0f}")
                print(f"  短期/长期均量比: {ratio:.2f}")

                vol_score = 0
                if target['volume_ma5'] < target['volume_ma90'] * 0.8:
                    vol_score += 30
                    print("  ✓ 条件1: 5日均量 < 90日均量80% (+30分)")
                elif target['volume_ma5'] < target['volume_ma90']:
                    vol_score += 20
                    print("  ✓ 条件1: 5日均量 < 90日均量 (+20分)")
                elif target['volume_ma5'] < target['volume_ma90'] * 1.2:
                    vol_score += 10
                    print("  ✓ 条件1: 5日均量 < 90日均量120% (+10分)")
                else:
                    print("  ✗ 条件1: 成交量未达标")

                print(f"\n【连续缩量】")
                print(f"  连续缩量天数: {consecutive_volume}")
                if consecutive_volume >= 3:
                    vol_score += 30
                    print(f"  ✓ 条件2: 连续{consecutive_volume}天缩量 (+30分)")
                elif consecutive_volume == 2:
                    vol_score += 20
                    print(f"  ✓ 条件2: 连续2天缩量 (+20分)")
                elif consecutive_volume == 1:
                    vol_score += 10
                    print(f"  ✓ 条件2: 连续1天缩量 (+10分)")
                else:
                    print("  ✗ 条件2: 无连续缩量")

                print(f"\n【成交量得分】: {min(vol_score, 60)}/60")

                # 价格分析
                print("\n【价格分析 - 连续阴线】")
                print(f"  连续阴线天数: {consecutive_down}")

                price_score = 0
                if consecutive_down >= 3:
                    price_score = 40
                    print(f"  ✓ 连续{consecutive_down}天阴线 (+40分)")
                elif consecutive_down == 2:
                    price_score = 30
                    print(f"  ✓ 连续2天阴线 (+30分)")
                elif consecutive_down == 1:
                    price_score = 20
                    print(f"  ✓ 连续1天阴线 (+20分)")
                else:
                    print("  ✗ 无连续阴线")

                print(f"\n【价格得分】: {price_score}/40")

                # 综合得分
                composite = vol_score + price_score
                print(f"\n【综合得分】: {composite:.0f}/100")

                if composite >= 80:
                    print("【信号等级】: A - 强烈信号")
                elif composite >= 60:
                    print("【信号等级】: B - 明显信号")
                elif composite >= 40:
                    print("【信号等级】: C - 一般信号")
                elif composite >= 20:
                    print("【信号等级】: D - 弱信号")
                else:
                    print("【信号等级】: E - 无信号")

            print("=" * 70)

        except Exception as e:
            print(f"分析失败: {str(e)}")
            import traceback
            traceback.print_exc()

    def get_down_days(self, start_date, end_date):
        """
        获取每日的阴线标记
        阴线定义：基于 ods_stock_kline_daily_ts 表中的 is_down 字段
        is_down = 1 表示阴线（收盘价 < 开盘价）
        Args:
            start_date: 开始日期 (YYYYMMDD)
            end_date: 结束日期 (YYYYMMDD)
        Returns:
            DataFrame: ymd, stock_code, is_down (True/False)
        """
        try:
            # 从 ods_stock_kline_daily_ts 表获取阴线数据
            down_df = Mysql_Utils.data_from_mysql_to_dataframe(
                user=self.user,
                password=self.password,
                host=self.host,
                database=self.database,
                table_name='ods_stock_kline_daily_ts',
                start_date=start_date,
                end_date=end_date,
                cols=['ymd', 'stock_code', 'is_down']
            )

            if down_df.empty:
                logger.warning(f"阴线数据为空: {start_date}~{end_date}")
                return pd.DataFrame(columns=['ymd', 'stock_code', 'is_down'])

            # 将 is_down 从 0/1 转换为 False/True
            down_df['is_down'] = down_df['is_down'].astype(bool)

            logger.info(f"获取阴线数据完成：共{len(down_df)}条记录")

            return down_df[['ymd', 'stock_code', 'is_down']]

        except Exception as e:
            logger.error(f"获取阴线数据失败：{str(e)}")
            return pd.DataFrame(columns=['ymd', 'stock_code', 'is_down'])

    def aggregate_factors(self, start_date, end_date, factors=None):
        """
        因子汇总 - 直接写入MySQL，严格按照表结构
        """
        import pandas as pd

        # 如果没有指定因子，用缓存中有的
        if factors is None:
            factors = list(self.cached_factors.keys())

        if not factors:
            logger.warning("没有指定因子，且缓存为空")
            return

        # 获取交易日列表
        trading_days = self.get_trading_days(start_date, end_date)
        if not trading_days:
            logger.warning(f"没有交易日数据: {start_date}~{end_date}")
            return

        # 构建基础DataFrame：所有股票 * 所有交易日
        base_data = []
        for date in trading_days:
            # 统一日期格式为 YYYY-MM-DD
            if isinstance(date, str) and len(date) == 8 and date.isdigit():
                date_str = f"{date[:4]}-{date[4:6]}-{date[6:8]}"
            else:
                date_str = str(date)

            for _, row in self.stocks_df.iterrows():
                base_data.append({
                    'ymd': date_str,
                    'stock_code': row['stock_code'],
                    'stock_name': row['stock_name']
                })

        summary_df = pd.DataFrame(base_data)

        # 初始化所有得分为0
        summary_df['pb_score'] = 0
        summary_df['zt_score'] = 0
        summary_df['shareholder_score'] = 0
        summary_df['volume_score'] = 0
        summary_df['price_score'] = 0
        summary_df['composite_score'] = 0
        summary_df['signal_level'] = ''

        # 定义因子列映射
        factor_cols = {
            'pb': ['pb_score'],
            'zt': ['zt_score'],
            'shareholder': ['shareholder_score'],
            'volume': ['volume_score', 'price_score', 'composite_score', 'signal_level']
        }

        # 逐个因子合并更新
        for factor_name in factors:
            if factor_name not in self.cached_factors:
                logger.warning(f"因子 {factor_name} 不在缓存中，跳过")
                continue

            df = self.cached_factors[factor_name].copy()

            # 统一因子数据的日期格式为 YYYY-MM-DD
            if 'ymd' in df.columns:
                if pd.api.types.is_datetime64_any_dtype(df['ymd']):
                    df['ymd'] = df['ymd'].dt.strftime('%Y-%m-%d')
                elif df['ymd'].dtype == 'object' and df['ymd'].astype(str).str.match(r'^\d{8}$').any():
                    df['ymd'] = pd.to_datetime(df['ymd'], format='%Y%m%d').dt.strftime('%Y-%m-%d')
                else:
                    df['ymd'] = df['ymd'].astype(str)

            cols_to_merge = factor_cols.get(factor_name, [])

            # 左连接合并
            merged = pd.merge(
                summary_df[['ymd', 'stock_code']],
                df[['ymd', 'stock_code'] + cols_to_merge],
                on=['ymd', 'stock_code'],
                how='left'
            )

            # 更新对应的列 - 修复版本
            for col in cols_to_merge:
                if col in merged.columns:
                    # 创建合并数据的副本用于更新
                    update_data = merged[['ymd', 'stock_code', col]].copy()
                    update_data = update_data[update_data[col].notna()]

                    if not update_data.empty:
                        # 直接使用布尔索引更新，避免索引问题
                        for _, row in update_data.iterrows():
                            mask = (summary_df['ymd'] == row['ymd']) & (summary_df['stock_code'] == row['stock_code'])
                            summary_df.loc[mask, col] = row[col]

        # 写入MySQL
        try:
            Mysql_Utils.data_from_dataframe_to_mysql(
                user=self.user,
                password=self.password,
                host=self.host,
                database=self.database,
                df=summary_df[['ymd', 'stock_code', 'stock_name', 'pb_score', 'zt_score',
                               'shareholder_score', 'volume_score', 'price_score',
                               'composite_score', 'signal_level']],
                table_name="dwd_factor_summary",
                merge_on=['ymd', 'stock_code']
            )
            logger.info(f"因子汇总已保存到 dwd_factor_summary，共{len(summary_df)}条")
        except Exception as e:
            logger.error(f"保存因子汇总失败：{str(e)}")

        logger.info(f"因子汇总完成，共{len(summary_df)}条，包含因子: {factors}")


    def setup(self):

        #  pb 因子计算
        self.pb_factor_score(start_date='20240101', end_date='20260227')

        #  涨停 因子计算
        self.zt_factor_score(start_date='20240101', end_date='20260227')

        #  股东数 因子计算
        self.shareholder_factor_score(start_date='20240101', end_date='20260227')

        #  缩量因子计算
        self.volume_shrinkage_factor(start_date='20240101', end_date='20260227')

        #  因子汇总
        self.aggregate_factors(start_date='20240101', end_date='20260227')



if __name__ == '__main__':
    factorlib = FactorLibrary()
    factorlib.setup()

    # 测试修复后的交易日获取
    # res = factorlib.get_trading_days(start_date='20260101', end_date='20260109')
    # print(f"交易日: {res}")
    #
    # pb_score = factorlib.pb_factor_score(start_date='20260101', end_date='20260109')
    # print(pb_score)

    # share_score = factorlib.shareholder_factor_score(start_date='20260101', end_date='20260109')
    # print(share_score)

    # 1. 计算因子
    # factor_df = factorlib.volume_shrinkage_factor(
    #     start_date='2026-02-01',
    #     end_date='2026-02-24'
    # )
    #
    # # 2. 查看高分股票（连续阴线+缩量）
    # high_score = factor_df[factor_df['signal_level'].isin(['A', 'B'])].sort_values(
    #     'composite_score', ascending=False
    # )
    # print("强烈信号股票：")
    # print(high_score[['ymd', 'stock_code', 'stock_name', 'consecutive_down_days',
    #                   'composite_score', 'signal_level']].head(10))

    # 3. 分析单只股票
    # factorlib.explain_volume_shrinkage('000001.SZ', '2026-02-24')

    # # 4. 统计连续3天阴线的股票
    # three_days_down = factor_df[factor_df['consecutive_down_days'] >= 3]
    # print(f"\n连续3天阴线的股票数量: {len(three_days_down)}")
    # print(three_days_down[['ymd', 'stock_code', 'consecutive_down_days',
    #                        'volume_score', 'composite_score']].head())

















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
