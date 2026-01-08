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