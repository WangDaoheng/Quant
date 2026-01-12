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

