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

