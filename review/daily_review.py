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