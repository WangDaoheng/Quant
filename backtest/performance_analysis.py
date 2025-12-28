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