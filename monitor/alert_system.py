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
