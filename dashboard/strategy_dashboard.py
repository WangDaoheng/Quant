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