"""
数据完整性详细检查脚本
集成到Backtrader_PJ1项目，复用现有工具类
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from typing import Dict, List, Optional
import warnings
import logging

warnings.filterwarnings('ignore')

# 导入您的项目模块
import sys
import os

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from CommonProperties import Mysql_Utils
from CommonProperties import Base_Properties
from CommonProperties import DateUtility
from CommonProperties.set_config import setup_logging_config


class DataIntegrityChecker:
    def __init__(self, use_remote_db=True):
        """
        初始化数据完整性检查器

        Args:
            use_remote_db: True=使用远程数据库，False=使用本地数据库
        """
        # 设置日志
        setup_logging_config()

        # 根据参数选择数据库配置
        if use_remote_db:
            self.db_config = {
                'user': Base_Properties.origin_user,
                'password': Base_Properties.origin_password,
                'host': Base_Properties.origin_host,
                'database': Base_Properties.origin_database
            }
            self.db_type = "远程数据库"
        else:
            self.db_config = {
                'user': Base_Properties.local_mysql_user,
                'password': Base_Properties.local_mysql_password,
                'host': Base_Properties.local_mysql_host,
                'database': Base_Properties.local_mysql_database
            }
            self.db_type = "本地数据库"

        logging.info(f"使用{self.db_type}进行数据完整性检查")

        # 结果存储
        self.results = []
        self.summary = {
            'total_tables': 0,
            'checked_tables': 0,
            'complete_tables': 0,
            'incomplete_tables': 0,
            'empty_tables': 0,
            'error_tables': 0
        }

    def load_table_data(self, table_name: str,
                        start_date: str = None,
                        end_date: str = None) -> pd.DataFrame:
        """
        从数据库加载表数据

        Args:
            table_name: 表名
            start_date: 开始日期
            end_date: 结束日期

        Returns:
            DataFrame数据
        """
        try:
            df = Mysql_Utils.data_from_mysql_to_dataframe(
                user=self.db_config['user'],
                password=self.db_config['password'],
                host=self.db_config['host'],
                database=self.db_config['database'],
                table_name=table_name,
                start_date=start_date,
                end_date=end_date
            )
            logging.info(f"成功加载表 {table_name}: {len(df)} 行")
            return df
        except Exception as e:
            logging.error(f"加载表 {table_name} 失败: {str(e)}")
            return pd.DataFrame()

    def get_trading_days(self, start_date: str, end_date: str) -> pd.DataFrame:
        """
        获取交易日历

        Args:
            start_date: 开始日期
            end_date: 结束日期

        Returns:
            DataFrame包含交易日
        """
        try:
            df = self.load_table_data(
                table_name='ods_trading_days_insight',
                start_date=start_date,
                end_date=end_date
            )
            if not df.empty and 'ymd' in df.columns:
                # 统一格式
                df['ymd'] = pd.to_datetime(df['ymd']).dt.date
                return df
            else:
                # 如果无法从数据库获取，生成模拟交易日历
                logging.warning("无法从数据库获取交易日历，生成模拟交易日")
                date_range = pd.date_range(start=start_date, end=end_date, freq='D')
                trading_days = date_range[date_range.weekday < 5]  # 周一到周五
                return pd.DataFrame({
                    'exchange': ['XSHG'] * len(trading_days),
                    'ymd': trading_days
                })
        except Exception as e:
            logging.error(f"获取交易日历失败: {str(e)}")
            return pd.DataFrame()

    def check_daily_data(self, table_name: str, df: pd.DataFrame) -> Dict:
        """
        检查日度数据完整性

        Args:
            table_name: 表名
            df: 数据表

        Returns:
            检查结果
        """
        result = {
            'table_name': table_name,
            'data_type': '日度数据',
            'total_rows': len(df),
            'date_range': None,
            'date_column': None,
            'missing_dates': [],
            'non_trading_dates': [],
            'completeness_percentage': '0%',
            'data_quality_issues': [],
            'recommendations': [],
            'is_complete': True
        }

        if df.empty:
            result['data_quality_issues'].append('表为空')
            result['is_complete'] = False
            return result

        # 查找日期列
        date_columns = [col for col in df.columns if 'date' in col.lower() or 'ymd' in col.lower()]
        if not date_columns:
            result['data_quality_issues'].append('未找到日期列')
            result['is_complete'] = False
            return result

        date_col = date_columns[0]
        result['date_column'] = date_col

        try:
            # 转换日期格式
            df[date_col] = pd.to_datetime(df[date_col]).dt.date

            # 按日期排序
            df = df.sort_values(date_col)

            # 获取日期范围
            min_date = df[date_col].min()
            max_date = df[date_col].max()
            result['date_range'] = f"{min_date} 到 {max_date}"

            # 获取交易日历
            trading_days_df = self.get_trading_days(
                start_date=min_date.strftime('%Y-%m-%d'),
                end_date=max_date.strftime('%Y-%m-%d')
            )

            if not trading_days_df.empty:
                # 获取表中所有唯一日期
                table_dates = set(df[date_col])
                trading_dates = set(trading_days_df['ymd'])

                # 计算完整度
                missing_dates = sorted(list(trading_dates - table_dates))
                if missing_dates:
                    result['missing_dates'] = missing_dates
                    result['is_complete'] = False

                    # 提供详细缺失信息
                    if len(missing_dates) <= 10:
                        result['data_quality_issues'].append(f'缺失{len(missing_dates)}个交易日')
                    else:
                        result['data_quality_issues'].append(
                            f'缺失{len(missing_dates)}个交易日（最近缺失: {missing_dates[-5:]}）')

                # 检查非交易日数据
                non_trading_dates = sorted(list(table_dates - trading_dates))
                if non_trading_dates:
                    result['non_trading_dates'] = non_trading_dates
                    result['data_quality_issues'].append(f'包含{len(non_trading_dates)}个非交易日数据')

                # 计算完整度百分比
                if trading_dates:
                    completeness = len(table_dates & trading_dates) / len(trading_dates) * 100
                    result['completeness_percentage'] = f"{completeness:.1f}%"

            # 检查重复数据
            dup_cols = [date_col]
            if 'stock_code' in df.columns:
                dup_cols.append('stock_code')

            duplicates = df.duplicated(subset=dup_cols).sum()
            if duplicates > 0:
                result['data_quality_issues'].append(f'发现{duplicates}条重复数据')

            # 检查空值
            null_info = {}
            for col in df.columns:
                null_count = df[col].isnull().sum()
                if null_count > 0:
                    null_pct = null_count / len(df) * 100
                    null_info[col] = f'{null_pct:.1f}%'

                    if null_pct > 50:
                        result['data_quality_issues'].append(f'列"{col}"空值过多({null_pct:.1f}%)')

            if null_info:
                result['null_columns'] = null_info

        except Exception as e:
            result['data_quality_issues'].append(f'日期处理错误: {str(e)}')
            result['is_complete'] = False

        return result

    def check_quarterly_data(self, table_name: str, df: pd.DataFrame) -> Dict:
        """
        检查季度数据完整性

        Args:
            table_name: 表名
            df: 数据表

        Returns:
            检查结果
        """
        result = {
            'table_name': table_name,
            'data_type': '季度数据',
            'total_rows': len(df),
            'date_range': None,
            'missing_quarters': [],
            'expected_quarters': [],
            'completeness_percentage': '0%',
            'data_quality_issues': [],
            'is_complete': True
        }

        if df.empty:
            result['data_quality_issues'].append('表为空')
            result['is_complete'] = False
            return result

        # 查找日期列
        date_columns = [col for col in df.columns if 'date' in col.lower() or 'ymd' in col.lower()]
        if not date_columns:
            result['data_quality_issues'].append('未找到日期列')
            result['is_complete'] = False
            return result

        date_col = date_columns[0]

        try:
            # 转换日期格式
            df[date_col] = pd.to_datetime(df[date_col])

            # 获取日期范围
            min_date = df[date_col].min()
            max_date = df[date_col].max()
            result['date_range'] = f"{min_date.date()} 到 {max_date.date()}"

            # 提取季度末日期
            df['quarter'] = df[date_col].dt.to_period('Q')
            df['quarter_end'] = df['quarter'].dt.end_time.dt.date

            # 获取所有唯一的季度末日期
            actual_quarters = set(df['quarter_end'].unique())

            # 生成预期的季度末日期
            current_year = min_date.year
            end_year = max_date.year
            expected_quarters = []

            for year in range(current_year, end_year + 1):
                for month in [3, 6, 9, 12]:
                    quarter_end = datetime(year, month, 1) + pd.offsets.MonthEnd(1)
                    quarter_date = quarter_end.date()

                    # 只包含在数据范围内的季度
                    if min_date.date() <= quarter_date <= max_date.date():
                        expected_quarters.append(quarter_date)

            result['expected_quarters'] = expected_quarters

            # 找出缺失的季度
            missing_quarters = sorted(list(set(expected_quarters) - actual_quarters))
            if missing_quarters:
                result['missing_quarters'] = missing_quarters
                result['is_complete'] = False
                result['data_quality_issues'].append(f'缺失{len(missing_quarters)}个季度数据')

            # 计算完整度
            if expected_quarters:
                completeness = len(actual_quarters) / len(expected_quarters) * 100
                result['completeness_percentage'] = f"{completeness:.1f}%"

        except Exception as e:
            result['data_quality_issues'].append(f'季度数据处理错误: {str(e)}')
            result['is_complete'] = False

        return result

    def check_table_by_name(self, table_name: str) -> Dict:
        """
        根据表名自动识别检查类型

        Args:
            table_name: 表名

        Returns:
            检查结果
        """
        # 加载数据
        df = self.load_table_data(table_name)

        # 空表检查
        if df.empty:
            return {
                'table_name': table_name,
                'data_type': '未知',
                'total_rows': 0,
                'data_quality_issues': ['表为空或无法读取'],
                'is_complete': False
            }

        # 根据表名判断检查类型
        table_lower = table_name.lower()

        # 日度数据表
        daily_keywords = ['daily', 'kline', 'insight_now', 'spot', 'limit_summary',
                          'stock_zt', 'stock_dt', 'zt_details', 'base_info']
        if any(keyword in table_lower for keyword in daily_keywords):
            return self.check_daily_data(table_name, df)

        # 季度数据表
        quarterly_keywords = ['shareholder', 'gdhs', 'yjkb', 'yjyg']
        if any(keyword in table_lower for keyword in quarterly_keywords):
            return self.check_quarterly_data(table_name, df)

        # 板块数据表
        plate_keywords = ['concept', 'industry', 'style', 'region', 'index', 'plate']
        if any(keyword in table_lower for keyword in plate_keywords):
            # 板块数据不强制要求日度完整性
            return {
                'table_name': table_name,
                'data_type': '板块数据',
                'total_rows': len(df),
                'unique_dates': df['ymd'].nunique() if 'ymd' in df.columns else 0,
                'is_complete': True if len(df) > 0 else False
            }

        # 默认检查日度数据
        return self.check_daily_data(table_name, df)

    def check_tables(self, table_list: List[str] = None) -> List[Dict]:
        """
        检查多个表的完整性

        Args:
            table_list: 表名列表，如果为None则检查所有表

        Returns:
            检查结果列表
        """
        if table_list is None:
            # 获取所有表名（这里简化处理，实际应该从数据库获取）
            table_list = self.get_all_table_names()

        self.summary['total_tables'] = len(table_list)

        results = []
        for i, table_name in enumerate(table_list, 1):
            logging.info(f"[{i}/{len(table_list)}] 检查表: {table_name}")

            try:
                result = self.check_table_by_name(table_name)
                results.append(result)

                # 更新统计
                self.summary['checked_tables'] += 1

                if result['total_rows'] == 0:
                    self.summary['empty_tables'] += 1
                elif result.get('is_complete', False):
                    self.summary['complete_tables'] += 1
                else:
                    self.summary['incomplete_tables'] += 1

            except Exception as e:
                logging.error(f"检查表 {table_name} 时出错: {str(e)}")
                self.summary['error_tables'] += 1
                results.append({
                    'table_name': table_name,
                    'error': str(e),
                    'is_complete': False
                })

        self.results = results
        return results

    def get_all_table_names(self) -> List[str]:
        """
        获取数据库中所有表名
        简化版：返回预定义的表列表
        """
        # 这里应该从数据库获取所有表名
        # 暂时返回预定义的44张表
        tables = [
            'dmart_stock_zt_details',
            'dmart_stock_zt_details_expanded',
            'dwd_ashare_stock_base_info',
            'dwd_stock_a_total_plate',
            'dwd_stock_dt_list',
            'dwd_stock_zt_list',
            'ods_akshare_stock_a_high_low_statistics',
            'ods_akshare_stock_board_concept_cons_em',
            'ods_akshare_stock_board_concept_hist_em',
            'ods_akshare_stock_board_concept_name_em',
            'ods_akshare_stock_cyq_em',
            'ods_akshare_stock_value_em',
            'ods_akshare_stock_yjkb_em',
            'ods_akshare_stock_yjyg_em',
            'ods_akshare_stock_zh_a_gdhs_detail_em',
            'ods_akshare_stock_zh_a_spot_em',
            'ods_astock_industry_detail',
            'ods_astock_industry_overview',
            'ods_exchange_dxy_vantage',
            'ods_exchange_rate_vantage_detail',
            'ods_future_inside_insight',
            'ods_future_inside_insight_now',
            'ods_index_a_share_insight',
            'ods_index_a_share_insight_now',
            'ods_north_bound_daily',
            'ods_north_bound_daily_now',
            'ods_shareholder_num',
            'ods_shareholder_num_now',
            'ods_stock_chouma_insight',
            'ods_stock_code_daily_insight',
            'ods_stock_exchange_market',
            'ods_stock_kline_daily_insight',
            'ods_stock_kline_daily_insight_now',
            'ods_stock_limit_summary_insight',
            'ods_stock_limit_summary_insight_now',
            'ods_stock_plate_redbook',
            'ods_tdx_stock_concept_plate',
            'ods_tdx_stock_index_plate',
            'ods_tdx_stock_industry_plate',
            'ods_tdx_stock_pepb_info',
            'ods_tdx_stock_region_plate',
            'ods_tdx_stock_style_plate',
            'ods_trading_days_insight',
            'ods_us_stock_daily_vantage'
        ]
        return tables

    def generate_report(self, output_file: str = None) -> str:
        """
        生成完整性检查报告

        Args:
            output_file: 输出文件路径

        Returns:
            报告字符串
        """
        if not self.results:
            return "未执行检查，请先调用check_tables()方法"

        report_lines = []

        # 报告头部
        report_lines.append("=" * 80)
        report_lines.append("量化数据库数据完整性检查报告")
        report_lines.append("=" * 80)
        report_lines.append(f"生成时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        report_lines.append(f"数据库: {self.db_type} ({self.db_config['host']})")
        report_lines.append(f"检查表数: {self.summary['total_tables']}")
        report_lines.append(f"已检查表数: {self.summary['checked_tables']}")
        report_lines.append(f"完整表数: {self.summary['complete_tables']}")
        report_lines.append(f"不完整表数: {self.summary['incomplete_tables']}")
        report_lines.append(f"空表数: {self.summary['empty_tables']}")
        report_lines.append(f"错误表数: {self.summary['error_tables']}")
        report_lines.append("=" * 80)

        # 按数据层分组
        layers = {'DMART': [], 'DWD': [], 'ODS': []}
        for result in self.results:
            table_name = result.get('table_name', '')
            if table_name.startswith('dmart_'):
                layers['DMART'].append(result)
            elif table_name.startswith('dwd_'):
                layers['DWD'].append(result)
            else:
                layers['ODS'].append(result)

        # 详细结果
        for layer_name, layer_results in layers.items():
            if layer_results:
                report_lines.append(f"\n【{layer_name}层】数据完整性")
                report_lines.append("-" * 60)

                for result in layer_results:
                    table_name = result.get('table_name', 'Unknown')
                    status = "✓ 完整" if result.get('is_complete', False) else "✗ 不完整"

                    report_lines.append(f"\n表名: {table_name} - {status}")
                    report_lines.append(f"  数据类型: {result.get('data_type', '未知')}")
                    report_lines.append(f"  数据行数: {result.get('total_rows', 0):,}")

                    if result.get('date_range'):
                        report_lines.append(f"  日期范围: {result['date_range']}")

                    if result.get('completeness_percentage'):
                        report_lines.append(f"  数据完整度: {result['completeness_percentage']}")

                    # 显示问题
                    issues = result.get('data_quality_issues', [])
                    if issues:
                        report_lines.append(f"  发现 {len(issues)} 个问题:")
                        for issue in issues[:3]:  # 只显示前3个问题
                            report_lines.append(f"    • {issue}")

                    # 缺失日期信息
                    if result.get('missing_dates'):
                        missing_count = len(result['missing_dates'])
                        if missing_count <= 5:
                            report_lines.append(f"    • 缺失日期: {result['missing_dates']}")
                        else:
                            report_lines.append(
                                f"    • 缺失 {missing_count} 个日期（最近缺失: {result['missing_dates'][-5:]}）")

        # 总结和建议
        report_lines.append("\n" + "=" * 80)
        report_lines.append("数据完整性总结与建议")
        report_lines.append("-" * 80)

        # 空表列表
        empty_tables = [r['table_name'] for r in self.results if r.get('total_rows', 0) == 0]
        if empty_tables:
            report_lines.append(f"\n空表列表 ({len(empty_tables)} 张):")
            for i in range(0, len(empty_tables), 5):
                report_lines.append(f"  {', '.join(empty_tables[i:i + 5])}")

        # 问题严重的表
        problematic_tables = []
        for result in self.results:
            if not result.get('is_complete', False) and result.get('total_rows', 0) > 0:
                table_name = result.get('table_name')
                issues = result.get('data_quality_issues', [])
                if issues:
                    problematic_tables.append((table_name, issues[0]))

        if problematic_tables:
            report_lines.append(f"\n需要重点关注的表 ({len(problematic_tables)} 张):")
            for table_name, issue in problematic_tables[:10]:
                report_lines.append(f"  • {table_name}: {issue}")

        # 建议
        report_lines.append("\n建议:")
        if self.summary['empty_tables'] > 0:
            report_lines.append("  1. 检查空表的数据源和ETL流程是否正常")
        if problematic_tables:
            report_lines.append("  2. 优先修复问题表的数据完整性问题")
        report_lines.append("  3. 建立定期数据质量检查机制")
        report_lines.append("  4. 对关键表设置数据监控告警")
        report_lines.append("  5. 完善数据缺失时的自动补数机制")

        report_str = "\n".join(report_lines)

        # 保存到文件
        if output_file:
            try:
                with open(output_file, 'w', encoding='utf-8') as f:
                    f.write(report_str)
                logging.info(f"报告已保存到: {output_file}")
            except Exception as e:
                logging.error(f"保存报告失败: {str(e)}")

        return report_str

    def export_to_csv(self, output_file: str = "data_integrity_summary.csv"):
        """
        将检查结果导出为CSV文件

        Args:
            output_file: 输出文件路径
        """
        if not self.results:
            logging.warning("没有检查结果可导出")
            return

        # 提取关键信息
        export_data = []
        for result in self.results:
            row = {
                'table_name': result.get('table_name', ''),
                'data_type': result.get('data_type', '未知'),
                'total_rows': result.get('total_rows', 0),
                'is_complete': '是' if result.get('is_complete', False) else '否',
                'completeness_percentage': result.get('completeness_percentage', '0%'),
                'date_range': result.get('date_range', ''),
                'issues_count': len(result.get('data_quality_issues', [])),
                'main_issue': result.get('data_quality_issues', [''])[0] if result.get('data_quality_issues') else ''
            }
            export_data.append(row)

        # 创建DataFrame并保存
        df = pd.DataFrame(export_data)
        df.to_csv(output_file, index=False, encoding='utf-8-sig')
        logging.info(f"检查结果已导出到: {output_file}")


def main():
    """主函数：运行完整性检查"""
    print("开始量化数据库数据完整性检查...")
    print("=" * 60)

    # 创建检查器（默认使用远程数据库）
    checker = DataIntegrityChecker(use_remote_db=True)

    # 获取所有表名
    all_tables = checker.get_all_table_names()
    print(f"将检查 {len(all_tables)} 张表")

    # 执行检查
    results = checker.check_tables(all_tables)

    # 生成报告
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    report_file = f"data_integrity_report_{timestamp}.txt"
    csv_file = f"data_integrity_summary_{timestamp}.csv"

    report = checker.generate_report(report_file)
    checker.export_to_csv(csv_file)

    # 打印摘要
    print("\n" + "=" * 60)
    print("检查完成!")
    print(f"完整表: {checker.summary['complete_tables']} 张")
    print(f"不完整表: {checker.summary['incomplete_tables']} 张")
    print(f"空表: {checker.summary['empty_tables']} 张")
    print(f"报告文件: {report_file}")
    print(f"CSV摘要: {csv_file}")
    print("=" * 60)


if __name__ == "__main__":
    main()