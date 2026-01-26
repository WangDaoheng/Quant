"""
数据完整性简易检查脚本
快速检查关键指标，适用于日常监控
"""

import pandas as pd
from datetime import datetime, timedelta
import logging
import sys
import os

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from CommonProperties import Mysql_Utils
from CommonProperties import Base_Properties
from CommonProperties.set_config import setup_logging_config


class QuickDataChecker:
    """快速数据检查器"""

    def __init__(self, use_remote_db=True):
        """初始化"""
        setup_logging_config()

        # 数据库配置
        if use_remote_db:
            self.db_config = {
                'user': Base_Properties.origin_user,
                'password': Base_Properties.origin_password,
                'host': Base_Properties.origin_host,
                'database': Base_Properties.origin_database
            }
        else:
            self.db_config = {
                'user': Base_Properties.local_mysql_user,
                'password': Base_Properties.local_mysql_password,
                'host': Base_Properties.local_mysql_host,
                'database': Base_Properties.local_mysql_database
            }

        # 关键表列表
        self.key_tables = [
            'ods_stock_kline_daily_insight',  # 日K线
            'ods_stock_kline_daily_insight_now',  # 最新K线
            'ods_index_a_share_insight',  # 指数
            'ods_shareholder_num',  # 股东数
            'ods_north_bound_daily',  # 北向资金
            'dwd_ashare_stock_base_info',  # 股票基础信息
            'dmart_stock_zt_details',  # 涨停明细
        ]

    def quick_check_table(self, table_name):
        """快速检查单个表"""
        try:
            # 加载最新一天的数据
            df = Mysql_Utils.data_from_mysql_to_dataframe_latest(
                user=self.db_config['user'],
                password=self.db_config['password'],
                host=self.db_config['host'],
                database=self.db_config['database'],
                table_name=table_name
            )

            if df.empty:
                return {
                    'table': table_name,
                    'status': '空表',
                    'latest_date': None,
                    'row_count': 0,
                    'issue': '无数据'
                }

            # 检查是否有日期列
            date_cols = [col for col in df.columns if 'date' in col.lower() or 'ymd' in col.lower()]
            if date_cols:
                latest_date = df[date_cols[0]].iloc[0]
                if isinstance(latest_date, pd.Timestamp):
                    latest_date = latest_date.date()
            else:
                latest_date = None

            # 检查数据行数
            row_count = len(df)

            # 检查空值率
            null_issues = []
            for col in df.columns:
                null_pct = df[col].isnull().sum() / row_count * 100
                if null_pct > 30:  # 空值率超过30%认为有问题
                    null_issues.append(f"{col}({null_pct:.1f}%)")

            # 判断状态
            if row_count == 0:
                status = '空表'
                issue = '无数据'
            elif latest_date and (datetime.now().date() - pd.to_datetime(latest_date).date()).days > 7:
                status = '数据滞后'
                issue = f'最新数据日期: {latest_date}'
            elif null_issues:
                status = '数据质量问题'
                issue = f'高空值列: {", ".join(null_issues[:2])}'
            else:
                status = '正常'
                issue = ''

            return {
                'table': table_name,
                'status': status,
                'latest_date': latest_date,
                'row_count': row_count,
                'issue': issue
            }

        except Exception as e:
            return {
                'table': table_name,
                'status': '检查失败',
                'latest_date': None,
                'row_count': 0,
                'issue': str(e)
            }

    def check_all_key_tables(self):
        """检查所有关键表"""
        print("开始快速数据完整性检查...")
        print("-" * 60)

        results = []
        issues_found = 0

        for i, table in enumerate(self.key_tables, 1):
            print(f"[{i}/{len(self.key_tables)}] 检查: {table}")
            result = self.quick_check_table(table)
            results.append(result)

            if result['status'] != '正常':
                issues_found += 1
                print(f"  ⚠ {result['status']}: {result['issue']}")
            else:
                print(f"  ✓ 正常: {result['row_count']} 行, 最新日期: {result['latest_date']}")

        # 生成报告
        print("\n" + "=" * 60)
        print("快速检查结果汇总")
        print("=" * 60)

        normal_tables = [r for r in results if r['status'] == '正常']
        problem_tables = [r for r in results if r['status'] != '正常']

        print(f"总检查表数: {len(results)}")
        print(f"正常表数: {len(normal_tables)}")
        print(f"问题表数: {len(problem_tables)}")

        if problem_tables:
            print("\n问题表详情:")
            for result in problem_tables:
                print(f"  • {result['table']}")
                print(f"    状态: {result['status']}")
                if result['issue']:
                    print(f"    问题: {result['issue']}")
                if result['latest_date']:
                    print(f"    最新日期: {result['latest_date']}")
                if result['row_count'] > 0:
                    print(f"    数据行数: {result['row_count']}")

        # 保存结果
        timestamp = datetime.now().strftime("%Y%m%d_%H%M")
        self.save_results(results, f"quick_check_{timestamp}.csv")

        return results

    def save_results(self, results, filename):
        """保存检查结果"""
        df = pd.DataFrame(results)
        df.to_csv(filename, index=False, encoding='utf-8-sig')
        print(f"\n检查结果已保存到: {filename}")

    def check_latest_date_coverage(self):
        """检查最新数据覆盖情况"""
        print("\n最新数据日期检查:")
        print("-" * 40)

        today = datetime.now().date()

        for table in self.key_tables[:5]:  # 只检查前5个关键表
            result = self.quick_check_table(table)

            if result['latest_date']:
                days_diff = (today - result['latest_date']).days

                if days_diff == 0:
                    status = "✓ 最新"
                elif days_diff == 1:
                    status = "⚠ 滞后1天"
                elif days_diff <= 3:
                    status = "⚠ 滞后3天内"
                else:
                    status = "✗ 严重滞后"

                print(f"{table:40} {status:15} 最新日期: {result['latest_date']} (滞后{days_diff}天)")


def main():
    """主函数"""
    # 创建检查器
    checker = QuickDataChecker(use_remote_db=True)

    # 执行快速检查
    results = checker.check_all_key_tables()

    # 检查最新数据覆盖
    checker.check_latest_date_coverage()

    print("\n" + "=" * 60)
    print("快速检查完成!")
    print("=" * 60)


if __name__ == "__main__":
    main()
