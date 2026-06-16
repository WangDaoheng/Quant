import os
import pandas as pd
import logging
from datetime import datetime
from sqlalchemy import create_engine, text
from pathlib import Path
import CommonProperties.Base_Properties as Base_Properties
from CommonProperties.set_config import setup_logging_config

# 配置日志
setup_logging_config()
logger = logging.getLogger(__name__)


class TableDataExporterFull:
    """导出数据库表数据样例到单个HTML文件 - 带目录导航和超链接"""

    # 定义固定的展示顺序
    PREFIX_ORDER = {
        'ods_': 1,
        'dwd_': 2,
        'dmart_': 3,
        'dwt_': 4,
        'dim_': 5,
        'other': 99  # 其他表放最后
    }

    def __init__(self):
        self.user = Base_Properties.origin_mysql_user
        self.password = Base_Properties.origin_mysql_password
        self.host = Base_Properties.origin_mysql_host
        self.database = Base_Properties.origin_mysql_database

        current_script_path = Path(__file__).resolve()
        current_dir = current_script_path.parent
        project_root = None
        while current_dir != current_dir.parent:
            if (current_dir / "CommonProperties").exists():
                project_root = current_dir
                break
            current_dir = current_dir.parent

        if not project_root or not (project_root / "CommonProperties").exists():
            raise FileNotFoundError("❌ 未找到项目根目录 Quant/（缺少 CommonProperties 目录）")

        others_dir = project_root / "Others"
        self.output_dir = others_dir / "output"
        self.output_dir.mkdir(parents=True, exist_ok=True)
        print(f"📁 自动创建/确认输出目录: {self.output_dir}")

        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_filename = f"quant_tables_full_{timestamp}.html"
        self.output_file = self.output_dir / output_filename

        self.trading_days_cache = None

        print(f"数据库配置:")
        print(f"  主机: {self.host}")
        print(f"  数据库: {self.database}")
        print(f"  用户: {self.user}")
        print(f"  输出文件将保存到: {self.output_file}")
        print("-" * 50)

    def test_connection(self):
        """测试数据库连接"""
        try:
            db_url = f'mysql+pymysql://{self.user}:{self.password}@{self.host}:3306/{self.database}'
            engine = create_engine(db_url)
            with engine.connect() as connection:
                result = connection.execute(text("SELECT 1"))
                print("✓ 数据库连接成功")
                return True
        except Exception as e:
            print(f"✗ 数据库连接失败: {str(e)}")
            return False

    def get_all_tables(self):
        """获取数据库中的所有表名"""
        try:
            db_url = f'mysql+pymysql://{self.user}:{self.password}@{self.host}:3306/{self.database}'
            engine = create_engine(db_url)
            print("正在获取表列表...")
            with engine.connect() as connection:
                result = connection.execute(text("SHOW TABLES"))
                tables = [row[0] for row in result]
            print(f"✓ 找到 {len(tables)} 张表")
            return tables
        except Exception as e:
            print(f"✗ 获取表列表失败: {str(e)}")
            return []

    def check_column_exists(self, connection, table_name, column_name):
        """检查表中是否存在指定列"""
        try:
            result = connection.execute(text(f"""
                SELECT COUNT(*) 
                FROM information_schema.COLUMNS 
                WHERE TABLE_NAME = '{table_name}' 
                AND COLUMN_NAME = '{column_name}'
                AND TABLE_SCHEMA = '{self.database}'
            """))
            count = result.fetchone()[0]
            return count > 0
        except:
            return False

    def get_trading_days(self, connection):
        """从 ods_trading_days_insight 获取最近10个交易日"""
        if self.trading_days_cache is not None:
            return self.trading_days_cache

        try:
            result = connection.execute(text("""
                SELECT COUNT(*) 
                FROM information_schema.TABLES 
                WHERE TABLE_NAME = 'ods_trading_days_insight' 
                AND TABLE_SCHEMA = :db
            """), {"db": self.database})
            if result.fetchone()[0] == 0:
                print("  ⚠️  ods_trading_days_insight 表不存在")
                return None

            if not self.check_column_exists(connection, 'ods_trading_days_insight', 'ymd'):
                print("  ⚠️  ods_trading_days_insight 表没有ymd列")
                return None

            query = text("""
                SELECT DISTINCT ymd 
                FROM ods_trading_days_insight 
                WHERE ymd IS NOT NULL 
                ORDER BY ymd DESC 
                LIMIT 10
            """)
            result = connection.execute(query)
            trading_days = [str(row[0]) for row in result]

            if trading_days:
                self.trading_days_cache = trading_days
                print(f"  ✓ 获取交易日历成功: {trading_days[-1]} ~ {trading_days[0]}")
                return trading_days
            else:
                return None

        except Exception as e:
            print(f"  ⚠️ 获取交易日历失败: {str(e)}")
            return None

    def check_trading_day_coverage(self, connection, table_name):
        """检查目标表最近10个交易日的数据覆盖情况"""
        try:
            trading_days = self.get_trading_days(connection)
            if not trading_days:
                return None

            if not self.check_column_exists(connection, table_name, 'ymd'):
                return None

            placeholders = ', '.join([f"'{d}'" for d in trading_days])
            query = text(f"""
                SELECT DISTINCT ymd 
                FROM `{table_name}` 
                WHERE ymd IN ({placeholders})
            """)
            result = connection.execute(query)
            table_days = set(str(row[0]) for row in result)

            expected_days = set(trading_days)
            missing_days = sorted(expected_days - table_days, reverse=True)
            covered_days = sorted(expected_days & table_days, reverse=True)
            coverage_rate = len(covered_days) / len(expected_days) * 100 if expected_days else 0

            return {
                'trading_days': trading_days,
                'covered_days': covered_days,
                'missing_days': missing_days,
                'coverage_rate': round(coverage_rate, 1),
                'total_trading_days': len(trading_days),
                'covered_count': len(covered_days),
                'missing_count': len(missing_days)
            }

        except Exception as e:
            print(f"  检查交易日覆盖失败: {str(e)}")
            return None

    def get_ymd_counts(self, connection, table_name):
        """获取表按ymd分组的每日条数统计（最近10天）"""
        try:
            if not self.check_column_exists(connection, table_name, 'ymd'):
                return None

            query = text(f"""
                SELECT ymd, COUNT(1) as daily_count 
                FROM `{table_name}` 
                WHERE ymd IS NOT NULL 
                GROUP BY ymd 
                ORDER BY ymd DESC 
                LIMIT 10
            """)

            result = connection.execute(query)
            counts = []
            total_count = 0
            for row in result:
                counts.append({
                    'ymd': str(row[0]),
                    'count': row[1]
                })
                total_count += row[1]

            if counts:
                return {
                    'has_ymd': True,
                    'daily_counts': counts,
                    'total_count': total_count,
                    'max_count': max(c['count'] for c in counts),
                    'min_count': min(c['count'] for c in counts),
                    'avg_count': round(total_count / len(counts), 1)
                }
            else:
                return {
                    'has_ymd': True,
                    'daily_counts': [],
                    'total_count': 0,
                    'max_count': 0,
                    'min_count': 0,
                    'avg_count': 0
                }
        except Exception as e:
            print(f"  获取ymd每日条数失败: {str(e)}")
            return None

    def get_ymd_info(self, connection, table_name):
        """获取表的ymd日期信息"""
        try:
            if not self.check_column_exists(connection, table_name, 'ymd'):
                return None

            # ====================== 修复：单独查全表的真实最早/最晚日期 ======================
            # 1. 查全表的真实最早和最晚日期（不带LIMIT）
            min_max_query = text(f"""
                SELECT MIN(ymd) as min_ymd, MAX(ymd) as max_ymd 
                FROM `{table_name}` 
                WHERE ymd IS NOT NULL
            """)
            min_max_result = connection.execute(min_max_query)
            min_max_row = min_max_result.fetchone()
            true_min = str(min_max_row[0]) if min_max_row[0] else None
            true_max = str(min_max_row[1]) if min_max_row[1] else None
            # ===================================================================

            # 2. 查最近10个日期（用于展示列表，带LIMIT）
            query = text(f"""
                SELECT DISTINCT ymd 
                FROM `{table_name}` 
                WHERE ymd IS NOT NULL 
                ORDER BY ymd DESC 
                LIMIT 10
            """)

            result = connection.execute(query)
            dates = [str(row[0]) for row in result]

            if dates or true_min or true_max:
                return {
                    'has_ymd': True,
                    'ymd_dates': dates,  # 最近10个日期（倒序）
                    'ymd_count': len(dates),  # 最近10天的数量
                    'ymd_min': true_min,  # ✅ 真正的最早日期（全表）
                    'ymd_max': true_max,  # ✅ 真正的最晚日期（全表）
                    'recent_ymd_min': min(dates) if dates else None,  # 最近10天里的最早（辅助参考）
                    'recent_ymd_max': max(dates) if dates else None  # 最近10天里的最晚（辅助参考）
                }
            else:
                return {
                    'has_ymd': True,
                    'ymd_dates': [],
                    'ymd_count': 0,
                    'ymd_min': None,
                    'ymd_max': None,
                    'recent_ymd_min': None,
                    'recent_ymd_max': None
                }
        except Exception as e:
            print(f"  获取ymd信息失败: {str(e)}")
            return None

    def get_table_info(self, table_name):
        """获取表的完整信息"""
        try:
            db_url = f'mysql+pymysql://{self.user}:{self.password}@{self.host}:3306/{self.database}'
            engine = create_engine(db_url)

            info = {
                'table_name': table_name,
                'structure': None,
                'sample_data': None,
                'row_count': 0,
                'column_count': 0,
                'ymd_info': None,
                'ymd_counts': None,
                'trading_coverage': None
            }

            with engine.connect() as connection:
                # 1. 获取表结构
                try:
                    result = connection.execute(text(f"SHOW CREATE TABLE `{table_name}`"))
                    create_table_sql = result.fetchone()[1]
                    info['create_sql'] = create_table_sql
                except:
                    info['create_sql'] = None

                # 2. 获取表描述
                try:
                    result = connection.execute(text(f"DESCRIBE `{table_name}`"))
                    columns_info = []
                    for row in result:
                        col_info = {
                            'Field': row[0],
                            'Type': row[1],
                            'Null': row[2],
                            'Key': row[3],
                            'Default': row[4],
                            'Extra': row[5] if len(row) > 5 else ''
                        }
                        columns_info.append(col_info)
                    info['structure'] = columns_info
                    info['column_count'] = len(columns_info)
                except:
                    pass

                # 3. 获取行数
                try:
                    result = connection.execute(text(f"SELECT COUNT(*) FROM `{table_name}`"))
                    info['row_count'] = result.fetchone()[0]
                except:
                    pass

                # 4. 获取各类信息
                info['ymd_info'] = self.get_ymd_info(connection, table_name)
                info['ymd_counts'] = self.get_ymd_counts(connection, table_name)
                info['trading_coverage'] = self.check_trading_day_coverage(connection, table_name)

                # 5. 获取样例数据
                if info['row_count'] > 0:
                    try:
                        limit = min(5, info['row_count'])
                        has_ymd = self.check_column_exists(connection, table_name, 'ymd')

                        if has_ymd:
                            query = text(f"SELECT * FROM `{table_name}` ORDER BY ymd DESC LIMIT {limit}")
                        else:
                            query = text(f"SELECT * FROM `{table_name}` LIMIT {limit}")

                        df = pd.read_sql(query, connection)
                        info['sample_data'] = df
                    except:
                        pass

            return info

        except Exception as e:
            print(f"  表 {table_name} 信息获取失败: {str(e)[:50]}...")
            return None

    def _get_anchor_id(self, table_name):
        """生成表对应的HTML锚点ID"""
        return f"table-{table_name}"

    def _get_prefix_display_name(self, prefix):
        """获取前缀的展示名称"""
        prefix_names = {
            'ods': 'ODS层（原始数据）',
            'dwd': 'DWD层（明细数据）',
            'dmart': 'DMART层（数据集市）',
            'dwt': 'DWT层（宽表）',
            'dim': 'DIM层（维度表）',
            'other': '其他表'
        }
        return prefix_names.get(prefix, f'{prefix.upper()}层')

    def _get_sort_key(self, table_name):
        """获取表的排序键，用于固定顺序：ods -> dwd -> dmart -> other"""
        for prefix, order in sorted(self.PREFIX_ORDER.items(), key=lambda x: x[1]):
            if prefix != 'other' and table_name.startswith(prefix):
                return (order, table_name)
        return (self.PREFIX_ORDER['other'], table_name)

    def _generate_html_toc(self, table_groups):
        """生成HTML目录（Table of Contents）"""
        toc_html = []
        toc_html.append('<nav class="toc" id="toc">')
        toc_html.append('  <h2>📑 目录导航</h2>')
        toc_html.append('  <p class="toc-hint">点击表名快速跳转到详情，点击"🔝"返回目录</p>')
        toc_html.append('  <ul class="toc-list">')

        for prefix in sorted(table_groups.keys(),
                             key=lambda p: self.PREFIX_ORDER.get(p + '_', self.PREFIX_ORDER['other'])):
            group_tables = sorted(table_groups[prefix], key=lambda t: t.lower())
            display_name = self._get_prefix_display_name(prefix)

            toc_html.append(f'    <li class="toc-group">')
            toc_html.append(f'      <span class="toc-group-title">{display_name}</span>')
            toc_html.append(f'      <span class="toc-count">({len(group_tables)}张)</span>')
            toc_html.append('      <ul class="toc-sublist">')

            for table in group_tables:
                anchor = self._get_anchor_id(table)
                toc_html.append(f'        <li><a href="#{anchor}" class="toc-link">{table}</a></li>')

            toc_html.append('      </ul>')
            toc_html.append('    </li>')

        toc_html.append('  </ul>')
        toc_html.append('</nav>')
        return '\n'.join(toc_html)

    def _generate_html_table_detail(self, table_info, table_num, total_tables):
        """生成单张表的HTML详情"""
        if not table_info:
            return ""

        table_name = table_info['table_name']
        anchor = self._get_anchor_id(table_name)

        html = []
        html.append(f'<section class="table-detail" id="{anchor}">')
        html.append(f'  <div class="table-header">')
        html.append(f'    <h2>【表 {table_num}/{total_tables}】{table_name}</h2>')
        html.append(f'    <a href="#toc" class="back-to-top" title="返回目录">🔝</a>')
        html.append(f'  </div>')

        # 基本信息
        html.append('  <div class="info-section">')
        html.append('    <h3>📊 基本信息</h3>')
        html.append('    <table class="info-table">')
        html.append(f'      <tr><td>行数</td><td>{table_info.get("row_count", "未知"):,}</td></tr>')
        html.append(f'      <tr><td>列数</td><td>{table_info.get("column_count", "未知")}</td></tr>')
        html.append('    </table>')
        html.append('  </div>')

        # ymd日期信息
        ymd_info = table_info.get('ymd_info')
        html.append('  <div class="info-section">')
        html.append('    <h3>📅 ymd日期信息</h3>')
        if ymd_info:
            html.append('    <table class="info-table">')
            html.append(f'      <tr><td>存在ymd列</td><td>✓</td></tr>')
            html.append(f'      <tr><td>日期总数</td><td>{ymd_info.get("ymd_count", 0)}</td></tr>')

            # ✅ 修复：展示真正的全表最早/最晚日期
            if ymd_info.get('ymd_min'):
                html.append(f'      <tr><td>全表最早日期</td><td>{ymd_info["ymd_min"]}</td></tr>')
            if ymd_info.get('ymd_max'):
                html.append(f'      <tr><td>全表最晚日期</td><td>{ymd_info["ymd_max"]}</td></tr>')

            # 最近10天的范围（辅助参考）
            if ymd_info.get('recent_ymd_min') and ymd_info.get('recent_ymd_max'):
                html.append(
                    f'      <tr><td>最近10天范围</td><td>{ymd_info["recent_ymd_min"]} ~ {ymd_info["recent_ymd_max"]}</td></tr>')

            html.append('    </table>')

            if ymd_info.get('ymd_dates'):
                html.append('    <p>最近10个日期(倒序):</p>')
                html.append('    <div class="date-list">')
                for i, date in enumerate(ymd_info['ymd_dates'], 1):
                    html.append(f'      <span class="date-tag">{i}. {date}</span>')
                html.append('    </div>')
        else:
            html.append('    <p class="no-data">表中不存在ymd列</p>')
        html.append('  </div>')

        # 交易日覆盖检查
        trading_coverage = table_info.get('trading_coverage')
        html.append('  <div class="info-section">')
        html.append('    <h3>🔍 交易日覆盖检查（以 ods_trading_days_insight 为基准）</h3>')
        if trading_coverage:
            html.append('    <table class="info-table">')
            html.append(
                f'      <tr><td>基准区间</td><td>{trading_coverage["trading_days"][-1]} ~ {trading_coverage["trading_days"][0]} (共{trading_coverage["total_trading_days"]}个交易日)</td></tr>')
            html.append(
                f'      <tr><td>覆盖情况</td><td><strong>{trading_coverage["covered_count"]}/{trading_coverage["total_trading_days"]}</strong> ({trading_coverage["coverage_rate"]}%)</td></tr>')
            html.append('    </table>')

            if trading_coverage['missing_days']:
                html.append(f'    <div class="alert alert-warning">')
                html.append(f'      <p>⚠️ 缺失日期 ({trading_coverage["missing_count"]}天):</p>')
                html.append('      <div class="missing-dates">')
                for d in trading_coverage['missing_days']:
                    html.append(f'        <span class="missing-tag">❌ {d}</span>')
                html.append('      </div>')
                if trading_coverage['missing_count'] == trading_coverage['total_trading_days']:
                    html.append('      <p class="severity severity-high">🔴 严重：最近10个交易日全部缺失！</p>')
                elif trading_coverage['missing_count'] >= 3:
                    html.append('      <p class="severity severity-medium">🟠 警告：缺失较多，请检查数据同步任务</p>')
                else:
                    html.append('      <p class="severity severity-low">🟡 提示：少量缺失，可能是非交易日或数据延迟</p>')
                html.append('    </div>')
            else:
                html.append('    <div class="alert alert-success">')
                html.append('      <p>✅ 全部覆盖：最近10个交易日数据完整</p>')
                html.append('    </div>')

            if trading_coverage['coverage_rate'] == 100 and table_info.get('ymd_counts'):
                ymd_counts = table_info['ymd_counts']
                if ymd_counts and ymd_counts.get('daily_counts'):
                    html.append('    <h4>每日数据量明细:</h4>')
                    html.append('    <table class="data-table">')
                    html.append('      <tr><th>日期</th><th>条数</th><th>状态</th></tr>')
                    for item in ymd_counts['daily_counts']:
                        status = "✓" if item['count'] > 0 else "✗"
                        status_class = "status-ok" if item['count'] > 0 else "status-error"
                        html.append(
                            f'      <tr><td>{item["ymd"]}</td><td>{item["count"]:,}</td><td class="{status_class}">{status}</td></tr>')
                    html.append('    </table>')
        elif table_info.get('ymd_info') is None:
            html.append('    <p class="no-data">表中不存在ymd列，无法检查</p>')
        else:
            html.append('    <p class="no-data">⚠️ 无法获取交易日历（ods_trading_days_insight表不可用）</p>')
        html.append('  </div>')

        # 每日数据量统计
        ymd_counts = table_info.get('ymd_counts')
        html.append('  <div class="info-section">')
        html.append('    <h3>📈 每日数据量统计（最近10个有数据日期）</h3>')
        if ymd_counts and ymd_counts.get('daily_counts'):
            html.append('    <table class="info-table">')
            html.append(f'      <tr><td>10天总计</td><td>{ymd_counts["total_count"]:,} 条</td></tr>')
            html.append(f'      <tr><td>单日最大</td><td>{ymd_counts["max_count"]:,} 条</td></tr>')
            html.append(f'      <tr><td>单日最小</td><td>{ymd_counts["min_count"]:,} 条</td></tr>')
            html.append(f'      <tr><td>单日平均</td><td>{ymd_counts["avg_count"]:,} 条</td></tr>')
            html.append('    </table>')

            html.append('    <h4>明细:</h4>')
            html.append('    <table class="data-table">')
            html.append('      <tr><th>日期</th><th>条数</th><th>占比</th><th>可视化</th></tr>')
            max_count = ymd_counts['max_count'] if ymd_counts['max_count'] > 0 else 1
            for item in ymd_counts['daily_counts']:
                ratio = item['count'] / ymd_counts['total_count'] * 100 if ymd_counts['total_count'] > 0 else 0
                bar_width = int(item['count'] / max_count * 200)
                html.append(f'      <tr>')
                html.append(f'        <td>{item["ymd"]}</td>')
                html.append(f'        <td>{item["count"]:,}</td>')
                html.append(f'        <td>{ratio:.1f}%</td>')
                html.append(f'        <td><div class="bar" style="width:{bar_width}px"></div></td>')
                html.append(f'      </tr>')
            html.append('    </table>')
        elif ymd_counts:
            html.append('    <p class="no-data">存在ymd列但无有效数据</p>')
        else:
            html.append('    <p class="no-data">表中不存在ymd列</p>')
        html.append('  </div>')

        # 表结构
        if table_info.get('structure'):
            html.append('  <div class="info-section">')
            html.append('    <h3>🏗️ 表结构</h3>')
            html.append('    <table class="structure-table">')
            html.append(
                '      <tr><th>字段名</th><th>类型</th><th>可空</th><th>键</th><th>默认值</th><th>额外</th></tr>')
            for col in table_info['structure']:
                field = col.get('Field', '')
                type_ = col.get('Type', '')
                null = col.get('Null', '')
                key = col.get('Key', '')
                default = str(col.get('Default', '')) if col.get('Default') is not None else 'NULL'
                extra = col.get('Extra', '')
                html.append(
                    f'      <tr><td>{field}</td><td>{type_}</td><td>{null}</td><td>{key}</td><td>{default}</td><td>{extra}</td></tr>')
            html.append('    </table>')
            html.append('  </div>')

        # 样例数据
        if table_info.get('sample_data') is not None and not table_info['sample_data'].empty:
            df = table_info['sample_data']
            html.append('  <div class="info-section">')
            if ymd_info and 'ymd' in df.columns:
                html.append(f'    <h3>📝 数据样例（按ymd倒序，前{len(df)}行）</h3>')
            else:
                html.append(f'    <h3>📝 数据样例（前{len(df)}行）</h3>')

            pd.set_option('display.max_columns', None)
            pd.set_option('display.width', None)
            pd.set_option('display.max_colwidth', 100)

            sample_html = df.to_html(index=False, classes='sample-table')
            html.append('    <div class="sample-data">')
            html.append(sample_html)
            html.append('    </div>')
            html.append('  </div>')
        else:
            html.append('  <div class="info-section">')
            html.append('    <h3>📝 数据样例</h3>')
            html.append('    <p class="no-data">表为空或无法读取数据</p>')
            html.append('  </div>')

        html.append('</section>')
        return '\n'.join(html)

    def _get_css_styles(self):
        """返回CSS样式"""
        return '''
        * { margin: 0; padding: 0; box-sizing: border-box; }
        body { font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, "Helvetica Neue", Arial, sans-serif; background: #f5f7fa; color: #333; line-height: 1.6; }

        .report-header { background: linear-gradient(135deg, #667eea 0%, #764ba2 100%); color: white; padding: 40px; text-align: center; }
        .report-header h1 { font-size: 2.2em; margin-bottom: 15px; }
        .meta-info { opacity: 0.9; font-size: 0.95em; }
        .meta-info p { margin: 5px 0; }

        .toc { background: white; margin: 20px auto; max-width: 1200px; padding: 30px; border-radius: 12px; box-shadow: 0 2px 8px rgba(0,0,0,0.1); }
        .toc h2 { color: #667eea; margin-bottom: 10px; }
        .toc-hint { color: #888; font-size: 0.9em; margin-bottom: 20px; }
        .toc-list { list-style: none; }
        .toc-group { margin: 15px 0; }
        .toc-group-title { font-weight: bold; font-size: 1.1em; color: #444; }
        .toc-count { color: #888; margin-left: 8px; }
        .toc-sublist { list-style: none; margin-left: 20px; margin-top: 8px; display: flex; flex-wrap: wrap; gap: 8px; }
        .toc-link { display: inline-block; padding: 4px 12px; background: #f0f4ff; color: #667eea; text-decoration: none; border-radius: 20px; font-size: 0.9em; transition: all 0.2s; }
        .toc-link:hover { background: #667eea; color: white; transform: translateY(-1px); }

        .summary-section { background: white; margin: 20px auto; max-width: 1200px; padding: 30px; border-radius: 12px; box-shadow: 0 2px 8px rgba(0,0,0,0.1); }
        .summary-section h2 { color: #667eea; margin-bottom: 20px; }
        .summary-table { width: 100%; border-collapse: collapse; }
        .summary-table th { background: #f8f9fa; padding: 12px; text-align: left; font-weight: 600; border-bottom: 2px solid #e9ecef; }
        .summary-table td { padding: 12px; border-bottom: 1px solid #e9ecef; }

        .table-detail { background: white; margin: 20px auto; max-width: 1200px; padding: 30px; border-radius: 12px; box-shadow: 0 2px 8px rgba(0,0,0,0.1); }
        .table-header { display: flex; justify-content: space-between; align-items: center; margin-bottom: 25px; padding-bottom: 15px; border-bottom: 3px solid #667eea; }
        .table-header h2 { color: #333; font-size: 1.5em; }
        .back-to-top { text-decoration: none; font-size: 1.3em; padding: 5px 10px; border-radius: 8px; transition: background 0.2s; }
        .back-to-top:hover { background: #f0f4ff; }

        .info-section { margin: 20px 0; padding: 20px; background: #fafbfc; border-radius: 8px; }
        .info-section h3 { color: #555; margin-bottom: 15px; font-size: 1.1em; border-left: 4px solid #667eea; padding-left: 10px; }
        .info-section h4 { color: #666; margin: 15px 0 10px; font-size: 1em; }

        .info-table { width: 100%; max-width: 600px; border-collapse: collapse; margin: 10px 0; }
        .info-table td { padding: 8px 12px; border-bottom: 1px solid #e9ecef; }
        .info-table td:first-child { font-weight: 600; color: #555; width: 120px; }

        .data-table { width: 100%; border-collapse: collapse; margin: 10px 0; font-size: 0.9em; }
        .data-table th { background: #667eea; color: white; padding: 10px; text-align: left; }
        .data-table td { padding: 10px; border-bottom: 1px solid #e9ecef; }
        .data-table tr:hover { background: #f8f9fa; }

        .structure-table { width: 100%; border-collapse: collapse; margin: 10px 0; font-size: 0.85em; }
        .structure-table th { background: #667eea; color: white; padding: 10px; text-align: left; }
        .structure-table td { padding: 10px; border-bottom: 1px solid #e9ecef; }
        .structure-table tr:nth-child(even) { background: #f8f9fa; }

        .sample-table { width: 100%; border-collapse: collapse; margin: 10px 0; font-size: 0.85em; overflow-x: auto; display: block; }
        .sample-table th { background: #667eea; color: white; padding: 10px; text-align: left; white-space: nowrap; }
        .sample-table td { padding: 10px; border-bottom: 1px solid #e9ecef; white-space: nowrap; }

        .date-list { display: flex; flex-wrap: wrap; gap: 8px; margin: 10px 0; }
        .date-tag { background: #e3f2fd; color: #1976d2; padding: 4px 10px; border-radius: 12px; font-size: 0.85em; }
        .missing-tag { background: #ffebee; color: #c62828; padding: 4px 10px; border-radius: 12px; font-size: 0.85em; }

        .alert { padding: 15px; border-radius: 8px; margin: 15px 0; }
        .alert-success { background: #e8f5e9; border-left: 4px solid #4caf50; }
        .alert-warning { background: #fff3e0; border-left: 4px solid #ff9800; }
        .severity { margin-top: 10px; font-weight: 600; }
        .severity-high { color: #c62828; }
        .severity-medium { color: #ef6c00; }
        .severity-low { color: #f9a825; }

        .status-ok { color: #4caf50; font-weight: bold; }
        .status-error { color: #f44336; font-weight: bold; }

        .bar { height: 20px; background: linear-gradient(90deg, #667eea, #764ba2); border-radius: 10px; transition: width 0.3s; }

        .no-data { color: #888; font-style: italic; padding: 10px 0; }

        .layer-section { margin: 30px auto; max-width: 1200px; }
        .layer-title { background: linear-gradient(135deg, #667eea 0%, #764ba2 100%); color: white; padding: 20px 30px; border-radius: 12px; font-size: 1.3em; margin-bottom: 20px; }

        @media (max-width: 768px) {
            .report-header { padding: 20px; }
            .report-header h1 { font-size: 1.5em; }
            .toc, .table-detail, .summary-section { margin: 10px; padding: 20px; }
            .toc-sublist { flex-direction: column; }
        }
        '''

    def export_important_tables(self):
        """导出重要的表（按前缀筛选，输出HTML带目录导航）"""
        print("开始导出数据库表信息...")

        if not self.test_connection():
            return

        tables = self.get_all_tables()
        if not tables:
            print("错误：数据库中没有找到任何表")
            return

        # 按固定顺序排序并分类
        tables.sort(key=self._get_sort_key)

        table_groups = {}
        important_tables = []
        other_tables = []

        for table in tables:
            assigned = False
            for prefix in ['ods_', 'dwd_', 'dmart_', 'dwt_', 'dim_']:
                if table.startswith(prefix):
                    prefix_key = prefix.rstrip('_')
                    if prefix_key not in table_groups:
                        table_groups[prefix_key] = []
                    table_groups[prefix_key].append(table)
                    important_tables.append(table)
                    assigned = True
                    break
            if not assigned:
                if 'other' not in table_groups:
                    table_groups['other'] = []
                table_groups['other'].append(table)
                other_tables.append(table)

        print(f"找到 {len(tables)} 张表，其中:")
        for prefix in sorted(table_groups.keys(),
                             key=lambda p: self.PREFIX_ORDER.get(p + '_', self.PREFIX_ORDER['other'])):
            print(f"  {self._get_prefix_display_name(prefix)}: {len(table_groups[prefix])} 张")

        print("\n导出选项:")
        print("1. 只导出重要表（ods/dwd/dmart/dwt/dim开头）")
        print("2. 导出所有表")
        print("3. 导出指定前缀的表")

        choice = input("请选择 (1/2/3, 默认1): ").strip()

        if choice == '2':
            tables_to_export = tables
        elif choice == '3':
            prefix = input("请输入表前缀 (如 ods_): ").strip()
            tables_to_export = [t for t in tables if t.startswith(prefix)]
            if not tables_to_export:
                print(f"没有以 {prefix} 开头的表")
                return
        else:
            tables_to_export = important_tables

        print(f"\n开始导出 {len(tables_to_export)} 张表...")

        # 预收集所有表信息
        all_table_infos = []
        for i, table in enumerate(tables_to_export):
            print(f"处理: {table} ({i + 1}/{len(tables_to_export)})")
            try:
                table_info = self.get_table_info(table)
                if table_info:
                    all_table_infos.append(table_info)
                    print(f"  ✓ 完成")
                else:
                    print(f"  ✗ 获取信息失败")
            except Exception as e:
                print(f"  ✗ 错误: {str(e)[:100]}")
                all_table_infos.append({
                    'table_name': table,
                    'structure': None,
                    'sample_data': None,
                    'row_count': 0,
                    'column_count': 0,
                    'ymd_info': None,
                    'ymd_counts': None,
                    'trading_coverage': None,
                    'error': str(e)
                })

        # 重新按固定顺序分组
        export_groups = {}
        for info in all_table_infos:
            table = info['table_name']
            assigned = False
            for prefix in ['ods_', 'dwd_', 'dmart_', 'dwt_', 'dim_']:
                if table.startswith(prefix):
                    key = prefix.rstrip('_')
                    if key not in export_groups:
                        export_groups[key] = []
                    export_groups[key].append(table)
                    assigned = True
                    break
            if not assigned:
                if 'other' not in export_groups:
                    export_groups['other'] = []
                export_groups['other'].append(table)

        # 生成HTML
        timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        total_exported = len([i for i in all_table_infos if not i.get('error')])

        html_parts = []
        html_parts.append('<!DOCTYPE html>')
        html_parts.append('<html lang="zh-CN">')
        html_parts.append('<head>')
        html_parts.append('  <meta charset="UTF-8">')
        html_parts.append('  <meta name="viewport" content="width=device-width, initial-scale=1.0">')
        html_parts.append(f'  <title>QUANT数据库表结构报告 - {timestamp}</title>')
        html_parts.append('  <style>')
        html_parts.append(self._get_css_styles())
        html_parts.append('  </style>')
        html_parts.append('</head>')
        html_parts.append('<body>')

        # 头部
        html_parts.append('  <header class="report-header">')
        html_parts.append('    <h1>📊 QUANT数据库表结构及数据样例报告</h1>')
        html_parts.append('    <div class="meta-info">')
        html_parts.append(f'      <p>生成时间: {timestamp}</p>')
        html_parts.append(f'      <p>数据库: {self.database} @ {self.host}</p>')
        html_parts.append(f'      <p>总表数: {len(tables)} | 本次导出: {total_exported}/{len(tables_to_export)}</p>')
        html_parts.append('    </div>')
        html_parts.append('  </header>')

        # 目录
        html_parts.append(self._generate_html_toc(export_groups))

        # 统计概览
        html_parts.append('  <div class="summary-section">')
        html_parts.append('    <h2>📋 各层统计概览</h2>')
        html_parts.append('    <table class="summary-table">')
        html_parts.append('      <tr><th>层级</th><th>表数量</th><th>占比</th></tr>')
        for prefix in sorted(export_groups.keys(),
                             key=lambda p: self.PREFIX_ORDER.get(p + '_', self.PREFIX_ORDER['other'])):
            count = len(export_groups[prefix])
            pct = count / len(tables_to_export) * 100 if tables_to_export else 0
            display_name = self._get_prefix_display_name(prefix)
            html_parts.append(f'      <tr><td>{display_name}</td><td>{count}</td><td>{pct:.1f}%</td></tr>')
        html_parts.append('    </table>')
        html_parts.append('  </div>')

        # 各层详情
        table_num = 0
        for prefix in sorted(export_groups.keys(),
                             key=lambda p: self.PREFIX_ORDER.get(p + '_', self.PREFIX_ORDER['other'])):
            group_tables = sorted(export_groups[prefix], key=lambda t: t.lower())
            display_name = self._get_prefix_display_name(prefix)

            html_parts.append(f'  <div class="layer-section" id="layer-{prefix}">')
            html_parts.append(f'    <h2 class="layer-title">{display_name} ({len(group_tables)}张表)</h2>')

            for table in group_tables:
                table_num += 1
                info = next((i for i in all_table_infos if i['table_name'] == table), None)
                if info:
                    html_parts.append(self._generate_html_table_detail(info, table_num, total_exported))

            html_parts.append('  </div>')

        html_parts.append('</body>')
        html_parts.append('</html>')

        # 写入文件
        html_content = '\n'.join(html_parts)
        with open(self.output_file, 'w', encoding='utf-8') as f:
            f.write(html_content)

        # 完成提示
        if self.output_file.exists():
            file_size = self.output_file.stat().st_size / 1024
            print("\n" + "=" * 60)
            print("导出完成！")
            print("=" * 60)
            print(f"输出文件: {self.output_file}")
            print(f"文件大小: {file_size:.1f} KB")
            print(f"导出表数: {total_exported}/{len(tables_to_export)}")
            print("=" * 60)

            print("\n文件特性:")
            print("✅ HTML格式，浏览器直接打开")
            print("✅ 顶部目录导航，点击表名跳转")
            print("✅ 每个表详情右上角 🔝 返回目录")
            print("✅ 固定顺序: ODS → DWD → DMART → DWT → DIM → 其他")
            print("✅ 全表真实最早/最晚日期（非近10天）")
            print("✅ 交易日覆盖检查 + 每日数据量统计")
            print("✅ 响应式设计，支持手机查看")

            print(f"\n{'=' * 60}")
            print("操作说明:")
            print("1. 用浏览器打开HTML文件")
            print("2. 点击目录中的表名跳转到详情")
            print("3. 点击 🔝 返回顶部目录")
            print("4. 红色 ❌ 标记缺失日期，绿色 ✅ 表示完整")
            print("=" * 60)
        else:
            print("错误：文件未生成")


def main():
    print("QUANT数据库表结构导出工具（HTML版）")
    print("=" * 60)
    print("输出格式: HTML（带目录导航和超链接）")
    print("展示顺序: ODS → DWD → DMART → DWT → DIM → 其他")
    print("日期修复: 全表真实最早/最晚日期（非近10天）")
    print("=" * 60)

    try:
        exporter = TableDataExporterFull()
        exporter.export_important_tables()
    except Exception as e:
        print(f"\n❌ 程序运行失败: {str(e)}")


if __name__ == "__main__":
    main()
    