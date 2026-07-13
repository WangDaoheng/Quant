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

    # ========== 表优先级定义：按用户指定顺序 ==========
    # 数字越小越靠前，不在此列表的表排最后，按字母序
    TABLE_PRIORITY = {
        # ODS 层 - 下午跑 - insight 行情源
        'ods_stock_code_daily_insight': (1, 1, 1),
        'ods_stock_limit_summary_insight': (1, 1, 2),
        'ods_stock_chouma_insight': (1, 1, 3),
        'ods_astock_industry_overview': (1, 1, 4),
        'ods_astock_industry_detail': (1, 1, 5),
        # ODS 层 - 下午跑 - tushare 行情源
        'ods_stock_kline_daily_ts': (1, 2, 1),
        'ods_tushare_board_concept_name_ths': (1, 2, 2),
        'ods_tushare_stock_board_concept_index_ths': (1, 2, 3),
        'ods_tushare_stock_board_concept_maps_ths': (1, 2, 4),
        # ODS 层 - 下午跑 - akshare 行情源
        'ods_akshare_stock_yjkb_em': (1, 3, 1),
        'ods_akshare_stock_yjyg_em': (1, 3, 2),
        'ods_akshare_stock_a_high_low_statistics': (1, 3, 3),
        # ODS 层 - 凌晨跑
        'ods_index_a_share_insight': (1, 4, 1),
        'ods_future_inside_insight': (1, 4, 2),
        'ods_shareholder_num': (1, 4, 3),
        # ODS 层 - 周末跑
        'ods_akshare_stock_value_em': (1, 5, 1),
        'ods_akshare_stock_zh_a_gdhs_detail_em': (1, 5, 2),
        # DWD 层
        'dwd_stock_a_total_plate': (2, 1, 1),
        'ods_stock_exchange_market': (2, 1, 2),  # 注意：用户写的是ods开头但放在DWD层
        'dwd_shareholder_num_latest': (2, 1, 3),
        'dwd_ashare_stock_base_info': (2, 1, 4),
        'dwd_stock_zt_list': (2, 1, 5),
        'dwd_stock_dt_list': (2, 1, 6),
        'dwd_stock_technical_indicators': (2, 1, 7),
        # MART 层
        'dmart_stock_zt_details': (3, 1, 1),
    }

    # 层级展示名称
    LAYER_NAMES = {
        1: 'ODS 层（原始数据）',
        2: 'DWD 层（明细数据）',
        3: 'MART 层（数据集市）',
        99: '其他表'
    }

    # ODS 子分组名称
    ODS_SUBGROUP_NAMES = {
        (1, 1): '（一）下午跑 - insight 行情源',
        (1, 2): '（一）下午跑 - tushare 行情源',
        (1, 3): '（一）下午跑 - akshare 行情源',
        (1, 4): '（二）凌晨跑',
        (1, 5): '（三）周末跑',
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
        """从 ods_trading_days_insight 获取最近10个已发生的交易日（<=今天）"""
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

            today = datetime.now().strftime('%Y%m%d')

            query = text("""
                SELECT DISTINCT ymd 
                FROM ods_trading_days_insight 
                WHERE ymd IS NOT NULL 
                AND ymd <= :today
                ORDER BY ymd DESC 
                LIMIT 10
            """)
            result = connection.execute(query, {"today": today})

            trading_days = [str(row[0]) for row in result]

            if trading_days:
                self.trading_days_cache = trading_days
                print(f"  ✓ 获取交易日历成功: {trading_days[-1]} ~ {trading_days[0]} (基准日期: {today})")
                return trading_days
            else:
                print(f"  ⚠️  ods_trading_days_insight 中无 {today} 及之前的交易日数据")
                return None

        except Exception as e:
            print(f"  ⚠️ 获取交易日历失败: {str(e)}")
            return None

    def check_trading_day_coverage(self, connection, table_name):
        """检查目标表最近10个已发生交易日的数据覆盖情况"""
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

    def get_ymd_info(self, connection, table_name):
        """获取表的ymd日期信息"""
        try:
            if not self.check_column_exists(connection, table_name, 'ymd'):
                return None

            min_max_query = text(f"""
                SELECT MIN(ymd) as min_ymd, MAX(ymd) as max_ymd 
                FROM `{table_name}` 
                WHERE ymd IS NOT NULL
            """)
            min_max_result = connection.execute(min_max_query)
            min_max_row = min_max_result.fetchone()
            true_min = str(min_max_row[0]) if min_max_row[0] else None
            true_max = str(min_max_row[1]) if min_max_row[1] else None

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
                    'ymd_dates': dates,
                    'ymd_count': len(dates),
                    'ymd_min': true_min,
                    'ymd_max': true_max,
                    'recent_ymd_min': min(dates) if dates else None,
                    'recent_ymd_max': max(dates) if dates else None
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

    def get_daily_counts(self, connection, table_name, trading_days):
        """获取指定交易日期的每日数据量，并检测波动"""
        try:
            if not trading_days:
                return None

            placeholders = ', '.join([f"'{d}'" for d in trading_days])
            query = text(f"""
                SELECT ymd, COUNT(1) as cnt 
                FROM `{table_name}` 
                WHERE ymd IN ({placeholders})
                GROUP BY ymd 
                ORDER BY ymd DESC
            """)
            result = connection.execute(query)
            daily_counts = {str(row[0]): row[1] for row in result}

            # 构建完整结果（包含缺失日期）
            result_list = []
            for day in trading_days:
                cnt = daily_counts.get(day, 0)
                result_list.append({
                    'ymd': day,
                    'count': cnt,
                    'has_data': cnt > 0
                })

            # 检测波动（只比较有数据的日期）
            data_counts = [item['count'] for item in result_list if item['count'] > 0]
            fluctuation_alert = None

            if len(data_counts) >= 2:
                # 计算相邻日期的波动率
                max_fluctuation = 0
                max_fluctuation_pair = None
                for i in range(len(data_counts) - 1):
                    if data_counts[i + 1] > 0:  # 避免除0
                        fluctuation = abs(data_counts[i] - data_counts[i + 1]) / data_counts[i + 1] * 100
                        if fluctuation > max_fluctuation:
                            max_fluctuation = fluctuation
                            max_fluctuation_pair = (data_counts[i + 1], data_counts[i])

                if max_fluctuation > 20:
                    fluctuation_alert = {
                        'rate': round(max_fluctuation, 1),
                        'from_count': max_fluctuation_pair[0],
                        'to_count': max_fluctuation_pair[1]
                    }

            return {
                'daily_counts': result_list,
                'fluctuation_alert': fluctuation_alert,
                'total_count': sum(data_counts),
                'avg_count': round(sum(data_counts) / len(data_counts), 1) if data_counts else 0
            }

        except Exception as e:
            print(f"  获取每日数据量失败: {str(e)}")
            return None

    def get_table_info(self, table_name):
        """获取表的完整信息"""
        try:
            db_url = f'mysql+pymysql://{self.user}:{self.password}@{self.host}:3306/{self.database}'
            engine = create_engine(db_url)

            info = {
                'table_name': table_name,
                'sample_data': None,
                'row_count': 0,
                'column_count': 0,
                'ymd_info': None,
                'trading_coverage': None,
                'daily_counts': None
            }

            with engine.connect() as connection:
                # 1. 获取行数
                try:
                    result = connection.execute(text(f"SELECT COUNT(*) FROM `{table_name}`"))
                    info['row_count'] = result.fetchone()[0]
                except:
                    pass

                # 2. 获取列数
                try:
                    result = connection.execute(text(f"DESCRIBE `{table_name}`"))
                    columns = [row[0] for row in result]
                    info['column_count'] = len(columns)
                except:
                    pass

                # 3. 获取各类信息
                info['ymd_info'] = self.get_ymd_info(connection, table_name)
                info['trading_coverage'] = self.check_trading_day_coverage(connection, table_name)

                # 4. 获取每日数据量（只要有ymd列就获取）
                if info['ymd_info'] and info['trading_coverage']:
                    info['daily_counts'] = self.get_daily_counts(
                        connection, table_name, info['trading_coverage']['trading_days']
                    )

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

    # ========== 排序核心：按用户定义的优先级 ==========
    def _get_sort_key(self, table_name):
        """
        返回排序键元组 (layer, subgroup, order, table_name)
        不在 TABLE_PRIORITY 中的表: layer=99, 按字母序排最后
        """
        if table_name in self.TABLE_PRIORITY:
            layer, subgroup, order = self.TABLE_PRIORITY[table_name]
            return (layer, subgroup, order, table_name)
        else:
            # 其他表：按字母顺序排最后
            return (99, 99, 99, table_name)

    def _get_layer_name(self, layer_num):
        """获取层级展示名称"""
        return self.LAYER_NAMES.get(layer_num, '其他表')

    def _get_subgroup_name(self, layer, subgroup):
        """获取ODS子分组名称"""
        if layer == 1:
            return self.ODS_SUBGROUP_NAMES.get((layer, subgroup), f'其他 ODS 表')
        return None

    def _generate_html_toc(self, layer_groups):
        """生成HTML目录（Table of Contents）"""
        toc_html = []
        toc_html.append('<nav class="toc" id="toc">')
        toc_html.append('  <h2>📑 目录导航</h2>')
        toc_html.append('  <p class="toc-hint">点击表名快速跳转到详情，点击"🔝"返回目录</p>')
        toc_html.append('  <ul class="toc-list">')

        # 按层级排序：1=ODS, 2=DWD, 3=MART, 99=其他
        for layer in sorted(layer_groups.keys()):
            layer_tables = layer_groups[layer]
            if not layer_tables:
                continue

            layer_name = self._get_layer_name(layer)
            toc_html.append(f'    <li class="toc-group">')
            toc_html.append(f'      <span class="toc-group-title">{layer_name}</span>')
            toc_html.append(f'      <span class="toc-count">({len(layer_tables)}张)</span>')

            # ODS 层需要再按子分组展示
            if layer == 1:
                # 按子分组归类
                subgroups = {}
                for table in layer_tables:
                    key = self.TABLE_PRIORITY.get(table, (1, 99, 99))
                    sg = key[1]
                    if sg not in subgroups:
                        subgroups[sg] = []
                    subgroups[sg].append(table)

                toc_html.append('      <ul class="toc-sublist">')
                for sg in sorted(subgroups.keys()):
                    sg_name = self._get_subgroup_name(layer, sg)
                    if sg_name:
                        toc_html.append(f'        <li class="toc-subgroup-title">{sg_name}</li>')
                    for table in subgroups[sg]:
                        anchor = self._get_anchor_id(table)
                        toc_html.append(f'        <li><a href="#{anchor}" class="toc-link">{table}</a></li>')
                toc_html.append('      </ul>')

            else:
                toc_html.append('      <ul class="toc-sublist">')
                for table in layer_tables:
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

        # 获取该表在优先级中的位置信息，用于展示标签
        priority_info = self.TABLE_PRIORITY.get(table_name)
        tags_html = ""
        if priority_info:
            layer, subgroup, order = priority_info
            layer_name = self._get_layer_name(layer)
            if layer == 1:
                sg_name = self._get_subgroup_name(layer, subgroup)
                tags_html = f'<span class="table-tag layer-ods">{sg_name}</span>'
            else:
                tags_html = f'<span class="table-tag layer-{layer}">{layer_name}</span>'

        html = []
        html.append(f'<section class="table-detail" id="{anchor}">')
        html.append(f'  <div class="table-header">')
        html.append(f'    <div class="table-title-wrap">')
        html.append(f'      <h2>【表 {table_num}/{total_tables}】{table_name}</h2>')
        if tags_html:
            html.append(f'      <div class="table-tags">{tags_html}</div>')
        html.append(f'    </div>')
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

            if ymd_info.get('ymd_min'):
                html.append(f'      <tr><td>全表最早日期</td><td>{ymd_info["ymd_min"]}</td></tr>')
            if ymd_info.get('ymd_max'):
                html.append(f'      <tr><td>全表最晚日期</td><td>{ymd_info["ymd_max"]}</td></tr>')

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
        daily_counts = table_info.get('daily_counts')
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

            # 每日数据量明细（只要有ymd列和覆盖率数据就显示）
            if daily_counts and daily_counts.get('daily_counts'):
                html.append('    <h4>📈 每日数据量明细:</h4>')

                # 波动警告
                fluctuation = daily_counts.get('fluctuation_alert')
                if fluctuation:
                    html.append(f'    <div class="alert alert-fluctuation">')
                    html.append(f'      <p>⚠️ 数据量波动警告：相邻日期间波动率达 {fluctuation["rate"]}%</p>')
                    html.append(f'      <p>从 {fluctuation["from_count"]:,} 条 → {fluctuation["to_count"]:,} 条</p>')
                    html.append('    </div>')

                html.append('    <table class="data-table">')
                html.append('      <tr><th>日期</th><th>条数</th><th>状态</th><th>环比</th></tr>')

                daily_list = daily_counts['daily_counts']
                for idx, item in enumerate(daily_list):
                    cnt = item['count']
                    status = "✓" if cnt > 0 else "✗"
                    status_class = "status-ok" if cnt > 0 else "status-error"

                    # 计算环比
                    change_str = "-"
                    if idx < len(daily_list) - 1:
                        next_cnt = daily_list[idx + 1]['count']
                        if next_cnt > 0 and cnt > 0:
                            change_pct = (cnt - next_cnt) / next_cnt * 100
                            change_str = f"{change_pct:+.1f}%"
                            if abs(change_pct) > 20:
                                change_str = f'<span class="fluctuation-high">{change_str}</span>'

                    html.append(f'      <tr>')
                    html.append(f'        <td>{item["ymd"]}</td>')
                    html.append(f'        <td>{cnt:,}</td>')
                    html.append(f'        <td class="{status_class}">{status}</td>')
                    html.append(f'        <td>{change_str}</td>')
                    html.append(f'      </tr>')

                html.append('    </table>')

                # 汇总
                html.append(f'    <p class="summary-line">10日总计: {daily_counts["total_count"]:,} 条 | 日均: {daily_counts["avg_count"]:,} 条</p>')

        elif table_info.get('ymd_info') is None:
            html.append('    <p class="no-data">表中不存在ymd列，无法检查</p>')
        else:
            html.append('    <p class="no-data">⚠️ 无法获取交易日历（ods_trading_days_insight表不可用）</p>')
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
        .toc-subgroup-title { width: 100%; font-weight: 600; color: #666; margin: 8px 0 4px 0; font-size: 0.95em; border-left: 3px solid #667eea; padding-left: 8px; }
        .toc-link { display: inline-block; padding: 4px 12px; background: #f0f4ff; color: #667eea; text-decoration: none; border-radius: 20px; font-size: 0.9em; transition: all 0.2s; }
        .toc-link:hover { background: #667eea; color: white; transform: translateY(-1px); }

        .summary-section { background: white; margin: 20px auto; max-width: 1200px; padding: 30px; border-radius: 12px; box-shadow: 0 2px 8px rgba(0,0,0,0.1); }
        .summary-section h2 { color: #667eea; margin-bottom: 20px; }
        .summary-table { width: 100%; border-collapse: collapse; }
        .summary-table th { background: #f8f9fa; padding: 12px; text-align: left; font-weight: 600; border-bottom: 2px solid #e9ecef; }
        .summary-table td { padding: 12px; border-bottom: 1px solid #e9ecef; }

        .table-detail { background: white; margin: 20px auto; max-width: 1200px; padding: 30px; border-radius: 12px; box-shadow: 0 2px 8px rgba(0,0,0,0.1); }
        .table-header { display: flex; justify-content: space-between; align-items: center; margin-bottom: 25px; padding-bottom: 15px; border-bottom: 3px solid #667eea; }
        .table-title-wrap { flex: 1; }
        .table-title-wrap h2 { color: #333; font-size: 1.5em; }
        .table-tags { margin-top: 8px; }
        .table-tag { display: inline-block; padding: 3px 10px; border-radius: 12px; font-size: 0.8em; margin-right: 6px; }
        .layer-ods { background: #e3f2fd; color: #1565c0; }
        .layer-2 { background: #f3e5f5; color: #6a1b9a; }
        .layer-3 { background: #e8f5e9; color: #2e7d32; }
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

        .sample-table { width: 100%; border-collapse: collapse; margin: 10px 0; font-size: 0.85em; overflow-x: auto; display: block; }
        .sample-table th { background: #667eea; color: white; padding: 10px; text-align: left; white-space: nowrap; }
        .sample-table td { padding: 10px; border-bottom: 1px solid #e9ecef; white-space: nowrap; }

        .date-list { display: flex; flex-wrap: wrap; gap: 8px; margin: 10px 0; }
        .date-tag { background: #e3f2fd; color: #1976d2; padding: 4px 10px; border-radius: 12px; font-size: 0.85em; }
        .missing-tag { background: #ffebee; color: #c62828; padding: 4px 10px; border-radius: 12px; font-size: 0.85em; }

        .alert { padding: 15px; border-radius: 8px; margin: 15px 0; }
        .alert-success { background: #e8f5e9; border-left: 4px solid #4caf50; }
        .alert-warning { background: #fff3e0; border-left: 4px solid #ff9800; }
        .alert-fluctuation { background: #fff8e1; border-left: 4px solid #ffc107; }
        .severity { margin-top: 10px; font-weight: 600; }
        .severity-high { color: #c62828; }
        .severity-medium { color: #ef6c00; }
        .severity-low { color: #f9a825; }

        .status-ok { color: #4caf50; font-weight: bold; }
        .status-error { color: #f44336; font-weight: bold; }
        .fluctuation-high { color: #ff6f00; font-weight: bold; }

        .summary-line { color: #666; font-size: 0.9em; margin-top: 10px; font-style: italic; }

        .bar { height: 20px; background: linear-gradient(90deg, #667eea, #764ba2); border-radius: 10px; transition: width 0.3s; }

        .no-data { color: #888; font-style: italic; padding: 10px 0; }

        .layer-section { margin: 30px auto; max-width: 1200px; }
        .layer-title { background: linear-gradient(135deg, #667eea 0%, #764ba2 100%); color: white; padding: 20px 30px; border-radius: 12px; font-size: 1.3em; margin-bottom: 20px; }

        .subgroup-title { background: #f0f4ff; color: #444; padding: 12px 20px; border-radius: 8px; font-size: 1.1em; margin: 20px 0 10px 0; border-left: 4px solid #667eea; }

        @media (max-width: 768px) {
            .report-header { padding: 20px; }
            .report-header h1 { font-size: 1.5em; }
            .toc, .table-detail, .summary-section { margin: 10px; padding: 20px; }
            .toc-sublist { flex-direction: column; }
        }
        '''

    def export_important_tables(self):
        """导出重要的表（按用户指定优先级，输出HTML带目录导航）"""
        print("开始导出数据库表信息...")

        if not self.test_connection():
            return

        tables = self.get_all_tables()
        if not tables:
            print("错误：数据库中没有找到任何表")
            return

        # 按用户定义的优先级排序
        tables.sort(key=self._get_sort_key)

        # 按层级分组
        layer_groups = {1: [], 2: [], 3: [], 99: []}
        for table in tables:
            if table in self.TABLE_PRIORITY:
                layer = self.TABLE_PRIORITY[table][0]
                layer_groups[layer].append(table)
            else:
                layer_groups[99].append(table)

        # 清理空分组
        layer_groups = {k: v for k, v in layer_groups.items() if v}

        print(f"找到 {len(tables)} 张表，其中:")
        for layer in sorted(layer_groups.keys()):
            layer_name = self._get_layer_name(layer)
            count = len(layer_groups[layer])
            print(f"  {layer_name}: {count} 张")

        # 导出所有表（不再询问，直接全量导出）
        tables_to_export = tables

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
                    'sample_data': None,
                    'row_count': 0,
                    'column_count': 0,
                    'ymd_info': None,
                    'trading_coverage': None,
                    'daily_counts': None,
                    'error': str(e)
                })

        # 重新按层级分组（用于生成HTML）
        export_layer_groups = {1: [], 2: [], 3: [], 99: []}
        for info in all_table_infos:
            table = info['table_name']
            if table in self.TABLE_PRIORITY:
                layer = self.TABLE_PRIORITY[table][0]
                export_layer_groups[layer].append(table)
            else:
                export_layer_groups[99].append(table)
        export_layer_groups = {k: v for k, v in export_layer_groups.items() if v}

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
        html_parts.append(self._generate_html_toc(export_layer_groups))

        # 统计概览
        html_parts.append('  <div class="summary-section">')
        html_parts.append('    <h2>📋 各层统计概览</h2>')
        html_parts.append('    <table class="summary-table">')
        html_parts.append('      <tr><th>层级</th><th>表数量</th><th>占比</th></tr>')
        for layer in sorted(export_layer_groups.keys()):
            count = len(export_layer_groups[layer])
            pct = count / len(tables_to_export) * 100 if tables_to_export else 0
            display_name = self._get_layer_name(layer)
            html_parts.append(f'      <tr><td>{display_name}</td><td>{count}</td><td>{pct:.1f}%</td></tr>')
        html_parts.append('    </table>')
        html_parts.append('  </div>')

        # 各层详情
        table_num = 0
        for layer in sorted(export_layer_groups.keys()):
            group_tables = export_layer_groups[layer]
            display_name = self._get_layer_name(layer)

            html_parts.append(f'  <div class="layer-section" id="layer-{layer}">')
            html_parts.append(f'    <h2 class="layer-title">{display_name} ({len(group_tables)}张表)</h2>')

            # ODS 层需要按子分组展示
            if layer == 1:
                # 按子分组归类
                subgroups = {}
                for table in group_tables:
                    key = self.TABLE_PRIORITY.get(table, (1, 99, 99))
                    sg = key[1]
                    if sg not in subgroups:
                        subgroups[sg] = []
                    subgroups[sg].append(table)

                for sg in sorted(subgroups.keys()):
                    sg_name = self._get_subgroup_name(layer, sg)
                    if sg_name:
                        html_parts.append(f'    <h3 class="subgroup-title">{sg_name}</h3>')

                    for table in subgroups[sg]:
                        table_num += 1
                        info = next((i for i in all_table_infos if i['table_name'] == table), None)
                        if info:
                            html_parts.append(self._generate_html_table_detail(info, table_num, total_exported))
            else:
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

            print("\n展示顺序:")
            print("1️⃣  ODS 层")
            print("    （一）下午跑")
            print("        · insight 行情源")
            print("        · tushare 行情源")
            print("        · akshare 行情源")
            print("    （二）凌晨跑")
            print("    （三）周末跑")
            print("2️⃣  DWD 层")
            print("3️⃣  MART 层")
            print("4️⃣  其他表（按字母顺序）")

            print("\n文件特性:")
            print("✅ HTML格式，浏览器直接打开")
            print("✅ 顶部目录导航，点击表名跳转")
            print("✅ 每个表详情右上角 🔝 返回目录")
            print("✅ 表头带层级标签（ODS/DWD/MART）")
            print("✅ 全表真实最早/最晚日期（非近10天）")
            print("✅ 交易日覆盖检查（只比对已发生的交易日）")
            print("✅ 每日数据量明细 + 波动检测（>20%标红）")
            print("✅ 响应式设计，支持手机查看")

            print(f"\n{'=' * 60}")
            print("操作说明:")
            print("1. 用浏览器打开HTML文件")
            print("2. 点击目录中的表名跳转到详情")
            print("3. 点击 🔝 返回顶部目录")
            print("4. 红色 ❌ 标记缺失日期，绿色 ✅ 表示完整")
            print("5. 橙色 ⚠️ 标记数据量波动超过20%")
            print("=" * 60)
        else:
            print("错误：文件未生成")


def main():
    print("QUANT数据库表结构导出工具（HTML版）")
    print("=" * 60)
    print("输出格式: HTML（带目录导航和超链接）")
    print("展示顺序: ODS → DWD → MART → 其他")
    print("ODS子分组: 下午跑(insight/tushare/akshare) → 凌晨跑 → 周末跑")
    print("日期修复: 全表真实最早/最晚日期（非近10天）")
    print("交易日历: 只比对今天及之前的已发生交易日")
    print("波动检测: 相邻日期间数据量波动超过20%会标红提示")
    print("=" * 60)

    try:
        exporter = TableDataExporterFull()
        exporter.export_important_tables()
    except Exception as e:
        print(f"\n❌ 程序运行失败: {str(e)}")


if __name__ == "__main__":
    main()