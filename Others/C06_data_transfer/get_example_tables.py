import os
import pandas as pd
import logging
from datetime import datetime
from sqlalchemy import create_engine, text
from pathlib import Path  # 新增：导入Path用于路径处理
import CommonProperties.Base_Properties as Base_Properties
from CommonProperties.set_config import setup_logging_config

# 配置日志
setup_logging_config()
logger = logging.getLogger(__name__)


class TableDataExporterFull:
    """导出数据库表数据样例到单个文件 - 显示完整数据"""

    def __init__(self):
        # 使用您的MySQL配置
        self.user = Base_Properties.origin_mysql_user
        self.password = Base_Properties.origin_mysql_password
        self.host = Base_Properties.origin_mysql_host
        self.database = Base_Properties.origin_mysql_database

        # ====================== 核心优化：精准推导 Quant/Others/output 路径 ======================
        # 1. 获取当前脚本（export_table_samples_full.py）的绝对路径
        current_script_path = Path(__file__).resolve()

        # 2. 向上追溯找到项目根目录 Quant/（关键：基于 CommonProperties 目录反向定位，更稳定）
        # 方案1：通过 CommonProperties 目录（项目中固定存在）定位 Quant/（推荐，兼容性更强）
        current_dir = current_script_path.parent
        project_root = None
        # 向上遍历目录，直到找到包含 CommonProperties 的目录（即 Quant/）
        while current_dir != current_dir.parent:
            if (current_dir / "CommonProperties").exists():
                project_root = current_dir
                break
            current_dir = current_dir.parent

        # 方案2：如果脚本目录结构固定，可直接向上追溯（备用，简洁但依赖目录结构）
        # project_root = current_script_path.parent.parent  # 若脚本在 Quant/xxx/ 下，直接向上两级到 Quant/

        # 校验项目根目录是否找到
        if not project_root or not (project_root / "CommonProperties").exists():
            raise FileNotFoundError("❌ 未找到项目根目录 Quant/（缺少 CommonProperties 目录）")

        # 3. 构造 Quant/Others 目录路径
        others_dir = project_root / "Others"

        # 4. 构造 Quant/Others/output 目录路径
        self.output_dir = others_dir / "output"

        # 5. 自动创建 Others 和 output 目录（若不存在）
        self.output_dir.mkdir(parents=True, exist_ok=True)
        print(f"📁 自动创建/确认输出目录: {self.output_dir}")

        # 6. 构造完整的输出文件路径（放入 Quant/Others/output 目录）
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_filename = f"quant_tables_full_{timestamp}.txt"
        self.output_file = self.output_dir / output_filename  # Path对象，支持后续直接操作
        # ======================================================================================

        print(f"数据库配置:")
        print(f"  主机: {self.host}")
        print(f"  数据库: {self.database}")
        print(f"  用户: {self.user}")
        print(f"  输出文件将保存到: {self.output_file}")  # 新增：提示输出文件路径
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

            # 使用SHOW TABLES
            with engine.connect() as connection:
                result = connection.execute(text("SHOW TABLES"))
                tables = [row[0] for row in result]

            print(f"✓ 找到 {len(tables)} 张表")
            return tables

        except Exception as e:
            print(f"✗ 获取表列表失败: {str(e)}")
            return []

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
                'column_count': 0
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

                # 4. 获取样例数据（最多5行）
                if info['row_count'] > 0:
                    try:
                        limit = min(5, info['row_count'])
                        query = text(f"SELECT * FROM `{table_name}` LIMIT {limit}")
                        df = pd.read_sql(query, connection)
                        info['sample_data'] = df
                    except:
                        pass

            return info

        except Exception as e:
            print(f"  表 {table_name} 信息获取失败: {str(e)[:50]}...")
            return None

    def write_table_info(self, f, table_info, table_num, total_tables):
        """写入单个表的完整信息到文件"""
        if not table_info:
            return

        table_name = table_info['table_name']

        f.write(f"\n【表 {table_num}/{total_tables}】{table_name}\n")
        f.write("=" * 100 + "\n")

        # 1. 基本信息
        f.write(f"基本信息:\n")
        f.write(f"  行数: {table_info.get('row_count', '未知')}\n")
        f.write(f"  列数: {table_info.get('column_count', '未知')}\n")
        f.write("\n")

        # 2. 表结构（完整）
        if table_info.get('structure'):
            f.write("表结构（完整）:\n")
            f.write("-" * 80 + "\n")
            f.write(f"{'字段名':<20} {'类型':<20} {'可空':<5} {'键':<5} {'默认值':<15} {'额外':<10}\n")
            f.write("-" * 80 + "\n")
            for col in table_info['structure']:
                field = col.get('Field', '')
                type_ = col.get('Type', '')
                null = col.get('Null', '')
                key = col.get('Key', '')
                default = str(col.get('Default', '')) if col.get('Default') is not None else 'NULL'
                extra = col.get('Extra', '')

                f.write(f"{field:<20} {type_:<20} {null:<5} {key:<5} {default:<15} {extra:<10}\n")
        f.write("\n")

        # 3. 样例数据（完整显示所有列）
        if table_info.get('sample_data') is not None and not table_info['sample_data'].empty:
            df = table_info['sample_data']
            f.write(f"数据样例（前{len(df)}行，完整列）:\n")
            f.write("-" * 80 + "\n")

            # 显示所有列名
            columns = df.columns.tolist()
            f.write(f"所有列({len(columns)}个):\n")
            for i, col in enumerate(columns, 1):
                f.write(f"  {i:2d}. {col}\n")
            f.write("\n")

            # 显示数据（表格格式）
            # 设置pandas显示选项
            pd.set_option('display.max_columns', None)
            pd.set_option('display.width', None)
            pd.set_option('display.max_colwidth', 50)

            # 转换为字符串
            data_str = df.to_string(index=False)

            # 如果数据太长，分块显示
            if len(data_str) > 5000:
                f.write("数据预览（前5000字符）:\n")
                f.write(data_str[:5000])
                f.write(f"\n... (数据过长，已截断，原始{len(data_str)}字符)\n")
            else:
                f.write(data_str)
        else:
            f.write("数据样例: 表为空或无法读取数据\n")

        f.write("\n" * 2)

    def export_important_tables(self):
        """导出重要的表（按前缀筛选）"""
        print("开始导出数据库表信息...")

        # 测试连接
        if not self.test_connection():
            return

        # 获取所有表
        tables = self.get_all_tables()
        if not tables:
            print("错误：数据库中没有找到任何表")
            return

        # 按重要性筛选表（先导出关键表）
        important_prefixes = ['ods_', 'dwd_', 'dmart_', 'dwt_']
        important_tables = []
        other_tables = []

        for table in tables:
            is_important = False
            for prefix in important_prefixes:
                if table.startswith(prefix):
                    important_tables.append(table)
                    is_important = True
                    break
            if not is_important:
                other_tables.append(table)

        print(f"找到 {len(tables)} 张表，其中:")
        print(f"  重要表（ods/dwd/dmart）: {len(important_tables)} 张")
        print(f"  其他表: {len(other_tables)} 张")

        # 询问用户要导出哪些表
        print("\n导出选项:")
        print("1. 只导出重要表（ods/dwd/dmart开头）")
        print("2. 导出所有表")
        print("3. 导出指定前缀的表")

        choice = input("请选择 (1/2/3, 默认1): ").strip()

        if choice == '2':
            tables_to_export = important_tables + other_tables
        elif choice == '3':
            prefix = input("请输入表前缀 (如 ods_): ").strip()
            tables_to_export = [t for t in tables if t.startswith(prefix)]
            if not tables_to_export:
                print(f"没有以 {prefix} 开头的表")
                return
        else:  # 默认选择1
            tables_to_export = important_tables

        print(f"\n开始导出 {len(tables_to_export)} 张表...")

        # 注意：self.output_file 是Path对象，open时会自动转换为字符串路径，兼容Python内置open函数
        with open(self.output_file, 'w', encoding='utf-8') as f:
            # 写入文件头
            f.write("QUANT数据库表结构及数据样例报告（完整版）\n")
            f.write("=" * 100 + "\n")
            f.write(f"生成时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
            f.write(f"数据库: {self.database} @ {self.host}\n")
            f.write(f"总表数: {len(tables)}\n")
            f.write(f"本次导出表数: {len(tables_to_export)}\n")
            f.write("=" * 100 + "\n\n")

            # 表目录
            f.write("导出表目录:\n")
            for i, table in enumerate(tables_to_export, 1):
                f.write(f"{i:3d}. {table}\n")
            f.write("\n" + "=" * 100 + "\n\n")

            # 按前缀分组导出
            table_groups = {}
            for table in tables_to_export:
                if '_' in table:
                    prefix = table.split('_')[0]
                else:
                    prefix = '其他'
                if prefix not in table_groups:
                    table_groups[prefix] = []
                table_groups[prefix].append(table)

            # 导出每个表
            total_exported = 0
            for prefix in sorted(table_groups.keys()):
                f.write(f"\n【{prefix.upper()}层】({len(table_groups[prefix])}张表)\n")
                f.write("=" * 80 + "\n\n")

                group_tables = sorted(table_groups[prefix])
                for i, table in enumerate(group_tables, 1):
                    print(f"处理: {table} ({total_exported + 1}/{len(tables_to_export)})")

                    try:
                        # 获取表信息
                        table_info = self.get_table_info(table)

                        if table_info:
                            # 写入文件
                            self.write_table_info(f, table_info, total_exported + 1, len(tables_to_export))
                            total_exported += 1

                    except Exception as e:
                        f.write(f"处理表 {table} 时出错: {str(e)[:100]}...\n\n")
                    print(f"  完成")

        # 完成提示（优化：显示完整的输出文件路径）
        if self.output_file.exists():  # Path对象直接调用exists()，比os.path.exists更优雅
            file_size = self.output_file.stat().st_size / 1024  # KB，Path对象直接获取文件信息
            print("\n" + "=" * 60)
            print("导出完成！")
            print("=" * 60)
            print(f"输出文件: {self.output_file}")
            print(f"文件大小: {file_size:.1f} KB")
            print(f"导出表数: {total_exported}/{len(tables_to_export)}")
            print("=" * 60)

            # 显示文件内容建议
            print("\n文件内容包含:")
            print("1. 完整的表结构（所有字段、类型、可空、默认值等）")
            print("2. 完整的数据样例（所有列，最多5行）")
            print("3. 每个表的基本信息（行数、列数）")

            if file_size > 200:
                print(f"\n⚠️  文件较大 ({file_size:.1f}KB)，建议:")
                print("1. 用Notepad++或VSCode打开查看")
                print("2. 可以分多次发送内容")
                print("3. 或压缩后发送文件")
            else:
                print(f"\n✓ 文件大小合适 ({file_size:.1f}KB)，可直接复制粘贴")

            # 显示文件头
            print("\n文件开头预览:")
            print("-" * 60)
            try:
                with open(self.output_file, 'r', encoding='utf-8') as f:
                    lines = []
                    for i in range(50):  # 显示前50行
                        line = f.readline()
                        if not line:
                            break
                        lines.append(line.rstrip())

                    for line in lines[:30]:  # 只显示前30行避免太长
                        if len(line) > 100:
                            print(line[:97] + "...")
                        else:
                            print(line)

                    if len(lines) > 30:
                        print("... (还有更多内容)")
            except Exception as e:
                print(f"预览失败: {str(e)}")

            print("\n" + "=" * 60)
            print("操作说明:")
            print("1. 打开文件，复制需要的内容发送给我")
            print("2. 重要表优先：ods_*, dwd_*, dmart_*")
            print("=" * 60)
        else:
            print("错误：文件未生成")


def main():
    """主函数"""
    print("QUANT数据库表结构导出工具（完整版）")
    print("=" * 60)
    print("本工具将导出完整的表结构和数据")
    print("=" * 60)

    # 创建导出器
    try:
        exporter = TableDataExporterFull()
        # 导出表
        exporter.export_important_tables()
    except Exception as e:
        print(f"\n❌ 程序运行失败: {str(e)}")


if __name__ == "__main__":
    main()