import os
import sys
from pathlib import Path
import datetime


class CodeOrganizer:
    """代码梳理工具，用于遍历项目目录并整理代码结构和内容"""

    def __init__(self, root_dir, output_path, selected_dirs=None):
        """
        初始化代码梳理工具

        Args:
            root_dir: 项目根目录（绝对/相对路径）
            output_path: 输出文件的完整路径（支持自动创建目录）
            selected_dirs: 选择的目录列表，None表示全部输出
        """
        # 强制解析为绝对路径，避免相对路径歧义
        self.root_dir = Path(root_dir).resolve()
        self.output_path = Path(output_path).resolve()
        self.selected_dirs = selected_dirs  # 选择的目录列表

        # 核心排除目录：重点标记 Others
        self.exclude_dir_names = {
            '.git', '__pycache__', '.idea', 'venv', 'env', 'Others',
            'objects', 'hooks', 'info', 'logs', 'refs', 'pack'  # Git子目录
        }
        # 额外排除路径包含这些关键词的目录（彻底过滤 Others 相关）
        self.exclude_dir_keywords = ['Others']
        self.exclude_files = {'.gitignore', '.DS_Store', '.env', '*.pyc', '*.pyo', '*.pyd'}
        # 明确需要梳理的文件类型
        self.file_types = {'.py', '.sql', '.sh', '.md', '.txt', '.yml', '.ini', '.bash'}

        # 自动创建输出目录
        self._create_output_dir()

    def _create_output_dir(self):
        """自动创建输出文件所在的目录"""
        output_dir = self.output_path.parent
        if not output_dir.exists():
            output_dir.mkdir(parents=True, exist_ok=True)
            print(f"📁 自动创建输出目录: {output_dir}")

    def should_include_dir(self, dir_path):
        """彻底排除 Others 目录：双重校验"""
        # 1. 校验目录名是否在排除列表
        dir_name = dir_path.name.lower()
        if dir_name in [name.lower() for name in self.exclude_dir_names]:
            return False

        # 2. 校验路径中是否包含 Others（彻底过滤所有层级的 Others）
        for keyword in self.exclude_dir_keywords:
            if keyword in dir_path.parts:
                return False

        # 3. 过滤 .git 所有子目录
        if '.git' in dir_path.parts:
            return False

        return True

    def should_include_file(self, file_path):
        """判断文件是否需要包含"""
        # 排除指定文件
        for exclude_pattern in self.exclude_files:
            if file_path.match(exclude_pattern):
                return False

        # 排除 Others 目录下的所有文件
        for keyword in self.exclude_dir_keywords:
            if keyword in file_path.parts:
                return False

        # 只包含指定类型的文件
        if file_path.suffix in self.file_types:
            return True
        return False

    def is_in_selected_dirs(self, dir_path):
        """检查目录是否在选中的目录范围内"""
        if self.selected_dirs is None:
            return True  # 未选择时包含所有目录

        # 获取相对于根目录的路径
        rel_path = dir_path.relative_to(self.root_dir)

        # 如果是根目录或空路径，包含
        if len(rel_path.parts) == 0:
            return True

        # 特殊规则：无论是否选择 datas_prepare，都包含其下的 C00_SQL 目录
        if len(rel_path.parts) >= 2:
            # 检查是否是 datas_prepare/C00_SQL 或其子目录
            if rel_path.parts[0] == "datas_prepare" and rel_path.parts[1] == "C00_SQL":
                return True

        # 检查一级目录是否在选中列表中
        first_level_dir = rel_path.parts[0]
        return first_level_dir in self.selected_dirs

    def should_show_in_tree(self, dir_path):
        """检查目录是否应该在目录树中显示（稍微宽松的规则）"""
        if self.selected_dirs is None:
            return True

        # 获取相对于根目录的路径
        rel_path = dir_path.relative_to(self.root_dir)

        # 如果是根目录或空路径，显示
        if len(rel_path.parts) == 0:
            return True

        # 特殊规则：总是显示 datas_prepare/C00_SQL
        if len(rel_path.parts) >= 2:
            if rel_path.parts[0] == "datas_prepare" and rel_path.parts[1] == "C00_SQL":
                return True

        # 如果一级目录在选中列表中，显示其所有子目录
        first_level_dir = rel_path.parts[0]
        return first_level_dir in self.selected_dirs

    def generate_directory_tree(self):
        """生成清晰的目录树（彻底排除 Others）"""
        tree = []
        tree.append(f"# {self.root_dir.name} 项目目录结构")
        tree.append(f"*生成时间: {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}*")

        # 显示选择的目录信息
        if self.selected_dirs:
            tree.append(f"*选中的目录: {', '.join(self.selected_dirs)}*")
        else:
            tree.append("*选中目录: 全部*")
        tree.append("*特殊包含: datas_prepare/C00_SQL 目录（无论是否选中）*")
        tree.append("")

        for root, dirs, files in os.walk(self.root_dir):
            root_path = Path(root)

            # 过滤目录：彻底移除 Others
            dirs[:] = [d for d in dirs if self.should_include_dir(root_path / d)]

            # 检查是否需要包含此目录在目录树中
            if not self.should_show_in_tree(root_path) and root_path != self.root_dir:
                continue

            # 计算层级
            rel_path = root_path.relative_to(self.root_dir)
            level = len(rel_path.parts) if rel_path.name else 0
            indent = "    " * level

            # 跳过 Others 目录本身的显示
            if not self.should_include_dir(root_path):
                continue

            # 添加目录名（如果包含 C00_SQL 则特殊标记）
            dir_name = root_path.name
            if level == 0:
                tree.append(f"📁 {self.root_dir.name}/")
            else:
                # 标记 C00_SQL 目录
                if len(rel_path.parts) >= 2 and rel_path.parts[0] == "datas_prepare" and rel_path.parts[1] == "C00_SQL":
                    tree.append(f"{indent}📁 {dir_name} 🔸")
                else:
                    tree.append(f"{indent}📁 {dir_name}/")

            # 添加文件
            sub_indent = "    " * (level + 1)
            for file in sorted(files):
                file_path = root_path / file
                if self.should_include_file(file_path):
                    # 标记 C00_SQL 中的文件
                    if len(rel_path.parts) >= 2 and rel_path.parts[0] == "datas_prepare" and rel_path.parts[
                        1] == "C00_SQL":
                        tree.append(f"{sub_indent}📄 {file} 🔸")
                    else:
                        tree.append(f"{sub_indent}📄 {file}")

        return "\n".join(tree)

    def generate_code_content(self):
        """生成所有代码文件的内容（排除 Others 下的文件）"""
        content = []
        content.append("# 项目代码内容")

        # 显示选择的目录信息
        if self.selected_dirs:
            content.append(f"*选中的目录: {', '.join(self.selected_dirs)}*")
        else:
            content.append("*选中目录: 全部*")
        content.append("*特殊包含: datas_prepare/C00_SQL 目录（无论是否选中）*")
        content.append("")

        file_count = 0
        for root, dirs, files in os.walk(self.root_dir):
            root_path = Path(root)

            # 过滤目录
            dirs[:] = [d for d in dirs if self.should_include_dir(root_path / d)]

            # 检查是否需要包含此目录
            if not self.is_in_selected_dirs(root_path):
                continue

            # 跳过 Others 目录下的所有文件处理
            if not self.should_include_dir(root_path):
                continue

            # 处理文件
            for file in sorted(files):
                file_path = root_path / file
                if self.should_include_file(file_path):
                    file_count += 1
                    rel_path = file_path.relative_to(self.root_dir)

                    # 添加文件分隔符
                    content.append("-" * 80)

                    # 标记 C00_SQL 中的文件
                    rel_path_str = str(rel_path)
                    if "datas_prepare/C00_SQL" in rel_path_str:
                        content.append(f"## {rel_path} 🔸")
                    else:
                        content.append(f"## {rel_path}")

                    content.append("")

                    # 读取文件内容
                    file_content = self.get_file_content(file_path)
                    # 语法高亮
                    suffix = file_path.suffix.lower()
                    if suffix == '.py':
                        content.append("```python")
                    elif suffix == '.sql':
                        content.append("```sql")
                    elif suffix == '.sh':
                        content.append("```bash")
                    else:
                        content.append("```")

                    content.append(file_content)
                    content.append("```")
                    content.append("")

        if file_count == 0:
            content.append("⚠️ 未找到需要梳理的文件，请检查：")
            content.append("- 项目根目录是否正确")
            content.append("- 文件类型配置（file_types）是否包含目标类型")
            content.append("- 排除规则是否过度过滤")

        return "\n".join(content)

    def get_file_content(self, file_path):
        """读取文件内容，处理编码问题"""
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                return f.read()
        except UnicodeDecodeError:
            try:
                with open(file_path, 'r', encoding='gbk') as f:
                    return f.read()
            except Exception as e:
                return f"[读取失败: {str(e)}]"

    def get_project_stats(self):
        """精准统计（排除 Others）"""
        stats = {
            'total_files': 0,
            'py_files': 0,
            'sql_files': 0,
            'sh_files': 0,
            'total_dirs': 0,
            'special_sql_files': 0  # 特殊包含的 C00_SQL 文件数
        }

        for root, dirs, files in os.walk(self.root_dir):
            root_path = Path(root)

            # 过滤目录
            dirs[:] = [d for d in dirs if self.should_include_dir(root_path / d)]

            # 检查是否需要包含此目录
            if not self.is_in_selected_dirs(root_path):
                continue

            # 跳过 Others 目录的统计
            if not self.should_include_dir(root_path):
                continue

            # 统计目录
            stats['total_dirs'] += 1

            # 统计文件
            for file in files:
                file_path = root_path / file
                if self.should_include_file(file_path):
                    stats['total_files'] += 1

                    # 检查是否是 C00_SQL 中的文件
                    rel_path = file_path.relative_to(self.root_dir)
                    rel_path_str = str(rel_path)
                    if "datas_prepare/C00_SQL" in rel_path_str:
                        stats['special_sql_files'] += 1

                    if file_path.suffix == '.py':
                        stats['py_files'] += 1
                    elif file_path.suffix == '.sql':
                        stats['sql_files'] += 1
                    elif file_path.suffix == '.sh':
                        stats['sh_files'] += 1

        return stats

    def generate_summary(self):
        """生成完整文档"""
        stats = self.get_project_stats()

        full_content = []
        full_content.append(f"# 量化工程V1.0 代码梳理文档")
        full_content.append(f"*生成时间: {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}*")

        # 显示选择的目录信息
        if self.selected_dirs:
            full_content.append(f"*选中的目录: {', '.join(self.selected_dirs)}*")
        else:
            full_content.append("*选中目录: 全部*")
        full_content.append("*特殊包含: datas_prepare/C00_SQL 目录（无论是否选中）*")
        full_content.append("")

        # 统计信息
        full_content.append("## 项目统计信息")
        full_content.append(f"- 项目根目录: {self.root_dir}")
        full_content.append(
            f"- 总文件数: {stats['total_files']} (其中包含 {stats['special_sql_files']} 个 C00_SQL 特殊文件)")
        full_content.append(f"- Python文件数: {stats['py_files']}")
        full_content.append(f"- SQL文件数: {stats['sql_files']}")
        full_content.append(f"- Shell文件数: {stats['sh_files']}")
        full_content.append(f"- 有效目录数: {stats['total_dirs']}")
        full_content.append("")

        # 目录树
        full_content.append(self.generate_directory_tree())
        full_content.append("")

        # 代码内容
        full_content.append(self.generate_code_content())

        return "\n".join(full_content)

    def save_summary(self):
        """保存文档"""
        summary = self.generate_summary()

        with open(self.output_path, 'w', encoding='utf-8') as f:
            f.write(summary)

        print(f"✅ 代码梳理完成！")
        print(f"📄 文档已保存到: {self.output_path}")

        # 打印统计信息
        stats = self.get_project_stats()
        print(f"\n📊 项目统计:")
        print(f"   - 总文件数: {stats['total_files']}")
        print(f"   - Python文件: {stats['py_files']}")
        print(f"   - SQL文件: {stats['sql_files']}")
        print(f"   - Shell文件: {stats['sh_files']}")
        print(f"   - 有效目录数: {stats['total_dirs']}")
        if stats['special_sql_files'] > 0:
            print(f"   - C00_SQL特殊包含: {stats['special_sql_files']} 个文件")


def main():
    """主函数（直接在代码中指定要处理的目录）"""
    # ====================== 动态路径配置 ======================
    current_script_path = Path(__file__).resolve()
    current_script_dir = current_script_path.parent
    project_root = current_script_dir.parent
    output_dir = current_script_dir / "output"
    output_path = output_dir / "quant_project_summary.md"
    # =====================================================

    # ====================== 在这里指定要处理的目录 ======================
    # 方法1：指定特定的目录
    # selected_dirs = ["backtest", "strategy", "CommonProperties"]

    # 方法2：处理所有目录（注释掉selected_dirs即可）
    # selected_dirs = None

    # 方法3：根据你的需要选择组合
    # 组合1：核心策略相关
    # selected_dirs = ["backtest", "strategy", "CommonProperties"]

    # 组合2：数据处理相关
    # selected_dirs = ["datas_prepare", "CommonProperties"]

    # 组合3：监控和仪表板
    # selected_dirs = ["monitor", "dashboard", "review"]

    # 组合4：所有目录（默认）
    selected_dirs = ['CommonProperties']  # 修改这一行即可

    # 示例1：只处理 backtest 和 CommonProperties（但仍会包含 C00_SQL）
    # selected_dirs = ["backtest", "CommonProperties"]

    # 示例2：完全不包含 datas_prepare（但仍会包含 C00_SQL）
    # selected_dirs = ["backtest", "strategy", "monitor"]
    # ===============================================================

    # 检查项目目录是否存在
    if not project_root.exists():
        print(f"❌ 错误：项目目录 {project_root} 不存在！")
        print(f"   当前脚本路径：{current_script_path}")
        sys.exit(1)

    # 创建实例并运行
    organizer = CodeOrganizer(
        root_dir=project_root,
        output_path=output_path,
        selected_dirs=selected_dirs
    )
    organizer.save_summary()


if __name__ == "__main__":
    main()