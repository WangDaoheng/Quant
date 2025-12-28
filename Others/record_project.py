import os
import sys
from pathlib import Path
import datetime


class CodeOrganizer:
    """代码梳理工具，用于遍历项目目录并整理代码结构和内容"""

    def __init__(self, root_dir, output_path):
        """
        初始化代码梳理工具

        Args:
            root_dir: 项目根目录（绝对/相对路径）
            output_path: 输出文件的完整路径（支持自动创建目录）
        """
        self.root_dir = Path(root_dir).absolute()
        self.output_path = Path(output_path).absolute()  # 转为绝对路径
        self.exclude_dirs = {'.git', '__pycache__', '.idea', 'venv', 'env'}
        self.exclude_files = {'.gitignore', '.DS_Store', '.env', '*.pyc', '*.pyo', '*.pyd'}
        self.file_types = {'.py', '.sql', '.sh', '.md', '.txt'}  # 需要梳理的文件类型

        # 自动创建输出目录（如果不存在）
        self._create_output_dir()

    def _create_output_dir(self):
        """自动创建输出文件所在的目录"""
        output_dir = self.output_path.parent  # 获取输出文件的上级目录
        if not output_dir.exists():
            output_dir.mkdir(parents=True, exist_ok=True)
            print(f"📁 自动创建输出目录: {output_dir}")

    def should_include(self, path):
        """判断文件/目录是否应该被包含"""
        # 排除指定目录
        for exclude_dir in self.exclude_dirs:
            if exclude_dir in path.parts:
                return False

        # 如果是目录，继续遍历
        if path.is_dir():
            return True

        # 排除指定文件
        for exclude_pattern in self.exclude_files:
            if path.match(exclude_pattern):
                return False

        # 只包含指定类型的文件
        if path.suffix in self.file_types:
            return True

        return False

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

    def generate_directory_tree(self):
        """生成目录树结构"""
        tree = []
        tree.append(f"# {self.root_dir.name} 项目目录结构")
        tree.append(f"*生成时间: {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}*")
        tree.append("")

        for root, dirs, files in os.walk(self.root_dir):
            # 排除不需要的目录
            dirs[:] = [d for d in dirs if not self.should_include(Path(root) / d)]

            # 计算层级
            level = len(Path(root).relative_to(self.root_dir).parts)
            indent = "    " * level

            # 添加目录名
            dir_name = Path(root).name
            if level == 0:
                tree.append(f"📁 {dir_name}/")
            else:
                tree.append(f"{indent}📁 {dir_name}/")

            # 添加文件
            sub_indent = "    " * (level + 1)
            for file in sorted(files):
                file_path = Path(root) / file
                if self.should_include(file_path):
                    tree.append(f"{sub_indent}📄 {file}")

        return "\n".join(tree)

    def generate_code_content(self):
        """生成所有代码文件的内容"""
        content = []
        content.append("# 项目代码内容")
        content.append("")

        # 遍历所有文件
        for root, dirs, files in os.walk(self.root_dir):
            # 排除不需要的目录
            dirs[:] = [d for d in dirs if not self.should_include(Path(root) / d)]

            # 处理文件
            for file in sorted(files):
                file_path = Path(root) / file
                if self.should_include(file_path):
                    # 相对路径
                    rel_path = file_path.relative_to(self.root_dir)

                    # 添加文件分隔符
                    content.append("-" * 80)
                    content.append(f"## {rel_path}")
                    content.append("")

                    # 添加文件内容
                    file_content = self.get_file_content(file_path)
                    # 根据文件类型添加语法高亮
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

        return "\n".join(content)

    def generate_summary(self):
        """生成完整的代码梳理文档"""
        # 统计信息
        stats = self.get_project_stats()

        # 拼接所有内容
        full_content = []
        full_content.append(f"# 量化工程V1.0 代码梳理文档")
        full_content.append(f"*生成时间: {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}*")
        full_content.append("")

        # 添加统计信息
        full_content.append("## 项目统计信息")
        full_content.append(f"- 项目根目录: {self.root_dir}")
        full_content.append(f"- 总文件数: {stats['total_files']}")
        full_content.append(f"- Python文件数: {stats['py_files']}")
        full_content.append(f"- SQL文件数: {stats['sql_files']}")
        full_content.append(f"- Shell文件数: {stats['sh_files']}")
        full_content.append(f"- 目录数: {stats['total_dirs']}")
        full_content.append("")

        # 添加目录树
        full_content.append(self.generate_directory_tree())
        full_content.append("")

        # 添加代码内容
        full_content.append(self.generate_code_content())

        return "\n".join(full_content)

    def get_project_stats(self):
        """获取项目统计信息"""
        stats = {
            'total_files': 0,
            'py_files': 0,
            'sql_files': 0,
            'sh_files': 0,
            'total_dirs': 0
        }

        for root, dirs, files in os.walk(self.root_dir):
            # 排除不需要的目录
            dirs[:] = [d for d in dirs if not self.should_include(Path(root) / d)]

            # 统计目录
            stats['total_dirs'] += 1

            # 统计文件
            for file in files:
                file_path = Path(root) / file
                if self.should_include(file_path):
                    stats['total_files'] += 1
                    if file_path.suffix == '.py':
                        stats['py_files'] += 1
                    elif file_path.suffix == '.sql':
                        stats['sql_files'] += 1
                    elif file_path.suffix == '.sh':
                        stats['sh_files'] += 1

        return stats

    def save_summary(self):
        """保存梳理文档到文件"""
        summary = self.generate_summary()

        # 保存到文件
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
        print(f"   - 目录数: {stats['total_dirs']}")


def main():
    """主函数"""
    # ====================== 核心配置区（按需修改）======================
    # 1. 项目根目录（你的Quant工程路径）
    project_root = "./Quant"  # 相对路径 | 或绝对路径："/Users/xxx/Projects/Quant"

    # 2. 输出文件路径（支持自定义目录，脚本会自动创建不存在的目录）
    # 示例1：输出到项目目录下的docs子目录
    output_path = "./Quant/docs/quant_project_summary.md"

    # 示例2：输出到桌面（绝对路径）
    # output_path = "/Users/xxx/Desktop/量化工程代码梳理.md"

    # 示例3：输出到当前脚本目录的output子目录
    # output_path = "./output/quant_summary.md"
    # ==================================================================

    # 检查项目目录是否存在
    if not os.path.exists(project_root):
        print(f"❌ 错误：项目目录 {project_root} 不存在！")
        print(f"   请修改脚本中的 project_root 变量为正确的路径。")
        sys.exit(1)

    # 创建代码梳理工具实例
    organizer = CodeOrganizer(
        root_dir=project_root,
        output_path=output_path  # 传入完整的输出路径（含文件名）
    )

    # 生成并保存梳理文档
    organizer.save_summary()


if __name__ == "__main__":
    main()