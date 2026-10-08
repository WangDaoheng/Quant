import pymysql
import CommonProperties.Base_Properties as Base_Properties
from datetime import datetime
import os  # 新增


def export_quant_ddl():
    """快速导出 quant 库所有建表语句"""

    # 连接数据库
    conn = pymysql.connect(
        host=Base_Properties.origin_mysql_host,
        user=Base_Properties.origin_mysql_user,
        password=Base_Properties.origin_mysql_password,
        database=Base_Properties.origin_mysql_database,
        charset='utf8mb4'
    )
    cursor = conn.cursor()

    # 获取所有表
    cursor.execute(f"""
        SELECT table_name 
        FROM information_schema.tables 
        WHERE table_schema = '{Base_Properties.origin_mysql_database}'
        ORDER BY table_name
    """)
    tables = [row[0] for row in cursor.fetchall()]

    # ========== 修改这里：使用脚本所在目录 ==========
    script_dir = os.path.dirname(os.path.abspath(__file__))
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_file = os.path.join(script_dir, f"quant_ddl_{timestamp}.sql")
    # ================================================

    with open(output_file, 'w', encoding='utf-8') as f:
        f.write(f"-- Database: {Base_Properties.origin_mysql_database}\n")
        f.write(f"-- Export Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
        f.write(f"-- Total Tables: {len(tables)}\n\n")

        for table in tables:
            cursor.execute(f"SHOW CREATE TABLE `{table}`")
            ddl = cursor.fetchone()[1]

            f.write(f"-- Table: {table}\n")
            f.write(f"DROP TABLE IF EXISTS `{table}`;\n")
            f.write(ddl)
            f.write(";\n\n")
            print(f"✅ {table}")

    cursor.close()
    conn.close()

    print(f"\n🎉 导出完成: {output_file}")
    print(f"📊 共导出 {len(tables)} 张表")
    return output_file


if __name__ == "__main__":
    export_quant_ddl()
