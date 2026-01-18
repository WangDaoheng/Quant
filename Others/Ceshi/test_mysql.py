
import pandas as pd
from sqlalchemy import create_engine
import sys
import mysql.connector
from mysql.connector import Error
import time
import pymysql

# import mysql.connector
# from mysql.connector import Error

def merge_stock_kline():
    """
    将 stock_kline 的历史数据和当月数据做merge
    :return:
     stock_kline_df  [ymd	stock_code	name	exchange]
    """

    #  读取历史数据和当下数据
    stock_kline_latest_file = r'F:\QDatas\insight_A\stock_kline\stock_kline_2024080620.csv'
    # stock_kline_history_latest_file = base_utils.get_latest_filename(self.dir_history_stock_kline_base)


    now_df = pd.read_csv(stock_kline_latest_file)

    # 设定 'time' 为索引，以便于数据合并
    now_df.set_index('time', inplace=True)


    # MySQL 数据库连接配置
    db_url = 'mysql+pymysql://root:123456@localhost:3306/quant'
    engine = create_engine(db_url)
    # 将结果写入 MySQL 数据库
    now_df.to_sql(name='stock_kline_daily_insight', con=engine, if_exists='replace', index=False)

    #  文件输出模块
    # kline_total_filename = base_utils.save_out_filename(filehead='stock_kline_latest', file_type='csv')
    # kline_total_filedir = os.path.join(self.dir_merge_stock_kline_base, kline_total_filename)
    # combined_df.to_csv(kline_total_filedir, index=False)


def test_Origin_Mysql():
    """
    测试远程服务器的 mysql 是否联通
    Returns:
    """

    host = "117.72.162.13"
    user = "root"
    password = "WZHwzh123!!!"
    database = "quant"

    try:
        # 连接到MySQL服务器
        connection = mysql.connector.connect(
            host=host,
            user=user,
            password=password,
            database=database
        )

        if connection.is_connected():
            print("成功连接到MySQL服务器")

            # 创建一个游标对象
            cursor = connection.cursor()

            # 执行 SQL 语句
            cursor.execute("SHOW DATABASES")

            # 获取结果
            databases = cursor.fetchall()
            print("可用的数据库:")
            for db in databases:
                print(db[0])

    except Error as e:
        print(f"连接错误: {e}")

    finally:
        if (connection.is_connected()):
            cursor.close()
            connection.close()
            print("MySQL连接已关闭")


def CAL_mysql_table_ymds():
    """
    测试远程服务器的 mysql 是否联通，并统计 quant 库各表的 ymd 日期
    Returns:
    """
    host = "117.72.162.13"
    user = "root"
    password = "WZHwzh123!!!"
    database = "quant"
    connection = None  # 初始化连接，避免作用域问题
    cursor = None      # 初始化游标，避免finally中报错

    try:
        # 1. 连接到 MySQL 服务器（指定数据库为 quant）
        connection = mysql.connector.connect(
            host=host,
            user=user,
            password=password,
            database=database,
            charset="utf8"  # 避免中文乱码（可选但推荐）
        )

        if connection.is_connected():
            print(f"✅ 成功连接到 {database} 数据库\n")
            cursor = connection.cursor()

            # 2. 第一步：获取 quant 库中所有表名
            # INFORMATION_SCHEMA.TABLES 是MySQL系统表，存储所有库的表信息
            cursor.execute(f"SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = '{database}'")
            tables = cursor.fetchall()  # 结果是列表，每个元素是 (表名,) 的元组

            if not tables:
                print(f"❌ {database} 库中没有找到任何表")
                return

            # 3. 第二步：逐个表查询 ymd 字段的所有日期（去重）
            for table in tables:
                table_name = table[0]  # 提取表名（从元组中取第一个元素）
                print(f"=== 正在查询表：{table_name} ===")

                try:
                    # 执行查询：去重获取当前表的所有 ymd 日期，按日期升序排列
                    # 注意：如果表中没有 ymd 字段，会触发异常，需要捕获
                    cursor.execute(f"SELECT DISTINCT ymd FROM {table_name} ORDER BY ymd ASC")
                    ymd_dates = cursor.fetchall()  # 结果是 (ymd日期,) 的元组列表

                    if not ymd_dates:
                        print(f"该表没有 ymd 数据\n")
                        continue

                    # 格式化输出日期（去掉元组括号，用逗号分隔）
                    date_list = [str(date[0]) for date in ymd_dates]
                    print(f"ymd 日期总数：{len(date_list)}")
                    print(f"ymd 日期列表：{', '.join(date_list)}\n")

                except Error as table_err:
                    # 捕获“表中没有ymd字段”或其他表级错误，不中断整体流程
                    print(f"❌ 查询该表失败：{str(table_err)}\n")
                    continue

    except Error as conn_err:
        print(f"❌ 数据库连接失败：{str(conn_err)}")

    finally:
        # 安全关闭游标和连接（避免资源泄漏）
        if cursor:
            cursor.close()
        if connection and connection.is_connected():
            connection.close()
            print("🔌 MySQL 连接已关闭")




if __name__ == '__main__':
    # test_Origin_Mysql()
    CAL_mysql_table_ymds()
    # print("hello world")
    # print(sys.version)




