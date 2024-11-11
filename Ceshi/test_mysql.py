
import pandas as pd
from sqlalchemy import create_engine
import time
import pymysql

import mysql.connector
from mysql.connector import Error

def merge_stock_kline():
    """
    将 stock_kline 的历史数据和当月数据做merge
    :return:
     stock_kline_df  [ymd	htsc_code	name	exchange]
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
    host = "49.4.94.223"
    user = "root"
    password = "000000"
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








if __name__ == '__main__':
    test_Origin_Mysql()





