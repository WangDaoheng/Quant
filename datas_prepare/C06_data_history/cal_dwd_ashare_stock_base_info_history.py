import pandas as pd
import numpy as np
from sqlalchemy import create_engine, text
import CommonProperties.Base_Properties as base_properties
import time
import os
from datetime import datetime


def calculate_base_info():
    """计算股票基础信息并分批插入dwd_ashare_stock_base_info"""

    print("=" * 50)
    print("开始计算股票基础信息...")
    print("=" * 50)
    start_time = time.time()

    # 数据库连接配置
    origin_user = base_properties.origin_mysql_user
    origin_password = base_properties.origin_mysql_password
    origin_database = base_properties.origin_mysql_database
    origin_host = base_properties.origin_mysql_host

    engine = create_engine(f'mysql+pymysql://{origin_user}:{origin_password}@{origin_host}/{origin_database}')

    # 1. 获取所有需要计算的日期（从ods_stock_kline_daily_ts）
    print("正在获取交易日历...")
    trading_days = pd.read_sql("""
        SELECT DISTINCT ymd 
        FROM ods_stock_kline_daily_ts 
        WHERE ymd >= '2021-01-01'
        ORDER BY ymd
    """, engine)

    dates = trading_days['ymd'].tolist()
    print(f"需要计算的交易日数量: {len(dates)}")

    # 2. 预加载一些不经常变动的数据
    print("正在加载静态数据...")

    # 股票名称（取最新）
    stock_names = pd.read_sql("""
        SELECT stock_code, stock_name 
        FROM ods_stock_code_daily_insight 
        WHERE ymd = (SELECT MAX(ymd) FROM ods_stock_code_daily_insight)
    """, engine)
    stock_names_dict = dict(zip(stock_names['stock_code'], stock_names['stock_name']))

    # 市场分类（取最新）
    market_info = pd.read_sql("""
        SELECT stock_code, market 
        FROM ods_stock_exchange_market 
        WHERE ymd = (SELECT MAX(ymd) FROM ods_stock_exchange_market)
    """, engine)
    market_dict = dict(zip(market_info['stock_code'], market_info['market']))

    # 板块信息（取最新）
    plate_info = pd.read_sql("""
        SELECT 
            stock_code,
            GROUP_CONCAT(board_name ORDER BY board_name SEPARATOR ',') AS plate_names
        FROM dwd_stock_a_total_plate
        WHERE ymd = (SELECT MAX(ymd) FROM dwd_stock_a_total_plate)
        GROUP BY stock_code
    """, engine)
    plate_dict = {}
    for _, row in plate_info.iterrows():
        # 处理股票代码后缀
        pure_code = row['stock_code'].split('.')[0] if '.' in row['stock_code'] else row['stock_code']
        plate_dict[pure_code] = row['plate_names']

    # 3. 清空目标表
    print("\n清空目标表...")
    with engine.connect() as conn:
        conn.execute(text("TRUNCATE TABLE dwd_ashare_stock_base_info"))
        conn.commit()
    print("目标表已清空")

    # 4. 按日期分批处理
    print("\n开始分批处理数据...")
    total_processed = 0
    batch_size = 50  # 每批处理50天

    for i in range(0, len(dates), batch_size):
        batch_dates = dates[i:i + batch_size]
        date_list = "','".join([str(d) for d in batch_dates])

        print(f"\n处理第 {i // batch_size + 1} 批，日期范围: {batch_dates[0]} 到 {batch_dates[-1]}")
        batch_start = time.time()

        # 4.1 获取这一批的K线数据
        kline_df = pd.read_sql(f"""
            SELECT 
                stock_code,
                ymd,
                close,
                change_pct,
                volume,
                trading_amount
            FROM ods_stock_kline_daily_ts
            WHERE ymd IN ('{date_list}')
        """, engine)

        if len(kline_df) == 0:
            continue

        # 4.2 获取这一批的市值数据
        value_df = pd.read_sql(f"""
            SELECT 
                stock_code,
                ymd,
                total_market,
                circulation_market,
                total_shares,
                circulation_shares,
                pe_ttm,
                pb,
                peg
            FROM ods_akshare_stock_value_em
            WHERE ymd IN ('{date_list}')
        """, engine)

        # 处理市值表的股票代码（去掉后缀）
        if len(value_df) > 0:
            value_df['pure_code'] = value_df['stock_code'].astype(str)
        else:
            value_df = pd.DataFrame(columns=['stock_code', 'ymd', 'total_market', 'circulation_market',
                                             'total_shares', 'circulation_shares', 'pe_ttm', 'pb', 'peg'])
            value_df['pure_code'] = []

        # 4.3 获取这一批的股东数据
        shareholder_df = pd.read_sql(f"""
            SELECT 
                stock_code,
                ymd,
                total_sh,
                pct_of_total_sh
            FROM dwd_shareholder_num_latest
            WHERE ymd IN ('{date_list}')
        """, engine)

        # 4.4 为K线数据添加纯净代码
        kline_df['pure_code'] = kline_df['stock_code'].str.split('.').str[0]

        # 4.5 合并数据
        # 先添加股票名称
        kline_df['stock_name'] = kline_df['stock_code'].map(stock_names_dict)

        # 合并市值数据
        result_df = kline_df.merge(
            value_df[['pure_code', 'ymd', 'total_market', 'circulation_market',
                      'total_shares', 'circulation_shares', 'pe_ttm', 'pb', 'peg']],
            on=['pure_code', 'ymd'],
            how='left'
        )

        # 合并股东数据
        result_df = result_df.merge(
            shareholder_df[['stock_code', 'ymd', 'total_sh', 'pct_of_total_sh']],
            on=['stock_code', 'ymd'],
            how='left'
        )

        # 添加市场信息
        result_df['market'] = result_df['stock_code'].map(market_dict)

        # 添加板块信息（用纯净代码匹配）
        result_df['plate_names'] = result_df['pure_code'].map(plate_dict)

        # 4.6 填充空值
        fill_cols = ['total_market', 'circulation_market', 'total_shares',
                     'circulation_shares', 'pb', 'pe_ttm']
        for col in fill_cols:
            result_df[col] = result_df[col].fillna(0)

        # 4.7 重命名列
        result_df = result_df.rename(columns={
            'circulation_market': 'market_value',
            'total_market': 'total_value',
            'total_shares': 'total_capital',
            'circulation_shares': 'float_capital',
            'total_sh': 'shareholder_num',
            'pe_ttm': 'pe'
        })

        # 4.8 选择需要的列
        final_df = result_df[[
            'ymd', 'stock_code', 'stock_name', 'close', 'change_pct',
            'volume', 'trading_amount', 'market_value', 'total_value',
            'total_capital', 'float_capital', 'shareholder_num', 'pct_of_total_sh',
            'pb', 'pe', 'market', 'plate_names'
        ]]

        # 4.9 分批写入（每批5000条）
        write_batch_size = 5000
        for j in range(0, len(final_df), write_batch_size):
            write_batch = final_df.iloc[j:j + write_batch_size]
            write_batch.to_sql('dwd_ashare_stock_base_info',
                               engine,
                               if_exists='append',
                               index=False,
                               method='multi',
                               chunksize=500)

        total_processed += len(final_df)
        batch_time = time.time() - batch_start
        print(f"  本批处理 {len(final_df)} 条，累计 {total_processed} 条，耗时: {batch_time:.2f}秒")

    print(f"\n✨ 全部完成！总处理记录数: {total_processed}")
    print(f"总耗时: {time.time() - start_time:.2f}秒")

    # 5. 验证
    print("\n验证数据...")
    verify_df = pd.read_sql("""
        SELECT 
            COUNT(*) as total,
            COUNT(DISTINCT ymd) as days,
            COUNT(DISTINCT stock_code) as stocks
        FROM dwd_ashare_stock_base_info
    """, engine)

    print(f"总记录数: {verify_df['total'].iloc[0]}")
    print(f"交易日数: {verify_df['days'].iloc[0]}")
    print(f"股票数: {verify_df['stocks'].iloc[0]}")


if __name__ == "__main__":
    calculate_base_info()