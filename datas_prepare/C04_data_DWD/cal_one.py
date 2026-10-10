# -*- coding: utf-8 -*-
"""
排查脚本：验证 000725.SZ 在 20260617 的涨停判断问题
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import CommonProperties.Base_Properties as base_properties
import CommonProperties.Mysql_Utils as mysql_utils

# 数据库配置
origin_user = base_properties.origin_mysql_user
origin_password = base_properties.origin_mysql_password
origin_host = base_properties.origin_mysql_host
origin_database = base_properties.origin_mysql_database


def debug_000725():
    target_stock = '000725.SZ'
    target_date = '20260617'

    print("=" * 60)
    print(f"排查对象: {target_stock} 在 {target_date} 的涨停判断")
    print("=" * 60)

    # 1. 读取K线数据（往前多查10天确保有昨收）
    print("\n步骤1：读取K线数据（含前10天）")
    query_start = (datetime.strptime(target_date, '%Y%m%d') - timedelta(days=10)).strftime('%Y%m%d')

    df = mysql_utils.data_from_mysql_to_dataframe(
        user=origin_user, password=origin_password, host=origin_host,
        database=origin_database,
        table_name='ods_stock_kline_daily_ts',
        start_date=query_start, end_date='20260620',
        cols=['ymd', 'stock_code', 'open', 'high', 'low', 'close', 'volume']
    )

    # 过滤目标股票
    stock_df = df[df['stock_code'] == target_stock].copy()
    stock_df = stock_df.sort_values('ymd')

    print(f"\n{target_stock} 原始数据：")
    print(stock_df[['ymd', 'open', 'high', 'low', 'close', 'volume']].to_string())

    # 2. 检查 ymd 格式
    print("\n" + "=" * 60)
    print("步骤2：检查 ymd 格式")
    print(f"ymd dtype: {stock_df['ymd'].dtype}")
    print(f"ymd 样例: {stock_df['ymd'].head(3).tolist()}")

    # 3. 计算昨收（shift方法）
    print("\n" + "=" * 60)
    print("步骤3：计算昨收价（shift方法）")
    stock_df['last_close'] = stock_df['close'].shift(1)

    print(f"\n计算后数据：")
    print(stock_df[['ymd', 'close', 'last_close']].to_string())

    # 4. 检查目标日期的数据
    print("\n" + "=" * 60)
    print(f"步骤4：检查 {target_date} 的具体数据")

    # 统一 ymd 格式为字符串比较
    stock_df['ymd_str'] = stock_df['ymd'].astype(str).str.replace('-', '').str[:8]
    target_row = stock_df[stock_df['ymd_str'] == target_date]

    if target_row.empty:
        print(f"错误：找不到 {target_date} 的数据！")
        return

    target_row = target_row.iloc[0]
    print(f"  日期: {target_row['ymd']}")
    print(f"  开盘: {target_row['open']}")
    print(f"  最高: {target_row['high']}")
    print(f"  最低: {target_row['low']}")
    print(f"  收盘: {target_row['close']}")
    print(f"  昨收: {target_row['last_close']}")

    # 5. 计算涨停价
    print("\n" + "=" * 60)
    print("步骤5：计算涨停价")

    if pd.isna(target_row['last_close']):
        print("错误：昨收为 NaN，无法计算涨停价！")
        print("原因：shift(1) 取不到前一条记录")
        return

    last_close = float(target_row['last_close'])
    close = float(target_row['close'])

    # 000725 是深市主板，10% 涨停
    zt_price = round(last_close * 1.1, 2)

    print(f"  昨收(last_close): {last_close}")
    print(f"  计算过程: {last_close} * 1.1 = {last_close * 1.1}")
    print(f"  涨停价(round2): {zt_price}")
    print(f"  实际收盘: {close}")
    print(f"  是否涨停(close >= zt_price): {close >= zt_price}")

    # 6. 检查新表是否有记录
    print("\n" + "=" * 60)
    print("步骤6：检查新表 dwd_stock_zt_list_v2")

    new_table = mysql_utils.data_from_mysql_to_dataframe(
        user=origin_user, password=origin_password, host=origin_host,
        database=origin_database,
        table_name='dwd_stock_zt_list_v2',
        start_date=target_date, end_date=target_date,
        cols=['ymd', 'stock_code', 'close', 'last_close', 'zt_price', 'zt_type']
    )

    stock_in_new = new_table[new_table['stock_code'] == target_stock]
    if stock_in_new.empty:
        print(f"  新表无 {target_stock} 记录")
    else:
        print(f"  新表记录: {stock_in_new.to_string()}")

    # 7. 检查旧表是否有记录
    print("\n" + "=" * 60)
    print("步骤7：检查旧表 dwd_stock_zt_list")

    old_table = mysql_utils.data_from_mysql_to_dataframe(
        user=origin_user, password=origin_password, host=origin_host,
        database=origin_database,
        table_name='dwd_stock_zt_list',
        start_date=target_date, end_date=target_date,
        cols=['ymd', 'stock_code', 'close', 'last_close', 'rate']
    )

    stock_in_old = old_table[old_table['stock_code'] == target_stock]
    if stock_in_old.empty:
        print(f"  旧表无 {target_stock} 记录")
    else:
        print(f"  旧表记录: {stock_in_old.to_string()}")

    # 8. 模拟完整计算流程
    print("\n" + "=" * 60)
    print("步骤8：模拟完整计算流程（含日期前移）")

    # 模拟正确的查询方式（往前多查10天）
    df_full = mysql_utils.data_from_mysql_to_dataframe(
        user=origin_user, password=origin_password, host=origin_host,
        database=origin_database,
        table_name='ods_stock_kline_daily_ts',
        start_date='20260601', end_date='20260620',  # 从月初开始查
        cols=['ymd', 'stock_code', 'open', 'high', 'low', 'close', 'volume']
    )

    stock_full = df_full[df_full['stock_code'] == target_stock].copy()
    stock_full = stock_full.sort_values('ymd')
    stock_full['last_close'] = stock_full['close'].shift(1)
    stock_full['zt_price'] = (stock_full['last_close'] * 1.1).round(2)
    stock_full['is_zt'] = stock_full['close'] >= stock_full['zt_price']

    print(f"\n从20260601开始查询的数据：")
    print(stock_full[['ymd', 'close', 'last_close', 'zt_price', 'is_zt']].to_string())

    # 检查20260617的判断结果
    check_row = stock_full[stock_full['ymd'].astype(str).str.replace('-', '').str[:8] == target_date]
    if not check_row.empty:
        print(f"\n{target_date} 判断结果: is_zt = {check_row.iloc[0]['is_zt']}")
    else:
        print(f"\n{target_date} 不在查询结果中！")


if __name__ == '__main__':
    debug_000725()
