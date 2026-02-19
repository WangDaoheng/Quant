import pandas as pd
import numpy as np
import pymysql
from sqlalchemy import create_engine
import CommonProperties.Base_Properties as base_properties
import os
from datetime import datetime
import time


def get_trading_days():
    """获取所有交易日（返回列表）- 修复版"""
    conn = pymysql.connect(
        host=base_properties.origin_mysql_host,
        user=base_properties.origin_mysql_user,
        password=base_properties.origin_mysql_password,
        database=base_properties.origin_mysql_database,
        charset='utf8mb4',
        cursorclass=pymysql.cursors.DictCursor
    )

    query = "SELECT ymd FROM ods_trading_days_insight ORDER BY ymd"

    # 使用 cursor.execute 获取真实数据
    cursor = conn.cursor()
    cursor.execute(query)
    rows = cursor.fetchall()

    # 提取真正的 ymd 值
    trading_days = [row['ymd'] for row in rows]

    cursor.close()
    conn.close()

    print(f"交易日数量: {len(trading_days)} 天")
    if trading_days:
        print(f"交易日示例: {trading_days[:3]}")
        print(f"交易日类型: {type(trading_days[0])}")
    return trading_days


def get_valid_stocks():
    """获取有效股票代码（返回列表）- 修复版"""
    conn = pymysql.connect(
        host=base_properties.origin_mysql_host,
        user=base_properties.origin_mysql_user,
        password=base_properties.origin_mysql_password,
        database=base_properties.origin_mysql_database,
        charset='utf8mb4',
        cursorclass=pymysql.cursors.DictCursor
    )

    query = """
    SELECT DISTINCT stock_code 
    FROM ods_stock_code_daily_insight 
    WHERE ymd = (SELECT MAX(ymd) FROM ods_stock_code_daily_insight)
    ORDER BY stock_code
    """

    # 使用 cursor.execute 获取真实数据
    cursor = conn.cursor()
    cursor.execute(query)
    rows = cursor.fetchall()

    # 提取真正的 stock_code 值
    valid_stocks = [row['stock_code'] for row in rows]

    cursor.close()
    conn.close()

    print(f"有效股票代码: {len(valid_stocks)} 只")
    if valid_stocks:
        print(f"股票代码示例: {valid_stocks[:3]}")
    return valid_stocks


def load_shareholder_data_batch(stock_batch):
    """分批加载股东数据"""
    if not stock_batch:
        return pd.DataFrame()

    conn = pymysql.connect(
        host=base_properties.origin_mysql_host,
        user=base_properties.origin_mysql_user,
        password=base_properties.origin_mysql_password,
        database=base_properties.origin_mysql_database,
        charset='utf8mb4',
        cursorclass=pymysql.cursors.DictCursor
    )

    # 构建IN查询
    placeholders = ','.join(['%s'] * len(stock_batch))
    query = f"""
    SELECT 
        ymd,
        stock_code,
        stock_name,
        total_sh,
        avg_share,
        pct_of_total_sh,
        pct_of_avg_sh
    FROM ods_shareholder_num
    WHERE stock_code IN ({placeholders})
    ORDER BY stock_code, ymd
    """

    # 使用 cursor.execute 获取数据
    cursor = conn.cursor()
    cursor.execute(query, stock_batch)
    rows = cursor.fetchall()

    # 转换为 DataFrame
    if rows:
        # 获取列名
        columns = [desc[0] for desc in cursor.description]
        df = pd.DataFrame(rows, columns=columns)
        print(f"  加载股东数据: {len(df)} 条")
        print(f"  股东数据ymd类型: {df['ymd'].dtype}")
        print(f"  股东数据ymd范围: {df['ymd'].min()} 到 {df['ymd'].max()}")
    else:
        df = pd.DataFrame()

    cursor.close()
    conn.close()

    return df


def process_stock_batch(stock_batch, trading_days):
    """处理一批股票"""
    if not stock_batch:
        return pd.DataFrame()

    print(f"\n处理 {len(stock_batch)} 只股票...")
    batch_start = time.time()

    # 1. 加载这批股票的股东数据
    shareholder_df = load_shareholder_data_batch(stock_batch)

    # 2. 生成这批股票的网格数据（交易日 × 股票）
    grid_data = []
    for stock_code in stock_batch:
        for ymd in trading_days:
            grid_data.append({
                'ymd': ymd,
                'stock_code': stock_code
            })

    grid_df = pd.DataFrame(grid_data)
    print(f"  生成网格: {len(grid_df)} 行")
    print(f"  网格ymd类型: {grid_df['ymd'].dtype}")
    if len(grid_df) > 0:
        print(f"  网格ymd示例: {grid_df['ymd'].iloc[0]}")

    # 3. 合并数据
    if len(shareholder_df) > 0:
        merged_df = pd.merge(
            grid_df,
            shareholder_df,
            on=['ymd', 'stock_code'],
            how='left'
        )

        # 4. 按股票排序并前向填充
        merged_df = merged_df.sort_values(['stock_code', 'ymd'])

        # 需要填充的字段
        fill_cols = ['stock_name', 'total_sh', 'avg_share', 'pct_of_total_sh', 'pct_of_avg_sh']

        # 对每个股票组进行前向填充
        result_batch = pd.DataFrame()
        for stock_code, group in merged_df.groupby('stock_code'):
            group[fill_cols] = group[fill_cols].fillna(method='ffill')
            result_batch = pd.concat([result_batch, group], ignore_index=True)

        # 统计这批股票的数据情况
        stock_data_count = result_batch['total_sh'].notna().sum()
        stock_total = len(result_batch)
        print(f"  数据覆盖率: {stock_data_count}/{stock_total} ({stock_data_count / stock_total * 100:.2f}%)")
    else:
        # 如果没有数据，所有字段都是NULL
        result_batch = grid_df.copy()
        result_batch['stock_name'] = None
        result_batch['total_sh'] = None
        result_batch['avg_share'] = None
        result_batch['pct_of_total_sh'] = None
        result_batch['pct_of_avg_sh'] = None
        print(f"  没有股东数据，全部填充为NULL")

    print(f"  处理完成，生成 {len(result_batch)} 行，耗时: {time.time() - batch_start:.2f}秒")

    return result_batch


def generate_full_data_batch(batch_size=100):
    """分批生成完整数据"""
    print("\n" + "=" * 50)
    print("开始分批生成完整数据...")
    print("=" * 50)
    total_start = time.time()

    # 1. 获取交易日和股票列表
    print("\n【第一步】获取基础数据...")
    trading_days = get_trading_days()
    all_stocks = get_valid_stocks()

    if len(trading_days) == 0:
        print("错误：没有获取到交易日数据")
        return pd.DataFrame()

    if len(all_stocks) == 0:
        print("错误：没有获取到股票代码数据")
        return pd.DataFrame()

    print(f"总共 {len(trading_days)} 个交易日，{len(all_stocks)} 只股票")
    print(f"如果一次性处理需要 {len(trading_days) * len(all_stocks):,} 行数据")

    # 2. 分批处理股票
    print("\n【第二步】分批处理股票...")
    all_results = []
    total_batches = (len(all_stocks) + batch_size - 1) // batch_size

    for i in range(0, len(all_stocks), batch_size):
        batch_num = i // batch_size + 1
        stock_batch = all_stocks[i:i + batch_size]

        print(
            f"\n--- 处理批次 {batch_num}/{total_batches} (股票 {i + 1} 到 {min(i + batch_size, len(all_stocks))}) ---")

        batch_result = process_stock_batch(stock_batch, trading_days)

        if len(batch_result) > 0:
            all_results.append(batch_result)
            current_total = sum(len(df) for df in all_results)
            print(f"  累计数据行数: {current_total:,}")

    # 3. 合并所有批次结果
    print("\n【第三步】合并所有批次结果...")
    if all_results:
        final_df = pd.concat(all_results, ignore_index=True)

        # 4. 最终统计
        print("\n【第四步】最终数据统计...")
        total_rows = len(final_df)
        filled_rows = final_df['total_sh'].notna().sum()

        print(f"总行数: {total_rows:,}")
        print(f"有数据行数: {filled_rows:,}")
        print(f"数据覆盖率: {filled_rows / total_rows * 100:.2f}%")
        print(f"数据日期范围: {final_df['ymd'].min()} 到 {final_df['ymd'].max()}")
        print(f"股票数量: {final_df['stock_code'].nunique()}")

        # 5. 检查数据类型
        print(f"\n【数据类型检查】")
        print(f"ymd类型: {final_df['ymd'].dtype}")
        print(f"ymd前5行: {final_df['ymd'].head(5).tolist()}")

        print(f"\n总耗时: {time.time() - total_start:.2f}秒")
        return final_df
    else:
        print("没有生成任何数据")
        return pd.DataFrame()


def save_to_csv(df, filename=None):
    """保存数据到CSV"""
    if filename is None:
        filename = f"dwd_shareholder_num_latest_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"

    print(f"\n保存数据到 {filename}...")
    print(f"保存前ymd类型: {df['ymd'].dtype}")
    print(f"保存前ymd前5行: {df['ymd'].head(5).tolist()}")

    df.to_csv(filename, index=False, encoding='utf-8')

    file_size = os.path.getsize(filename) / 1024 / 1024
    print(f"保存完成，文件大小: {file_size:.2f} MB")
    return filename


def upload_to_mysql(df, chunksize=50000):
    """上传数据到MySQL"""
    print("\n开始上传数据到MySQL...")

    # 上传前检查
    print(f"上传前ymd类型: {df['ymd'].dtype}")
    print(f"上传前ymd前5行: {df['ymd'].head(5).tolist()}")

    # 确保ymd是datetime类型
    if df['ymd'].dtype != 'datetime64[ns]':
        print("转换ymd为datetime类型...")
        df['ymd'] = pd.to_datetime(df['ymd'])

    # 数据库连接
    engine = create_engine(
        f"mysql+pymysql://{base_properties.origin_mysql_user}:{base_properties.origin_mysql_password}@"
        f"{base_properties.origin_mysql_host}/{base_properties.origin_mysql_database}?charset=utf8mb4"
    )

    # 先清空目标表
    conn = pymysql.connect(
        host=base_properties.origin_mysql_host,
        user=base_properties.origin_mysql_user,
        password=base_properties.origin_mysql_password,
        database=base_properties.origin_mysql_database,
        charset='utf8mb4'
    )

    with conn.cursor() as cursor:
        cursor.execute("TRUNCATE TABLE dwd_shareholder_num_latest")
        conn.commit()
    conn.close()
    print("目标表已清空")

    # 分批上传
    total_rows = len(df)
    for i in range(0, total_rows, chunksize):
        batch = df.iloc[i:i + chunksize]
        batch.to_sql(
            'dwd_shareholder_num_latest',
            engine,
            if_exists='append',
            index=False,
            method='multi'
        )
        print(f"已上传 {min(i + chunksize, total_rows):,}/{total_rows:,} 条")

    print("上传完成！")


def main():
    """主函数"""
    print("=" * 60)
    print("重新生成 dwd_shareholder_num_latest (分批处理 - 修复版)")
    print("=" * 60)
    total_start = time.time()

    # 1. 分批生成完整数据（每批处理100只股票）
    result_df = generate_full_data_batch(batch_size=100)

    if len(result_df) == 0:
        print("没有生成数据，程序退出")
        return

    # 2. 保存到CSV
    csv_file = save_to_csv(result_df)

    # 3. 询问是否上传
    print("\n" + "=" * 50)
    confirm = input("是否上传到MySQL？(yes/no): ")
    if confirm.lower() == 'yes':
        upload_to_mysql(result_df)
        print("\n✨ 全部完成！")
    else:
        print(f"\n数据已保存到本地文件: {csv_file}")
        print("如需上传，请手动执行 upload_to_mysql() 函数")

    print(f"总耗时: {time.time() - total_start:.2f}秒")


if __name__ == "__main__":
    main()