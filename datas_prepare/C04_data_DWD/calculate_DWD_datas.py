# -*- coding: utf-8 -*-

import pandas as pd
import logging

from CommonProperties import Base_Properties
from CommonProperties.DateUtility import DateUtility
from CommonProperties.Base_utils import timing_decorator
import CommonProperties.Mysql_Utils as mysql_utils
from CommonProperties import set_config

# ************************************************************************
#  调用日志配置
set_config.setup_logging_config()
######################  mysql 配置信息  本地和远端服务器  ####################
local_user = Base_Properties.local_mysql_user
local_password = Base_Properties.local_mysql_password
local_database = Base_Properties.local_mysql_database
local_host = Base_Properties.local_mysql_host

origin_user = Base_Properties.origin_mysql_user
origin_password = Base_Properties.origin_mysql_password
origin_database = Base_Properties.origin_mysql_database
origin_host = Base_Properties.origin_mysql_host


class CalDWD:

    def __init__(self):
        pass

    @timing_decorator
    def cal_ashare_plate(self):
        """
        聚合股票的板块，把各个板块数据聚合在一起
        写入  dwd_stock_a_total_plate
        """
        #  1.获取日期
        ymd = DateUtility.today()
        # ymd = DateUtility.next_day(-1)

        # 2.定义 SQL 模板
        sql_statements_template = [
            """
            DELETE FROM quant.dwd_stock_a_total_plate WHERE ymd='{ymd}';
            """,
            """
            INSERT IGNORE INTO quant.dwd_stock_a_total_plate
            SELECT 
                 ymd
                ,concept_name AS board_code
                ,concept_name AS board_name
                ,stock_code
                ,stock_name
                ,'ods_tdx_stock_concept_plate' AS source_table
                ,'' AS remark
            FROM quant.ods_tdx_stock_concept_plate
            WHERE ymd='{ymd}'
            UNION ALL
            SELECT 
                 ymd
                ,style_code   AS board_code
                ,style_name   AS board_name
                ,stock_code
                ,stock_name
                ,'ods_tdx_stock_style_plate'   AS source_table
                ,'' AS remark
            FROM quant.ods_tdx_stock_style_plate
            WHERE ymd='{ymd}'
            UNION ALL
            SELECT 
                ymd
               ,industry_code AS board_code
               ,industry_name AS board_name
               ,stock_code
               ,stock_name
               ,'ods_tdx_stock_industry_plate' AS source_table
               ,'' AS remark
            FROM quant.ods_tdx_stock_industry_plate
            WHERE ymd='{ymd}'
            UNION ALL
            SELECT 
                ymd
               ,region_name   AS board_code
               ,region_name   AS board_name
               ,stock_code
               ,stock_name
               ,'ods_tdx_stock_region_plate'   AS source_table
               ,'' AS remark
            FROM quant.ods_tdx_stock_region_plate
            WHERE ymd='{ymd}'
            UNION ALL
            SELECT 
                ymd
               ,index_code    AS board_code
               ,index_name    AS board_name
               ,stock_code
               ,stock_name
               ,'ods_tdx_stock_index_plate'    AS source_table
               ,'' AS remark
            FROM quant.ods_tdx_stock_index_plate
            WHERE ymd='{ymd}'
            UNION ALL
            SELECT 
                ymd
               ,''            AS board_code
               ,plate_name    AS board_name
               ,stock_code
               ,stock_name
               ,'ods_stock_plate_redbook'      AS source_table
               ,remark
            FROM quant.ods_stock_plate_redbook
            WHERE ymd='{ymd}'
            UNION ALL
            SELECT
                tboard_name.ymd
               ,tboard_name.board_code
               ,tboard_name.board_name
               ,tboard_stock.stock_code
               ,tboard_stock.stock_name
               ,'ods_akshare_board_concept_name_ths'      AS source_table
               ,tboard_stock.weight                       AS remark
            FROM 
            (SELECT
                 ymd         -- 数据日期（核心日期维度，适配量化数据统一归档）
                ,board_name  -- 板块名称
                ,board_code  -- 板块代码
             FROM  ods_akshare_board_concept_name_ths
             WHERE ymd ='{ymd}'
            ) tboard_name
            inner JOIN
            (SELECT 
               ymd         -- 数据日期
              ,board_name  -- 板块名称
              ,board_code  -- 板块代码
              ,stock_code  -- 股票代码
              ,stock_name  -- 股票名称
              ,weight      -- 权重
             FROM  ods_tushare_stock_board_concept_maps_ths
             WHERE ymd=(SELECT MAX(ymd) FROM  ods_tushare_stock_board_concept_maps_ths)
            ) tboard_stock
            ON REPLACE(tboard_name.board_name, ' ', '') = REPLACE(tboard_stock.board_name, ' ', '');
            """
        ]

        # 3.主程序替换 {ymd} 占位符
        sql_statements = [stmt.format(ymd=ymd) for stmt in sql_statements_template]

        # 4.执行远端MySQL
        mysql_utils.execute_sql_statements(
            user=origin_user,
            password=origin_password,
            host=origin_host,
            database=origin_database,
            sql_statements=sql_statements)


    @timing_decorator
    def cal_stock_exchange(self):
        """
        计算股票所归属的交易所，判断其是主办、创业板、科创板、北交所等等
        写入  ods_stock_exchange_market
        """
        #  1.获取日期
        ymd = DateUtility.today()
        # ymd = DateUtility.next_day(-1)

        #  2.定义 SQL 模板
        sql_statements_template = [
            """
            DELETE  FROM quant.ods_stock_exchange_market WHERE  ymd = '{ymd}';
            """,
            """
            INSERT INTO quant.ods_stock_exchange_market (ymd, stock_code, stock_name, market)
            SELECT 
                t1.ymd
               ,t1.stock_code
               ,t1.stock_name
               ,CASE
                 -- 北交所 (30%涨跌停)
                 WHEN t1.stock_code LIKE '%.BJ' 
                      OR t1.stock_code LIKE '8%'   
                      OR t1.stock_code LIKE '4%'   
                      OR t1.stock_code LIKE '9%' THEN '北交所'

                 -- 创业板 (20%涨跌停，深交所)
                 WHEN t1.stock_code LIKE '300%' 
                      OR t1.stock_code LIKE '301%' 
                      OR t1.stock_code LIKE '302%' THEN '创业板'

                 -- 科创板 (20%涨跌停，上交所)
                 WHEN t1.stock_code LIKE '688%' 
                      OR t1.stock_code LIKE '689%' THEN '科创板'

                 -- 深交所主板 (10%涨跌停)
                 WHEN t1.stock_code LIKE '%.SZ' 
                      OR t1.stock_code LIKE '000%' 
                      OR t1.stock_code LIKE '001%' 
                      OR t1.stock_code LIKE '002%' 
                      OR t1.stock_code LIKE '003%' 
                      OR t1.stock_code LIKE '200%' THEN '深圳主板'

                 -- 上交所主板 (10%涨跌停)
                 WHEN t1.stock_code LIKE '%.SH' 
                      OR t1.stock_code LIKE '600%' 
                      OR t1.stock_code LIKE '601%' 
                      OR t1.stock_code LIKE '603%' 
                      OR t1.stock_code LIKE '605%' THEN '上海主板'

                 ELSE '未知类型' 
               END AS market
            FROM quant.ods_stock_code_daily_insight     t1
            WHERE  t1.ymd = '{ymd}';
            """
        ]


        # 3.主程序替换 {ymd} 占位符
        sql_statements = [stmt.format(ymd=ymd) for stmt in sql_statements_template]

        # 4.执行远端MySQL
        mysql_utils.execute_sql_statements(
            user=origin_user,
            password=origin_password,
            host=origin_host,
            database=origin_database,
            sql_statements=sql_statements)


    @timing_decorator
    def cal_shareholder_num_latest(self):
        """
        计算每个股票的最新股东数数据（按股票代码分组，更准确）
        写入  dwd_shareholder_num_latest
        """
        # 1.获取日期
        ymd = DateUtility.today()
        # ymd = DateUtility.next_day(-1)

        # 2.定义 SQL 模板
        sql_statements_template = [
            """
            DELETE FROM quant.dwd_shareholder_num_latest WHERE ymd = '{ymd}';
            """,
            """
            INSERT INTO quant.dwd_shareholder_num_latest 
            (ymd, stock_code, stock_name, total_sh, avg_share, pct_of_total_sh, pct_of_avg_sh)
            SELECT 
                '{ymd}' as ymd,
                t1.stock_code,
                t1.stock_name,
                t1.total_sh,
                t1.avg_share,
                t1.pct_of_total_sh,
                t1.pct_of_avg_sh
            FROM quant.ods_shareholder_num t1
            INNER JOIN (
                SELECT 
                    stock_code,
                    MAX(ymd) as latest_ymd
                FROM quant.ods_shareholder_num
                GROUP BY stock_code
            ) t2 
            ON t1.stock_code = t2.stock_code 
            AND t1.ymd = t2.latest_ymd;
            """
        ]

        # 3.主程序替换 {ymd} 占位符
        sql_statements = [stmt.format(ymd=ymd) for stmt in sql_statements_template]

        # 4.执行远端MySQL
        mysql_utils.execute_sql_statements(
            user=origin_user,
            password=origin_password,
            host=origin_host,
            database=origin_database,
            sql_statements=sql_statements)


    @timing_decorator
    def cal_stock_base_info(self):
        """
        计算股票基础信息，汇总表，名称、编码、板块、股本、市值、净资产
        """
        #  1.获取日期
        ymd = DateUtility.today()
        # ymd = DateUtility.next_day(-1)

        # 2.定义 SQL 模板
        sql_statements_template = [
            """
            DELETE  FROM quant.dwd_ashare_stock_base_info WHERE  ymd = '{ymd}';
            """,
            """
            insert IGNORE  into quant.dwd_ashare_stock_base_info 
            select 
                  tkline.ymd       
                 ,tkline.stock_code
                 ,tcode.stock_name 
                 ,tkline.close      
                 ,tkline.change_pct
                 ,tkline.volume
                 ,tkline.trading_amount
                 ,IFNULL(tpepb.circulation_market, 0)                 AS  market_value
                 ,IFNULL(tpepb.total_market, 0)                       AS  total_value
                 ,IFNULL(tpepb.total_shares, 0)                       AS  total_capital
                 ,IFNULL(tpepb.circulation_shares, 0)                 AS  float_capital
                 ,tshare.total_sh                                     AS  shareholder_num
                 ,tshare.pct_of_total_sh                              AS  pct_of_total_sh
                 ,IFNULL(tpepb.pb, 0)                                 AS  pb
                 ,IFNULL(tpepb.pe_ttm, 0)                             AS  pe
                 ,texchange.market                                    AS  market
                 ,tplate.plate_names                                  AS  plate_names
            from  
            ( select
                  stock_code
                 ,ymd
                 ,close
                 ,change_pct
                 ,volume
                 ,trading_amount
              from  quant.ods_stock_kline_daily_ts
              where  ymd='{ymd}'
            ) tkline
            left join
            ( select
                  ymd
                 ,stock_code
                 ,stock_name
                 ,exchange
              from quant.ods_stock_code_daily_insight
              where ymd=(select max(ymd) from quant.ods_stock_code_daily_insight)
            ) tcode
            on tkline.stock_code = tcode.stock_code
            left join
            ( select
                  ymd                  
                 ,stock_code           
                 ,total_market         -- 总市值
                 ,circulation_market   -- 流通市值
                 ,total_shares         -- 总股本
                 ,circulation_shares   -- 流通股本
                 ,pe_ttm               -- PE_TTM
                 ,pb                   -- 市净率
                 ,peg                  -- PEG值
              from  quant.ods_akshare_stock_value_em
              where ymd=(select max(ymd) from quant.ods_akshare_stock_value_em)
            ) tpepb
            ON SUBSTRING_INDEX(tkline.stock_code, '.', 1) = tpepb.stock_code
            left join 
            ( select
                  ymd            
                 ,stock_code     
                 ,stock_name     
                 ,total_sh       
                 ,avg_share      
                 ,pct_of_total_sh
                 ,pct_of_avg_sh  
              from  quant.dwd_shareholder_num_latest
              where ymd=(select max(ymd) from quant.dwd_shareholder_num_latest)
            ) tshare
            on tkline.stock_code = tshare.stock_code
            left join 
            ( select 
                  ymd                                               
                 ,stock_code                                        
                 ,stock_name                                        
                 ,market                                            
              from  quant.ods_stock_exchange_market 
              where ymd = (SELECT MAX(ymd) FROM quant.ods_stock_exchange_market)
            ) texchange 
            on tkline.stock_code = texchange.stock_code
            left join 
            (
              select 
                  ymd                                              
                 ,stock_code                                       
                 ,stock_name                                       
                 ,GROUP_CONCAT(board_name ORDER BY board_name SEPARATOR ',') AS plate_names   
              from  quant.dwd_stock_a_total_plate  
              where ymd = (SELECT MAX(ymd) FROM quant.dwd_stock_a_total_plate)
              group by ymd, stock_code, stock_name 
            ) tplate
            ON SUBSTRING_INDEX(tkline.stock_code, '.', 1)=SUBSTRING_INDEX(tplate.stock_code, '.', 1);
            """]

        # 3.主程序替换 {ymd} 占位符
        sql_statements = [stmt.format(ymd=ymd) for stmt in sql_statements_template]

        # 4.执行远端MySQL
        mysql_utils.execute_sql_statements(
            user=origin_user,
            password=origin_password,
            host=origin_host,
            database=origin_database,
            sql_statements=sql_statements)


    @timing_decorator
    def cal_ZT_DT(self):
        """
        计算一只股票是否 涨停 / 跌停
        添加了详细的进度日志（兼容GBK编码）
        """
        import time
        start_time = time.time()

        # 1.确定起止日期
        time_start_date = DateUtility.next_day(-5)
        time_end_date = DateUtility.today()

        logging.info("=" * 60)
        logging.info(f"开始计算涨跌停数据，日期范围: {time_start_date} 至 {time_end_date}")
        logging.info("=" * 60)

        # 2.获取起止日期范围内的日K线数据
        logging.info(f"【步骤1/7】正在从 ods_stock_kline_daily_ts 读取K线数据...")
        step_start = time.time()

        df = mysql_utils.data_from_mysql_to_dataframe(
            user=origin_user, password=origin_password, host=origin_host,
            database=origin_database,
            table_name='ods_stock_kline_daily_ts',
            start_date=time_start_date, end_date=time_end_date)

        step_time = time.time() - step_start
        # 移除特殊字符 ✓，使用 [完成] 代替
        logging.info(f"[完成] 读取完成，获取到 {len(df)} 条K线记录，耗时: {step_time:.2f}秒")

        if df.empty:
            logging.warning(f"[警告] {time_start_date} - {time_end_date} 日期的K线数据为空，终止运行！")
            return

        # 3.数据预处理
        logging.info(f"【步骤2/7】正在对K线数据进行排序和计算昨收价...")
        step_start = time.time()

        # 获取唯一的股票代码数量
        unique_stocks = df['stock_code'].nunique()
        unique_dates = df['ymd'].nunique()
        logging.info(f"   - 涉及股票数量: {unique_stocks} 只")
        logging.info(f"   - 涉及交易日: {unique_dates} 天")

        # 按照 ymd 排序
        latest_15_days = df.sort_values(by=['stock_code', 'ymd'])

        # 按股票代码分组计算昨日close
        latest_15_days['last_close'] = latest_15_days.groupby('stock_code')['close'].shift(1)

        # 记录计算前后的数据量
        before_drop = len(latest_15_days)
        latest_15_days = latest_15_days.dropna(subset=['last_close'])
        after_drop = len(latest_15_days)

        step_time = time.time() - step_start
        logging.info(f"[完成] 预处理完成，删除了 {before_drop - after_drop} 条无昨收数据的记录")
        logging.info(f"  剩余 {after_drop} 条有效记录，耗时: {step_time:.2f}秒")

        if latest_15_days.empty:
            logging.warning(f"[警告] {time_start_date} - {time_end_date} 日期的日期差值时间为空，终止运行！")
            return

        # 4.获取股票基础信息
        logging.info(f"【步骤3/7】正在获取股票基础信息...")
        step_start = time.time()

        stock_market_init = mysql_utils.data_from_mysql_to_dataframe_latest(
            user=origin_user, password=origin_password, host=origin_host,
            database=origin_database, table_name='dwd_ashare_stock_base_info')

        step_time = time.time() - step_start
        logging.info(f"[完成] 获取到 {len(stock_market_init)} 条股票基础信息，耗时: {step_time:.2f}秒")

        # 显示基础信息的日期和示例
        if not stock_market_init.empty:
            latest_date = stock_market_init['ymd'].max() if 'ymd' in stock_market_init.columns else '未知'
            logging.info(f"   - 基础信息最新日期: {latest_date}")
            logging.info(f"   - 股票代码示例: {stock_market_init['stock_code'].head(3).tolist()}")

        stock_base_info = stock_market_init[['stock_code', 'stock_name', 'market_value', 'total_value',
                                             'total_capital', 'float_capital', 'shareholder_num',
                                             'pb', 'pe', 'market', 'plate_names']]

        # 5.合并数据
        logging.info(f"【步骤4/7】正在合并K线数据和股票基础信息...")
        step_start = time.time()

        # 只选择需要的列
        latest_15_days = latest_15_days[['ymd', 'stock_code', 'close', 'last_close']]

        # 记录合并前的数据情况
        logging.info(f"   - K线数据中的股票代码示例: {latest_15_days['stock_code'].head(3).tolist()}")

        # 执行合并
        latest_15_days = pd.merge(
            latest_15_days,
            stock_base_info,
            on='stock_code',
            how='left'
        )

        step_time = time.time() - step_start
        logging.info(f"[完成] 合并完成，结果数据量: {len(latest_15_days)} 条，耗时: {step_time:.2f}秒")

        # 检查合并质量
        missing_names = latest_15_days['stock_name'].isna().sum()
        missing_percent = (missing_names / len(latest_15_days)) * 100
        logging.info(f"   - 股票名称缺失: {missing_names} 条 ({missing_percent:.2f}%)")

        if missing_names > 0:
            # 显示部分缺失的股票代码
            missing_stocks = latest_15_days[latest_15_days['stock_name'].isna()]['stock_code'].unique()[:5]
            logging.info(f"   - 缺失信息的股票代码示例: {missing_stocks.tolist()}")

        # 6.计算涨跌停价格
        logging.info(f"【步骤5/7】正在计算涨跌停价格...")
        step_start = time.time()

        # 统计各市场类型的数量
        market_counts = latest_15_days['market'].value_counts()
        logging.info(f"   - 市场类型分布: {dict(market_counts)}")

        def calculate_ZT_DT(row):
            if pd.isna(row['market']):
                up_limit = row['last_close'] * 1.10
                down_limit = row['last_close'] * 0.90
            elif row['market'] in ['创业板', '科创板']:
                up_limit = row['last_close'] * 1.20
                down_limit = row['last_close'] * 0.80
            else:  # 上海主板、深圳主板
                up_limit = row['last_close'] * 1.10
                down_limit = row['last_close'] * 0.90
            return pd.Series([up_limit, down_limit])

        latest_15_days[['昨日ZT价', '昨日DT价']] = latest_15_days.apply(
            calculate_ZT_DT, axis=1, result_type='expand')

        step_time = time.time() - step_start
        logging.info(f"[完成] 涨跌停价格计算完成，耗时: {step_time:.2f}秒")

        # 7.判断涨跌停
        logging.info(f"【步骤6/7】正在判断涨跌停...")
        step_start = time.time()

        def ZT_DT_orz(price, target_price):
            if pd.isna(target_price):
                return False
            if abs(target_price - price) <= 0.01:
                left_price = price - 0.01
                right_price = price + 0.01
                left_delta = abs(left_price - target_price)
                mid_delta = abs(price - target_price)
                right_delta = abs(right_price - target_price)
                min_delta = min(left_delta, mid_delta, right_delta)
                if mid_delta == min_delta:
                    return True
            return False

        # 应用判断
        latest_15_days['是否涨停'] = latest_15_days.apply(
            lambda row: ZT_DT_orz(row['close'], row['昨日ZT价']), axis=1)
        latest_15_days['是否跌停'] = latest_15_days.apply(
            lambda row: ZT_DT_orz(row['close'], row['昨日DT价']), axis=1)

        step_time = time.time() - step_start
        logging.info(f"[完成] 涨跌停判断完成，耗时: {step_time:.2f}秒")

        # 8.筛选和保存结果
        logging.info(f"【步骤7/7】正在筛选和保存结果...")
        step_start = time.time()

        # 涨停数据处理
        zt_records = latest_15_days[latest_15_days['是否涨停'] == True].copy()
        zt_count = len(zt_records)
        logging.info(f"   - 发现涨停记录: {zt_count} 条")

        if zt_count > 0:
            zt_records['rate'] = ((zt_records['close'] - zt_records['last_close']) /
                                  zt_records['last_close'] * 100).round(2)
            zt_df = zt_records[
                ['ymd', 'stock_code', 'stock_name', 'last_close', 'close', 'rate',
                 'market_value', 'total_value', 'total_capital', 'float_capital',
                 'shareholder_num', 'pb', 'pe', 'market', 'plate_names']]
            zt_df = zt_df.sort_values(by=['ymd', 'stock_code'])

            # 显示涨停日期分布
            zt_dates = zt_df['ymd'].value_counts().sort_index()
            logging.info(f"   - 涨停日期分布: {dict(list(zt_dates.head().items()))}...")

            # 保存涨停数据
            save_start = time.time()
            mysql_utils.data_from_dataframe_to_mysql(
                user=origin_user,
                password=origin_password,
                host=origin_host,
                database=origin_database,
                df=zt_df,
                table_name="dwd_stock_zt_list",
                merge_on=['ymd', 'stock_code'])
            logging.info(f"   [完成] 涨停数据保存完成，耗时: {time.time() - save_start:.2f}秒")

        # 跌停数据处理
        dt_records = latest_15_days[latest_15_days['是否跌停'] == True].copy()
        dt_count = len(dt_records)
        logging.info(f"   - 发现跌停记录: {dt_count} 条")

        if dt_count > 0:
            dt_records['rate'] = ((dt_records['close'] - dt_records['last_close']) /
                                  dt_records['last_close'] * 100).round(2)
            dt_df = dt_records[
                ['ymd', 'stock_code', 'stock_name', 'last_close', 'close', 'rate',
                 'market_value', 'total_value', 'total_capital', 'float_capital',
                 'shareholder_num', 'pb', 'pe', 'market', 'plate_names']]
            dt_df = dt_df.sort_values(by=['ymd', 'stock_code'])

            # 显示跌停日期分布
            dt_dates = dt_df['ymd'].value_counts().sort_index()
            logging.info(f"   - 跌停日期分布: {dict(list(dt_dates.head().items()))}...")

            # 保存跌停数据
            save_start = time.time()
            mysql_utils.data_from_dataframe_to_mysql(
                user=origin_user,
                password=origin_password,
                host=origin_host,
                database=origin_database,
                df=dt_df,
                table_name="dwd_stock_dt_list",
                merge_on=['ymd', 'stock_code'])
            logging.info(f"   [完成] 跌停数据保存完成，耗时: {time.time() - save_start:.2f}秒")

        # 9.最终统计
        total_time = time.time() - start_time
        logging.info("=" * 60)
        logging.info(f"【处理完成】总耗时: {total_time:.2f}秒")
        logging.info(f"   - 处理总记录数: {len(latest_15_days)} 条")
        logging.info(f"   - 涨停记录: {zt_count} 条")
        logging.info(f"   - 跌停记录: {dt_count} 条")
        if zt_count > 0 or dt_count > 0:
            logging.info(f"   - 涨跌停合计: {zt_count + dt_count} 条")
        logging.info("=" * 60)

        # 从日志中可以看到数据质量
        logging.info(f"【数据质量检查】")
        logging.info(f"   - 股票名称匹配率: {(1 - missing_percent / 100) * 100:.2f}%")
        if missing_names > 0:
            logging.info(f"   - 建议检查缺失的股票代码，可能需要更新基础信息表")


    @timing_decorator
    def cal_technical_indicators(self):
        """
        计算股票技术指标（均线等）并存入 dwd_stock_technical_indicators 表

        Args:
            start_date: 开始日期 (YYYYMMDD)
            end_date: 结束日期 (YYYYMMDD)
        """
        try:
            import numpy as np

            # start_date = '20210101'
            # end_date = '20260213'
            start_date = DateUtility.first_day_of_month()
            end_date = DateUtility.today()
            # 1. 获取原始K线数据（需要多取一些历史数据用于计算均线）
            start_dt = pd.to_datetime(start_date)
            # 往前多取300天用于计算年线（确保足够的数据）
            query_start = (start_dt - pd.Timedelta(days=300)).strftime('%Y%m%d')

            kline_df = mysql_utils.data_from_mysql_to_dataframe(
                user=origin_user,
                password=origin_password,
                host=origin_host,
                database=origin_database,
                table_name='ods_stock_kline_daily_ts',
                start_date=query_start,
                end_date=end_date,
                cols=['stock_code', 'ymd', 'close', 'volume']
            )

            if kline_df.empty:
                logging.warning(f"K线数据为空: {query_start}~{end_date}")
                return pd.DataFrame()

            # 2. 数据预处理
            kline_df = kline_df.sort_values(['stock_code', 'ymd'])
            kline_df['ymd'] = pd.to_datetime(kline_df['ymd'])

            # 3. 按股票分组计算均线
            result_dfs = []

            for stock, stock_df in kline_df.groupby('stock_code'):
                stock_df = stock_df.copy()
                stock_df = stock_df.sort_values('ymd')

                # 计算价格均线（FLOAT类型）
                stock_df['ma5'] = stock_df['close'].rolling(window=5, min_periods=5).mean()
                stock_df['ma10'] = stock_df['close'].rolling(window=10, min_periods=10).mean()
                stock_df['ma20'] = stock_df['close'].rolling(window=20, min_periods=20).mean()
                stock_df['ma60'] = stock_df['close'].rolling(window=60, min_periods=60).mean()
                stock_df['ma120'] = stock_df['close'].rolling(window=120, min_periods=120).mean()
                stock_df['ma250'] = stock_df['close'].rolling(window=250, min_periods=250).mean()

                # 计算成交量均线（FLOAT类型）
                stock_df['vol_ma5'] = stock_df['volume'].rolling(window=5, min_periods=5).mean()
                stock_df['vol_ma10'] = stock_df['volume'].rolling(window=10, min_periods=10).mean()
                stock_df['vol_ma20'] = stock_df['volume'].rolling(window=20, min_periods=20).mean()
                stock_df['vol_ma60'] = stock_df['volume'].rolling(window=60, min_periods=60).mean()
                stock_df['vol_ma120'] = stock_df['volume'].rolling(window=120, min_periods=120).mean()
                stock_df['vol_ma250'] = stock_df['volume'].rolling(window=250, min_periods=250).mean()

                # 计算价格偏离度（百分比，保留2位小数）
                stock_df['price_vs_ma5'] = ((stock_df['close'] / stock_df['ma5'] - 1) * 100).round(2)
                stock_df['price_vs_ma20'] = ((stock_df['close'] / stock_df['ma20'] - 1) * 100).round(2)
                stock_df['price_vs_ma60'] = ((stock_df['close'] / stock_df['ma60'] - 1) * 100).round(2)

                # 计算成交量偏离度（百分比，保留2位小数）
                stock_df['volume_vs_ma5'] = ((stock_df['volume'] / stock_df['vol_ma5'] - 1) * 100).round(2)
                stock_df['volume_vs_ma20'] = ((stock_df['volume'] / stock_df['vol_ma20'] - 1) * 100).round(2)

                # 只保留需要的列
                keep_cols = ['ymd', 'stock_code', 'close', 'volume',
                             'ma5', 'ma10', 'ma20', 'ma60', 'ma120', 'ma250',
                             'vol_ma5', 'vol_ma10', 'vol_ma20', 'vol_ma60', 'vol_ma120', 'vol_ma250',
                             'price_vs_ma5', 'price_vs_ma20', 'price_vs_ma60',
                             'volume_vs_ma5', 'volume_vs_ma20']

                result_dfs.append(stock_df[keep_cols])

            # 合并所有股票
            all_df = pd.concat(result_dfs, ignore_index=True)

            # 4. 只保留需要的日期范围（过滤掉用于计算的历史数据）
            all_df = all_df[all_df['ymd'].between(pd.to_datetime(start_date),
                                                  pd.to_datetime(end_date))]

            if all_df.empty:
                logging.warning(f"没有需要的数据: {start_date}~{end_date}")
                return pd.DataFrame()

            # 5. 存入数据库
            mysql_utils.data_from_dataframe_to_mysql(
                user=origin_user,
                password=origin_password,
                host=origin_host,
                database=origin_database,
                df=all_df,
                table_name="dwd_stock_technical_indicators",
                merge_on=['ymd', 'stock_code']
            )

            logging.info(f"技术指标计算完成：共{len(all_df)}条记录，日期范围{start_date}~{end_date}")

            return all_df

        except Exception as e:
            logging.error(f"计算技术指标失败：{str(e)}")
            import traceback
            logging.error(traceback.format_exc())
            return pd.DataFrame()



    def setup(self):

        # 聚合股票的板块，把各个板块数据聚合在一起   周末手动执行
        self.cal_ashare_plate()

        # 计算股票所归属的交易所，判断其是主办、创业板、科创板、北交所等等
        self.cal_stock_exchange()

        # 全量票的最新股东数数据
        self.cal_shareholder_num_latest()

        # 计算股票基础信息，汇总表，名称、编码、板块、股本、市值、净资产
        self.cal_stock_base_info()

        # 计算一只股票是否 涨停 / 跌停
        self.cal_ZT_DT()

        # 计算行情衍生指标  均线等
        self.cal_technical_indicators()


if __name__ == '__main__':
    save_insight_data = CalDWD()
    save_insight_data.setup()