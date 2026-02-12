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
        """
        #  1.获取日期
        ymd = DateUtility.today()

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
            LEFT JOIN
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
        """
        #  1.获取日期
        ymd = DateUtility.today()

        # 2.定义 SQL 模板
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
               WHEN t1.stock_code LIKE '300%' OR t1.stock_code LIKE '301%' THEN '创业板' 
               WHEN t1.stock_code LIKE '8%'   OR t1.stock_code LIKE '4%'   THEN '北交所'  
               WHEN t1.stock_code LIKE '000%' OR t1.stock_code LIKE '001%' OR t1.stock_code LIKE '002%' OR t1.stock_code LIKE '003%' THEN '深圳主板' 
               WHEN t1.stock_code LIKE '688%' OR t1.stock_code LIKE '689%' THEN '科创板'  
               WHEN t1.stock_code LIKE '600%' OR t1.stock_code LIKE '601%' OR t1.stock_code LIKE '603%' OR t1.stock_code LIKE '605%' THEN '上海主板' 
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
        """
        # 1.获取日期
        ymd = DateUtility.today()

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
            ) t2 ON t1.stock_code = t2.stock_code AND t1.ymd = t2.latest_ymd;
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
        # ymd = DateUtility.today()
        ymd = DateUtility.next_day(-1)

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
                 ,tpepb.circulation_market                 AS  market_value
                 ,tpepb.total_market                       AS  total_value
                 ,tpepb.total_shares                       AS  total_capital
                 ,tpepb.circulation_shares                 AS  float_capital
                 ,tshare.total_sh                          AS  shareholder_num
                 ,tshare.pct_of_total_sh                   AS  pct_of_total_sh
                 ,tpepb.pb                                 AS  pb
                 ,tpepb.pe_ttm                             AS  pe
                 ,texchange.market                         AS  market
                 ,tplate.plate_names                       AS  plate_names
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
        Returns:
        """
        # 1.确定起止日期
        time_start_date = DateUtility.next_day(-7)
        time_end_date = DateUtility.next_day(0)

        # 2.获取起止日期范围内的日K线数据
        df = mysql_utils.data_from_mysql_to_dataframe(user=origin_user, password=origin_password, host=origin_host,
                                                      database=origin_database,
                                                      table_name='ods_stock_kline_daily_insight',
                                                      start_date=time_start_date, end_date=time_end_date)

        if df.empty:
            # print(f"{time_start_date} - {time_end_date}日期的K线数据为空，终止 cal_ZT_DT 运行！")
            logging.info(f"{time_start_date} - {time_end_date}日期的K线数据为空，终止 cal_ZT_DT 运行！")
            return

        # 按照 ymd 排序，确保数据是按日期排列的
        latest_15_days = df.sort_values(by=['stock_code', 'ymd'])

        # 按股票代码分组，然后对每个分组进行 shift(1) 操作, 计算昨日close
        latest_15_days['last_close'] = latest_15_days.groupby('stock_code')['close'].shift(1)

        # 过滤掉没有昨日数据的行
        latest_15_days = latest_15_days.dropna(subset=['last_close'])

        if latest_15_days.empty:
            # print(f"{time_start_date} - {time_end_date}日期的日期差值时间为空，终止 cal_ZT_DT 运行！")
            logging.info(f"{time_start_date} - {time_end_date}日期的日期差值时间为空，终止 cal_ZT_DT 运行！")
            return

        # 获取市场特征
        stock_market_init = mysql_utils.data_from_mysql_to_dataframe_latest(
            user=origin_user, password=origin_password, host=origin_host,
            database=origin_database, table_name='dwd_ashare_stock_base_info')

        stock_base_info = stock_market_init[['stock_code', 'stock_name', 'market_value', 'total_value',
                                            'total_capital', 'float_capital', 'shareholder_num', 'pb', 'pe', 'market', 'plate_names']]

        # 合并市场信息到最新的15天数据
        latest_15_days['stock_code'] = latest_15_days['stock_code'].str.split('.').str[0]

        latest_15_days = latest_15_days[['ymd', 'stock_code', 'close', 'last_close']]

        latest_15_days = pd.merge(latest_15_days, stock_base_info, on='stock_code', how='left', suffixes=('_latest', '_base'))

        def calculate_ZT_DT(row):
            if row['market'] in ['创业板', '科创板']:
                up_limit = row['last_close'] * 1.20
                down_limit = row['last_close'] * 0.80
            else:  # 上海主板、深圳主板
                up_limit = row['last_close'] * 1.10
                down_limit = row['last_close'] * 0.90
            return pd.Series([up_limit, down_limit])  # 确保返回两个值

        # 应用计算
        latest_15_days[['昨日ZT价', '昨日DT价']] = latest_15_days.apply(calculate_ZT_DT, axis=1, result_type='expand')

        def ZT_DT_orz(price, target_price):
            # 如果 price 和 target_price 之间的差距小于等于0.01，才进一步计算
            if abs(target_price - price) <= 0.01:
                # 计算 price 周围 0.01 范围内的最接近的2个价格
                left_price = price - 0.01
                right_price = price + 0.01

                # 算价差
                left_delta = abs(left_price - target_price)
                mid_delta = abs(price - target_price)
                right_delta = abs(right_price - target_price)
                min_delta = min(left_delta, mid_delta, right_delta)

                # 判断为ZT or DT
                if mid_delta == min_delta:
                    return True

            # 不可能 ZT or DT
            return False

        # 3. 判断每日的涨停或跌停
        latest_15_days['是否涨停'] = latest_15_days.apply(
            lambda row: ZT_DT_orz(row['close'], row['昨日ZT价']), axis=1)
        latest_15_days['是否跌停'] = latest_15_days.apply(
            lambda row: ZT_DT_orz(row['close'], row['昨日DT价']), axis=1)

        # 4. 筛选出涨停和跌停的记录，分别存入两个 DataFrame
        zt_records = latest_15_days[latest_15_days['是否涨停'] == True].copy()
        zt_records['rate'] = ((zt_records['close'] - zt_records['last_close']) / zt_records['last_close'] * 100).round(2)
        zt_df = zt_records[
            ['ymd', 'stock_code', 'stock_name', 'last_close', 'close', 'rate', 'market_value', 'total_value',
             'total_capital', 'float_capital', 'shareholder_num', 'pb', 'pe', 'market', 'plate_names']]
        zt_df = zt_df.sort_values(by=['ymd', 'stock_code'])

        dt_records = latest_15_days[latest_15_days['是否跌停'] == True].copy()
        dt_records['rate'] = ((dt_records['close'] - dt_records['last_close']) / dt_records['last_close'] * 100).round(2)
        dt_df = dt_records[
            ['ymd', 'stock_code', 'stock_name', 'last_close', 'close', 'rate', 'market_value', 'total_value',
             'total_capital', 'float_capital', 'shareholder_num', 'pb', 'pe', 'market', 'plate_names']]
        dt_df = dt_df.sort_values(by=['ymd', 'stock_code'])

        ############################   文件输出模块     ############################
        # 总是保存到远端数据库
        # 涨停数据保存到远端mysql中
        mysql_utils.data_from_dataframe_to_mysql(
            user=origin_user,
            password=origin_password,
            host=origin_host,
            database=origin_database,
            df=zt_df,
            table_name="dwd_stock_zt_list",
            merge_on=['ymd', 'stock_code'])

        # 跌停数据保存到远端mysql中
        mysql_utils.data_from_dataframe_to_mysql(
            user=origin_user,
            password=origin_password,
            host=origin_host,
            database=origin_database,
            df=dt_df,
            table_name="dwd_stock_dt_list",
            merge_on=['ymd', 'stock_code'])


    def setup(self):

        # # 聚合股票的板块，把各个板块数据聚合在一起   周末手动执行
        # self.cal_ashare_plate()
        #
        # # 计算股票所归属的交易所，判断其是主办、创业板、科创板、北交所等等
        # self.cal_stock_exchange()
        #
        # # 全量票的最新股东数数据
        # self.cal_shareholder_num_latest()

        # 计算股票基础信息，汇总表，名称、编码、板块、股本、市值、净资产
        self.cal_stock_base_info()

        # # 计算一只股票是否 涨停 / 跌停
        # self.cal_ZT_DT()


if __name__ == '__main__':
    save_insight_data = CalDWD()
    save_insight_data.setup()