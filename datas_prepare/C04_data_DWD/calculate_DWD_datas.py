# -*- coding: utf-8 -*-

import pandas as pd
import numpy as np
import logging

from CommonProperties import Base_Properties
from CommonProperties.DateUtility import DateUtility
from CommonProperties.Base_utils import timing_decorator, script_run
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
        self.stocks_df = mysql_utils.get_stock_codes_latest()


    @timing_decorator
    def cal_ashare_plate(self):
        """
        聚合股票的板块，把各个板块数据聚合在一起
        写入 dwd_stock_a_total_plate
        """
        ymd = DateUtility.today()

        sql_statements_template = [
            """
            DELETE FROM quant.dwd_stock_a_total_plate WHERE ymd='{ymd}';
            """,
            """
            INSERT IGNORE INTO quant.dwd_stock_a_total_plate
                (ymd, board_code, board_name, stock_code, stock_name, source_table, remark)
            SELECT 
                ymd,
                '' AS board_code,
                plate_name AS board_name,
                stock_code,
                stock_name,
                'ods_stock_plate_redbook' AS source_table,
                remark
            FROM quant.ods_stock_plate_redbook
            WHERE ymd='{ymd}'
            UNION ALL
            SELECT
                tboard_stock.ymd,
                tboard_stock.board_code,
                tboard_stock.board_name,
                tboard_stock.stock_code,
                tboard_stock.stock_name,
                'ods_tushare_board_concept_name_ths' AS source_table,
                tboard_stock.weight AS remark
            FROM 
            (SELECT ymd, board_code
             FROM quant.ods_tushare_board_concept_name_ths
             WHERE ymd ='{ymd}'
            ) tboard_name
            INNER JOIN
            (SELECT ymd, board_code, board_name, stock_code, stock_name, weight
             FROM quant.ods_tushare_stock_board_concept_maps_ths
             WHERE ymd='{ymd}'
            ) tboard_stock
            ON tboard_name.board_code = tboard_stock.board_code;
            """
        ]

        sql_statements = [stmt.format(ymd=ymd) for stmt in sql_statements_template]

        mysql_utils.execute_sql_statements(
            user=origin_user,
            password=origin_password,
            host=origin_host,
            database=origin_database,
            sql_statements=sql_statements
        )


    @timing_decorator
    def cal_stock_exchange(self):
        """
        计算股票所归属的交易所，判断其是主办、创业板、科创板、北交所等等
        写入  ods_stock_exchange_market
        """
        ymd = DateUtility.today()

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
                 WHEN t1.stock_code LIKE '%.BJ' 
                      OR t1.stock_code LIKE '8%'   
                      OR t1.stock_code LIKE '4%'   
                      OR t1.stock_code LIKE '9%' THEN '北交所'
                 WHEN t1.stock_code LIKE '300%' 
                      OR t1.stock_code LIKE '301%' 
                      OR t1.stock_code LIKE '302%' THEN '创业板'
                 WHEN t1.stock_code LIKE '688%' 
                      OR t1.stock_code LIKE '689%' THEN '科创板'
                 WHEN t1.stock_code LIKE '%.SZ' 
                      OR t1.stock_code LIKE '000%' 
                      OR t1.stock_code LIKE '001%' 
                      OR t1.stock_code LIKE '002%' 
                      OR t1.stock_code LIKE '003%' 
                      OR t1.stock_code LIKE '200%' THEN '深圳主板'
                 WHEN t1.stock_code LIKE '%.SH' 
                      OR t1.stock_code LIKE '600%' 
                      OR t1.stock_code LIKE '601%' 
                      OR t1.stock_code LIKE '603%' 
                      OR t1.stock_code LIKE '605%' THEN '上海主板'
                 ELSE '未知类型' 
               END AS market
            FROM quant.ods_stock_code_daily_insight t1
            WHERE  t1.ymd = '{ymd}';
            """
        ]

        sql_statements = [stmt.format(ymd=ymd) for stmt in sql_statements_template]

        mysql_utils.execute_sql_statements(
            user=origin_user,
            password=origin_password,
            host=origin_host,
            database=origin_database,
            sql_statements=sql_statements)

    @timing_decorator
    def cal_shareholder_num_event(self):
        """
        计算股东数披露事件表（对齐环比 + 紧邻环比）
        写入 dwd_shareholder_num_event
        逻辑：一行 = 一次披露事件，基于 load_time（可知日）增量处理
        过滤：total_sh/avg_share 为 NULL 或 0 的脏数据
        """
        ymd = DateUtility.today()

        sql_statements_template = [
            """
            DELETE FROM quant.dwd_shareholder_num_event WHERE load_time = '{ymd}';
            """,
            """
            INSERT INTO quant.dwd_shareholder_num_event
            (stock_code, stock_name, ymd, load_time, total_sh, avg_share,
             base_ymd, base_total_sh, qoq_sh, qoq_avg_sh, gap_days, align_flag,
             prev_ymd, mom_sh)
            SELECT 
                t.stock_code, 
                t.stock_name, 
                t.ymd, 
                t.load_time, 
                t.total_sh, 
                t.avg_share,
                b.ymd AS base_ymd, 
                b.total_sh AS base_total_sh,
                CASE WHEN b.total_sh IS NULL OR b.total_sh = 0 THEN NULL
                     ELSE ROUND((t.total_sh / b.total_sh - 1) * 100, 4) END AS qoq_sh,
                CASE WHEN b.avg_share IS NULL OR b.avg_share = 0 THEN NULL
                     ELSE ROUND((t.avg_share / b.avg_share - 1) * 100, 4) END AS qoq_avg_sh,
                DATEDIFF(t.ymd, b.ymd) AS gap_days,
                CASE 
                    WHEN b.stock_code IS NULL THEN 'first'
                    WHEN DATEDIFF(t.ymd, b.ymd) BETWEEN 60 AND 200 THEN 'ok'
                    WHEN DATEDIFF(t.ymd, b.ymd) < 60 THEN 'short_gap'
                    ELSE 'long_gap'
                END AS align_flag,
                p.ymd AS prev_ymd,
                CASE WHEN p.total_sh IS NULL OR p.total_sh = 0 THEN NULL
                     ELSE ROUND((t.total_sh / p.total_sh - 1) * 100, 4) END AS mom_sh
            FROM quant.ods_shareholder_num t
            LEFT JOIN quant.ods_shareholder_num b
              ON b.stock_code = t.stock_code
             AND b.ymd = (
                 SELECT MAX(o.ymd) 
                 FROM quant.ods_shareholder_num o
                 WHERE o.stock_code = t.stock_code
                   AND YEAR(o.ymd)*4 + QUARTER(o.ymd) < YEAR(t.ymd)*4 + QUARTER(t.ymd)
                   AND o.total_sh IS NOT NULL AND o.total_sh > 0
                   AND o.avg_share IS NOT NULL AND o.avg_share > 0
             )
            LEFT JOIN quant.ods_shareholder_num p
              ON p.stock_code = t.stock_code
             AND p.ymd = (
                 SELECT MAX(o.ymd) 
                 FROM quant.ods_shareholder_num o
                 WHERE o.stock_code = t.stock_code AND o.ymd < t.ymd
                   AND o.total_sh IS NOT NULL AND o.total_sh > 0
                   AND o.avg_share IS NOT NULL AND o.avg_share > 0
             )
            WHERE t.load_time = '{ymd}'
              AND t.total_sh IS NOT NULL AND t.total_sh > 0
              AND t.avg_share IS NOT NULL AND t.avg_share > 0;
            """
        ]

        sql_statements = [stmt.format(ymd=ymd) for stmt in sql_statements_template]

        mysql_utils.execute_sql_statements(
            user=origin_user,
            password=origin_password,
            host=origin_host,
            database=origin_database,
            sql_statements=sql_statements)

    @timing_decorator
    def cal_shareholder_num_daily(self):
        """
        计算股东数每日宽表（策略直接消费）
        写入 dwd_shareholder_num_daily
        逻辑：一行 = 交易日 × 股票，前向填充最新事件 + 新鲜度衰减
        """
        ymd = DateUtility.today()

        sql_statements_template = [
            """
            DELETE FROM quant.dwd_shareholder_num_daily WHERE ymd = '{ymd}';
            """,
            """
            INSERT INTO quant.dwd_shareholder_num_daily
            (ymd, stock_code, latest_load_time, latest_total_sh, 
             latest_qoq_sh, latest_mom_sh, days_since_load, is_fresh, fresh_weight,
             surprise_weighted, sh_pctile_cs)
            WITH ranked_event AS (
                SELECT e.*,
                       ROW_NUMBER() OVER (PARTITION BY e.stock_code ORDER BY e.ymd DESC, e.load_time DESC) as rn
                FROM quant.dwd_shareholder_num_event e
                WHERE e.load_time <= '{ymd}'
            )
            SELECT 
                '{ymd}' AS ymd,
                e.stock_code,
                e.load_time AS latest_load_time,
                e.total_sh AS latest_total_sh,
                e.qoq_sh AS latest_qoq_sh,
                e.mom_sh AS latest_mom_sh,
                DATEDIFF('{ymd}', e.load_time) AS days_since_load,
                IF(DATEDIFF('{ymd}', e.load_time) <= 3, 1, 0) AS is_fresh,
                EXP(-DATEDIFF('{ymd}', e.load_time) / 10) AS fresh_weight,
                e.qoq_sh * EXP(-DATEDIFF('{ymd}', e.load_time) / 10) AS surprise_weighted,
                PERCENT_RANK() OVER (ORDER BY e.qoq_sh) AS sh_pctile_cs
            FROM ranked_event e
            WHERE e.rn = 1;
            """
        ]

        sql_statements = [stmt.format(ymd=ymd) for stmt in sql_statements_template]

        mysql_utils.execute_sql_statements(
            user=origin_user,
            password=origin_password,
            host=origin_host,
            database=origin_database,
            sql_statements=sql_statements)

    @timing_decorator
    def cal_shareholder_num_event_batch(self, start_ymd='20210801', end_ymd=None):
        """
        dwd_shareholder_num_event 历史数据批量更新
        高性能版：一次性读取 -> 内存向量化计算 -> 批量写入
        """
        if end_ymd is None:
            end_ymd = DateUtility.today()

        logging.info("步骤1/4：一次性读取全部有效数据...")

        data_sql = f"""
            SELECT stock_code, stock_name, ymd, load_time, total_sh, avg_share
            FROM quant.ods_shareholder_num
            WHERE ymd >= DATE_SUB('{start_ymd}', INTERVAL 400 DAY)
              AND total_sh IS NOT NULL AND total_sh > 0
              AND avg_share IS NOT NULL AND avg_share > 0
            ORDER BY stock_code, ymd;
        """

        df = mysql_utils.execute_query(
            user=origin_user, password=origin_password, host=origin_host,
            database=origin_database, sql=data_sql
        )

        if df.empty:
            logging.warning("无数据")
            return

        logging.info(f"读取完成：{len(df)} 条记录，涉及 {df['stock_code'].nunique()} 只股票")

        logging.info("步骤2/4：内存计算环比（向量化操作）...")

        df['ymd'] = pd.to_datetime(df['ymd'])
        df['load_time'] = pd.to_datetime(df['load_time'])

        # 紧邻环比
        df = df.sort_values(['stock_code', 'ymd'])
        df['prev_ymd'] = df.groupby('stock_code')['ymd'].shift(1)
        df['prev_total_sh'] = df.groupby('stock_code')['total_sh'].shift(1)
        df['mom_sh'] = ((df['total_sh'] / df['prev_total_sh'] - 1) * 100).round(4)
        df.loc[df['prev_total_sh'] == 0, 'mom_sh'] = None

        # 季度锚定
        df['quarter'] = df['ymd'].dt.to_period('Q')
        quarter_last = df.groupby(['stock_code', 'quarter']).agg({
            'ymd': 'max',
            'total_sh': 'last',
            'avg_share': 'last'
        }).reset_index()
        quarter_last.columns = ['stock_code', 'quarter', 'base_ymd', 'base_total_sh', 'base_avg_share']
        quarter_last['prev_quarter'] = quarter_last['quarter'] + 1

        df = df.merge(
            quarter_last[['stock_code', 'prev_quarter', 'base_ymd', 'base_total_sh', 'base_avg_share']],
            left_on=['stock_code', 'quarter'],
            right_on=['stock_code', 'prev_quarter'],
            how='left'
        )

        df['gap_days'] = (df['ymd'] - df['base_ymd']).dt.days
        df['qoq_sh'] = ((df['total_sh'] / df['base_total_sh'] - 1) * 100).round(4)
        df['qoq_avg_sh'] = ((df['avg_share'] / df['base_avg_share'] - 1) * 100).round(4)
        df.loc[df['base_total_sh'].isna() | (df['base_total_sh'] == 0), 'qoq_sh'] = None
        df.loc[df['base_avg_share'].isna() | (df['base_avg_share'] == 0), 'qoq_avg_sh'] = None

        df['align_flag'] = 'first'
        df.loc[df['base_ymd'].notna(), 'align_flag'] = 'ok'
        df.loc[df['gap_days'] < 60, 'align_flag'] = 'short_gap'
        df.loc[df['gap_days'] > 200, 'align_flag'] = 'long_gap'

        logging.info("步骤3/4：过滤目标时间范围...")

        mask = (df['load_time'] >= pd.to_datetime(start_ymd)) & (df['load_time'] <= pd.to_datetime(end_ymd))
        result_df = df[mask].copy()

        # 去掉 is_fresh 和 days_since_load（event 表不存这些相对属性）
        output_columns = [
            'stock_code', 'stock_name', 'ymd', 'load_time', 'total_sh', 'avg_share',
            'base_ymd', 'base_total_sh', 'qoq_sh', 'qoq_avg_sh', 'gap_days', 'align_flag',
            'prev_ymd', 'mom_sh'
        ]
        result_df = result_df[output_columns]

        result_df['ymd'] = result_df['ymd'].dt.strftime('%Y-%m-%d')
        result_df['load_time'] = result_df['load_time'].dt.strftime('%Y-%m-%d')
        result_df['prev_ymd'] = result_df['prev_ymd'].dt.strftime('%Y-%m-%d')
        result_df['base_ymd'] = result_df['base_ymd'].dt.strftime('%Y-%m-%d')

        logging.info(f"计算完成：{len(result_df)} 条事件记录")

        logging.info("步骤4/4：清空目标范围并批量写入...")

        delete_sql = f"""
            DELETE FROM quant.dwd_shareholder_num_event 
            WHERE load_time BETWEEN '{start_ymd}' AND '{end_ymd}';
        """
        mysql_utils.execute_sql_statements(
            user=origin_user, password=origin_password, host=origin_host,
            database=origin_database, sql_statements=[delete_sql]
        )

        mysql_utils.data_from_dataframe_to_mysql(
            user=origin_user, password=origin_password, host=origin_host,
            database=origin_database, df=result_df,
            table_name="dwd_shareholder_num_event",
            merge_on=['stock_code', 'ymd']
        )

        logging.info(f"完成！共写入 {len(result_df)} 条股东数事件")

    @timing_decorator
    def cal_shareholder_num_daily_batch(self, start_ymd='20200101', end_ymd=None):
        """
        批量回填历史每日宽表（Python 高性能版）
        逻辑：读取 event 表 -> 按交易日生成快照 -> 批量写入
        """
        if end_ymd is None:
            end_ymd = DateUtility.today()

        logging.info("步骤1/4：读取交易日历...")

        trading_days_sql = f"""
            SELECT ymd 
            FROM quant.ods_trading_days_insight 
            WHERE ymd >= '{start_ymd}' AND ymd <= '{end_ymd}' 
            ORDER BY ymd;
        """
        trading_days_df = mysql_utils.execute_query(
            user=origin_user, password=origin_password, host=origin_host,
            database=origin_database, sql=trading_days_sql
        )

        if trading_days_df.empty:
            logging.warning(f"未找到 {start_ymd} 到 {end_ymd} 之间的交易日")
            return

        trading_days = trading_days_df['ymd'].tolist()
        total_days = len(trading_days)
        logging.info(f"共需回填 {total_days} 个交易日")

        logging.info("步骤2/4：读取全部 event 数据...")

        event_sql = """
            SELECT stock_code, ymd, load_time, total_sh, qoq_sh, mom_sh
            FROM quant.dwd_shareholder_num_event
            ORDER BY stock_code, ymd;
        """
        event_df = mysql_utils.execute_query(
            user=origin_user, password=origin_password, host=origin_host,
            database=origin_database, sql=event_sql
        )

        if event_df.empty:
            logging.warning("event 表无数据")
            return

        logging.info(f"读取完成：{len(event_df)} 条 event 记录")

        # 转换日期类型
        event_df['ymd'] = pd.to_datetime(event_df['ymd'])
        event_df['load_time'] = pd.to_datetime(event_df['load_time'])

        logging.info("步骤3/4：按交易日生成快照（向量化计算）...")

        all_daily = []

        for i, trade_day in enumerate(trading_days, 1):
            trade_dt = pd.to_datetime(trade_day)

            # 找每个股票最新的一条（load_time <= 当前交易日，ymd 最新）
            # 先过滤 load_time <= 当前交易日
            available = event_df[event_df['load_time'] <= trade_dt].copy()

            if available.empty:
                continue

            # 按股票分组，取 ymd 最新的一条
            idx = available.groupby('stock_code')['ymd'].idxmax()
            latest = available.loc[idx].copy()

            # 计算新鲜度
            latest['ymd'] = trade_dt  # 当前交易日
            latest['days_since_load'] = (trade_dt - latest['load_time']).dt.days
            latest['is_fresh'] = (latest['days_since_load'] <= 3).astype(int)
            latest['fresh_weight'] = np.exp(-latest['days_since_load'] / 10)
            latest['surprise_weighted'] = latest['qoq_sh'] * latest['fresh_weight']

            # 横截面分位（按 qoq_sh 排序）
            latest['sh_pctile_cs'] = latest['qoq_sh'].rank(pct=True)

            # 选择输出列
            daily_df = latest[[
                'ymd', 'stock_code', 'load_time', 'total_sh', 'qoq_sh', 'mom_sh',
                'days_since_load', 'is_fresh', 'fresh_weight', 'surprise_weighted', 'sh_pctile_cs'
            ]].copy()

            # 重命名列以匹配表结构
            daily_df.columns = [
                'ymd', 'stock_code', 'latest_load_time', 'latest_total_sh',
                'latest_qoq_sh', 'latest_mom_sh', 'days_since_load', 'is_fresh',
                'fresh_weight', 'surprise_weighted', 'sh_pctile_cs'
            ]

            all_daily.append(daily_df)

            if i % 50 == 0 or i == total_days:
                logging.info(f"已处理 {i}/{total_days} 个交易日...")

        if not all_daily:
            logging.warning("无数据可写入")
            return

        final_df = pd.concat(all_daily, ignore_index=True)

        # 转换日期格式
        final_df['ymd'] = final_df['ymd'].dt.strftime('%Y-%m-%d')
        final_df['latest_load_time'] = final_df['latest_load_time'].dt.strftime('%Y-%m-%d')

        logging.info(f"计算完成：{len(final_df)} 条 daily 记录")

        logging.info("步骤4/4：清空目标范围并批量写入...")

        delete_sql = f"""
            DELETE FROM quant.dwd_shareholder_num_daily 
            WHERE ymd BETWEEN '{start_ymd}' AND '{end_ymd}';
        """
        mysql_utils.execute_sql_statements(
            user=origin_user, password=origin_password, host=origin_host,
            database=origin_database, sql_statements=[delete_sql]
        )

        mysql_utils.data_from_dataframe_to_mysql(
            user=origin_user, password=origin_password, host=origin_host,
            database=origin_database, df=final_df,
            table_name="dwd_shareholder_num_daily",
            merge_on=['ymd', 'stock_code']
        )

        logging.info(f"完成！共写入 {len(final_df)} 条 daily 记录")


    @timing_decorator
    def cal_stock_base_info(self, ymd=None):
        """
        计算股票基础信息，汇总表，写入 dwd_ashare_stock_base_info
        修复：
          1. 市值/股本单位：元 -> 亿
          2. 市值数据：按股票取最近可用日期（<=当前交易日），避免前视
          3. 股票名称：按股票取各自最新一天的名称（修复退市/停牌股关联不上导致的 NULL）
        """
        if ymd is None:
            ymd = DateUtility.today()

        sql_statements_template = [
            """
            DELETE FROM quant.dwd_ashare_stock_base_info WHERE ymd = '{ymd}';
            """,
            """
            INSERT IGNORE INTO quant.dwd_ashare_stock_base_info 
            SELECT 
                tkline.ymd,
                tkline.stock_code,
                tcode.stock_name,
                tkline.close,
                tkline.change_pct,
                tkline.volume,
                tkline.trading_amount,
                ROUND(IFNULL(tpepb.circulation_market, 0) / 100000000, 2) AS market_value,
                ROUND(IFNULL(tpepb.total_market, 0) / 100000000, 2)       AS total_value,
                ROUND(IFNULL(tpepb.total_shares, 0) / 100000000, 2)       AS total_capital,
                ROUND(IFNULL(tpepb.circulation_shares, 0) / 100000000, 2) AS float_capital,
                tshare.latest_total_sh                              AS shareholder_num,
                tshare.latest_qoq_sh                                AS pct_of_total_sh,
                IFNULL(tpepb.pb, 0)                                 AS pb,
                IFNULL(tpepb.pe_ttm, 0)                             AS pe,
                texchange.market                                    AS market,
                tplate.plate_names                                  AS plate_names
            FROM (
                SELECT 
                    stock_code, stock_code_pure, ymd, close, change_pct, volume, trading_amount
                FROM quant.ods_stock_kline_daily_ts
                WHERE ymd = '{ymd}'
            ) tkline
            -- 修复3：按股票取各自最新一天的名称
            LEFT JOIN (
                SELECT a.ymd, a.stock_code, a.stock_name
                FROM quant.ods_stock_code_daily_insight a
                INNER JOIN (
                    SELECT stock_code, MAX(ymd) AS max_ymd
                    FROM quant.ods_stock_code_daily_insight
                    GROUP BY stock_code
                ) b ON a.stock_code = b.stock_code AND a.ymd = b.max_ymd
            ) tcode
                ON tkline.stock_code = tcode.stock_code
            -- 修复2：按股票取最近一个有市值数据的日期
            LEFT JOIN (
                SELECT a.ymd, a.stock_code, a.total_market, a.circulation_market, 
                       a.total_shares, a.circulation_shares, a.pe_ttm, a.pb, a.peg
                FROM quant.ods_akshare_stock_value_em a
                INNER JOIN (
                    SELECT stock_code, MAX(ymd) as max_ymd
                    FROM quant.ods_akshare_stock_value_em
                    WHERE ymd <= '{ymd}'
                    GROUP BY stock_code
                ) b ON a.stock_code = b.stock_code AND a.ymd = b.max_ymd
            ) tpepb
                ON tkline.stock_code_pure = tpepb.stock_code      
            LEFT JOIN (
                SELECT ymd, stock_code, latest_total_sh, latest_qoq_sh
                FROM quant.dwd_shareholder_num_daily
                WHERE ymd = '{ymd}'
            ) tshare
                ON tkline.stock_code = tshare.stock_code
            LEFT JOIN (
                SELECT ymd, stock_code, market
                FROM quant.ods_stock_exchange_market
                WHERE ymd = (SELECT MAX(ymd) FROM quant.ods_stock_exchange_market)
            ) texchange
                ON tkline.stock_code = texchange.stock_code
            LEFT JOIN (
                SELECT ymd, stock_code_pure,
                       GROUP_CONCAT(board_name ORDER BY board_name SEPARATOR ',') AS plate_names
                FROM quant.dwd_stock_a_total_plate
                WHERE ymd = (SELECT MAX(ymd) FROM quant.dwd_stock_a_total_plate)
                GROUP BY ymd, stock_code_pure                      
            ) tplate
                ON tkline.stock_code_pure = tplate.stock_code_pure;  
            """
        ]

        sql_statements = [stmt.format(ymd=ymd) for stmt in sql_statements_template]

        mysql_utils.execute_sql_statements(
            user=origin_user,
            password=origin_password,
            host=origin_host,
            database=origin_database,
            sql_statements=sql_statements
        )


    @timing_decorator
    def cal_stock_base_info_batch(self, start_ymd='20240801', end_ymd=None):
        """
        批量重跑 dwd_ashare_stock_base_info
        修复：市值单位已改为亿，历史数据需要重跑才能生效
        """
        if end_ymd is None:
            end_ymd = DateUtility.today()

        trading_days_sql = f"""
            SELECT ymd 
            FROM quant.ods_trading_days_insight 
            WHERE ymd >= '{start_ymd}' AND ymd <= '{end_ymd}' 
            ORDER BY ymd;
        """

        trading_days_df = mysql_utils.execute_query(
            user=origin_user,
            password=origin_password,
            host=origin_host,
            database=origin_database,
            sql=trading_days_sql
        )

        if trading_days_df.empty:
            logging.warning(f"未找到 {start_ymd} 到 {end_ymd} 之间的交易日")
            return

        trading_days = trading_days_df['ymd'].astype(str).tolist()
        total_days = len(trading_days)
        logging.info(f"共需处理 {total_days} 个交易日，范围：{start_ymd} ~ {end_ymd}")

        success_count = 0
        fail_count = 0
        fail_days = []

        for idx, day_ymd in enumerate(trading_days, 1):
            logging.info(f"【{idx}/{total_days}】正在处理日期：{day_ymd}")
            try:
                self.cal_stock_base_info(day_ymd)
                success_count += 1
            except Exception as e:
                fail_count += 1
                fail_days.append(day_ymd)
                logging.error(f"日期 {day_ymd} 处理失败：{str(e)}")
                continue

        logging.info(f"批量重跑完成：成功 {success_count} 天，失败 {fail_count} 天")
        if fail_days:
            logging.warning(f"失败日期：{fail_days}")

        return {
            'total': total_days,
            'success': success_count,
            'fail': fail_count,
            'fail_days': fail_days
        }


    @timing_decorator
    def cal_ZT_DT(self):
        """
        计算一只股票是否 涨停 / 跌停
        写入  dwd_stock_zt_list
             dwd_stock_dt_list
        """
        import time
        start_time = time.time()

        time_start_date = DateUtility.first_day_of_month()
        time_end_date = DateUtility.today()

        logging.info("=" * 60)
        logging.info(f"开始计算涨跌停数据，日期范围: {time_start_date} 至 {time_end_date}")
        logging.info("=" * 60)

        logging.info(f"【步骤1/7】正在从 ods_stock_kline_daily_ts 读取K线数据...")
        step_start = time.time()

        df = mysql_utils.data_from_mysql_to_dataframe(
            user=origin_user, password=origin_password, host=origin_host,
            database=origin_database,
            table_name='ods_stock_kline_daily_ts',
            start_date=time_start_date, end_date=time_end_date)

        step_time = time.time() - step_start
        logging.info(f"[完成] 读取完成，获取到 {len(df)} 条K线记录，耗时: {step_time:.2f}秒")

        if df.empty:
            logging.warning(f"[警告] {time_start_date} - {time_end_date} 日期的K线数据为空，终止运行！")
            return

        logging.info(f"【步骤2/7】正在对K线数据进行排序和计算昨收价...")
        step_start = time.time()

        unique_stocks = df['stock_code'].nunique()
        unique_dates = df['ymd'].nunique()
        logging.info(f"   - 涉及股票数量: {unique_stocks} 只")
        logging.info(f"   - 涉及交易日: {unique_dates} 天")

        latest_15_days = df.sort_values(by=['stock_code', 'ymd'])
        latest_15_days['last_close'] = latest_15_days.groupby('stock_code')['close'].shift(1)

        before_drop = len(latest_15_days)
        latest_15_days = latest_15_days.dropna(subset=['last_close'])
        after_drop = len(latest_15_days)

        step_time = time.time() - step_start
        logging.info(f"[完成] 预处理完成，删除了 {before_drop - after_drop} 条无昨收数据的记录")
        logging.info(f"  剩余 {after_drop} 条有效记录，耗时: {step_time:.2f}秒")

        if latest_15_days.empty:
            logging.warning(f"[警告] {time_start_date} - {time_end_date} 日期的日期差值时间为空，终止运行！")
            return

        logging.info(f"【步骤3/7】正在获取股票基础信息...")
        step_start = time.time()

        stock_market_init = mysql_utils.data_from_mysql_to_dataframe_latest(
            user=origin_user, password=origin_password, host=origin_host,
            database=origin_database, table_name='dwd_ashare_stock_base_info')

        step_time = time.time() - step_start
        logging.info(f"[完成] 获取到 {len(stock_market_init)} 条股票基础信息，耗时: {step_time:.2f}秒")

        if not stock_market_init.empty:
            latest_date = stock_market_init['ymd'].max() if 'ymd' in stock_market_init.columns else '未知'
            logging.info(f"   - 基础信息最新日期: {latest_date}")
            logging.info(f"   - 股票代码示例: {stock_market_init['stock_code'].head(3).tolist()}")

        stock_base_info = stock_market_init[['stock_code', 'stock_name', 'market_value', 'total_value',
                                             'total_capital', 'float_capital', 'shareholder_num',
                                             'pb', 'pe', 'market', 'plate_names']]

        logging.info(f"【步骤4/7】正在合并K线数据和股票基础信息...")
        step_start = time.time()

        latest_15_days = latest_15_days[['ymd', 'stock_code', 'close', 'last_close']]
        logging.info(f"   - K线数据中的股票代码示例: {latest_15_days['stock_code'].head(3).tolist()}")

        latest_15_days = pd.merge(
            latest_15_days,
            stock_base_info,
            on='stock_code',
            how='left'
        )

        step_time = time.time() - step_start
        logging.info(f"[完成] 合并完成，结果数据量: {len(latest_15_days)} 条，耗时: {step_time:.2f}秒")

        missing_names = latest_15_days['stock_name'].isna().sum()
        missing_percent = (missing_names / len(latest_15_days)) * 100
        logging.info(f"   - 股票名称缺失: {missing_names} 条 ({missing_percent:.2f}%)")

        if missing_names > 0:
            missing_stocks = latest_15_days[latest_15_days['stock_name'].isna()]['stock_code'].unique()[:5]
            logging.info(f"   - 缺失信息的股票代码示例: {missing_stocks.tolist()}")

        logging.info(f"【步骤5/7】正在计算涨跌停价格...")
        step_start = time.time()

        market_counts = latest_15_days['market'].value_counts()
        logging.info(f"   - 市场类型分布: {dict(market_counts)}")

        def calculate_ZT_DT(row):
            if pd.isna(row['market']):
                up_limit = row['last_close'] * 1.10
                down_limit = row['last_close'] * 0.90
            elif row['market'] in ['创业板', '科创板']:
                up_limit = row['last_close'] * 1.20
                down_limit = row['last_close'] * 0.80
            else:
                up_limit = row['last_close'] * 1.10
                down_limit = row['last_close'] * 0.90
            return pd.Series([up_limit, down_limit])

        latest_15_days[['昨日ZT价', '昨日DT价']] = latest_15_days.apply(
            calculate_ZT_DT, axis=1, result_type='expand')

        step_time = time.time() - step_start
        logging.info(f"[完成] 涨跌停价格计算完成，耗时: {step_time:.2f}秒")

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

        latest_15_days['是否涨停'] = latest_15_days.apply(
            lambda row: ZT_DT_orz(row['close'], row['昨日ZT价']), axis=1)
        latest_15_days['是否跌停'] = latest_15_days.apply(
            lambda row: ZT_DT_orz(row['close'], row['昨日DT价']), axis=1)

        step_time = time.time() - step_start
        logging.info(f"[完成] 涨跌停判断完成，耗时: {step_time:.2f}秒")

        logging.info(f"【步骤7/7】正在筛选和保存结果...")
        step_start = time.time()

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

            zt_dates = zt_df['ymd'].value_counts().sort_index()
            logging.info(f"   - 涨停日期分布: {dict(list(zt_dates.head().items()))}...")

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

            dt_dates = dt_df['ymd'].value_counts().sort_index()
            logging.info(f"   - 跌停日期分布: {dict(list(dt_dates.head().items()))}...")

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

        total_time = time.time() - start_time
        logging.info("=" * 60)
        logging.info(f"【处理完成】总耗时: {total_time:.2f}秒")
        logging.info(f"   - 处理总记录数: {len(latest_15_days)} 条")
        logging.info(f"   - 涨停记录: {zt_count} 条")
        logging.info(f"   - 跌停记录: {dt_count} 条")
        if zt_count > 0 or dt_count > 0:
            logging.info(f"   - 涨跌停合计: {zt_count + dt_count} 条")
        logging.info("=" * 60)

        logging.info(f"【数据质量检查】")
        logging.info(f"   - 股票名称匹配率: {(1 - missing_percent / 100) * 100:.2f}%")
        if missing_names > 0:
            logging.info(f"   - 建议检查缺失的股票代码，可能需要更新基础信息表")


    @timing_decorator
    def cal_technical_indicators(self):
        """
        计算股票技术指标（均线等）并存入 dwd_stock_technical_indicators 表
        """
        try:
            start_date = DateUtility.first_day_of_month()
            end_date = DateUtility.today()

            start_dt = pd.to_datetime(start_date)
            query_start = (start_dt - pd.Timedelta(days=400)).strftime('%Y%m%d')

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

            stock_name_map = self.stocks_df.set_index('stock_code')['stock_name'].to_dict()

            kline_df = kline_df.sort_values(['stock_code', 'ymd'])
            kline_df['ymd'] = pd.to_datetime(kline_df['ymd'])

            result_dfs = []

            for stock, stock_df in kline_df.groupby('stock_code'):
                stock_df = stock_df.copy()
                stock_df = stock_df.sort_values('ymd')
                stock_df['stock_name'] = stock_name_map.get(stock, '')

                stock_df['ma5'] = stock_df['close'].rolling(window=5, min_periods=5).mean()
                stock_df['ma10'] = stock_df['close'].rolling(window=10, min_periods=10).mean()
                stock_df['ma20'] = stock_df['close'].rolling(window=20, min_periods=20).mean()
                stock_df['ma60'] = stock_df['close'].rolling(window=60, min_periods=60).mean()
                stock_df['ma120'] = stock_df['close'].rolling(window=120, min_periods=120).mean()
                stock_df['ma250'] = stock_df['close'].rolling(window=250, min_periods=250).mean()

                stock_df['vol_ma5'] = stock_df['volume'].rolling(window=5, min_periods=5).mean()
                stock_df['vol_ma10'] = stock_df['volume'].rolling(window=10, min_periods=10).mean()
                stock_df['vol_ma20'] = stock_df['volume'].rolling(window=20, min_periods=20).mean()
                stock_df['vol_ma60'] = stock_df['volume'].rolling(window=60, min_periods=60).mean()
                stock_df['vol_ma120'] = stock_df['volume'].rolling(window=120, min_periods=120).mean()
                stock_df['vol_ma250'] = stock_df['volume'].rolling(window=250, min_periods=250).mean()

                stock_df['price_vs_ma5'] = ((stock_df['close'] / stock_df['ma5'] - 1) * 100).round(2)
                stock_df['price_vs_ma20'] = ((stock_df['close'] / stock_df['ma20'] - 1) * 100).round(2)
                stock_df['price_vs_ma60'] = ((stock_df['close'] / stock_df['ma60'] - 1) * 100).round(2)

                stock_df['volume_vs_ma5'] = ((stock_df['volume'] / stock_df['vol_ma5'] - 1) * 100).round(2)
                stock_df['volume_vs_ma20'] = ((stock_df['volume'] / stock_df['vol_ma20'] - 1) * 100).round(2)

                keep_cols = ['ymd', 'stock_code', 'stock_name', 'close', 'volume',
                             'ma5', 'ma10', 'ma20', 'ma60', 'ma120', 'ma250',
                             'vol_ma5', 'vol_ma10', 'vol_ma20', 'vol_ma60', 'vol_ma120', 'vol_ma250',
                             'price_vs_ma5', 'price_vs_ma20', 'price_vs_ma60',
                             'volume_vs_ma5', 'volume_vs_ma20']

                result_dfs.append(stock_df[keep_cols])

            all_df = pd.concat(result_dfs, ignore_index=True)
            all_df = all_df[all_df['ymd'].between(pd.to_datetime(start_date),
                                                  pd.to_datetime(end_date))]

            if all_df.empty:
                logging.warning(f"没有需要的数据: {start_date}~{end_date}")
                return pd.DataFrame()

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


    @script_run(script_name="calculate_DWD_datas.py")
    def setup(self):

        # 聚合股票的板块，把各个板块数据聚合在一起   周末手动执行
        self.cal_ashare_plate()

        # 计算股票所归属的交易所，判断其是主办、创业板、科创板、北交所等等
        self.cal_stock_exchange()

        # 股东数事件表（对齐环比 + 紧邻环比 + 新鲜度）
        self.cal_shareholder_num_event()

        # 股东数每日宽表（策略直接消费）
        self.cal_shareholder_num_daily()

        # 计算股票基础信息，汇总表，名称、编码、板块、股本、市值、净资产
        self.cal_stock_base_info()

        # 计算一只股票是否 涨停 / 跌停
        self.cal_ZT_DT()

        # 计算行情衍生指标  均线等
        self.cal_technical_indicators()

        # # 补录 base_info 的历史数据
        # self.cal_stock_base_info_batch()


if __name__ == '__main__':
    save_insight_data = CalDWD()

    # ===== 首次初始化（跑一次后注释掉）=====
    # # 第一步：回填 event 表历史数据
    # save_insight_data.cal_shareholder_num_event_batch('20260918')

    # # 第二步：回填 daily 宽表历史数据
    # save_insight_data.cal_shareholder_num_daily_batch('20240801')

    # save_insight_data.cal_stock_base_info_batch()

    # ===== 日常调度（每天跑）=====
    save_insight_data.setup()
