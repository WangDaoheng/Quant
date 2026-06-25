# -*- coding: utf-8 -*-

import sys
import io
import numpy as np
from insight_python.com.insight import common
from insight_python.com.insight.query import *
from insight_python.com.insight.market_service import market_service
from datetime import datetime
import contextlib

import time
import logging
import platform

import CommonProperties.Base_Properties as base_properties
import CommonProperties.Mysql_Utils as mysql_utils
from CommonProperties.DateUtility import DateUtility
from CommonProperties.Base_utils import timing_decorator, script_run
from CommonProperties import set_config


# ************************************************************************
#  调用日志配置
set_config.setup_logging_config()
#  调用mysql日志配置
local_user = base_properties.local_mysql_user
local_password = base_properties.local_mysql_password
local_database = base_properties.local_mysql_database
local_host = base_properties.local_mysql_host

origin_user = base_properties.origin_mysql_user
origin_password = base_properties.origin_mysql_password
origin_database = base_properties.origin_mysql_database
origin_host = base_properties.origin_mysql_host
# ************************************************************************

class SaveInsightData:

    def __init__(self):
        """
        结果变量初始化
        """
        #  除去 ST|退|B 的五要素   [ymd	stock_code	stock_name	exchange]
        self.stock_code_df = pd.DataFrame()


    # @timing_decorator
    def login(self):
        # 登陆前 初始化，没有密码可以访问进行自动化注册
        # https://findata-insight.htsc.com:9151/terminalWeb/#/signup
        user = base_properties.user
        password = base_properties.password
        common.login(market_service, user, password)


    @timing_decorator
    def get_stock_codes(self):
        """
        获取当日的stock代码合集
        :return: 写入 ods_stock_code_daily_insight
         stock_code_df  [ymd	stock_code	stock_name	exchange]
        """

        #  1.获取日期
        formatted_date = DateUtility.today()
        # formatted_date = '20240930'

        #  2.请求insight数据   get_all_stocks_info
        stock_all_df = get_all_stocks_info(listing_state="上市交易")
        #  3.日期格式转换
        stock_all_df.insert(0, 'ymd', formatted_date)

        #  4.声明所有的列名，去除多余列
        stock_all_df = stock_all_df[['ymd', 'htsc_code', 'name', 'exchange']]
        filtered_df = stock_all_df[~stock_all_df['name'].str.contains('ST|退|B')]

        #  5.删除重复记录，只保留每组 (ymd, stock_code) 中的第一个记录
        filtered_df = filtered_df.drop_duplicates(subset=['ymd', 'htsc_code'], keep='first')

        filtered_df = filtered_df.rename(columns={'htsc_code': 'stock_code', 'name': 'stock_name'})

        #  6.更新dataframe ymd  htsc_code  name  exchange
        self.stock_code_df = filtered_df

        ############################   文件输出模块     ############################
        # 远端数据库总是保存
        mysql_utils.data_from_dataframe_to_mysql(
            user=origin_user,
            password=origin_password,
            host=origin_host,
            database=origin_database,
            df=filtered_df,
            table_name="ods_stock_code_daily_insight",
            merge_on=['ymd', 'stock_code']
        )

    @timing_decorator
    def get_limit_summary(self):
        """
        大盘涨跌停分析数据
        Returns: 写入 ods_stock_limit_summary_insight_now
                 'today_ZT', 'today_DT', 'yesterday_ZT', 'yesterday_DT', 'yesterday_ZT_rate'
                 [time	name	今日涨停	今日跌停	昨日涨停	昨日跌停	昨日涨停今日表现]
        """

        #  1.当月数据的起止时间
        start_date = DateUtility.first_day_of_month()
        end_date = DateUtility.today()

        start_date = datetime.strptime(start_date, '%Y%m%d')
        end_date = datetime.strptime(end_date, '%Y%m%d')

        #  2.请求insight数据   get_kline
        res = get_change_summary(market=["a_share"], trading_day=[start_date, end_date])

        #  3.limit_summary 的总和dataframe
        limit_summary_df = pd.DataFrame()
        limit_summary_df = pd.concat([limit_summary_df, res], ignore_index=True)

        ##  insight 返回值的非空判断
        if not limit_summary_df.empty:
            #  4.声明所有的列名，去除多余列
            limit_summary_df = limit_summary_df[['time',
                                                 'name',
                                                 'ups_downs_limit_count_up_limits',
                                                 'ups_downs_limit_count_down_limits',
                                                 'ups_downs_limit_count_pre_up_limits',
                                                 'ups_downs_limit_count_pre_down_limits',
                                                 'ups_downs_limit_count_pre_up_limits_average_change_percent']]
            limit_summary_df.columns = ['ymd', 'name', 'today_ZT', 'today_DT', 'yesterday_ZT', 'yesterday_DT',
                                        'yesterday_ZT_rate']

            #  5.日期格式转换
            limit_summary_df['ymd'] = pd.to_datetime(limit_summary_df['ymd']).dt.strftime('%Y%m%d')

            #  6.删除重复记录，只保留每组 (ymd, stock_code) 中的第一个记录
            limit_summary_df = limit_summary_df.drop_duplicates(subset=['ymd', 'name'], keep='first')

            ############################   文件输出模块     ############################
            # 总是保存到远端数据库
            mysql_utils.data_from_dataframe_to_mysql(
                user=origin_user,
                password=origin_password,
                host=origin_host,
                database=origin_database,
                df=limit_summary_df,
                table_name="ods_stock_limit_summary_insight_now",
                merge_on=['ymd', 'name']
            )

        else:
            ## insight 返回为空值
            logging.info('    get_limit_summary 的返回值为空值')


    @timing_decorator
    def get_stock_kline(self):
        """
        根据当日上市的stock_codes，来获得全部(去除ST|退|B)股票的历史数据
        :return: 【已废弃】
         stock_kline_df  [ymd	htsc_code	name	exchange]
        """

        # 1. 获取今天日期
        today = DateUtility.today()

        # 2. 计算当月15天前的日期
        if int(today[6:8]) > 15:
            time_start_date = DateUtility.next_day(-15)  # 15天前
        else:
            time_start_date = DateUtility.first_day_of_month()  # 当月1号

        # 3. 设置结束日期为今天
        time_end_date = today

        # time_start_date = '20241026'
        # time_end_date = '20241026'

        time_start_date = datetime.strptime(time_start_date, '%Y%m%d')
        time_end_date = datetime.strptime(time_end_date, '%Y%m%d')

        #  2.每个批次取 100 个元素
        batch_size = 100

        #  3.这是一个切分批次的内部函数
        def get_batches(lst, batch_size):
            for start in range(0, len(lst), batch_size):
                yield lst[start:start + batch_size]

        #  4.获取最新的stock_code 的list
        stock_code_list = mysql_utils.get_stock_codes_latest()['stock_code'].tolist()

        #  5.计算总批次数
        total_batches = (len(stock_code_list) + batch_size - 1) // batch_size

        #  6.kline的总和dataframe
        kline_total_df = pd.DataFrame()

        #  7.请求insight数据  get_kline
        for i, batch_list in enumerate(get_batches(stock_code_list, batch_size), start=1):
            #  一种非常巧妙的循环打印日志的方式
            sys.stdout.write(f"\r当前执行get_stock_kline的 第 {i} 次循环，总共 {total_batches} 个批次")
            sys.stdout.flush()
            time.sleep(0.01)

            res = get_kline(htsc_code=batch_list, time=[time_start_date, time_end_date], frequency="daily", fq="pre")
            kline_total_df = pd.concat([kline_total_df, res], ignore_index=True)

        sys.stdout.write("\n")

        ##  insight 返回值的非空判断
        if not kline_total_df.empty:

            #  8.日期格式转换
            kline_total_df['time'] = pd.to_datetime(kline_total_df['time']).dt.strftime('%Y%m%d')
            kline_total_df.rename(columns={'time': 'ymd', 'htsc_code': 'stock_code'}, inplace=True)

            #  9.声明所有的列名，去除多余列
            kline_total_df = kline_total_df[
                ['stock_code', 'ymd', 'open', 'close', 'high', 'low', 'num_trades', 'volume']]

            #  10.删除重复记录，只保留每组 (ymd, stock_code) 中的第一个记录
            kline_total_df = kline_total_df.drop_duplicates(subset=['ymd', 'stock_code'], keep='first')

            ############################   文件输出模块     ############################
            # 总是保存到远端数据库
            mysql_utils.data_from_dataframe_to_mysql(
                user=origin_user,
                password=origin_password,
                host=origin_host,
                database=origin_database,
                df=kline_total_df,
                table_name="ods_stock_kline_daily_insight_now",
                merge_on=['ymd', 'stock_code']
            )

        else:
            ## insight 返回为空值
            logging.info('    get_stock_kline 的返回值为空值')


    @timing_decorator
    def get_chouma_datas(self):
        """
        1.获取每日的筹码分布数据
        2.找到那些当日能够拿到筹码数据的codes
        :return: 写入 ods_stock_chouma_insight
        """
        #  1.起止时间 查询起始时间写本月月初
        time_start_date = DateUtility.first_day_of_month()
        #  结束时间必须大于等于当日，这里取明天的日期
        time_end_date = DateUtility.next_day(1)

        time_start_date = datetime.strptime(time_start_date, '%Y%m%d')
        time_end_date = datetime.strptime(time_end_date, '%Y%m%d')

        #  2.每个批次取 100 个元素（原代码是1，保留你的配置）
        batch_size = 1

        #  3.这是一个切分批次的内部函数
        def get_batches(lst, batch_size):
            for start in range(0, len(lst), batch_size):
                yield lst[start:start + batch_size]

        #  4.获取最新的stock_code_list
        stock_code_list = mysql_utils.get_stock_codes_latest()['stock_code'].tolist()

        #  5.计算总批次数
        total_batches = (len(stock_code_list) + batch_size - 1) // batch_size

        #  6.chouma 的总和dataframe
        chouma_total_df = pd.DataFrame()

        # ========== 新增：统计变量 ==========
        failed_codes = []      # 失败代码
        failed_reasons = []    # 失败原因
        empty_codes = []       # 返回空数据的代码
        slow_codes = []        # 慢查询记录 (code, 耗时秒)

        #  7.调用insight数据  get_chip_distribution
        for i, code_list in enumerate(get_batches(stock_code_list, batch_size), start=1):
            #  每100个打印一次进度（改用logging，确保写入日志文件）
            if i % 100 == 0 or i == 1:
                logging.info(f"【进度】第 {i}/{total_batches} 批次，当前成功: {chouma_total_df.shape[0]} 条，正在处理: {code_list}")

            batch_start = time.time()

            try:
                res = get_chip_distribution(htsc_code=code_list, trading_day=[time_start_date, time_end_date])

                # 返回空数据的判断
                if res is None or res.empty:
                    empty_codes.extend(code_list)
                    logging.warning(f"【空数据】代码 {code_list} 返回空数据")
                    continue

                chouma_total_df = pd.concat([chouma_total_df, res], ignore_index=True)

            except Exception as e:
                failed_codes.extend(code_list)
                failed_reasons.append(f"{code_list}: {str(e)}")
                logging.error(f"【失败】代码 {code_list} 获取筹码数据失败: {e}")
                continue

            # 记录慢查询
            batch_elapsed = time.time() - batch_start
            if batch_elapsed > 10:  # 超过10秒算慢
                slow_codes.append((code_list[0], batch_elapsed))  # code_list是列表，取第一个
                logging.warning(f"【慢查询】代码 {code_list} 耗时 {batch_elapsed:.2f} 秒")

            time.sleep(0.01)

        # ========== 循环结束后：汇总日志 + 明细落文件 ==========
        logging.info("=" * 60)
        logging.info(f"【筹码数据获取完成】总代码数: {len(stock_code_list)}")
        logging.info(f"【筹码数据获取完成】成功获取: {chouma_total_df.shape[0]} 条")
        logging.info(f"【筹码数据获取完成】返回空数据: {len(empty_codes)} 个代码")
        logging.info(f"【筹码数据获取完成】失败: {len(failed_codes)} 个代码")
        if slow_codes:
            logging.info(f"【筹码数据获取完成】慢查询: {len(slow_codes)} 个")

        # 慢查询前10个
        if slow_codes:
            slow_sorted = sorted(slow_codes, key=lambda x: x[1], reverse=True)[:10]
            logging.warning(f"【最慢10个】{slow_sorted}")

        # ========== 明细落文件（固定目录 /opt/Logs） ==========
        output_dir = "/opt/Logs"
        os.makedirs(output_dir, exist_ok=True)

        # 失败明细落文件
        if failed_codes:
            failed_file = os.path.join(output_dir, f"failed_chouma_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt")
            with open(failed_file, 'w', encoding='utf-8') as f:
                f.write("stock_code\treason\n")
                for code, reason in zip(failed_codes, failed_reasons):
                    f.write(f"{code}\t{reason}\n")
            logging.info(f"【失败明细】已写入 {failed_file}，共 {len(failed_codes)} 条")

        # 空数据明细落文件
        if empty_codes:
            empty_file = os.path.join(output_dir, f"empty_chouma_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt")
            with open(empty_file, 'w', encoding='utf-8') as f:
                for code in empty_codes:
                    f.write(f"{code}\n")
            logging.info(f"【空数据明细】已写入 {empty_file}，共 {len(empty_codes)} 条")

        logging.info("=" * 60)

        ##  insight 返回值的非空判断
        if not chouma_total_df.empty:
            #  8.日期格式转换
            chouma_total_df['time'] = pd.to_datetime(chouma_total_df['time']).dt.strftime('%Y%m%d')
            chouma_total_df.rename(columns={'time': 'ymd',
                                            'total_share': 'total_shares',
                                            'last': 'close',
                                            'htsc_code': 'stock_code'}, inplace=True)

            #  9.数据格式调整
            cols_to_clean = ['close', 'prev_close', 'avg_cost', 'max_cost', 'min_cost', 'winner_rate', 'diversity',
                             'pre_winner_rate', 'restricted_avg_cost', 'restricted_max_cost', 'restricted_min_cost',
                             'large_shareholders_avg_cost', 'large_shareholders_total_share_pct']

            for col in cols_to_clean:
                chouma_total_df[col] = (
                    chouma_total_df[col]
                    .astype(str)
                    .replace({'': np.nan, 'nan': np.nan})
                    .pipe(lambda s: pd.to_numeric(s, errors='coerce'))
                    .fillna(0)
                    .apply(lambda x: round(x * 10000, 2) if x < 1 else x)
                )

            chouma_total_df[cols_to_clean] = chouma_total_df[cols_to_clean].apply(
                lambda s: s.map(lambda x: f"{x:.2f}")
            )
            ############################   文件输出模块     ############################
            # 总是保存到远端数据库
            mysql_utils.data_from_dataframe_to_mysql(
                user=origin_user,
                password=origin_password,
                host=origin_host,
                database=origin_database,
                df=chouma_total_df,
                table_name="ods_stock_chouma_insight",
                merge_on=['ymd', 'stock_code']
            )

        else:
            # insight 返回为空值
            logging.info('    get_chouma_datas 的返回值为空值')



    @timing_decorator
    def get_Ashare_industry_overview(self):
        """
        获取行业信息 申万三级 的行业信息
        :return: 写入 ods_astock_industry_overview
         industry_overview  ['ymd', 'classified', 'industry_name', 'industry_code', 'l1_code', 'l1_name', 'l2_code', 'l2_name', 'l3_code', 'l3_name']
        """
        #  1.当月数据的起止时间
        time_today = DateUtility.today()
        time_today = datetime.strptime(time_today, '%Y%m%d')

        #  2.行业信息的总和dataframe
        industry_df = pd.DataFrame()

        #  3.请求insight 上的申万三级行业 数据
        res = get_industries(classified='sw_l3')
        industry_df = pd.concat([industry_df, res], ignore_index=True)

        ##  insight 返回值的非空判断
        if not industry_df.empty:
            #  4.日期格式转换
            industry_df.insert(0, 'ymd', time_today)
            industry_df['ymd'] = pd.to_datetime(industry_df['ymd']).dt.strftime('%Y%m%d')

            #  5.声明所有的列名，去除多余列
            industry_df = industry_df[
                ['ymd', 'classified', 'industry_name', 'industry_code', 'l1_code', 'l1_name', 'l2_code', 'l2_name',
                 'l3_code', 'l3_name']]

            #  6.删除重复记录，只保留每组 (ymd, stock_code) 中的第一个记录
            industry_df = industry_df.drop_duplicates(subset=['ymd', 'industry_code'], keep='first')

            ############################   文件输出模块     ############################
            # 总是保存到远端数据库
            mysql_utils.data_from_dataframe_to_mysql(
                user=origin_user,
                password=origin_password,
                host=origin_host,
                database=origin_database,
                df=industry_df,
                table_name="ods_astock_industry_overview",
                merge_on=['ymd', 'industry_code']
            )

        else:
            ## insight 返回为空值
            logging.info('    get_Ashare_industry_overview 的返回值为空值')


    @timing_decorator
    def get_Ashare_industry_detail(self):
        """
        获取股票的行业信息 申万三级 的行业信息
        :return: 写入 ods_astock_industry_detail
         industry_detail  ['ymd', 'htsc_code', 'name', 'industry_name', 'industry_code', 'l1_code', 'l1_name', 'l2_code', 'l2_name', 'l3_code', 'l3_name']
        """

        # 如果今天不是周五，跳过逻辑
        if not DateUtility.is_friday():
            logging.info("今天不是周五，跳过行业信息获取逻辑。")
            return

        #  1.当月数据的起止时间
        time_today = DateUtility.today()
        time_today = datetime.strptime(time_today, '%Y%m%d')

        #  2.行业信息的总和dataframe
        stock_in_industry_df = pd.DataFrame()

        #  3.获取最新的 stock_code 数据
        index_list = mysql_utils.get_stock_codes_latest()['stock_code'].tolist()

        #  4.请求insight 上的申万三级行业 数据
        i = 1                                     # 总第 i个 循环标记
        total_batches = len(index_list)           # 总批次数量

        for stock_code in index_list:

            valid_num = stock_in_industry_df.shape[0]
            sys.stdout.write(f"\r当前执行 get_Ashare_industry_detail  第 {i} 次循环，总共 {total_batches} 个批次, {valid_num}个有效股票行业数据")
            sys.stdout.flush()
            time.sleep(0.03)

            res = get_industry(htsc_code=stock_code, classified='sw')
            stock_in_industry_df = pd.concat([stock_in_industry_df, res], ignore_index=True)

            i += 1

        sys.stdout.write("\n")

        ##  insight 返回值的非空判断
        if not stock_in_industry_df.empty:
            #  5.日期格式转换
            stock_in_industry_df.insert(0, 'ymd', time_today)
            stock_in_industry_df['ymd'] = pd.to_datetime(stock_in_industry_df['ymd']).dt.strftime('%Y%m%d')

            stock_in_industry_df.rename(columns={'htsc_code': 'stock_code', 'name': 'stock_name'}, inplace=True)

            #  6.声明所有的列名，去除多余列
            stock_in_industry_df = stock_in_industry_df[
                ['ymd', 'stock_code', 'stock_name', 'industry_name', 'industry_code', 'l1_code', 'l1_name', 'l2_code',
                 'l2_name', 'l3_code', 'l3_name']]

            #  7.删除重复记录，只保留每组 (ymd, stock_code) 中的第一个记录
            stock_in_industry_df = stock_in_industry_df.drop_duplicates(subset=['ymd', 'stock_code'], keep='first')

            ############################   文件输出模块     ############################
            # 总是保存到远端数据库
            mysql_utils.data_from_dataframe_to_mysql(
                user=origin_user,
                password=origin_password,
                host=origin_host,
                database=origin_database,
                df=stock_in_industry_df,
                table_name="ods_astock_industry_detail",
                merge_on=['ymd', 'stock_code']
            )
        else:
            ## insight 返回为空值
            logging.info('    get_Ashare_industry_detail 的返回值为空值')



    @script_run(script_name="download_insight_data_afternoon.py")
    def setup(self):
        #  登陆insight数据源
        self.login()

        #  除去 ST |  退  | B 的股票集合
        self.get_stock_codes()

        #  大盘涨跌概览
        self.get_limit_summary()

        # #  获取上述股票的当月日K  【已废弃，现使用 get_stock_kline_tushare() 】
        # self.get_stock_kline()

        # 筹码概览
        self.get_chouma_datas()

        # 获取A股的行业分类数据, 是行业数据
        self.get_Ashare_industry_overview()

        # 获取A股的行业分类数据, 是stock_code & industry 关联后的大表数据
        self.get_Ashare_industry_detail()





if __name__ == '__main__':
    downloader = SaveInsightData()
    downloader.setup()


