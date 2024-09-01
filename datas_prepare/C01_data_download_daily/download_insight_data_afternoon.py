import os
import sys
import io
from insight_python.com.insight import common
from insight_python.com.insight.query import *
from insight_python.com.insight.market_service import market_service
from datetime import datetime
import contextlib

import time
import logging


import CommonProperties.Base_Properties as base_properties
import CommonProperties.Base_utils as base_utils
import CommonProperties.Mysql_Utils as mysql_utils
from CommonProperties.DateUtility import DateUtility
from CommonProperties.Base_utils import timing_decorator
from CommonProperties import set_config

# ************************************************************************
# 本代码的作用是下午收盘后下载 insight 行情源数据, 本地保存,用于后续分析
# 需要下载的数据:
# 1.上市股票代码   get_all_stocks()
# 2.筹码分布数据   get_chouma_datas()


# ************************************************************************


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

        self.init_dirs()

        self.init_variant()

    def init_dirs(self):
        """
        关键路径初始化
        """
        #  文件路径_____insight文件基础路径
        self.dir_insight_base = base_properties.dir_insight_base

        #  文件路径_____上市交易股票codes
        self.dir_stock_codes_base = os.path.join(self.dir_insight_base, 'stock_codes')

        #  文件路径_____上市交易股票的日k线数据
        self.dir_stock_kline_base = os.path.join(self.dir_insight_base, 'stock_kline')

        #  文件路径_____关键大盘指数
        self.dir_index_a_share_base = os.path.join(self.dir_insight_base, 'index_a_share')

        #  文件路径_____涨跌停数量
        self.dir_limit_summary_base = os.path.join(self.dir_insight_base, 'limit_summary')

        #  文件路径_____内盘期货
        self.dir_future_inside_base = os.path.join(self.dir_insight_base, 'future_inside')

        #  文件路径_____筹码数据
        self.dir_chouma_base = os.path.join(self.dir_insight_base, 'chouma')

        #  文件路径_____行业分类数据_概览
        self.dir_industry_overview_base = os.path.join(self.dir_insight_base, 'industry_overview')

        #  文件路径_____行业分类数据_明细
        self.dir_industry_detail_base = os.path.join(self.dir_insight_base, 'industry_detail')

        #  文件路径_____个股的股东数_明细
        self.dir_shareholder_num_base = os.path.join(self.dir_insight_base, 'shareholder_num')

        #  文件路径_____北向持仓数据_明细
        self.dir_north_bound_base = os.path.join(self.dir_insight_base, 'north_bound')


    def init_variant(self):
        """
        结果变量初始化
        """
        #  除去 ST|退|B 的五要素   [ymd	htsc_code	name	exchange]
        self.stock_code_df = pd.DataFrame()

        #  上述stock_code 对应的日K
        self.stock_kline_df = pd.DataFrame()

        #  获得A股市场的股指 [htsc_code 	time	frequency	open	close	high	low	volume	value]
        self.index_a_share = pd.DataFrame()

        #  大盘涨跌停数量          [time	name	今日涨停	今日跌停	昨日涨停	昨日跌停	昨日涨停表现]
        self.limit_summary_df = pd.DataFrame()

        #  期货市场数据    原油  贵金属  有色
        self.future_index = pd.DataFrame()

        #  可以获取筹码的股票数据
        self.stock_chouma_available = pd.DataFrame()

        #  可以获取insight中的 行业分类 数据概览
        self.industry_overview = pd.DataFrame()

        #  可以获取insight中的 行业分类 数据明细
        self.industry_detail = pd.DataFrame()

        #  获取insight 中个股的 股东数
        self.shareholder_num_df = pd.DataFrame()

        #  获取insight 中北向的 持仓数据
        self.north_bound_df = pd.DataFrame()


    @timing_decorator
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
        :return:
         stock_code_df  [ymd	htsc_code	name	exchange]
        """

        #  1.获取日期
        formatted_date = DateUtility.today()

        #  2.请求insight数据   get_all_stocks_info
        stock_all_df = get_all_stocks_info(listing_state="上市交易")

        #  3.日期格式转换
        stock_all_df.insert(0, 'ymd', formatted_date)

        #  4.声明所有的列名，去除多余列
        stock_all_df = stock_all_df[['ymd', 'htsc_code', 'name', 'exchange']]
        filtered_df = stock_all_df[~stock_all_df['name'].str.contains('ST|退|B')]

        #  5.删除重复记录，只保留每组 (ymd, stock_code) 中的第一个记录
        filtered_df = filtered_df.drop_duplicates(subset=['ymd', 'htsc_code'], keep='first')

        #  6.更新dataframe ymd  htsc_code  name  exchange
        self.stock_code_df = filtered_df

        #  7.本地csv文件的落盘保存
        filehead = 'stocks_codes_all'
        stock_codes_listed_filename = base_utils.save_out_filename(filehead=filehead, file_type='csv')
        stock_codes_listed_dir = os.path.join(self.dir_stock_codes_base, stock_codes_listed_filename)
        filtered_df.to_csv(stock_codes_listed_dir, index=False)

        #  8.结果数据保存到 本地 mysql中
        mysql_utils.data_from_dataframe_to_mysql(user=local_user,
                                                 password=local_password,
                                                 host=local_host,
                                                 database=local_database,
                                                 df=filtered_df,
                                                 table_name="stock_code_daily_insight",
                                                 merge_on=['ymd', 'htsc_code'])

        #  9.结果数据保存到 远端 mysql中
        mysql_utils.data_from_dataframe_to_mysql(user=origin_user,
                                                 password=origin_password,
                                                 host=origin_host,
                                                 database=origin_database,
                                                 df=filtered_df,
                                                 table_name="stock_code_daily_insight",
                                                 merge_on=['ymd', 'htsc_code'])



    @timing_decorator
    def get_stock_kline(self):
        """
        根据当日上市的stock_codes，来获得全部(去除ST|退|B)股票的历史数据
        :return:
         stock_kline_df  [ymd	htsc_code	name	exchange]
        """

        #  1.当月数据的起止时间
        time_start_date = DateUtility.first_day_of_month()
        time_end_date = DateUtility.today()

        time_start_date = datetime.strptime(time_start_date, '%Y%m%d')
        time_end_date = datetime.strptime(time_end_date, '%Y%m%d')

        #  2.每个批次取 100 个元素
        batch_size = 100

        #  3.这是一个切分批次的内部函数
        def get_batches(lst, batch_size):
            for start in range(0, len(lst), batch_size):
                yield lst[start:start + batch_size]

        #  4.获取最新的stock_code 的list
        stock_code_list = mysql_utils.get_stock_codes_latest(self.stock_code_df)

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

        #  8.日期格式转换
        kline_total_df['time'] = pd.to_datetime(kline_total_df['time']).dt.strftime('%Y%m%d')
        kline_total_df.rename(columns={'time': 'ymd'}, inplace=True)

        #  9.声明所有的列名，去除多余列
        kline_total_df = kline_total_df[['htsc_code', 'ymd', 'open', 'close', 'high', 'low', 'num_trades', 'volume']]

        #  10.删除重复记录，只保留每组 (ymd, stock_code) 中的第一个记录
        kline_total_df = kline_total_df.drop_duplicates(subset=['ymd', 'htsc_code'], keep='first')

        #  11.更新dataframe
        self.stock_kline_df = kline_total_df

        #  12.本地csv文件的落盘保存
        stock_kline_filename = base_utils.save_out_filename(filehead='stock_kline', file_type='csv')
        stcok_kline_filedir = os.path.join(self.dir_stock_kline_base, stock_kline_filename)
        kline_total_df.to_csv(stcok_kline_filedir, index=False)

        #  13.结果数据保存到 本地 mysql中
        mysql_utils.data_from_dataframe_to_mysql(user=local_user,
                                                 password=local_password,
                                                 host=local_host,
                                                 database=local_database,
                                                 df=kline_total_df,
                                                 table_name="stock_kline_daily_insight_now",
                                                 merge_on=['ymd', 'htsc_code'])

        #  14.结果数据保存到 远端 mysql中
        mysql_utils.data_from_dataframe_to_mysql(user=origin_user,
                                                 password=origin_password,
                                                 host=origin_host,
                                                 database=origin_database,
                                                 df=kline_total_df,
                                                 table_name="stock_kline_daily_insight_now",
                                                 merge_on=['ymd', 'htsc_code'])


    @timing_decorator
    def get_index_a_share(self):
        """
        000001.SH    上证指数
        399002.SZ    深成指
        399006.SZ	 创业板指
        000016.SH    上证50
        000300.SH    沪深300
        000849.SH    沪深300非银行金融指数
        000905.SH	 中证500
        399852.SZ    中证1000
        000688.SH    科创50

        Returns:
             index_a_share   [htsc_code 	time	frequency	open	close	high	low	volume	value]
        """

        #  1.当月数据的起止时间
        start_date = DateUtility.first_day_of_month()
        end_date = DateUtility.today()

        start_date = datetime.strptime(start_date, '%Y%m%d')
        end_date = datetime.strptime(end_date, '%Y%m%d')

        #  2.查询标的
        index_dict = {"000001.SH": "上证指数"
            , "399002.SZ": "深成指"
            , "399006.SZ": "创业板指"
            , "000016.SH": "上证50"
            , "000300.SH": "沪深300"
            , "000849.SH": "300非银"
            , "000905.SH": "中证500"
            , "399852.SZ": "中证1000"
            , "000688.SH": "科创50"}
        index_list = list(index_dict.keys())

        #  3.index_a_share 的总和dataframe
        index_df = pd.DataFrame()

        #  4.请求insight数据   get_kline
        res = get_kline(htsc_code=index_list, time=[start_date, end_date],
                        frequency="daily", fq="pre")
        index_df = pd.concat([index_df, res], ignore_index=True)

        #  5.日期格式转换
        index_df['time'] = pd.to_datetime(index_df['time']).dt.strftime('%Y%m%d')
        index_df.rename(columns={'time': 'ymd'}, inplace=True)

        #  6.根据映射关系，添加stock_name
        index_df['name'] = index_df['htsc_code'].map(index_dict)

        #  7.声明所有的列名，去除多余列
        index_df = index_df[['htsc_code', 'name', 'ymd', 'open', 'close', 'high', 'low', 'volume']]

        #  8.删除重复记录，只保留每组 (ymd, stock_code) 中的第一个记录
        index_df = index_df.drop_duplicates(subset=['ymd', 'htsc_code'], keep='first')

        ############################   文件输出模块     ############################
        #  9.更新dataframe
        self.index_a_share = index_df

        #  10.本地csv文件的落盘保存
        index_filename = base_utils.save_out_filename(filehead='index_a_share', file_type='csv')
        index_filedir = os.path.join(self.dir_index_a_share_base, index_filename)
        index_df.to_csv(index_filedir, index=False)

        #  11.结果数据保存到 本地 mysql中
        mysql_utils.data_from_dataframe_to_mysql(user=local_user,
                                                 password=local_password,
                                                 host=local_host,
                                                 database=local_database,
                                                 df=index_df,
                                                 table_name="index_a_share_insight_now",
                                                 merge_on=['ymd', 'htsc_code'])

        #  12.结果数据保存到 远端 mysql中
        mysql_utils.data_from_dataframe_to_mysql(user=origin_user,
                                                 password=origin_password,
                                                 host=origin_host,
                                                 database=origin_database,
                                                 df=index_df,
                                                 table_name="index_a_share_insight_now",
                                                 merge_on=['ymd', 'htsc_code'])


    @timing_decorator
    def get_limit_summary(self):
        """
        大盘涨跌停分析数据
        Args:
            market:
                1	sh_a_share	上海A股
                2	sz_a_share	深圳A股
                3	a_share	A股
                4	a_share	B股
                5	gem	创业
                6	sme	中小板
                7	star	科创板
            trading_day: List<datetime>	交易日期范围，[start_date, end_date]

        Returns: ups_downs_limit_count_up_limits
                 ups_downs_limit_count_down_limits
                 ups_downs_limit_count_pre_up_limits
                 ups_downs_limit_count_pre_down_limits
                 ups_downs_limit_count_pre_up_limits_average_change_percent

                 [time	name	今日涨停	今日跌停	昨日涨停	昨日跌停	昨日涨停表现]

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
        #  7.更新dataframe
        self.limit_summary_df = limit_summary_df

        #  8.本地csv文件的落盘保存
        test_summary_filename = base_utils.save_out_filename(filehead='stock_limit_summary', file_type='csv')
        test_summary_dir = os.path.join(self.dir_limit_summary_base, test_summary_filename)
        limit_summary_df.to_csv(test_summary_dir, index=False)

        #  9.结果数据保存到 本地 mysql中
        mysql_utils.data_from_dataframe_to_mysql(user=local_user,
                                                 password=local_password,
                                                 host=local_host,
                                                 database=local_database,
                                                 df=limit_summary_df,
                                                 table_name="stock_limit_summary_insight_now",
                                                 merge_on=['ymd', 'name'])

        #  10.结果数据保存到 远端 mysql中
        mysql_utils.data_from_dataframe_to_mysql(user=origin_user,
                                                 password=origin_password,
                                                 host=origin_host,
                                                 database=origin_database,
                                                 df=limit_summary_df,
                                                 table_name="stock_limit_summary_insight_now",
                                                 merge_on=['ymd', 'name'])


    @timing_decorator
    def get_future_inside(self):
        """
        期货市场数据
        贵金属,  有色数据
        国际市场  国内市场
        AU9999.SHF    沪金主连
        AU2409.SHF	  沪金
        AG9999.SHF    沪银主连
        AG2409.SHF    沪银
        CU9999.SHF    沪铜主连
        CU2409.SHF    沪铜

        EC9999.INE    欧线集运主连
        EC2410.INE    欧线集运
        SC9999.INE    原油主连
        SC2410.INE    原油

        V9999.DCE     PVC主连
        V2409.DCE     PVC
        MA9999.ZCE    甲醇主连      (找不到)
        MA2409.ZCE    甲醇         (找不到)
        目前主连找不到数据，只有月份的，暂时用 t+2 月去代替主连吧

        Returns:
        """

        #  1.起止时间 查询起始时间写2月前的月初第1天
        time_start_date = DateUtility.first_day_of_month_after_n_months(-2)
        time_end_date = DateUtility.today()

        time_start_date = datetime.strptime(time_start_date, '%Y%m%d')
        time_end_date = datetime.strptime(time_end_date, '%Y%m%d')

        #  2.查询标的
        index_list = ["AU{}.SHF", "AG{}.SHF", "CU{}.SHF", "EC{}.INE", "SC{}.INE", "V{}.DCE"]
        replacement = DateUtility.first_day_of_month_after_n_months(2)[2:6]

        future_index_list = [index.format(replacement) for index in index_list]

        #  3.future_inside 的总和dataframe
        future_inside_df = pd.DataFrame()

        #  4.请求insight数据   get_kline
        res = get_kline(htsc_code=future_index_list, time=[time_start_date, time_end_date],
                        frequency="daily", fq="pre")
        future_inside_df = pd.concat([future_inside_df, res], ignore_index=True)

        #  5.日期格式转换
        future_inside_df['time'] = pd.to_datetime(future_inside_df['time']).dt.strftime('%Y%m%d')
        future_inside_df.rename(columns={'time': 'ymd'}, inplace=True)

        #  6.声明所有的列名，去除多余列
        future_inside_df = future_inside_df[
            ['htsc_code', 'ymd', 'open', 'close', 'high', 'low', 'volume', 'open_interest', 'settle']]

        #  7.删除重复记录，只保留每组 (ymd, stock_code) 中的第一个记录
        future_inside_df = future_inside_df.drop_duplicates(subset=['ymd', 'htsc_code'], keep='first')

        ############################   文件输出模块     ############################
        #  8.更新dataframe
        self.future_index = future_inside_df

        #  9.本地csv文件的落盘保存
        future_inside_df_filename = base_utils.save_out_filename(filehead='future_inside', file_type='csv')
        future_inside_df_filedir = os.path.join(self.dir_future_inside_base, future_inside_df_filename)
        future_inside_df.to_csv(future_inside_df_filedir, index=False)

        #  10.结果数据保存到 本地 mysql中
        mysql_utils.data_from_dataframe_to_mysql(user=local_user,
                                                 password=local_password,
                                                 host=local_host,
                                                 database=local_database,
                                                 df=future_inside_df,
                                                 table_name="future_inside_insight_now",
                                                 merge_on=['ymd', 'htsc_code'])

        #  11.结果数据保存到 远端 mysql中
        mysql_utils.data_from_dataframe_to_mysql(user=origin_user,
                                                 password=origin_password,
                                                 host=origin_host,
                                                 database=origin_database,
                                                 df=future_inside_df,
                                                 table_name="future_inside_insight_now",
                                                 merge_on=['ymd', 'htsc_code'])


    @timing_decorator
    def get_chouma_datas(self):
        """
        1.获取每日的筹码分布数据
        2.找到那些当日能够拿到筹码数据的codes
        :return:
        """
        #  1.起止时间 查询起始时间写本月月初
        time_start_date = DateUtility.first_day_of_month()
        #  结束时间必须大于等于当日，这里取明天的日期
        time_end_date = DateUtility.next_day(1)

        time_start_date = datetime.strptime(time_start_date, '%Y%m%d')
        time_end_date = datetime.strptime(time_end_date, '%Y%m%d')

        #  2.每个批次取 100 个元素
        batch_size = 1

        #  3.这是一个切分批次的内部函数
        def get_batches(lst, batch_size):
            for start in range(0, len(lst), batch_size):
                yield lst[start:start + batch_size]

        #  4.获取最新的stock_code_list
        stock_code_list = mysql_utils.get_stock_codes_latest(self.stock_code_df)

        #  5.计算总批次数
        total_batches = (len(stock_code_list) + batch_size - 1) // batch_size

        #  6.chouma 的总和dataframe
        chouma_total_df = pd.DataFrame()

        #  7.调用insight数据  get_chip_distribution
        valid_num = 0

        for i, code_list in enumerate(get_batches(stock_code_list, batch_size), start=1):
            #  一种非常巧妙的循环打印日志的方式
            sys.stdout.write(f"\r当前执行 get_chouma_datas  第 {i} 次循环，总共 {total_batches} 个批次, {valid_num}个有效筹码数据")
            sys.stdout.flush()
            time.sleep(0.01)

            try:
                res = get_chip_distribution(htsc_code=code_list, trading_day=[time_start_date, time_end_date])
                chouma_total_df = pd.concat([chouma_total_df, res], ignore_index=True)
                valid_num += 1
            except Exception as e:
                continue
            time.sleep(0.1)

        sys.stdout.write("\n")

        #  8.日期格式转换
        chouma_total_df['time'] = pd.to_datetime(chouma_total_df['time']).dt.strftime('%Y%m%d')
        chouma_total_df.rename(columns={'time': 'ymd'}, inplace=True)

        ############################   文件输出模块     ############################
        #  9.更新dataframe
        self.stock_chouma_available = chouma_total_df

        #  10.本地csv文件的落盘保存
        chouma_filename = base_utils.save_out_filename(filehead=f"stock_chouma", file_type='csv')
        chouma_data_filedir = os.path.join(self.dir_chouma_base, 'chouma_data', chouma_filename)
        chouma_total_df.to_csv(chouma_data_filedir, index=False)

        #  11.结果数据保存到 本地 mysql中
        mysql_utils.data_from_dataframe_to_mysql(user=local_user,
                                                 password=local_password,
                                                 host=local_host,
                                                 database=local_database,
                                                 df=chouma_total_df,
                                                 table_name="stock_chouma_insight",
                                                 merge_on=['ymd', 'htsc_code'])

        #  12.结果数据保存到 远端 mysql中
        mysql_utils.data_from_dataframe_to_mysql(user=origin_user,
                                                 password=origin_password,
                                                 host=origin_host,
                                                 database=origin_database,
                                                 df=chouma_total_df,
                                                 table_name="stock_chouma_insight",
                                                 merge_on=['ymd', 'htsc_code'])


    @timing_decorator
    def get_Ashare_industry_overview(self):
        """
        获取行业信息 申万三级 的行业信息
        :return:
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

        #  4.日期格式转换
        industry_df.insert(0, 'ymd', time_today)
        industry_df['ymd'] = pd.to_datetime(industry_df['ymd']).dt.strftime('%Y%m%d')

        #  5.声明所有的列名，去除多余列
        industry_df = industry_df[['ymd', 'classified', 'industry_name', 'industry_code', 'l1_code', 'l1_name', 'l2_code', 'l2_name', 'l3_code', 'l3_name']]

        #  6.删除重复记录，只保留每组 (ymd, stock_code) 中的第一个记录
        industry_df = industry_df.drop_duplicates(subset=['ymd', 'industry_code'], keep='first')

        ############################   文件输出模块     ############################
        #  7.更新dataframe
        self.industry_overview = industry_df

        #  8.本地csv文件的落盘保存
        sw_industry_filename = base_utils.save_out_filename(filehead='sw_industry', file_type='csv')
        sw_industry_filedir = os.path.join(self.dir_industry_overview_base, sw_industry_filename)
        industry_df.to_csv(sw_industry_filedir, index=False)

        #  9.结果数据保存到 本地 mysql中
        mysql_utils.data_from_dataframe_to_mysql(user=local_user,
                                                 password=local_password,
                                                 host=local_host,
                                                 database=local_database,
                                                 df=industry_df,
                                                 table_name="astock_industry_overview",
                                                 merge_on=['ymd', 'industry_code'])

        #  10.结果数据保存到 远端 mysql中
        mysql_utils.data_from_dataframe_to_mysql(user=origin_user,
                                                 password=origin_password,
                                                 host=origin_host,
                                                 database=origin_database,
                                                 df=industry_df,
                                                 table_name="astock_industry_overview",
                                                 merge_on=['ymd', 'industry_code'])


    @timing_decorator
    def get_Ashare_industry_detail(self):
        """
        获取股票的行业信息 申万三级 的行业信息
        :return:
         industry_detail  ['ymd', 'htsc_code', 'name', 'industry_name', 'industry_code', 'l1_code', 'l1_name', 'l2_code', 'l2_name', 'l3_code', 'l3_name']
        """

        #  1.当月数据的起止时间
        time_today = DateUtility.today()
        time_today = datetime.strptime(time_today, '%Y%m%d')

        #  2.行业信息的总和dataframe
        stock_in_industry_df = pd.DataFrame()

        #  3.获取最新的 stock_code 数据
        index_list = mysql_utils.get_stock_codes_latest(self.stock_code_df)

        #  4.请求insight 上的申万三级行业 数据
        for stock_code in index_list:
            res = get_industry(htsc_code=stock_code, classified='sw')
            stock_in_industry_df = pd.concat([stock_in_industry_df, res], ignore_index=True)
            time.sleep(0.01)

        #  5.日期格式转换
        stock_in_industry_df.insert(0, 'ymd', time_today)
        stock_in_industry_df['ymd'] = pd.to_datetime(stock_in_industry_df['ymd']).dt.strftime('%Y%m%d')

        #  6.声明所有的列名，去除多余列
        stock_in_industry_df = stock_in_industry_df[['ymd', 'htsc_code', 'name', 'industry_name', 'industry_code', 'l1_code', 'l1_name', 'l2_code', 'l2_name', 'l3_code', 'l3_name']]

        #  7.删除重复记录，只保留每组 (ymd, stock_code) 中的第一个记录
        stock_in_industry_df = stock_in_industry_df.drop_duplicates(subset=['ymd', 'htsc_code'], keep='first')

        ############################   文件输出模块     ############################
        #  8.更新dataframe
        self.industry_detail = stock_in_industry_df

        #  9.本地csv文件的落盘保存
        sw_industry_filename = base_utils.save_out_filename(filehead='sw_industry', file_type='csv')
        sw_industry_filedir = os.path.join(self.dir_industry_detail_base, sw_industry_filename)
        stock_in_industry_df.to_csv(sw_industry_filedir, index=False)

        #  10.结果数据保存到 本地 mysql中
        mysql_utils.data_from_dataframe_to_mysql(user=local_user,
                                                 password=local_password,
                                                 host=local_host,
                                                 database=local_database,
                                                 df=stock_in_industry_df,
                                                 table_name="astock_industry_detail",
                                                 merge_on=['ymd', 'htsc_code'])

        #  11.结果数据保存到 远端 mysql中
        mysql_utils.data_from_dataframe_to_mysql(user=origin_user,
                                                 password=origin_password,
                                                 host=origin_host,
                                                 database=origin_database,
                                                 df=stock_in_industry_df,
                                                 table_name="astock_industry_detail",
                                                 merge_on=['ymd', 'htsc_code'])

    @timing_decorator
    def get_shareholder_north_bound_num(self):
        """
        获取 股东数 & 北向资金情况
        Returns:
        """

        #  1.起止时间 查询起始时间写 2月前的月初
        time_start_date = DateUtility.first_day_of_month_after_n_months(-2)
        #  结束时间必须大于等于当日，这里取明天的日期
        time_end_date = DateUtility.next_day(1)

        time_start_date = datetime.strptime(time_start_date, '%Y%m%d')
        time_end_date = datetime.strptime(time_end_date, '%Y%m%d')

        #  2.行业信息的总和dataframe
        shareholder_num_df = pd.DataFrame()
        #  北向资金的总和dataframe
        north_bound_df = pd.DataFrame()

        #  3.获取最新的stock_codes 数据
        code_list = mysql_utils.get_stock_codes_latest(self.stock_code_df)

        #  4.请求insight  个股股东数   数据
        #    请求insight  北向资金持仓  数据
        total_xunhuan = len(code_list)
        i = 1                       # 总循环标记
        valid_shareholder = 1       # 个股股东数有效标记
        valid_north_bound = 1       # 北向资金持仓有效标记

        for stock_code in code_list:
            # 屏蔽 stdout 和 stderr
            with contextlib.redirect_stdout(io.StringIO()), contextlib.redirect_stderr(io.StringIO()):
                res_shareholder = get_shareholder_num(htsc_code=stock_code, end_date=[time_start_date, time_end_date])
                res_north_bound =get_north_bound(htsc_code=stock_code, trading_day=[time_start_date, time_end_date])

            if res_shareholder is not None:
                shareholder_num_df = pd.concat([shareholder_num_df, res_shareholder], ignore_index=True)
                sys.stdout.write(f"\r当前执行 get_shareholder_num  第 {i} 次循环，总共 {total_xunhuan} 个批次, {valid_shareholder}个有效股东数据")
                sys.stdout.flush()
                valid_shareholder += 1

            if res_north_bound is not None:
                north_bound_df = pd.concat([north_bound_df, res_north_bound], ignore_index=True)
                sys.stdout.write(f"\r当前执行 get_north_bound  第 {i} 次循环，总共 {total_xunhuan} 个批次, {valid_north_bound}个有效北向持仓数据")
                sys.stdout.flush()
                valid_north_bound += 1

            i += 1

        sys.stdout.write("\n")

        #  5.日期格式转换
        shareholder_num_df.rename(columns={'end_date': 'ymd'}, inplace=True)
        shareholder_num_df['ymd'] = pd.to_datetime(shareholder_num_df['ymd']).dt.strftime('%Y%m%d')

        north_bound_df.rename(columns={'trading_day': 'ymd'}, inplace=True)
        north_bound_df['ymd'] = pd.to_datetime(shareholder_num_df['ymd']).dt.strftime('%Y%m%d')

        #  6.声明所有的列名，去除多余列
        shareholder_num_df = shareholder_num_df[['htsc_code', 'name', 'ymd', 'total_sh', 'avg_share', 'pct_of_total_sh', 'pct_of_avg_sh']]
        north_bound_df = north_bound_df[['htsc_code', 'ymd', 'sh_hkshare_hold', 'pct_total_share']]

        #  7.删除重复记录，只保留每组 (ymd, stock_code) 中的第一个记录
        shareholder_num_df = shareholder_num_df.drop_duplicates(subset=['ymd', 'htsc_code'], keep='first')
        north_bound_df = north_bound_df.drop_duplicates(subset=['ymd', 'htsc_code'], keep='first')

        ############################   文件输出模块     ############################
        #  8.更新dataframe
        self.shareholder_num_df = shareholder_num_df
        self.north_bound_df = north_bound_df

        #  9.本地csv文件的落盘保存
        shareholder_num_filename = base_utils.save_out_filename(filehead='shareholder_num', file_type='csv')
        shareholder_num_filedir = os.path.join(self.dir_shareholder_num_base, shareholder_num_filename)
        shareholder_num_df.to_csv(shareholder_num_filedir, index=False)

        north_bound_filename = base_utils.save_out_filename(filehead='north_bound', file_type='csv')
        north_bound_filedir = os.path.join(self.dir_north_bound_base, north_bound_filename)
        north_bound_df.to_csv(north_bound_filedir, index=False)

        #  10.结果数据保存到 本地 mysql中
        mysql_utils.data_from_dataframe_to_mysql(user=local_user,
                                                 password=local_password,
                                                 host=local_host,
                                                 database=local_database,
                                                 df=shareholder_num_df,
                                                 table_name="shareholder_num_now",
                                                 merge_on=['ymd', 'htsc_code'])

        mysql_utils.data_from_dataframe_to_mysql(user=local_user,
                                                 password=local_password,
                                                 host=local_host,
                                                 database=local_database,
                                                 df=north_bound_df,
                                                 table_name="north_bound_daily_now",
                                                 merge_on=['ymd', 'htsc_code'])

        #  11.结果数据保存到 远端 mysql中
        mysql_utils.data_from_dataframe_to_mysql(user=origin_user,
                                                 password=origin_password,
                                                 host=origin_host,
                                                 database=origin_database,
                                                 df=shareholder_num_df,
                                                 table_name="shareholder_num_now",
                                                 merge_on=['ymd', 'htsc_code'])

        mysql_utils.data_from_dataframe_to_mysql(user=origin_user,
                                                 password=origin_password,
                                                 host=origin_host,
                                                 database=origin_database,
                                                 df=north_bound_df,
                                                 table_name="north_bound_daily_now",
                                                 merge_on=['ymd', 'htsc_code'])




    @timing_decorator
    def setup(self):
        #  登陆insight数据源
        self.login()

        #  除去 ST |  退  | B 的股票集合
        self.get_stock_codes()

        #  获取上述股票的当月日K
        self.get_stock_kline()

        #  获取主要股指
        self.get_index_a_share()

        #  大盘涨跌概览
        self.get_limit_summary()

        #  期货__内盘
        self.get_future_inside()

        #  筹码概览
        self.get_chouma_datas()

        #  获取A股的行业分类数据, 是行业数据
        self.get_Ashare_industry_overview()

        #  获取A股的行业分类数据, 是stock_code & industry 关联后的大表数据
        self.get_Ashare_industry_detail()

        #  个股股东数
        self.get_shareholder_north_bound_num()



if __name__ == '__main__':
    save_insight_data = SaveInsightData()
    save_insight_data.setup()
