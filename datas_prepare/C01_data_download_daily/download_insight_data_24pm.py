# -*- coding: utf-8 -*-

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
import CommonProperties.Mysql_Utils as mysql_utils
from CommonProperties.DateUtility import DateUtility
from CommonProperties.Base_utils import timing_decorator
from CommonProperties import set_config

# ************************************************************************
# 本代码的作用是凌晨24点后下载 insight 行情源数据, 本地保存,用于后续分析
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


class SaveInsightData24PM:

    @timing_decorator
    def login(self):
        # 登陆前 初始化，没有密码可以访问进行自动化注册
        # https://findata-insight.htsc.com:9151/terminalWeb/#/signup
        user = base_properties.user
        password = base_properties.password
        common.login(market_service, user, password)


    # @timing_decorator
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
        当月至今的指数
        Returns:
             index_a_share   [htsc_code 	time	frequency	open	close	high	low	volume	value]
        """

        #  1.当月数据的起止时间
        start_date = DateUtility.first_day_of_month()
        end_date = DateUtility.next_day(-1)
        # start_date = '20240901'
        # end_date = '20240930'

        start_date = datetime.strptime(start_date, '%Y%m%d')
        end_date = datetime.strptime(end_date, '%Y%m%d').replace(hour=23, minute=59, second=59)

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

        ##  insight 返回值的非空判断
        if not index_df.empty:

            #  5.日期格式转换
            index_df['time'] = pd.to_datetime(index_df['time']).dt.strftime('%Y%m%d')
            index_df.rename(columns={'time': 'ymd', 'htsc_code': 'index_code', 'name': 'index_name'}, inplace=True)

            #  6.根据映射关系，添加stock_name
            index_df['index_name'] = index_df['index_code'].map(index_dict)

            #  7.声明所有的列名，去除多余列
            index_df = index_df[['index_code', 'index_name', 'ymd', 'open', 'close', 'high', 'low', 'volume']]

            #  8.删除重复记录，只保留每组 (ymd, stock_code) 中的第一个记录
            index_df = index_df.drop_duplicates(subset=['ymd', 'index_code'], keep='first')

            ############################   文件输出模块     ############################
            # 总是保存到远端数据库
            mysql_utils.data_from_dataframe_to_mysql(
                user=origin_user,
                password=origin_password,
                host=origin_host,
                database=origin_database,
                df=index_df,
                table_name="ods_index_a_share_insight_now",
                merge_on=['ymd', 'index_code']
            )

        else:
            ## insight 返回为空值
            logging.info('    get_index_a_share 的返回值为空值')


    # @timing_decorator
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
        time_start_date = DateUtility.first_day_of_month(-2)
        time_end_date = DateUtility.next_day(-1)

        time_start_date = datetime.strptime(time_start_date, '%Y%m%d')
        time_end_date = datetime.strptime(time_end_date, '%Y%m%d').replace(hour=23, minute=59, second=59)

        #  2.查询标的
        index_list = ["AU{}.SHF", "AG{}.SHF", "CU{}.SHF", "EC{}.INE", "SC{}.INE", "V{}.DCE"]
        replacement = DateUtility.first_day_of_month(2)[2:6]

        future_index_list = [index.format(replacement) for index in index_list]

        #  3.future_inside 的总和dataframe
        future_inside_df = pd.DataFrame()

        #  4.请求insight数据   get_kline
        res = get_kline(htsc_code=future_index_list, time=[time_start_date, time_end_date],
                        frequency="daily", fq="pre")
        future_inside_df = pd.concat([future_inside_df, res], ignore_index=True)

        ##  insight 返回值的非空判断
        if not future_inside_df.empty:

            #  5.日期格式转换
            future_inside_df['time'] = pd.to_datetime(future_inside_df['time']).dt.strftime('%Y%m%d')
            future_inside_df.rename(columns={'time': 'ymd', 'htsc_code': 'stock_code'}, inplace=True)

            #  6.声明所有的列名，去除多余列
            future_inside_df = future_inside_df[
                ['stock_code', 'ymd', 'open', 'close', 'high', 'low', 'volume', 'open_interest', 'settle']]

            #  7.删除重复记录，只保留每组 (ymd, stock_code) 中的第一个记录
            future_inside_df = future_inside_df.drop_duplicates(subset=['ymd', 'stock_code'], keep='first')

            ############################   文件输出模块     ############################
            # 总是保存到远端数据库
            mysql_utils.data_from_dataframe_to_mysql(
                user=origin_user,
                password=origin_password,
                host=origin_host,
                database=origin_database,
                df=future_inside_df,
                table_name="ods_future_inside_insight_now",
                merge_on=['ymd', 'stock_code']
            )

        else:
            ## insight 返回为空值
            logging.info('    get_future_inside 的返回值为空值')


    # @timing_decorator
    def get_shareholder_num(self):
        """
        获取 股东数 & 北向资金情况
        Returns: 写入 ods_shareholder_num_now
        """
        #  1.起止时间 查询起始时间写 2月前的月初
        time_start_date = DateUtility.first_day_of_month(-2)
        #  结束时间必须大于等于当日，这里取明天的日期，如果是凌晨执行，就可以取当日了
        time_end_date = DateUtility.next_day(-1)

        time_start_date = datetime.strptime(time_start_date, '%Y%m%d')
        time_end_date = datetime.strptime(time_end_date, '%Y%m%d').replace(hour=23, minute=59, second=59)

        #  2.行业信息的总和dataframe
        shareholder_num_df = pd.DataFrame()

        #  3.获取最新的stock_codes 数据
        code_list = mysql_utils.get_stock_codes_latest()

        #  4.请求insight  个股股东数   数据
        total_xunhuan = len(code_list)
        i = 1                       # 总循环标记

        for stock_code in code_list:
            # 屏蔽 stdout 和 stderr
            with contextlib.redirect_stdout(io.StringIO()), contextlib.redirect_stderr(io.StringIO()):
                res_shareholder = get_shareholder_num(htsc_code=stock_code, end_date=[time_start_date, time_end_date])
                valid_shareholder = shareholder_num_df.shape[0]
            if res_shareholder is not None:
                shareholder_num_df = pd.concat([shareholder_num_df, res_shareholder], ignore_index=True)
                sys.stdout.write(f"\r当前执行 get_shareholder_num  第 {i} 次循环，总共 {total_xunhuan} 个批次, {valid_shareholder}个有效股东数据")
                sys.stdout.flush()

            time.sleep(0.03)
            i += 1
        sys.stdout.write("\n")

        ##  insight 返回值的非空判断
        if not shareholder_num_df.empty:

            #  5.日期格式转换
            shareholder_num_df.rename(columns={'end_date': 'ymd', 'htsc_code': 'stock_code', 'name': 'stock_name'}, inplace=True)
            shareholder_num_df['ymd'] = pd.to_datetime(shareholder_num_df['ymd']).dt.strftime('%Y%m%d')

            #  6.声明所有的列名，去除多余列
            shareholder_num_df = shareholder_num_df[
                ['stock_code', 'stock_name', 'ymd', 'total_sh', 'avg_share', 'pct_of_total_sh', 'pct_of_avg_sh']]

            #  7.删除重复记录，只保留每组 (ymd, stock_code) 中的第一个记录
            shareholder_num_df = shareholder_num_df.drop_duplicates(subset=['ymd', 'stock_code'], keep='first')

            ############################   文件输出模块     ############################
            # 总是保存到远端数据库
            mysql_utils.data_from_dataframe_to_mysql(
                user=origin_user,
                password=origin_password,
                host=origin_host,
                database=origin_database,
                df=shareholder_num_df,
                table_name="ods_shareholder_num_now",
                merge_on=['ymd', 'stock_code']
            )

        else:
            ## insight 返回为空值
            logging.info('    get_shareholder_num 的返回值为空值')


    def setup(self):
        #  登陆insight数据源
        self.login()

        #  获取主要股指
        self.get_index_a_share()

        #  期货__内盘
        self.get_future_inside()

        #  个股股东数
        self.get_shareholder_num()


if __name__ == '__main__':
    save_insight_data = SaveInsightData24PM()
    save_insight_data.setup()
