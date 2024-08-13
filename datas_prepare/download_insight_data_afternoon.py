import os
import sys
from insight_python.com.insight import common
from insight_python.com.insight.query import *
from insight_python.com.insight.market_service import market_service
from datetime import datetime
import time
import logging


import CommonProperties.Base_Properties as base_properties
import CommonProperties.Base_utils as base_utils
import CommonProperties.Mysql_Utils as mysql_utils
from CommonProperties.DateUtility import DateUtility
from CommonProperties.Base_utils import timing_decorator
from CommonProperties.logging_config import setup_logging

# ************************************************************************
# 本代码的作用是下午收盘后下载 insight 行情源数据, 本地保存,用于后续分析
# 需要下载的数据:
# 1.上市股票代码   get_all_stocks()
# 2.筹码分布数据   get_chouma_datas()


# ************************************************************************


# ************************************************************************
# 调用日志配置
setup_logging()

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

        formatted_date = DateUtility.today()

        ##  获取所有已上市codes
        stock_all_df = get_all_stocks_info(listing_state="上市交易")
        stock_all_df = stock_all_df[['htsc_code', 'name', 'exchange']]
        stock_all_df.insert(0, 'ymd', formatted_date)
        filtered_df = stock_all_df[~stock_all_df['name'].str.contains('ST|退|B')]
        filtered_df = filtered_df[['ymd', 'htsc_code', 'name', 'exchange']]

        # 删除重复记录，只保留每组 (ymd, stock_code) 中的第一个记录
        filtered_df = filtered_df.drop_duplicates(subset=['ymd', 'htsc_code'], keep='first')

        #  导出当日上市交易的股票信息 ymd  htsc_code  name  exchange
        self.stock_code_df = filtered_df

        #  本地csv文件的落盘保存
        filehead = 'stocks_codes_all'
        stock_codes_listed_filename = base_utils.save_out_filename(filehead=filehead, file_type='csv')
        stock_codes_listed_dir = os.path.join(self.dir_stock_codes_base, stock_codes_listed_filename)
        filtered_df.to_csv(stock_codes_listed_dir, index=False)

        #  结果数据保存到mysql中
        mysql_utils.data_from_dataframe_to_mysql(df=filtered_df, table_name="stock_code_daily_insight", database="quant")


    @timing_decorator
    def get_stock_kline(self):
        """
        根据当日上市的stock_codes，来获得全部(去除ST|退|B)股票的历史数据
        :return:
         stock_kline_df  [ymd	htsc_code	name	exchange]
        """

        #  当月数据的起止时间
        time_start_date = DateUtility.first_day_of_month()
        time_end_date = DateUtility.today()

        time_start_date = datetime.strptime(time_start_date, '%Y%m%d')
        time_end_date = datetime.strptime(time_end_date, '%Y%m%d')

        #  每个批次取 100 个元素
        batch_size = 100

        #  这是一个切分批次的内部函数
        def get_batches(df, batch_size):
            for start in range(0, len(df), batch_size):
                yield df[start:start + batch_size]

        #  计算总批次数
        total_batches = (len(self.stock_code_df) + batch_size - 1) // batch_size

        #  kline的总和dataframe
        kline_total_df = pd.DataFrame()

        for i, batch_df in enumerate(get_batches(self.stock_code_df, batch_size), start=1):
            #  一种非常巧妙的循环打印日志的方式
            sys.stdout.write(f"\r当前执行get_stock_kline的 第 {i} 次循环，总共 {total_batches} 个批次")
            sys.stdout.flush()

            index_list = batch_df['htsc_code'].tolist()
            res = get_kline(htsc_code=index_list, time=[time_start_date, time_end_date], frequency="daily", fq="none")
            kline_total_df = pd.concat([kline_total_df, res], ignore_index=True)

        # 循环结束后打印换行符，以确保后续输出在新行开始
        sys.stdout.write("\n")

        #  日期格式转换
        kline_total_df['time'] = pd.to_datetime(kline_total_df['time']).dt.strftime('%Y%m%d')
        kline_total_df.rename(columns={'time': 'ymd'}, inplace=True)

        #  声明所有的列名，去除value列
        kline_total_df = kline_total_df[['htsc_code', 'ymd', 'open', 'close', 'high', 'low', 'num_trades', 'volume']]

        # 删除重复记录，只保留每组 (ymd, stock_code) 中的第一个记录
        kline_total_df = kline_total_df.drop_duplicates(subset=['ymd', 'htsc_code'], keep='first')


        #  文件输出模块
        self.stock_kline_df = kline_total_df

        #  本地csv文件的落盘保存
        stock_kline_filename = base_utils.save_out_filename(filehead='stock_kline', file_type='csv')
        stcok_kline_filedir = os.path.join(self.dir_stock_kline_base, stock_kline_filename)
        kline_total_df.to_csv(stcok_kline_filedir, index=False)

        #  结果数据保存到mysql中
        mysql_utils.data_from_dataframe_to_mysql(df=kline_total_df, table_name="stock_kline_daily_insight_now", database="quant")



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

        start_date = DateUtility.first_day_of_month()
        end_date = DateUtility.today()

        start_date = datetime.strptime(start_date, '%Y%m%d')
        end_date = datetime.strptime(end_date, '%Y%m%d')

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

        index_df = pd.DataFrame()

        res = get_kline(htsc_code=index_list, time=[start_date, end_date],
                        frequency="daily", fq="none")

        index_df = pd.concat([index_df, res], ignore_index=True)

        #  日期格式转换
        index_df['time'] = pd.to_datetime(index_df['time']).dt.strftime('%Y%m%d')
        index_df.rename(columns={'time': 'ymd'}, inplace=True)

        #  根据映射关系，添加stock_name
        index_df['name'] = index_df['htsc_code'].map(index_dict)

        #  声明所有的列名，去除value列
        index_df = index_df[['htsc_code', 'name', 'ymd', 'open', 'close', 'high', 'low', 'volume']]

        # 删除重复记录，只保留每组 (ymd, stock_code) 中的第一个记录
        index_df = index_df.drop_duplicates(subset=['ymd', 'htsc_code'], keep='first')

        ############################   文件输出模块     ############################
        self.index_a_share = index_df

        #  本地csv文件的落盘保存
        index_filename = base_utils.save_out_filename(filehead='index_a_share', file_type='csv')
        index_filedir = os.path.join(self.dir_index_a_share_base, index_filename)
        index_df.to_csv(index_filedir, index=False)

        #  结果数据保存到mysql中
        mysql_utils.data_from_dataframe_to_mysql(df=index_df, table_name="index_a_share_insight_now", database="quant")



    # @timing_decorator
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

        start_date = DateUtility.first_day_of_month()
        end_date = DateUtility.today()

        # 转为时间格式  get_change_summary 强制要求的
        start_date = datetime.strptime(start_date, '%Y%m%d')
        end_date = datetime.strptime(end_date, '%Y%m%d')

        res = get_change_summary(market=["a_share"], trading_day=[start_date, end_date])
        limit_summary_df = pd.DataFrame()
        limit_summary_df = pd.concat([limit_summary_df, res], ignore_index=True)


        limit_summary_df = limit_summary_df[['time',
                                     'name',
                                     'ups_downs_limit_count_up_limits',
                                     'ups_downs_limit_count_down_limits',
                                     'ups_downs_limit_count_pre_up_limits',
                                     'ups_downs_limit_count_pre_down_limits',
                                     'ups_downs_limit_count_pre_up_limits_average_change_percent']]
        limit_summary_df.columns = ['ymd', 'name', 'today_ZT', 'today_DT', 'yesterday_ZT', 'yesterday_DT',
                                   'yesterday_ZT_rate']

        # 日期格式转换   使用 .loc 保证是在原 DataFrame 上进行操作
        limit_summary_df['ymd'] = pd.to_datetime(limit_summary_df['ymd']).dt.strftime('%Y%m%d')

        # 删除重复记录，只保留每组 (ymd, stock_code) 中的第一个记录
        limit_summary_df = limit_summary_df.drop_duplicates(subset=['ymd', 'name'], keep='first')


        #  大盘涨跌停数量情况，默认是从年初到今天
        self.limit_summary_df = limit_summary_df

        #  本地csv文件的落盘保存
        test_summary_filename = base_utils.save_out_filename(filehead='stock_limit_summary', file_type='csv')
        test_summary_dir = os.path.join(self.dir_limit_summary_base, test_summary_filename)
        limit_summary_df.to_csv(test_summary_dir, index=False)

        #  结果数据保存到mysql中
        mysql_utils.data_from_dataframe_to_mysql(df=limit_summary_df, table_name="stock_limit_summary_insight_now", database="quant")



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

        index_list = ["AU{}.SHF", "AG{}.SHF", "CU{}.SHF", "EC{}.INE", "SC{}.INE", "V{}.DCE"]
        replacement = DateUtility.first_day_of_month_after_n_months(2)[2:6]

        future_index_list = [index.format(replacement) for index in index_list]

        #  查询起始时间写2月前的月初第1天
        time_start_date = DateUtility.first_day_of_month()
        time_end_date = DateUtility.today()

        time_start_date = datetime.strptime(time_start_date, '%Y%m%d')
        time_end_date = datetime.strptime(time_end_date, '%Y%m%d')

        future_inside_df = pd.DataFrame()

        # for index in future_index_list:

        #  获取数据的关键调用
        res = get_kline(htsc_code=future_index_list, time=[time_start_date, time_end_date],
                        frequency="daily", fq="pre")

        future_inside_df = pd.concat([future_inside_df, res], ignore_index=True)

        #  日期格式转换
        future_inside_df['time'] = pd.to_datetime(future_inside_df['time']).dt.strftime('%Y%m%d')
        future_inside_df.rename(columns={'time': 'ymd'}, inplace=True)

        #  声明所有的列名，去除value列
        future_inside_df = future_inside_df[
            ['htsc_code', 'ymd', 'open', 'close', 'high', 'low', 'volume', 'open_interest', 'settle']]

        # 删除重复记录，只保留每组 (ymd, stock_code) 中的第一个记录
        future_inside_df = future_inside_df.drop_duplicates(subset=['ymd', 'htsc_code'], keep='first')

        ## 文件输出模块
        self.future_index = future_inside_df

        #  本地csv文件的落盘保存
        future_inside_df_filename = base_utils.save_out_filename(filehead='future_inside', file_type='csv')
        future_inside_df_filedir = os.path.join(self.dir_future_inside_base, future_inside_df_filename)
        future_inside_df.to_csv(future_inside_df_filedir, index=False)

        #  结果数据保存到mysql中
        mysql_utils.data_from_dataframe_to_mysql(df=future_inside_df, table_name="future_inside_insight_now", database="quant")



    @timing_decorator
    def get_chouma_datas(self):
        """
        1.获取每日的筹码分布数据
        2.找到那些当日能够拿到筹码数据的codes
        :return:
        """
        #  当月数据的起止时间
        time_start_date = DateUtility.first_day_of_month()
        time_end_date = DateUtility.today()

        time_start_date = datetime.strptime(time_start_date, '%Y%m%d')
        time_end_date = datetime.strptime(time_end_date, '%Y%m%d')

        #  每个批次取 100 个元素
        batch_size = 100

        #  这是一个切分批次的内部函数
        def get_batches(df, batch_size):
            for start in range(0, len(df), batch_size):
                yield df[start:start + batch_size]

        stock_code_df = mysql_utils.data_from_mysql_to_dataframe_latest(table_name='stock_code_daily_insight', database='quant')

        #  计算总批次数
        total_batches = (len(stock_code_df) + batch_size - 1) // batch_size

        #  chouma 的总和dataframe
        chouma_total_df = pd.DataFrame()

        for i, batch_df in enumerate(get_batches(stock_code_df, batch_size), start=1):
            #  一种非常巧妙的循环打印日志的方式
            sys.stdout.write(f"\r当前执行 get_chouma_datas  第 {i} 次循环，总共 {total_batches} 个批次")
            sys.stdout.flush()

            code_list = batch_df['htsc_code'].tolist()

            # 添加重试机制的部分
            max_retries = 3  # 最大重试次数
            retry_delay = 5  # 每次重试的延迟时间（秒）

            for attempt in range(max_retries):
                try:
                    res = get_chip_distribution(htsc_code=code_list, trading_day=[time_start_date, time_end_date])
                    chouma_total_df = pd.concat([chouma_total_df, res], ignore_index=True)
                    break  # 如果请求成功，则跳出重试循环
                except Exception as e:
                    logging.warning(f"尝试 {attempt + 1} 失败: {e}")
                    if attempt < max_retries - 1:
                        logging.info(f"等待 {retry_delay} 秒后重试...")
                        time.sleep(retry_delay)  # 等待一段时间后重试
                    else:
                        logging.error(f"多次尝试后仍然失败，跳过当前批次: {e}")
                        break  # 如果达到最大重试次数，跳过当前批次

        # 循环结束后打印换行符，以确保后续输出在新行开始
        sys.stdout.write("\n")

        #  日期格式转换
        chouma_total_df['time'] = pd.to_datetime(chouma_total_df['time']).dt.strftime('%Y%m%d')
        chouma_total_df.rename(columns={'time': 'ymd'}, inplace=True)

        self.stock_chouma_available = chouma_total_df

        #################  记录无异常，能够找到筹码数据的codes   ###########################
        chouma_filename = base_utils.save_out_filename(filehead=f"stock_chouma", file_type='csv')
        chouma_data_filedir = os.path.join(self.dir_chouma_base, 'chouma_data', chouma_filename)
        chouma_total_df.to_csv(chouma_data_filedir, index=False)

        #  结果数据保存到mysql中
        mysql_utils.data_from_dataframe_to_mysql(df=chouma_total_df, table_name="stock_chouma_insight_now", database="quant")



    def setup(self):
        #  登陆insight数据源
        self.login()

        #  除去 ST |  退  | B 的股票集合
        self.get_stock_codes()

        #  获取上述股票的当月日K
        # self.get_stock_kline()

        #  获取主要股指
        # self.get_index_a_share()

        #  大盘涨跌概览
        # self.get_limit_summary()

        #  期货__内盘
        # self.get_future_inside()

        #  筹码概览
        self.get_chouma_datas()


if __name__ == '__main__':
    save_insight_data = SaveInsightData()
    save_insight_data.setup()
