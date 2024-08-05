import os
from datetime import datetime

import pandas as pd
from sqlalchemy import create_engine
import time

# import dataprepare_properties
# import dataprepare_utils
import CommonProperties.Base_Properties as base_properties
import CommonProperties.Base_utils as base_utils
from CommonProperties.DateUtility import DateUtility
from CommonProperties.Base_utils import timing_decorator


# ************************************************************************
# 本代码的作用是下午收盘后针对 insight 行情源数据的本地保存部分开展merge
# 需要下载的数据:
# 1.上市股票代码   get_all_stocks()
# 2.筹码分布数据   get_chouma_datas()


# ************************************************************************


class SaveInsightHistoryData:

    def __init__(self):

        self.init_dirs()

        self.init_variant()

    def init_dirs(self):
        """
        关键路径初始化
        """
        #  文件路径_____insight文件当下数据基础路径
        self.dir_insight_base = base_properties.dir_insight_base

        #  文件路径_____insight文件历史数据基础路径
        self.dir_history_insight_base = base_properties.dir_history_insight_base

        #  文件路径_____insight文件merge数据基础路径
        self.dir_merge_insight_base = base_properties.dir_merge_insight_base


        ##  聚合全量的日k 数据
        #  文件路径_____上市交易股票的当下日k线数据
        self.dir_stock_kline_base = os.path.join(self.dir_insight_base, 'stock_kline')

        #  文件路径_____上市交易股票的历史日k线数据
        self.dir_history_stock_kline_base = os.path.join(self.dir_history_insight_base, 'stock_kline')

        #  文件路径_____上市交易股票的merge日k线数据
        self.dir_merge_stock_kline_base = os.path.join(self.dir_merge_insight_base, 'stock_kline')



        #  文件路径_____关键大盘指数
        self.dir_history_index_a_share_base = os.path.join(self.dir_history_insight_base, 'index_a_share')

        #  文件路径_____涨跌停数量
        self.dir_history_limit_summary_base = os.path.join(self.dir_history_insight_base, 'limit_summary')

        #  文件路径_____内盘期货
        self.dir_history_future_inside_base = os.path.join(self.dir_history_insight_base, 'future_inside')

        #  文件路径_____筹码数据
        self.dir_history_chouma_base = os.path.join(self.dir_history_insight_base, 'chouma')


    def init_variant(self):
        """
        结果变量初始化
        """
        #  除去 ST|退|B 的五要素   [ymd	htsc_code	name	exchange]
        self.stock_code_df = pd.DataFrame()

        #  获取上述股票的历史数据   日K级别
        self.kline_total_history = pd.DataFrame()

        #  获得A股市场的股指 [htsc_code 	time	frequency	open	close	high	low	volume	value]
        self.index_a_share = pd.DataFrame()

        #  大盘涨跌停数量          [time	name	今日涨停	今日跌停	昨日涨停	昨日跌停	昨日涨停表现]
        self.limit_summary_df = pd.DataFrame()

        #  期货市场数据    原油  贵金属  有色
        self.future_index = pd.DataFrame()

        #  可以获取筹码的股票数据
        self.stock_chouma_available = ""




    @timing_decorator
    def merge_stock_kline(self):
        """
        将 stock_kline 的历史数据和当月数据做merge
        :return:
         stock_kline_df  [ymd	htsc_code	name	exchange]
        """

        #  读取历史数据和当下数据
        stock_kline_latest_file = base_utils.get_latest_filename(self.dir_stock_kline_base)
        stock_kline_history_latest_file = base_utils.get_latest_filename(self.dir_history_stock_kline_base)


        history_df = pd.read_csv(stock_kline_history_latest_file)
        now_df = pd.read_csv(stock_kline_latest_file)

        # 设定 'time' 为索引，以便于数据合并
        history_df.set_index('time', inplace=True)
        now_df.set_index('time', inplace=True)

        # 合并数据，以 now_df 为准
        combined_df = now_df.combine_first(history_df).reset_index()

        # MySQL 数据库连接配置
        db_url = 'mysql+pymysql://username:password@host:port/database'
        engine = create_engine(db_url)
        # 将结果写入 MySQL 数据库
        combined_df.to_sql(name='stock_data', con=engine, if_exists='replace', index=False)

        #  文件输出模块
        kline_total_filename = base_utils.save_out_filename(filehead='stock_kline_latest', file_type='csv')
        kline_total_filedir = os.path.join(self.dir_merge_stock_kline_base, kline_total_filename)
        combined_df.to_csv(kline_total_filedir, index=False)



    @timing_decorator
    def get_index_a_share(self):
        """
        000001.SH    上证指数
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

        time_start_date = DateUtility.first_day_of_year_after_n_years(-3)
        time_end_date = DateUtility.today()

        time_start_date = datetime.strptime(time_start_date, '%Y%m%d')
        time_end_date = datetime.strptime(time_end_date, '%Y%m%d')

        index_list = ["000001.SH", "399006.SZ", "000016.SH", "000300.SH", "000849.SH", "000905.SH", "399852.SZ",
                      "000688.SH", ""]
        index_df = pd.DataFrame()

        for index in index_list:
            res = get_kline(htsc_code=[index], time=[time_start_date, time_end_date],
                            frequency="daily", fq="none")

            index_df = pd.concat([index_df, res], ignore_index=True)

        ## 文件输出模块
        index_filename = base_utils.save_out_filename(filehead='index_a_share', file_type='csv')
        index_filedir = os.path.join(self.dir_history_index_a_share_base, index_filename)

        index_df.to_csv(index_filedir, index=False)
        self.index_a_share = index_df
        print("------------- get_index_a_share 完成测试文件输出 ---------------------")


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

        start_date = DateUtility.first_day_of_year()
        end_date = DateUtility.today()

        # 转为时间格式  get_change_summary 强制要求的
        start_date = datetime.strptime(start_date, '%Y%m%d')
        end_date = datetime.strptime(end_date, '%Y%m%d')

        result_df = get_change_summary(market=["a_share"], trading_day=[start_date, end_date])

        filter_limit_df = result_df[['time',
                                     'name',
                                     'ups_downs_limit_count_up_limits',
                                     'ups_downs_limit_count_down_limits',
                                     'ups_downs_limit_count_pre_up_limits',
                                     'ups_downs_limit_count_pre_down_limits',
                                     'ups_downs_limit_count_pre_up_limits_average_change_percent']]
        filter_limit_df.columns = ['time', 'name', '今日涨停', '今日跌停', '昨日涨停', '昨日跌停', '昨日涨停表现']

        test_summary_filename = base_utils.save_out_filename(filehead='stock_limit_summary', file_type='csv')
        test_summary_dir = os.path.join(self.dir_history_limit_summary_base, test_summary_filename)

        #  大盘涨跌停数量情况，默认是从年初到今天
        self.limit_summary_df = filter_limit_df
        filter_limit_df.to_csv(test_summary_dir, index=False)
        print("------------- get_limit_summary 完成测试文件输出 ---------------------")


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
        time_start_date = DateUtility.first_day_of_month_after_n_months(-2)
        time_end_date = DateUtility.today()

        time_start_date = datetime.strptime(time_start_date, '%Y%m%d')
        time_end_date = datetime.strptime(time_end_date, '%Y%m%d')

        index_df = pd.DataFrame()

        for index in future_index_list:
            #  获取数据的关键调用
            res = get_kline(htsc_code=[index], time=[time_start_date, time_end_date],
                            frequency="daily", fq="pre")

            index_df = pd.concat([index_df, res], ignore_index=True)

        ## 文件输出模块
        index_filename = base_utils.save_out_filename(filehead='future_inside', file_type='csv')
        index_filedir = os.path.join(self.dir_history_future_inside_base, index_filename)
        index_df.to_csv(index_filedir, index=False)
        print("------------- get_future_inside 完成测试文件输出 ---------------------")


    @timing_decorator
    def get_chouma_datas(self):
        """
        1.获取每日的筹码分布数据
        2.找到那些当日能够拿到筹码数据的codes
        :return:
        """
        print("----------------- get_chouma_datas() 开始执行 ------------------")

        start_time = time.time()  # 记录开始时间

        dt = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
        formatted_date = dt.strftime('%Y%m%d')

        ##  所有已上市股票
        # list_stock = self.stock_all_list

        latest_stock_codes_file = os.path.join(self.dir_history_stock_codes_base,
                                               base_utils.get_latest_filename(self.dir_history_stock_codes_base))
        with open(latest_stock_codes_file, 'r') as f:
            content = f.readlines()
        ##  取出当日 上市交易  的股票代码
        list_stock = eval(content[0].strip())
        print(r'   在 {} 日，共有 {} 个已上市stocks'.format(formatted_date, len(list_stock)))

        ############################  准备记录能够查到筹码数据的结果list  ###########################
        list_suc_res = []  # 用于存放遍历时不发生报错的 enum
        err_dict = {}  # 用于存放发生报错的 enum 和具体的报错原因

        ## 存放拼接结果
        chouma_total_df = pd.DataFrame()

        # 获取在指定时间范围的筹码分布数据
        for enum in list_stock:
            try:
                chouma_df = get_chip_distribution(htsc_code=enum, trading_day=[dt])
                # print("     =======  拉取第{}条筹码数据：{}返回的结果条数为{}".format(ttflag, enum, chouma_df.shape[0]))
                chouma_total_df = pd.concat([chouma_total_df, chouma_df], ignore_index=True)

            except Exception as e:
                err_dict[enum] = str(e)

            else:
                ##  成功获取筹码数据的股票代码
                list_suc_res.append(enum)

        #################  记录有异常，不能找到筹码数据的codes   ###########################
        chouma_err_filename = base_utils.save_out_filename(filehead='chouma_err', file_type='txt')
        chouma_err_file = os.path.join(self.dir_history_chouma_base, 'err_chouma_codes', chouma_err_filename)
        with open(chouma_err_file, 'w') as f:
            f.write(str(err_dict))

        #################  记录无异常，能够找到筹码数据的codes   ###########################
        chouma_filename = base_utils.save_out_filename(filehead=f"chouma_data", file_type='xlsx')
        chouma_data_file = os.path.join(self.dir_history_chouma_base, 'suc_chouma_data', chouma_filename)
        chouma_total_df.to_excel(chouma_data_file, float_format='%.0f', index=False)

        print("-------------  {} 日股票筹码数据获取完毕，获得{}个股票的筹码数据".format(formatted_date, len(list_suc_res)))

        self.stock_chouma_available = chouma_total_df

        end_time = time.time()  # 记录结束时间
        elapsed_time = end_time - start_time  # 计算时间差
        print(f"获取筹码数据的get_chouma_datas() 代码执行时间: {elapsed_time} 秒")


    def setup(self):
        #  登陆insight数据源
        self.login()

        #  除去 ST |  退  | B 的股票集合
        self.get_stock_codes()

        #  获取当前已上市股票过去3年到今天的历史kline
        self.get_stock_kline()


        #  获取主要股指
        # self.get_index_a_share()

        #  大盘涨跌概览
        # self.get_limit_summary()

        #  期货__内盘
        # self.get_future_inside()

        #  筹码概览
        # self.get_chouma_datas()


if __name__ == '__main__':
    save_insight_data = SaveInsightHistoryData()
    save_insight_data.setup()
