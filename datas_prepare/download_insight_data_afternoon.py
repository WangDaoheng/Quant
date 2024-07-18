import os
from insight_python.com.insight import common
from insight_python.com.insight.query import *
from insight_python.com.insight.market_service import market_service
from datetime import datetime
import time

# import dataprepare_properties
# import dataprepare_utils
import CommonProperties.Base_Properties as base_properties
import CommonProperties.Base_utils as base_utils
from CommonProperties.DateUtility import DateUtility


# ************************************************************************
# 本代码的作用是下午收盘后下载 insight 行情源数据, 本地保存,用于后续分析
# 需要下载的数据:
# 1.上市股票代码   get_all_stocks()
# 2.筹码分布数据   get_chouma_datas()


# ************************************************************************


class SaveData:

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

        #  文件路径_____筹码数据
        self.dir_chouma_base = os.path.join(self.dir_insight_base, 'chouma')

        #  文件路径_____涨跌停数量
        self.dir_limit_summary_base = os.path.join(self.dir_insight_base, 'limit_summary')

        #  文件路径_____关键大盘指数
        self.dir_index_a_share_base = os.path.join(self.dir_insight_base, 'a_share_index')




    def init_variant(self):
        """
        结果变量初始化
        """
        self.stock_code_df = pd.DataFrame()

        #  大盘涨跌停数量
        self.limit_summary_df = pd.DataFrame()

        #  可以获取筹码的股票数据
        self.stock_chouma_available = ""






    def login(self):
        # 登陆前 初始化，没有密码可以访问进行自动化注册
        # https://findata-insight.htsc.com:9151/terminalWeb/#/signup
        user = base_properties.user
        password = base_properties.password
        common.login(market_service, user, password)


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
        """

        time_start_date = DateUtility.first_day_of_month()
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
        index_filedir = os.path.join(self.dir_index_a_share_base, index_filename)

        print("------------- get_kline_for_Ashare_index_demo ---------------------")

        index_df.to_csv(index_filedir)




    def get_all_stocks(self):
        """
        获取当日的stock代码合集
        :return:
        """

        # dt = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
        # formatted_date = dt.strftime('%Y%m%d')
        formatted_date = DateUtility.today()


        ##  获取所有已上市codes
        stock_all_df = get_all_stocks_info(listing_state="上市交易")
        stock_all_df = stock_all_df[['htsc_code', 'name', 'exchange']]
        stock_all_df.insert(0, 'ymd', formatted_date)
        filtered_df = stock_all_df[~stock_all_df['name'].str.contains('ST|退|B')]

        ## 导出当日上市交易的股票信息 ymd  htsc_code  name  exchange
        filehead = 'stocks_codes_all'
        stock_codes_listed_filename = base_utils.save_out_filename(filehead=filehead, file_type='csv')
        stock_codes_listed_dir = os.path.join(self.dir_stock_codes_base, stock_codes_listed_filename)
        stock_all_df.to_csv(stock_codes_listed_dir, index=False)
        #  已上市状态stock_codes
        self.stock_code_df = filtered_df


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
        test_summary_dir = os.path.join(self.dir_limit_summary_base, test_summary_filename)
        filter_limit_df.to_csv(test_summary_dir)

        #  大盘涨跌停数量情况，默认是从年初到今天
        self.limit_summary_df = filter_limit_df








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

        latest_stock_codes_file = os.path.join(self.dir_stock_codes_base,
                                               base_utils.get_latest_filename(self.dir_stock_codes_base))
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
        chouma_err_file = os.path.join(self.dir_chouma_base, 'err_chouma_codes', chouma_err_filename)
        with open(chouma_err_file, 'w') as f:
            f.write(str(err_dict))

        #################  记录无异常，能够找到筹码数据的codes   ###########################
        chouma_filename = base_utils.save_out_filename(filehead=f"chouma_data", file_type='xlsx')
        chouma_data_file = os.path.join(self.dir_chouma_base, 'suc_chouma_data', chouma_filename)
        chouma_total_df.to_excel(chouma_data_file, float_format='%.0f', index=False)

        print("-------------  {} 日股票筹码数据获取完毕，获得{}个股票的筹码数据".format(formatted_date, len(list_suc_res)))

        self.stock_chouma_available = chouma_total_df

        end_time = time.time()  # 记录结束时间
        elapsed_time = end_time - start_time  # 计算时间差
        print(f"获取筹码数据的get_chouma_datas() 代码执行时间: {elapsed_time} 秒")

    def setup(self):
        self.login()
        self.get_all_stocks()
        self.get_limit_summary()
        # self.get_chouma_datas()


if __name__ == '__main__':
    savedata = SaveData()
    savedata.setup()
