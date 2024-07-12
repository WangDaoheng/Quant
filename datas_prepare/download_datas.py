import os
from insight_python.com.insight import common
from insight_python.com.insight.query import *
from insight_python.com.insight.market_service import market_service
from datetime import datetime
import time

# import dataprepare_properties
# import dataprepare_utils
import CommonProperties.Base_Properties as dataprepare_properties
import datas_prepare.dataprepare_utils as dataprepare_utils

# ************************************************************************
# 本代码的作用是下载一些数据,本地保存,用于后续分析
# 需要下载的数据:
# 1.筹码分布数据   get_chouma_datas()


# ************************************************************************


class SaveData:

    def __init__(self):
        ## 文件保存路径
        self.dir_base = dataprepare_properties.dir_base
        self.dir_chouma_base = os.path.join(self.dir_base, 'chouma')
        self.dir_stock_codes_base = os.path.join(self.dir_chouma_base, 'stock_codes')

        ## 当日处于上市状态的stock   list:全量股票   dict:按前三位分组
        self.stock_all_list = []
        self.stock_all_dict = {}

        ## 可以获取筹码的股票数据
        self.stock_chouma_available = ""


    def login(self):
        # 登陆前 初始化，没有密码可以访问进行自动化注册
        # https://findata-insight.htsc.com:9151/terminalWeb/#/signup
        user = dataprepare_properties.user
        password = dataprepare_properties.password
        common.login(market_service, user, password)

    def get_all_stocks(self):
        """
        获取当日的stock代码合集
        :return:
        """

        dt = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
        formatted_date = dt.strftime('%Y%m%d')

        ##  获取所有已上市codes
        stock_all_list = get_all_stocks_info(listing_state="上市交易")['htsc_code'].to_list()

        ## 导出全量合理状态codes, 用list形式导出
        stock_codes_listed_filename = dataprepare_utils.save_out_filename(filehead='stocks_codes_all', file_type='txt')
        stock_codes_listed_dir = os.path.join(self.dir_stock_codes_base, stock_codes_listed_filename)
        with open(stock_codes_listed_dir, 'w') as f:
            f.write(str(stock_all_list))
        ## 获取当日"上市交易"状态的所有股票代码
        self.stock_all_list = stock_all_list


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

        latest_stock_codes_file = os.path.join(self.dir_stock_codes_base, dataprepare_utils.get_latest_filename(self.dir_stock_codes_base))
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
        chouma_err_filename = dataprepare_utils.save_out_filename(filehead='chouma_err', file_type='txt')
        chouma_err_file = os.path.join(self.dir_chouma_base, 'err_chouma_codes', chouma_err_filename)
        with open(chouma_err_file, 'w') as f:
            f.write(str(err_dict))


        #################  记录无异常，能够找到筹码数据的codes   ###########################
        chouma_filename = dataprepare_utils.save_out_filename(filehead=f"chouma_data", file_type='xlsx')
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
        self.get_chouma_datas()


if __name__ == '__main__':
    savedata = SaveData()
    savedata.setup()
