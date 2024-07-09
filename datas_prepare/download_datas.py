import os
from insight_python.com.insight import common
from insight_python.com.insight.query import *
from insight_python.com.insight.market_service import market_service
from datetime import datetime

import dataprepare_properties
import dataprepare_utils

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

        ## 可以获取筹码的stock
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

        self.stock_all_list = stock_all_list


    def get_chouma_datas(self):
        """
        1.获取每日的筹码分布数据
        2.找到那些当日能够拿到筹码数据的codes
        :return:
        """
        print("----------------- get_chouma_datas() 开始执行 ------------------")
        dt = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
        formatted_date = dt.strftime('%Y%m%d')

        ##  所有已上市股票
        # list_stock = self.stock_all_list

        latest_stock_codes_file = os.path.join(self.dir_stock_codes_base, dataprepare_utils.get_latest_filename(self.dir_stock_codes_base))
        with open(latest_stock_codes_file, 'r') as f:
            content = f.readlines()

        list_stock = eval(content[0].strip())
        print(r'   在 {} 日，共有 {} 个已上市stocks'.format(formatted_date,len(list_stock)))

        ############################  准备记录能够查到筹码数据的结果list  ###########################
        list_res = []  # 用于存放遍历时不发生报错的 enum
        err_dict = {}  # 用于存放发生报错的 enum 和具体的报错原因
        ttflag = 0

        ## 存放拼接结果
        chouma_total_df = pd.DataFrame()

        # 获取在指定时间范围的筹码分布数据
        for enum in list_stock:
            ttflag = ttflag + 1
            try:
                chouma_df = get_chip_distribution(htsc_code=enum, trading_day=[dt])
                # print("     =======  拉取第{}条筹码数据：{}返回的结果条数为{}".format(ttflag, enum, chouma_df.shape[0]))
                chouma_total_df = pd.concat([chouma_total_df, chouma_df], ignore_index=True)

            except Exception as e:
                err_dict[enum] = str(e)

            else:
                list_res.append(enum)

        #################  记录有异常，不能找到筹码数据的codes   ###########################
        chouma_err_filename = dataprepare_utils.save_out_filename(filehead='chouma_err', file_type='txt')
        chouma_err_file = os.path.join(self.dir_chouma_base, 'err_chouma_codes', chouma_err_filename)
        with open(chouma_err_file, 'w') as f:
            f.write(str(err_dict))


        #################  记录无异常，能够找到筹码数据的codes   ###########################
        chouma_filename = dataprepare_utils.save_out_filename(filehead=f"chouma_data", file_type='xlsx')
        chouma_data_file = os.path.join(self.dir_chouma_base, 'chouma_data', chouma_filename)
        chouma_total_df.to_excel(chouma_data_file, float_format='%.0f', index=False)

        print("-------------  {} 可获得筹码数据的股票   共有{}个元素：, 不能获得筹码数据的股票有 {} 个".format(data_date, len(list_res), len(err_dict)))

        self.stock_chouma_available = chouma_total_df



    def setup(self):
        self.login()
        self.get_all_stocks()
        self.get_chouma_datas()


if __name__ == '__main__':
    savedata = SaveData()
    savedata.setup()
