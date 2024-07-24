from insight_python.com.insight import common
from insight_python.com.insight.query import *
from insight_python.com.insight.market_service import market_service
from datetime import datetime
import os

import CommonProperties.Base_Properties as dataprepare_properties
import CommonProperties.Base_utils as dataprepare_utils
from CommonProperties.DateUtility import DateUtility

#  测试环境文件保存目录
insight_test_dir = os.path.join(dataprepare_properties.dir_insight_base, 'test')


def login():
    # 登陆前 初始化，没有密码可以访问进行自动化注册
    # https://findata-insight.htsc.com:9151/terminalWeb/#/signup
    user = dataprepare_properties.user
    password = dataprepare_properties.password
    common.login(market_service, user, password)


def get_kline_index_a_share_demo():
    """
        000001.SH    上证指数
        399006.SZ	 创业板指
        000016.SH    上证50
        000300.SH    沪深300
        000849.SH    沪深300非银行金融指数
        000905.SH	 中证500
        399852.SZ    中证1000
        000688.SH    科创50
    港交所  .HK
    外汇   .CFE

    """

    time_start_date = DateUtility.first_day_of_month()
    time_end_date = DateUtility.today()

    time_start_date = datetime.strptime(time_start_date, '%Y%m%d')
    time_end_date = datetime.strptime(time_end_date, '%Y%m%d')

    index_list = ["000001.SH", "399006.SZ", "000016.SH", "000300.SH", "000849.SH", "000905.SH", "399852.SZ",
                  "000688.SH", ""]
    index_df = pd.DataFrame()

    for index in index_list:
        #  获取数据的关键调用
        res = get_kline(htsc_code=[index], time=[time_start_date, time_end_date],
                        frequency="daily", fq="none")

        index_df = pd.concat([index_df, res], ignore_index=True)

    ## 文件输出模块
    index_filename = dataprepare_utils.save_out_filename(filehead='index_a_share', file_type='csv')
    index_filedir = os.path.join(insight_test_dir, index_filename)
    index_df.to_csv(index_filedir)
    print("------------- get_index_a_share 完成测试文件输出 ---------------------")


def get_kline_future_demo():
    """
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

    """
    index_list = ["AU{}.SHF", "AG{}.SHF", "CU{}.SHF", "EC{}.INE", "SC{}.INE", "V{}.DCE"]
    replacement = DateUtility.first_day_of_month_after_n_months(2)[2:6]

    furture_index_list = [index.format(replacement) for index in index_list]

    #  查询起始时间写2月前的月初第1天
    time_start_date = DateUtility.first_day_of_month_after_n_months(-2)
    time_end_date = DateUtility.today()

    time_start_date = datetime.strptime(time_start_date, '%Y%m%d')
    time_end_date = datetime.strptime(time_end_date, '%Y%m%d')

    index_df = pd.DataFrame()

    for index in furture_index_list:
        #  获取数据的关键调用
        res = get_kline(htsc_code=[index], time=[time_start_date, time_end_date],
                        frequency="daily", fq="pre")

        index_df = pd.concat([index_df, res], ignore_index=True)

    ## 文件输出模块
    index_filename = dataprepare_utils.save_out_filename(filehead='future', file_type='csv')
    index_filedir = os.path.join(insight_test_dir, index_filename)
    index_df.to_csv(index_filedir)
    print("------------- get_kline_future_demo 完成测试文件输出 ---------------------")


def get_foreign_exchange_demo():
    """
    本质上这是一个将stock_codes 跟 exchange 随意组合的 get_kline 的demo
    这里主要用于对外汇中的美元指数进行探索，查找数据源

    000001.SH    上证指数
    000016.SH    上证50
    000300.SH    沪深300
    000849.SH    沪深300非银行金融指数
    000905.SH	 中证500
    000688.SH    科创50
    港交所  .HK
    外汇   .CFE
    """

    time_start_date = "2024-01-14"
    time_end_date = "2024-04-03"
    time_start_date = datetime.strptime(time_start_date, '%Y-%m-%d')
    time_end_date = datetime.strptime(time_end_date, '%Y-%m-%d')

    stock_code = ['DX0W', 'DX0Y', 'DXY', 'USDIND', 'USD']
    exchange_code = ['CF', 'CFE', 'CNI', 'CSI', 'HT']
    res_df = pd.DataFrame()

    for stock in stock_code:
        for exchange in exchange_code:
            stock_exchange = stock + '.' + exchange

            #  获取数据的关键调用
            res = get_kline(htsc_code=[stock_exchange], time=[time_start_date, time_end_date],
                            frequency="daily", fq="none")
            res_df = pd.concat([res_df, res], ignore_index=True)

    test_summary_filename = dataprepare_utils.save_out_filename(filehead='foreign_exchange', file_type='csv')
    test_summary_dir = os.path.join(insight_test_dir, test_summary_filename)
    res_df.to_csv(test_summary_dir)
    print("------------- get_foreign_exchange_demo() 完成测试文件输出 ---------------------")


def insight_billboard(function_type='inc_list', market=['sh_a_share', 'sz_a_share']):
    """
    Args:
        function_type:
            inc_list         涨幅榜
            inc_list_5min    5min涨速榜
        market:
            1	sh_a_share	上海A股
            2	sz_a_share	深圳A股
            3	a_share	A股
            4	a_share	B股
            5	gem	创业
            6	sme	中小板
            7	star	科创板
    Returns: 问题是只返回榜单前十，只有5min涨速有点用     这个基本没用
    """

    test_billboard_filename = dataprepare_utils.save_out_filename(filehead='stock_inc_billboard', file_type='csv')
    test_billboard_dir = os.path.join(insight_test_dir, test_billboard_filename)
    #  涨幅榜数据
    #  获取数据的关键调用
    result = get_billboard(type=function_type, market=market)
    result.to_csv(test_billboard_dir)


def get_change_summary_demo():
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
    start_date = '2024-07-15'
    end_date = '2024-07-16'
    # 转为时间格式
    start_date = datetime.strptime(start_date, '%Y-%m-%d')
    end_date = datetime.strptime(end_date, '%Y-%m-%d')

    #  获取数据的关键调用
    result_df = get_change_summary(market=["a_share"], trading_day=[start_date, end_date])

    filter_limit_df = result_df[['time',
                                 'name',
                                 'ups_downs_limit_count_up_limits',
                                 'ups_downs_limit_count_down_limits',
                                 'ups_downs_limit_count_pre_up_limits',
                                 'ups_downs_limit_count_pre_down_limits',
                                 'ups_downs_limit_count_pre_up_limits_average_change_percent']]
    filter_limit_df.columns = ['time', 'name', '今日涨停', '今日跌停', '昨日涨停', '昨日跌停', '昨日涨停表现']

    test_summary_filename = dataprepare_utils.save_out_filename(filehead='stock_summary', file_type='csv')
    test_summary_dir = os.path.join(insight_test_dir, test_summary_filename)
    filter_limit_df.to_csv(test_summary_dir)


if __name__ == "__main__":
    login()
    get_kline_future_demo()
    # insight_billboard()
    # get_change_summary_demo()
