

from insight_python.com.insight import common
from insight_python.com.insight.query import *
from insight_python.com.insight.market_service import market_service
from datetime import datetime
import os


import CommonProperties.Base_Properties as dataprepare_properties
import CommonProperties.Base_utils      as dataprepare_utils

insight_test_dir = os.path.join(dataprepare_properties.dir_insight_base, 'test')


def login():
    # 登陆前 初始化，没有密码可以访问进行自动化注册
    # https://findata-insight.htsc.com:9151/terminalWeb/#/signup
    user = dataprepare_properties.user
    password = dataprepare_properties.password
    common.login(market_service, user, password)


def get_kline_demo():
    """
    :param htsc_code: 华泰证券代码，支持多个code查询，列表类型
    :param time: 时间范围，list类型，开始结束时间为datetime
    :param frequency: 频率，分钟K（‘1min’，’5min’，’15min’，’60min’），日K（‘daily’），周K（‘weekly’），月K（‘monthly’）
    :param fq: 复权，默认前复权”pre”，后复权为”post”，不复权“none”
    :return:pandas.DataFrame

    000001.SH    上证指数
    000016.SH    上证50
    000300.SH    沪深300
    000849.SH    沪深300非银行金融指数
    000905.SH	 中证500
    000688.SH    科创50
    港交所  .HK
    外汇   .CFE

    """

    # time_start_date = "2022-12-31 15:10:11"
    # time_end_date = "2024-01-02 15:20:50"
    # time_start_date = datetime.strptime(time_start_date, '%Y-%m-%d %H:%M:%S')
    # time_end_date = datetime.strptime(time_end_date, '%Y-%m-%d %H:%M:%S')

    time_start_date = "2024-01-14"
    time_end_date = "2024-04-03"
    time_start_date = datetime.strptime(time_start_date, '%Y-%m-%d')
    time_end_date = datetime.strptime(time_end_date, '%Y-%m-%d')

    result01 = get_kline(htsc_code=["DX0W.CF"], time=[time_start_date, time_end_date],
                       frequency="daily", fq="none")

    result02 = get_kline(htsc_code=["DX0W.CFE"], time=[time_start_date, time_end_date],
                       frequency="daily", fq="none")

    result011 = get_kline(htsc_code=["DX0W.CNI"], time=[time_start_date, time_end_date],
                       frequency="daily", fq="none")

    result022 = get_kline(htsc_code=["DX0W.CSI"], time=[time_start_date, time_end_date],
                       frequency="daily", fq="none")

    result023 = get_kline(htsc_code=["DX0W.HT"], time=[time_start_date, time_end_date],
                       frequency="daily", fq="none")

    result03 = get_kline(htsc_code=["DX0Y.CF"], time=[time_start_date, time_end_date],
                         frequency="daily", fq="none")

    result04 = get_kline(htsc_code=["DX0Y.CFE"], time=[time_start_date, time_end_date],
                         frequency="daily", fq="none")


    result011 = get_kline(htsc_code=["DX0Y.CNI"], time=[time_start_date, time_end_date],
                       frequency="daily", fq="none")

    result022 = get_kline(htsc_code=["DX0Y.CSI"], time=[time_start_date, time_end_date],
                       frequency="daily", fq="none")

    result023 = get_kline(htsc_code=["DX0Y.HT"], time=[time_start_date, time_end_date],
                       frequency="daily", fq="none")



    result05 = get_kline(htsc_code=["DXY.CF"], time=[time_start_date, time_end_date],
                         frequency="daily", fq="none")

    result06 = get_kline(htsc_code=["DXY.CFE"], time=[time_start_date, time_end_date],
                         frequency="daily", fq="none")

    result011 = get_kline(htsc_code=["DXY.CNI"], time=[time_start_date, time_end_date],
                       frequency="daily", fq="none")

    result022 = get_kline(htsc_code=["DXY.CSI"], time=[time_start_date, time_end_date],
                       frequency="daily", fq="none")

    result023 = get_kline(htsc_code=["DXY.HT"], time=[time_start_date, time_end_date],
                       frequency="daily", fq="none")



    result07 = get_kline(htsc_code=["USDIND.CF"], time=[time_start_date, time_end_date],
                         frequency="daily", fq="none")

    result08 = get_kline(htsc_code=["USDIND.CFE"], time=[time_start_date, time_end_date],
                         frequency="daily", fq="none")


    result011 = get_kline(htsc_code=["USDIND.CNI"], time=[time_start_date, time_end_date],
                       frequency="daily", fq="none")

    result022 = get_kline(htsc_code=["USDIND.CSI"], time=[time_start_date, time_end_date],
                       frequency="daily", fq="none")

    result023 = get_kline(htsc_code=["USDIND.HT"], time=[time_start_date, time_end_date],
                       frequency="daily", fq="none")


    result07 = get_kline(htsc_code=["USD.CF"], time=[time_start_date, time_end_date],
                         frequency="daily", fq="none")

    result08 = get_kline(htsc_code=["USD.CFE"], time=[time_start_date, time_end_date],
                         frequency="daily", fq="none")


    result011 = get_kline(htsc_code=["USD.CNI"], time=[time_start_date, time_end_date],
                       frequency="daily", fq="none")

    result022 = get_kline(htsc_code=["USD.CSI"], time=[time_start_date, time_end_date],
                       frequency="daily", fq="none")

    result023 = get_kline(htsc_code=["USD.HT"], time=[time_start_date, time_end_date],
                       frequency="daily", fq="none")


    print("------------- hello world ---------------------")
    # result.to_csv('./cfe.txt', sep=',')
    # print(result)


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
    Returns: 问题是只返回榜单前十，只有5min涨速有点用
    """

    test_billboard_filename = dataprepare_utils.save_out_filename(filehead='stock_inc_billboard', file_type='csv')
    test_billboard_dir = os.path.join(insight_test_dir, test_billboard_filename)
    ##  涨幅榜数据
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


    result_df = get_change_summary(market=["a_share"], trading_day=[start_date, end_date])

    filter_limit_df = result_df[['time',
                                 'name',
                                 'ups_downs_limit_count_up_limits',
                                 'ups_downs_limit_count_down_limits',
                                 'ups_downs_limit_count_pre_up_limits',
                                 'ups_downs_limit_count_pre_down_limits',
                                 'ups_downs_limit_count_pre_up_limits_average_change_percent']]
    filter_limit_df.columns=['time','name','今日涨停','今日跌停','昨日涨停','昨日跌停','昨日涨停表现']

    test_summary_filename = dataprepare_utils.save_out_filename(filehead='stock_summary', file_type='csv')
    test_summary_dir = os.path.join(insight_test_dir, test_summary_filename)
    filter_limit_df.to_csv(test_summary_dir)



if __name__ == "__main__":
    login()
    get_kline_demo()
    # insight_billboard()
    # get_change_summary_demo()








