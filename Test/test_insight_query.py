

from insight_python.com.insight import common
from insight_python.com.insight.query import *
from insight_python.com.insight.market_service import market_service
from datetime import datetime

import CommonProperties.Base_Properties as dataprepare_properties


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
    """

    # time_start_date = "2022-12-31 15:10:11"
    # time_end_date = "2024-01-02 15:20:50"
    # time_start_date = datetime.strptime(time_start_date, '%Y-%m-%d %H:%M:%S')
    # time_end_date = datetime.strptime(time_end_date, '%Y-%m-%d %H:%M:%S')

    time_start_date = "2024-01-14"
    time_end_date = "2024-04-03"
    time_start_date = datetime.strptime(time_start_date, '%Y-%m-%d')
    time_end_date = datetime.strptime(time_end_date, '%Y-%m-%d')

    result = get_kline(htsc_code=["DINIW.CFE"], time=[time_start_date, time_end_date],
                       frequency="daily", fq="none")
    result.to_csv('./DINIW.txt', sep=',')
    print(result)


def insight_billboard():
    ##  inc_list:涨幅榜
    result = get_billboard(type="inc_list", market=["sz_a_share", "sh_a_share"])
    result.to_csv()
    print(result)


if __name__ == "__main__":
    login()
    # get_kline_demo()
    insight_billboard()








