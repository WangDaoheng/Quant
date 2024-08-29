from insight_python.com.insight import common
from insight_python.com.insight.query import *
from insight_python.com.insight.market_service import market_service
from datetime import datetime
import os

import CommonProperties.Base_Properties as base_properties
import CommonProperties.Base_utils as base_utils
from CommonProperties.DateUtility import DateUtility

#  insight  测试环境文件保存目录
insight_test_dir = os.path.join(base_properties.dir_insight_base, 'test')


def login():
    # 登陆前 初始化，没有密码可以访问进行自动化注册
    # https://findata-insight.htsc.com:9151/terminalWeb/#/signup
    user = base_properties.user
    password = base_properties.password
    common.login(market_service, user, password)


def get_kline_daily_demo():
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

    # for index in index_list:

    #  获取数据的关键调用
    res = get_kline(htsc_code=index_list, time=[time_start_date, time_end_date],
                    frequency="daily", fq="none")

    index_df = pd.concat([index_df, res], ignore_index=True)


    ## 文件输出模块
    index_filename = base_utils.save_out_filename(filehead='index_a_share', file_type='csv')
    index_filedir = os.path.join(insight_test_dir, index_filename)
    index_df.to_csv(index_filedir, index=False)
    print("------------- get_index_a_share 完成测试文件输出 ---------------------")


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

    test_summary_filename = base_utils.save_out_filename(filehead='foreign_exchange', file_type='csv')
    test_summary_dir = os.path.join(insight_test_dir, test_summary_filename)
    res_df.to_csv(test_summary_dir, index=False)
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

    test_billboard_filename = base_utils.save_out_filename(filehead='stock_inc_billboard', file_type='csv')
    test_billboard_dir = os.path.join(insight_test_dir, test_billboard_filename)
    #  涨幅榜数据
    #  获取数据的关键调用
    result = get_billboard(type=function_type, market=market)
    result.to_csv(test_billboard_dir, index=False)



# 股东人数
def get_shareholder_num_demo():
    """
    param htsc_code: 华泰证券ID
    param name: 证券简称(和htsc_code任选其一)
    param end_date: 截止日期范围
    return: pandas.DataFrame
    """

    htsc_code = '601688.SH'
    name = '华泰证券'
    end_date_start_date = "2012-01-14"
    end_date_end_date = "2022-10-27"
    end_date_start_date = datetime.strptime(end_date_start_date, '%Y-%m-%d')
    end_date_end_date = datetime.strptime(end_date_end_date, '%Y-%m-%d')

    result = get_shareholder_num(htsc_code=htsc_code, name=name, end_date=[end_date_start_date, end_date_end_date])
    print(result)



# 沪深港通持股记录
def get_north_bound_demo():
    """
    :param htsc_code: 华泰证券ID
    :param trading_day: 时间范围
    :return: pandas.DataFrame
    """

    start_date = "2021-01-14"
    end_date = "2022-10-20"
    start_date = datetime.strptime(start_date, '%Y-%m-%d')
    end_date = datetime.strptime(end_date, '%Y-%m-%d')

    result = get_north_bound(htsc_code='601688.SH', trading_day=[start_date, end_date])
    print(result)


# 融资融券列表
def get_margin_target_demo():
    """
    :param htsc_code: 华泰证券ID
    :param exchange: 交易市场
    :return: pandas.DataFrame
    """

    result = get_margin_target(htsc_code='', exchange='XSHG')
    print(result)


# 融资融券交易汇总
def get_margin_summary_demo():
    """
    :param htsc_code: 华泰证券ID
    :param trading_day: 时间范围
    :return: pandas.DataFrame
    """

    start_date = "2010-01-14"
    end_date = "2022-10-20"
    start_date = datetime.strptime(start_date, '%Y-%m-%d')
    end_date = datetime.strptime(end_date, '%Y-%m-%d')

    result = get_margin_summary(htsc_code='601688.SH', trading_day=[start_date, end_date])
    print(result)


# 融资融券交易明细
def get_margin_detail_demo():
    """
    :param exchange: 交易市场，101 上海证券交易所 105 深圳证券交易所
    :param trading_day: 时间范围
    :return: pandas.DataFrame
    """

    start_date = "2014-01-14"
    end_date = "2022-10-20"
    start_date = datetime.strptime(start_date, '%Y-%m-%d')
    end_date = datetime.strptime(end_date, '%Y-%m-%d')

    result = get_margin_detail(exchange='XSHG', trading_day=[start_date, end_date])
    print(result)



# 港股行业分类-按证券ID查询
def get_hk_industry_demo():
    """
    :param htsc_code: 华泰证券代码
    :return: pandas.DataFrame
    """

    result = get_hk_industry(htsc_code="00750.HK")
    print(result)


# 港股行业分类-按行业代码查询
def get_hk_industry_stocks_demo():
    """
    :param industry_code: 行业代码
    :param classified: 行业分类 申万行业划分“sw”，证监会行业划分“zjh”，默认为申万行业划分
    :return: pandas.DataFrame
    """
    result = get_hk_industry_stocks(industry_code='430102')
    print(result)


# 港股交易日行情
def get_hk_daily_basic_demo():
    """
    :param htsc_code: 华泰证券ID
    :param trading_day: 查询时间范围
    :return: pandas.DataFrame
    """

    start_date = "2023-04-01"
    end_date = "2023-04-30"
    start_date = datetime.strptime(start_date, '%Y-%m-%d')
    end_date = datetime.strptime(end_date, '%Y-%m-%d')

    result = get_hk_daily_basic(htsc_code="00750.HK",
                                trading_day=[start_date, end_date])
    print(result)


# 港股估值
def get_hk_stock_valuation_demo():
    """
    :param htsc_code: 华泰证券ID
    :param trading_day: 查询时间范围
    :return: pandas.DataFrame
    """

    start_date = "2023-04-01"
    end_date = "2023-04-30"
    start_date = datetime.strptime(start_date, '%Y-%m-%d')
    end_date = datetime.strptime(end_date, '%Y-%m-%d')

    result = get_hk_stock_valuation(htsc_code="00750.HK",
                                    trading_day=[start_date, end_date])
    print(result)


# 港股基本信息
def get_hk_stock_basic_info_demo():
    """
    :param htsc_code: 华泰证券代码 字符串类型
    :param listing_date: 上市时间范围，列表类型，datetime格式 [start_date, end_date]
    :param listing_state: 上市状态: 未上市/上市/退市
    :return: pandas.DataFrame
    """

    listing_start_date = "2007-01-14"
    listing_end_date = "2018-10-20"
    listing_start_date = datetime.strptime(listing_start_date, '%Y-%m-%d')
    listing_end_date = datetime.strptime(listing_end_date, '%Y-%m-%d')

    result = get_hk_stock_basic_info(htsc_code='00817.HK',
                                     listing_date=[listing_start_date, listing_end_date],
                                     listing_state="上市")
    print(result)




if __name__ == "__main__":
    login()
    # get_kline_daily_demo()
    # get_foreign_exchange_demo()
    # insight_billboard()
    # get_shareholder_num_demo()
    get_north_bound_demo()




