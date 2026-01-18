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


def get_trading_days_demo():
    trading_day_start_date = "2022-10-14"
    trading_day_end_date = "2027-10-20"
    trading_day_start_date = datetime.strptime(trading_day_start_date, '%Y-%m-%d')
    trading_day_end_date = datetime.strptime(trading_day_end_date, '%Y-%m-%d')

    # 调用获取交易日历结果
    result = get_trading_days(trading_day=[trading_day_start_date,
                                           trading_day_end_date], exchange='XSHG')

    # 步骤1：解析真实 result 结构（关键修正）
    exchange_name = result[0]  # 提取第一个元素：交易所名称（XSHG）
    trading_series = result[1]  # 提取第二个元素：pandas Series（包含所有交易日）
    # 将 Series 转换为列表（获取所有交易日数据，解决只取到1个元素的问题）
    trading_dates_list = trading_series.tolist()  # 核心方法：Series.tolist()

    # 步骤2：构造 DataFrame 所需的数据源
    df_data = {
        'exchange': [exchange_name] * len(trading_dates_list),  # 生成匹配长度的交易所列表
        'trading_days': trading_dates_list
    }
    trading_df = pd.DataFrame(df_data)

    # 步骤3：按 exchange 和 trading_days 升序排序
    trading_df.sort_values(by=['exchange', 'trading_days'], ascending=True, inplace=True)

    # 可选：重置排序后的索引（避免索引混乱）
    trading_df.reset_index(drop=True, inplace=True)

    return trading_df



def get_stock_code_demo():
    #  1.获取日期
    formatted_date = DateUtility.today()
    # formatted_date = '20240930'

    #  2.请求insight数据   get_all_stocks_info
    res = get_stock_info(htsc_code='873169.BJ')

    listing_start_date = "2018-01-14"
    listing_end_date = "2024-11-20"
    listing_start_date = datetime.strptime(listing_start_date, '%Y-%m-%d')
    listing_end_date = datetime.strptime(listing_end_date, '%Y-%m-%d')
    # 获取股票603980.SH的基础信息
    result = get_stock_info(htsc_code="873169.BJ",
                                  listing_date=[listing_start_date, listing_end_date])


    stock_all_df = get_all_stocks_info(exchange='XSHG', listing_state="上市交易")
    # stock_all_df.to_csv(r'F:\QDatas\tttt.csv')
    #
    # df01 = get_all_stocks_info(exchange='XBJE')
    # df01.to_csv(r'F:\QDatas\df01.csv')

    df011 = get_all_stocks_info(exchange='XBSE')
    df011.to_csv(r'F:\QDatas\df01.csv')

    df02 = get_all_stocks_info(exchange=['XSHG', 'XSHE', 'XBSE'])
    df02.to_csv(r'F:\QDatas\df02.csv')


    #  3.日期格式转换
    stock_all_df.insert(0, 'ymd', formatted_date)

    #  4.声明所有的列名，去除多余列
    stock_all_df = stock_all_df[['ymd', 'htsc_code', 'name', 'exchange']]
    filtered_df = stock_all_df[~stock_all_df['name'].str.contains('ST|退|B')]

    #  5.删除重复记录，只保留每组 (ymd, stock_code) 中的第一个记录
    filtered_df = filtered_df.drop_duplicates(subset=['ymd', 'htsc_code'], keep='first')

    pass



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

    # index_list = ["000001.SH", "399006.SZ", "000016.SH", "000300.SH", "000849.SH", "000905.SH", "399852.SZ",
    #               "000688.SH"]


    # index_list = ["240202.SH", "240202.SZ", "240202", "240202.sw", "240202.SW", "240202.HTSC", "240202.HT", ""
    #               "240202.sw_l3", "240202.SW_L3"]

    index_list = ["873169.BJ"]

    # 定义要替换的部分和新值
    old_value = "240202"
    new_value = "270000"

    # 使用列表推导式来进行替换
    new_index_list = [item.replace(old_value, new_value) for item in index_list]

    index_df = pd.DataFrame()

    #  获取数据的关键调用
    for code in new_index_list:
        print(f'------------------- 执行 {code} 的查询 ---------------------')
        res = get_kline(htsc_code=code, time=[time_start_date, time_end_date],
                        frequency="daily", fq="none")
        index_df = pd.concat([index_df, res], ignore_index=True)

    ## 文件输出模块
    print(index_df)

    index_filename = base_utils.save_out_filename(filehead='index_a_share', file_type='csv')
    index_filedir = os.path.join(insight_test_dir, index_filename)
    index_df.to_csv(index_filedir, index=False)
    print("------------- get_kline_daily_demo  完成测试文件输出 ---------------------")


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
    get_trading_days_demo()
    # get_stock_code_demo()
    # get_kline_daily_demo()
    # get_foreign_exchange_demo()
    # insight_billboard()
    # get_shareholder_num_demo()
    # get_north_bound_demo()




