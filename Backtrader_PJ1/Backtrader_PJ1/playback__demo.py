#!/usr/bin/python3
# -*- coding: utf-8 -*-

from insight_python.com.insight import common
from insight_python.com.insight.playback import *
from insight_python.com.insight.market_service import market_service
from datetime import datetime


def playback_tick_demo():
    """
    :param htsc_code: 华泰证券ID，入参为list或者string
    :param replay_time: 回放范围，默认为一天，list类型 [datetime，datetime]
    :param fq: 复权，默认前复权”pre”，后复权为”post”，不复权“none”
    """

    htsc_code = ["601688.SH", "000014.SZ"]
    fq = 'pre'
    start_time = "2022-04-20 09:00:00"
    stop_time = "2024-04-20 15:00:00"
    start_time = datetime.strptime(start_time, '%Y-%m-%d %H:%M:%S')
    stop_time = datetime.strptime(stop_time, '%Y-%m-%d %H:%M:%S')

    result = playback_tick(htsc_code=htsc_code, replay_time=[start_time, stop_time], fq=fq)

    print("------------ result的type:{}".format(type(result)))
    print(result)

def playback_trans_and_order_demo():
    """
    :param htsc_code: 华泰证券ID，入参为list或者string
    :param replay_time: 回放范围，默认为一天，list类型 [datetime，datetime]
    :param fq: 复权，默认前复权”pre”，后复权为”post”，不复权“none”
    """

    htsc_code = ["601688.SH", "000014.SZ"]
    fq = 'none'
    start_time = "2022-04-20 09:00:00"
    stop_time = "2022-04-20 15:00:00"
    start_time = datetime.strptime(start_time, '%Y-%m-%d %H:%M:%S')
    stop_time = datetime.strptime(stop_time, '%Y-%m-%d %H:%M:%S')

    playback_trans_and_order(htsc_code=htsc_code, replay_time=[start_time, stop_time], fq=fq)


# ************************************处理数据回放返回结果************************************
class insightmarketservice(market_service):

    def on_playback_tick(self, result):
        # pass
        print(result)

    def on_playback_trans_and_order(self, result):
        # pass
        print(result)


# ************************************用户登录************************************
# 登陆
# user 用户名
# password 密码
def login():
    markets = insightmarketservice()
    # 登陆前 初始化
    user = "USER019331L1"
    password = "F_Y+.3mtc4tU"
    result = common.login(markets, user, password)

    print(result)


# 配置日志打开
# open_trace trace日志开关     True为打开日志False关闭日志
# open_file_log  本地file日志开关     True为打开日志False关闭日志
# open_cout_log  控制台日志开关     True为打开日志False关闭日志
def config(open_trace=True, open_file_log=True, open_cout_log=True):
    common.config(open_trace, open_file_log, open_cout_log)


# 获取当前版本号
def get_version():
    print(common.get_version())


# 释放资源
def fini():
    common.fini()


# 使用指导：登陆 -> 订阅/查询/回放 -> 退出
def main(markets=None):
    # 登陆部分调用
    get_version()
    login()
    # 配置日志开关
    config(False, False, False)
    # config(True, True, True)

    # 回放部分接口调用
    playback_tick_demo()
    # playback_trans_and_order_demo()

    # 退出释放资源
    fini()


if __name__ == '__main__':
    main()
