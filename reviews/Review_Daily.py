
import os
from insight_python.com.insight import common
from insight_python.com.insight.query import *
from insight_python.com.insight.market_service import market_service
from datetime import datetime
import time


from CommonProperties import Base_Properties


class ReviewDaily:

    def __init__(self):
        pass


    def login(self):
        # 登陆前 初始化，没有密码可以访问进行自动化注册
        # https://findata-insight.htsc.com:9151/terminalWeb/#/signup
        user = Base_Properties.user
        password = Base_Properties.password
        common.login(market_service, user, password)

    def Overall_US(self):
        """
        外部市场指标: US市场风格
        类型:       外部因素大盘
        Returns:
            USDX: 美元指数   参考 get_US_stocks.get_vantage_DXY  手动计算了美元指数，仅供参考


        """
        pass

    def Overall_North(self):
        """
        外部市场指标: 北向资金力度
        类型:       外部因素大盘
        Returns:
        """
        pass

    def Overall_Margin_Trading(self):
        """
        外部市场指标: 市场融资融券
        类型:       内部因素大盘
        Returns:
        """
        pass



    def Short_Lianban_Max(self):
        """
        短线情绪指标: 最高连板
        类型:       短线情绪整体
        Returns:
        """
        pass

    def Short_Lianban_Num(self):
        """
        短线情绪指标: 二板以上数量 & 首板数量
        类型:       短线情绪整体
        Returns:  Lianban_Num
                  Firstban_Num
        """
        pass




    def Sector_Precious_Metal(self):
        """
        关键板块指标: 贵金属板块
        类型:
        Returns:    gold  
        """
        pass



    def setup(self):
        pass






if __name__ == "__main__":
    pass






















