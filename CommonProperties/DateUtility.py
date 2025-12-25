from datetime import datetime, timedelta
import calendar


class DateUtility:
    """
    日期工具类：统一偏移规则（0=当前周期，正数=往后n周期，负数=往前n周期）
    输出格式：所有日期均返回 YYYYMMDD 字符串
    """
    @staticmethod
    def today():
        """获取今日日期"""
        return datetime.today().strftime('%Y%m%d')


    @staticmethod
    def next_day(n=0):
        """
        获取偏移n天的日期
        :param n: 天数偏移量，0=今日，正数=往后n天，负数=往前n天
        """
        next_date = datetime.today() + timedelta(days=n)
        return next_date.strftime('%Y%m%d')


    @staticmethod
    def is_monday():
        """判断今日是否是周一"""
        today = datetime.today()
        return today.weekday() == 0  # 星期一的weekday()返回值是0

    @staticmethod
    def is_friday():
        """判断今日是否是周五"""
        today = datetime.today()
        return today.weekday() == 4  # 星期五的weekday()返回值是4

    @staticmethod
    def is_weekend():
        """判断今日是否是周末（周六/周日）"""
        today = datetime.today()
        # 在大多数国家，周末是周六和周日，即weekday()返回5（周六）或6（周日）
        return today.weekday() >= 5


    @staticmethod
    def first_day_of_week(n=0):
        """
        获取指定偏移周的第一天（周一）
        :param n: 周偏移量，0=本周，1=下周，-1=上周
        """
        today = datetime.today()
        offset_days = -today.weekday() + n * 7
        start_of_week = today + timedelta(days=offset_days)
        return start_of_week.strftime('%Y%m%d')


    @staticmethod
    def last_day_of_week(n=0):
        """
        获取指定偏移周的最后一天（周日）
        :param n: 周偏移量，0=本周，1=下周，-1=上周
        """
        today = datetime.today()
        offset_days = (6 - today.weekday()) + n * 7
        last_day = today + timedelta(days=offset_days)
        return last_day.strftime('%Y%m%d')


    @staticmethod
    def first_day_of_month(n=0):
        """
        获取指定偏移月的第一天
        :param n: 月偏移量，0=本月，1=下月，-1=上月
        """
        today = datetime.today()
        month = today.month - 1 + n  ## 先转0-11月（便于计算）
        year = today.year + month // 12
        month = month % 12 + 1  ## 转回1-12月
        first_day = datetime(year, month, 1)
        return first_day.strftime('%Y%m%d')

    @staticmethod
    def last_day_of_month(n=0):
        """
        获取指定偏移月的最后一天
        :param n: 月偏移量，0=本月，1=下月，-1=上月
        """
        today = datetime.today()
        month = today.month - 1 + n
        year = today.year + month // 12
        month = month % 12 + 1
        last_day = calendar.monthrange(year, month)[1]  # 获取当月最后一天
        last_day_date = datetime(year, month, last_day)
        return last_day_date.strftime('%Y%m%d')

    # 季度相关
    @staticmethod
    def first_day_of_quarter(n=0):
        """
        获取指定偏移季度的第一天（季首：1/4/7/10月）
        :param n: 季度偏移量，0=本季度，1=下季度，-1=上季度
        """
        today = datetime.today()
        current_quarter = (today.month - 1) // 3 + 1
        target_quarter = current_quarter + n

        year = today.year + (target_quarter - 1) // 4
        quarter_month = ((target_quarter - 1) % 4) * 3 + 1
        first_day = datetime(year, quarter_month, 1)
        return first_day.strftime('%Y%m%d')

    @staticmethod
    def last_day_of_quarter(n=0):
        """
        获取指定偏移季度的最后一天（季末：3/6/9/12月）
        :param n: 季度偏移量，0=本季度，1=下季度，-1=上季度
        """
        today = datetime.today()
        current_quarter = (today.month - 1) // 3 + 1
        target_quarter = current_quarter + n

        year = today.year + (target_quarter - 1) // 4
        quarter_month = ((target_quarter - 1) % 4) * 3 + 3
        last_day = calendar.monthrange(year, quarter_month)[1]

        last_day_date = datetime(year, quarter_month, last_day)
        return last_day_date.strftime('%Y%m%d')


    @staticmethod
    def first_day_of_year(n=0):
        """
        获取指定偏移年的第一天
        :param n: 年偏移量，0=本年，1=下一年，-1=上一年
        """
        today = datetime.today()
        first_day = datetime(today.year + n, 1, 1)
        return first_day.strftime('%Y%m%d')


    @staticmethod
    def last_day_of_year(n=0):
        """
        获取指定偏移年的最后一天
        :param n: 年偏移量，0=本年，1=下一年，-1=上一年
        """
        today = datetime.today()
        last_day = datetime(today.year + n, 12, 31)
        return last_day.strftime('%Y%m%d')



# 测试
if __name__ == "__main__":
    date_utility = DateUtility()

    print("今日日期:", date_utility.today())
    print("当前是否是周末:", date_utility.is_weekend())
    print("-----------------------------------------------")
    print("本周第一天日期:", date_utility.first_day_of_week())
    print("本月第1天日期:", date_utility.first_day_of_month())
    print("本季度第一天日期:", date_utility.first_day_of_quarter())
    print("本年第一天日期:", date_utility.first_day_of_year())

























