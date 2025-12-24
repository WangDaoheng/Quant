from datetime import datetime, timedelta
import calendar


class DateUtility:
    @staticmethod
    def today():
        return datetime.today().strftime('%Y%m%d')


    @staticmethod
    def next_day(n=0):
        next_date = datetime.today() + timedelta(days=n)
        return next_date.strftime('%Y%m%d')


    @staticmethod
    def is_monday():
        today = datetime.today()
        return today.weekday() == 0  # 星期一的weekday()返回值是0

    @staticmethod
    def is_friday():
        today = datetime.today()
        return today.weekday() == 4  # 星期五的weekday()返回值是4

    @staticmethod
    def is_weekend():
        today = datetime.today()
        # 在大多数国家，周末是周六和周日，即weekday()返回5（周六）或6（周日）
        return today.weekday() >= 5


    @staticmethod
    def first_day_of_week(n=0):
        """获取指定偏移周的第一天（周一，默认本周）"""
        today = datetime.today()
        offset_days = -today.weekday() + n * 7
        start_of_week = today + timedelta(days=offset_days)
        return start_of_week.strftime('%Y%m%d')


    @staticmethod
    def last_day_of_week(n=0):
        """获取指定偏移周的最后一天（周日，默认本周）"""
        today = datetime.today()
        offset_days = (6 - today.weekday()) + n * 7
        last_day = today + timedelta(days=offset_days)
        return last_day.strftime('%Y%m%d')


    @staticmethod
    def first_day_of_month(n=0):
        """
         n月后的月的第一天, n=0 就是本月的第1天; n=1 就是1月后的第1天; n=-1 就是上1月的第1天
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
        获取指定偏移月的最后一天（默认本月）
        :param n: 月份偏移量【可选】，0=本月，正数=往后n月，负数=往前n月
        :return: 日期字符串（YYYYMMDD）
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
        """获取指定偏移季度的第一天（默认本季度）"""
        today = datetime.today()
        current_quarter = (today.month - 1) // 3 + 1
        target_quarter = current_quarter + n

        year = today.year + (target_quarter - 1) // 4
        quarter_month = ((target_quarter - 1) % 4) * 3 + 1
        first_day = datetime(year, quarter_month, 1)
        return first_day.strftime('%Y%m%d')

    @staticmethod
    def last_day_of_quarter(n=0):
        """获取指定偏移季度的最后一天（默认本季度）"""
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
        """获取指定偏移年的第一天（默认本年）"""
        today = datetime.today()
        first_day = datetime(today.year + n, 1, 1)
        return first_day.strftime('%Y%m%d')


    @staticmethod
    def last_day_of_year(n=0):
        """获取指定偏移年的最后一天（默认本年）"""
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






























