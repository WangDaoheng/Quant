from datetime import datetime, timedelta
import calendar


class DateUtility:
    @staticmethod
    def today():
        return datetime.today().strftime('%Y%m%d')

    @staticmethod
    def is_weekend():
        today = datetime.today()
        # 在大多数国家，周末是周六和周日，即weekday()返回5（周六）或6（周日）
        return today.weekday() >= 5


    @staticmethod
    def first_day_of_week():
        today = datetime.today()
        start_of_week = today - timedelta(days=today.weekday())
        return start_of_week.strftime('%Y%m%d')

    @staticmethod
    def first_day_of_month():
        today = datetime.today()
        first_day = today.replace(day=1)
        return first_day.strftime('%Y%m%d')

    @staticmethod
    def first_day_of_quarter():
        today = datetime.today()
        current_month = today.month
        # Determine the first month of the current quarter
        first_month_of_quarter = current_month - (current_month - 1) % 3
        first_day = today.replace(month=first_month_of_quarter, day=1)
        return first_day.strftime('%Y%m%d')

    @staticmethod
    def first_day_of_year():
        today = datetime.today()
        first_day = today.replace(month=1, day=1)
        return first_day.strftime('%Y%m%d')


    @staticmethod
    def last_day_of_week_before_n_weeks(n=0):
        """
        n周前的周的最后一天, n=0 就是本周的最后一天; n=1 就是上周的最后一天; n=-1 就是下1周的最后一天
        """
        today = datetime.today()
        start_of_this_week = today - timedelta(days=today.weekday())
        end_of_nth_last_week = start_of_this_week - timedelta(days=1 + (n - 1) * 7)
        return end_of_nth_last_week.strftime('%Y%m%d')

    @staticmethod
    def last_day_of_month_before_n_months(n=0):
        """
        n月前的月的最后一天, n=0 就是本月的最后一天; n=1 就是上月的最后一天; n=-1 就是下1月的最后一天
        """
        today = datetime.today()
        month = today.month - n
        year = today.year
        while month <= 0:
            month += 12
            year -= 1
        last_day = calendar.monthrange(year, month)[1]
        last_day_of_nth_last_month = datetime(year, month, last_day)
        return last_day_of_nth_last_month.strftime('%Y%m%d')

    @staticmethod
    def last_day_of_quarter_before_n_quarters(n=0):
        """
        n季前的季的最后一天, n=0 就是本季的最后一天; n=1 就是上季的最后一天; n=-1 就是下1季的最后一天
        """
        today = datetime.today()
        current_month = today.month
        current_quarter = (current_month - 1) // 3 + 1
        last_quarter = current_quarter - n
        year = today.year
        while last_quarter <= 0:
            last_quarter += 4
            year -= 1
        quarter_month = (last_quarter - 1) * 3 + 3
        last_day_of_nth_last_quarter = datetime(year, quarter_month, calendar.monthrange(year, quarter_month)[1])
        return last_day_of_nth_last_quarter.strftime('%Y%m%d')

    @staticmethod
    def last_day_of_year_before_n_years(n):
        """
        n年前的年的最后一天, n=0 就是本年的最后一天; n=1 就是上年的最后一天; n=-1 就是下1年的最后一天
        """
        today = datetime.today()
        last_day_of_nth_last_year = today.replace(year=today.year - n, month=12, day=31)
        return last_day_of_nth_last_year.strftime('%Y%m%d')


    @staticmethod
    def first_day_of_week_after_n_weeks(n):
        """
        n周后的周的第一天, n=0 就是本周的第1天; n=1 就是1周后的第1天; n=-1 就是上1周的第1天
        """
        today = datetime.today()
        start_of_nth_week = today + timedelta(days=(7 - today.weekday()) + (n - 1) * 7)
        return start_of_nth_week.strftime('%Y%m%d')


    @staticmethod
    def first_day_of_month_after_n_months(n):
        """
        n月后的月的第一天, n=0 就是本月的第1天; n=1 就是1月后的第1天; n=-1 就是上1月的第1天
        """
        today = datetime.today()
        month = today.month - 1 + n
        year = today.year + month // 12
        month = month % 12 + 1
        first_day = datetime(year, month, 1)
        return first_day.strftime('%Y%m%d')


    @staticmethod
    def first_day_of_quarter_after_n_quarters(n):
        """
        n季后的季的第一天, n=0 就是本季的第1天; n=1 就是1季后的第1天; n=-1 就是上1季的第1天
        """
        today = datetime.today()
        current_month = today.month
        current_quarter = (current_month - 1) // 3 + 1
        next_quarter = current_quarter + n
        year = today.year + (next_quarter - 1) // 4
        quarter_month = ((next_quarter - 1) % 4) * 3 + 1
        first_day_of_nth_quarter = datetime(year, quarter_month, 1)
        return first_day_of_nth_quarter.strftime('%Y%m%d')


    @staticmethod
    def first_day_of_year_after_n_years(n):
        """
        n年后的年初的第一天, n=0 就是本年的第1天; n=1 就是1年后的第1天; n=-1 就是上1年的第1天
        """
        today = datetime.today()
        first_day_of_nth_year = today.replace(year=today.year + n, month=1, day=1)
        return first_day_of_nth_year.strftime('%Y%m%d')





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
    print("-----------------------------------------------")
    print("上个周最后一天日期:", date_utility.last_day_of_last_week())
    print("上个月最后一天日期:", date_utility.last_day_of_last_month())
    print("上个季度最后一天日期:", date_utility.last_day_of_last_quarter())
    print("上年最后一天日期:", date_utility.last_day_of_last_year())
    print("-----------------------------------------------")
    print("下个周周一的日期:", date_utility.first_day_of_next_week())
    print("下个月第一天日期:", date_utility.first_day_of_next_month())
    print("下个季度第一天日期:", date_utility.first_day_of_next_quarter())
    print("下年第一天日期:", date_utility.first_day_of_next_year())






























