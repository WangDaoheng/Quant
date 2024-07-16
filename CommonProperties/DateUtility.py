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
    def last_day_of_last_week():
        today = datetime.today()
        start_of_this_week = today - timedelta(days=today.weekday())
        end_of_last_week = start_of_this_week - timedelta(days=1)
        return end_of_last_week.strftime('%Y%m%d')


    @staticmethod
    def last_day_of_last_month():
        today = datetime.today()
        first_day_of_this_month = today.replace(day=1)
        last_day_of_last_month = first_day_of_this_month - timedelta(days=1)
        return last_day_of_last_month.strftime('%Y%m%d')


    @staticmethod
    def last_day_of_last_quarter():
        today = datetime.today()
        current_month = today.month
        first_month_of_current_quarter = current_month - (current_month - 1) % 3
        first_day_of_current_quarter = today.replace(month=first_month_of_current_quarter, day=1)
        last_day_of_last_quarter = first_day_of_current_quarter - timedelta(days=1)
        return last_day_of_last_quarter.strftime('%Y%m%d')


    @staticmethod
    def last_day_of_last_year():
        today = datetime.today()
        last_day_of_last_year = today.replace(month=1, day=1) - timedelta(days=1)
        return last_day_of_last_year.strftime('%Y%m%d')


    @staticmethod
    def first_day_of_next_week():
        today = datetime.today()
        start_of_next_week = today + timedelta(days=(7 - today.weekday()))
        return start_of_next_week.strftime('%Y%m%d')

    @staticmethod
    def first_day_of_next_month():
        today = datetime.today()
        next_month = today.month + 1 if today.month < 12 else 1
        next_year = today.year if today.month < 12 else today.year + 1
        first_day = datetime(next_year, next_month, 1)
        return first_day.strftime('%Y%m%d')


    @staticmethod
    def first_day_of_next_quarter():
        today = datetime.today()
        current_month = today.month
        first_month_of_current_quarter = current_month - (current_month - 1) % 3
        next_quarter_month = first_month_of_current_quarter + 3
        next_quarter_year = today.year
        if next_quarter_month > 12:
            next_quarter_month -= 12
            next_quarter_year += 1
        first_day_of_next_quarter = today.replace(year=next_quarter_year, month=next_quarter_month, day=1)
        return first_day_of_next_quarter.strftime('%Y%m%d')


    @staticmethod
    def first_day_of_next_year():
        today = datetime.today()
        first_day_of_next_year = today.replace(year=today.year + 1, month=1, day=1)
        return first_day_of_next_year.strftime('%Y%m%d')


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






























