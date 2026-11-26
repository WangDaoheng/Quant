# 写个临时脚本 backfill_factors.py
from strategy.factor_library import FactorLibrary
from CommonProperties.DateUtility import DateUtility

if __name__ == '__main__':
    factor_lib = FactorLibrary()

    # 按月分段回填，避免内存溢出
    months = [
        ('20260101', '20260131'),
        ('20260201', '20260228'),
        ('20260301', '20260331'),
        ('20260401', '20260430'),
        ('20260501', '20260531'),
        ('20260601', '20260630'),
        ('20260701', '20260731'),
        ('20260801', '20260831'),
        ('20260901', '20260930'),
        ('20261001', '20261031'),
        ('20261101', '20261125'),
    ]

    for start, end in months:
        print(f"回填 {start} ~ {end}")
        factor_lib.pb_factor_score(start_date=start, end_date=end)
        factor_lib.zt_factor_score(start_date=start, end_date=end)
        factor_lib.shareholder_factor_score(start_date=start, end_date=end)
        factor_lib.volume_shrink_factor(start_date=start, end_date=end)
        factor_lib.price_shrink_factor(start_date=start, end_date=end)
        factor_lib.chip_concentration_score(start_date=start, end_date=end)
        factor_lib.chip_low_position_score(start_date=start, end_date=end)
        factor_lib.chip_gather_trend_score(start_date=start, end_date=end)
        factor_lib.aggregate_factors(start_date=start, end_date=end)
        factor_lib.cached_factors.clear()  # 清缓存，释放内存


