import pandas as pd
from sqlalchemy import create_engine
import CommonProperties.Base_Properties as base_properties


if __name__ == '__main__':
    engine = create_engine(
        f'mysql+pymysql://{base_properties.origin_mysql_user}:{base_properties.origin_mysql_password}'
        f'@{base_properties.origin_mysql_host}:3306/{base_properties.origin_mysql_database}'
    )

    tables = [
        # ODS
        'ods_trading_days_insight', 'ods_stock_code_daily_insight', 'ods_stock_limit_summary_insight',
        'ods_stock_chouma_insight', 'ods_astock_industry_overview', 'ods_astock_industry_detail',
        'ods_index_a_share_insight', 'ods_future_inside_insight', 'ods_shareholder_num',
        'ods_akshare_stock_yjkb_em', 'ods_akshare_stock_yjyg_em', 'ods_akshare_stock_a_high_low_statistics',
        'ods_akshare_stock_value_em', 'ods_akshare_stock_zh_a_gdhs_detail_em',
        'ods_stock_kline_daily_ts', 'ods_tushare_board_concept_name_ths',
        'ods_tushare_stock_board_concept_index_ths', 'ods_tushare_stock_board_concept_maps_ths',
        'ods_us_stock_daily_vantage', 'ods_exchange_rate_vantage_detail', 'ods_exchange_dxy_vantage',
        'ods_stock_plate_redbook', 'ods_stock_exchange_market',
        # DWD
        'dwd_ashare_stock_base_info', 'dwd_stock_zt_list', 'dwd_stock_dt_list',
        'dwd_stock_technical_indicators', 'dwd_factor_volume_shrinkage', 'dwd_factor_summary',
        'dwd_stock_a_total_plate',
        # MART
        'dmart_stock_zt_details', 'dmart_stock_zt_details_expanded',
    ]

    with open('prod_schema_dump.txt', 'w', encoding='utf-8') as f:
        for t in tables:
            try:
                ddl = pd.read_sql(f"SHOW CREATE TABLE {t}", engine).iloc[0, 1]
                f.write(f"\n-- ===== {t} =====\n{ddl};\n")
            except Exception as e:
                f.write(f"\n-- ===== {t} ===== ERROR: {e}\n")

    print("导出完成：prod_schema_dump.txt")


