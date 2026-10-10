# -*- coding: utf-8 -*-
"""
核对脚本：用 Tushare 接口查询 000725.SZ 在 20260616-20260617 的原始数据
"""

import tushare as ts
import CommonProperties.Base_Properties as base_properties


def check_tushare_data():
    # 设置token
    ts.set_token(base_properties.ts_token)
    pro = ts.pro_api()

    # 查询 000725.SZ 在 20260615-20260618 的数据
    print("正在查询 Tushare 数据...")
    df = pro.daily(ts_code='000725.SZ', start_date='20260615', end_date='20260618')

    if df is None or df.empty:
        print("Tushare 返回空数据！")
        return

    print("\nTushare 返回的原始数据：")
    print(df[['trade_date', 'open', 'high', 'low', 'close', 'pre_close', 'change', 'pct_chg', 'vol',
              'amount']].to_string())

    print(f"\n数据类型：")
    print(df.dtypes)

    print(f"\n关键字段值：")
    for idx, row in df.iterrows():
        print(f"日期: {row['trade_date']}")
        print(f"  close: {row['close']}")
        print(f"  pre_close: {row['pre_close']}")
        print(f"  pct_chg: {row['pct_chg']}")
        # 计算涨停价
        zt_price = round(row['pre_close'] * 1.1, 2)
        print(f"  理论涨停价(pre_close*1.1): {zt_price}")
        print(f"  是否涨停(close>=zt_price): {row['close'] >= zt_price}")
        print()


if __name__ == '__main__':
    check_tushare_data()
