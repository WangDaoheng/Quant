

import pandas as pd

if __name__ =="__main__":
    import pandas as pd

    # 构造测试数据
    data = {
        'ymd': [20231101, 20231102, 20231103, 20231104, 20231031, 20231030],
        'open': [4.95, 5.00, 5.20, 5.50, 4.80, 4.85],
        'high': [5.10, 5.30, 5.40, 5.60, 5.10, 5.15],
        'low': [4.80, 4.95, 5.10, 5.40, 4.70, 4.80],
        'close': [5.12, 5.23, 5.28, 5.54, 5.01, 5.06],
        'volume': [1000, 1200, 1500, 1300, 1100, 1400]
    }

    df = pd.DataFrame(data)

    # 计算昨日的涨停价和跌停价（保留4位小数）
    df['昨日close'] = df['close'].shift(1)  # 上一个交易日的收盘价
    df['昨日涨停价'] = (df['昨日close'] * 1.10).round(4)  # 昨日涨停价
    df['昨日跌停价'] = (df['昨日close'] * 0.90).round(4)  # 昨日跌停价

    # 计算当前 close 与昨日涨停价和昨日跌停价的差距
    df['涨停差距'] = abs(df['close'] - df['昨日涨停价'])
    df['跌停差距'] = abs(df['close'] - df['昨日跌停价'])


    # 找出最接近昨日涨停价或跌停价的价格
    def get_closest_price(close, target_price):
        """
        获取与目标价格（涨停或跌停）最接近的价格。
        """
        diff_1 = abs(close - target_price)
        diff_2 = abs(round(target_price + 0.01, 2) - target_price)
        if diff_1 < diff_2:
            return close
        return round(target_price + 0.01, 2)


    # 计算最接近的涨停价格和跌停价格
    df['最接近涨停价'] = df.apply(lambda row: get_closest_price(row['close'], row['昨日涨停价']), axis=1)
    df['最接近跌停价'] = df.apply(lambda row: get_closest_price(row['close'], row['昨日跌停价']), axis=1)

    # 输出结果
    print(df[['ymd', 'close', '昨日涨停价', '昨日跌停价', '最接近涨停价', '最接近跌停价']])



























