import tushare as ts


if __name__ == '__main__':
    pro = ts.pro_api()
    df = pro.ths_index(exchange='A')
    df.to_csv('data.csv')

    print(df)
