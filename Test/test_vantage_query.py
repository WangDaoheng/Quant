from yahoo_fin.stock_info import *


def use_vantage():
    # 替换为你的 Alpha Vantage API 密钥
    api_key = 'ICTN 9 P9 ES 00 EADUF'

    # 构建 API 请求 URL
    base_url = 'https://www.alphavantage.co/query'
    function = 'TIME_SERIES_DAILY'
    symbol = 'TSLA'  # 特斯拉的股票代码
    start_date = '2023-01-01'  # 开始日期
    end_date = '2024-01-01'  # 结束日期
    url = f'{base_url}?function={function}&symbol={symbol}&apikey={api_key}&outputsize=full&datatype=csv'

    # 发送 GET 请求
    response = requests.get(url)

    # 处理响应数据
    if response.status_code == 200:
        data = response.json()  # 将 JSON 响应转换为 Python 字典或列表
        # 在这里可以进一步处理数据，比如解析并显示每日收盘价等信息
        print("TSLA 数据:")
        print(data)
    else:
        print(f'Error fetching TSLA data: {response.status_code} - {response.text}')


if __name__ == "__main__":
    use_vantage()
