
import pandas as pd
from yahoo_fin.stock_info import *

from CommonProperties.DateUtility import DateUtility
import CommonProperties.Base_Properties as base_properties
import CommonProperties.Mysql_Utils as mysql_utils
from CommonProperties.Base_utils import timing_decorator


def put_csv_to_mysql():

    #  读取csv
    # file_dir = r'F:\QDatas\vantage\USD_FX\USD_FX_2024081114.csv'
    # table_name = r'exchange_dxy_vantage'

    file_dir = r'F:\QDatas\vantage\USD_FX_detail\USD_FX_detail_2024081114.csv'
    table_name = r'exchange_rate_vantage_detail'


    df = pd.read_csv(file_dir)
    df.columns = ['name', 'ymd', 'open', 'high', 'low', 'close']

    mysql_utils.data_from_dataframe_to_mysql(df=df, table_name=table_name, database='quant')











if __name__ == "__main__":
    put_csv_to_mysql()























