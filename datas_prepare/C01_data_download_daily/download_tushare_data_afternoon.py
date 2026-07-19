import tushare as ts
import pandas as pd
import sys
import time
import logging
import warnings

import CommonProperties.Base_Properties as base_properties
import CommonProperties.Mysql_Utils as mysql_utils
from CommonProperties.DateUtility import DateUtility
from CommonProperties.Base_utils import timing_decorator, script_run

warnings.filterwarnings('ignore', category=FutureWarning)

# 配置日志（如果外部已配置，这行可移除）
# logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


class SaveTushareDailyData:
    def __init__(self):
        ts.set_token(base_properties.ts_token)
        self.pro = ts.pro_api()

    @timing_decorator
    def get_stock_kline_tushare(self):
        """
        使用Tushare获取全部股票的历史日K线数据，并存入数据库
        添加阴线判断字段 today_pct 和 is_down
        写入 ods_stock_kline_daily_ts
        """
        today = DateUtility.today()
        time_start_date = DateUtility.first_day_of_month()
        time_end_date = today

        # 获取交易日列表
        try:
            trade_cal_df = self.pro.trade_cal(
                start_date=time_start_date,
                end_date=time_end_date,
                is_open='1'
            )
            if trade_cal_df is None or trade_cal_df.empty:
                logging.warning("未获取到交易日历数据")
                return pd.DataFrame()

            trade_dates = sorted(trade_cal_df['cal_date'].tolist())
            logging.info(f"交易日历: {len(trade_dates)} 天, {trade_dates[0]} ~ {trade_dates[-1]}")
        except Exception as e:
            logging.error(f"获取交易日历失败: {e}")
            return pd.DataFrame()

        # 速率控制
        rate_limit_max = 180
        request_timestamps = []

        def check_rate_limit():
            now = time.time()
            request_timestamps[:] = [t for t in request_timestamps if now - t < 60]
            if len(request_timestamps) >= rate_limit_max:
                wait_time = 60 - (now - request_timestamps[0]) + 1.0
                logging.info(f"速率限制等待 {wait_time:.1f}s")
                time.sleep(max(wait_time, 0))
                now = time.time()
                request_timestamps[:] = [t for t in request_timestamps if now - t < 60]
            request_timestamps.append(time.time())

        kline_total_df = pd.DataFrame()
        successful_dates = 0
        failed_dates = 0
        rate_limit_hits = 0

        for i, trade_date in enumerate(trade_dates, start=1):
            sys.stdout.write(f"\rK线下载: {i}/{len(trade_dates)} {trade_date}")
            sys.stdout.flush()

            try:
                check_rate_limit()
                df_daily = self.pro.daily(trade_date=trade_date)

                if df_daily is not None and not df_daily.empty:
                    kline_total_df = pd.concat([kline_total_df, df_daily], ignore_index=True)
                    successful_dates += 1
                else:
                    failed_dates += 1

            except Exception as e:
                failed_dates += 1
                error_msg = str(e)
                if "频率超限" in error_msg or "频次" in error_msg:
                    rate_limit_hits += 1
                    time.sleep(60)
                    request_timestamps.clear()
                continue

        sys.stdout.write("\n")
        logging.info(f"K线请求: 成功 {successful_dates} 天, 失败 {failed_dates} 天, 频率限制 {rate_limit_hits} 次")

        if kline_total_df.empty:
            logging.warning("K线数据为空")
            return pd.DataFrame()

        # 数据处理
        column_mapping = {
            'trade_date': 'ymd',
            'ts_code': 'stock_code',
            'pct_chg': 'change_pct',
            'vol': 'volume',
            'amount': 'trading_amount'
        }
        kline_total_df.rename(columns=column_mapping, inplace=True)
        kline_total_df['ymd'] = pd.to_datetime(kline_total_df['ymd']).dt.strftime('%Y%m%d')

        kline_total_df['today_pct'] = ((kline_total_df['close'] - kline_total_df['open']) /
                                       kline_total_df['open'] * 100).round(2)
        kline_total_df['is_down'] = (kline_total_df['close'] < kline_total_df['open']).astype(int)

        down_count = kline_total_df['is_down'].sum()
        logging.info(f"阴线统计: {down_count}/{len(kline_total_df)} ({down_count/len(kline_total_df)*100:.1f}%)")

        required_columns = ['stock_code', 'ymd', 'open', 'close', 'high', 'low',
                            'change_pct', 'today_pct', 'is_down', 'volume', 'trading_amount']
        existing_columns = [col for col in required_columns if col in kline_total_df.columns]
        kline_total_df = kline_total_df[existing_columns]
        kline_total_df = kline_total_df.drop_duplicates(subset=['ymd', 'stock_code'], keep='first')

        # 日期统计
        date_counts = kline_total_df['ymd'].value_counts().sort_index()
        logging.info(f"K线数据: {len(kline_total_df)} 条, 日期范围 {date_counts.index[0]} ~ {date_counts.index[-1]}")

        try:
            mysql_utils.data_from_dataframe_to_mysql(
                user=base_properties.origin_mysql_user,
                password=base_properties.origin_mysql_password,
                host=base_properties.origin_mysql_host,
                database=base_properties.origin_mysql_database,
                df=kline_total_df,
                table_name="ods_stock_kline_daily_ts",
                merge_on=['ymd', 'stock_code']
            )
            logging.info(f"K线写入完成: {len(kline_total_df)} 条")
        except Exception as e:
            logging.error(f"K线保存失败: {e}")
            return pd.DataFrame()

        return kline_total_df

    @timing_decorator
    def download_board_list(self):
        """下载同花顺板块列表，写入 ods_tushare_board_concept_name_ths"""
        df = self.pro.ths_index(exchange='A')
        if df is None or df.empty:
            logging.warning("ths_index 返回为空")
            return pd.DataFrame()

        today = DateUtility.today()
        df['ymd'] = today

        df.rename(columns={
            'ts_code': 'board_code',
            'name': 'board_name',
            'count': 'component_count',
            'exchange': 'market',
            'list_date': 'list_date'
        }, inplace=True)

        if 'list_date' in df.columns:
            df['list_date'] = pd.to_datetime(df['list_date'], errors='coerce').dt.strftime('%Y-%m-%d')

        target_cols = ['ymd', 'board_name', 'board_code', 'component_count', 'market', 'list_date']
        df = df[[c for c in target_cols if c in df.columns]]

        mysql_utils.data_from_dataframe_to_mysql(
            user=base_properties.origin_mysql_user,
            password=base_properties.origin_mysql_password,
            host=base_properties.origin_mysql_host,
            database=base_properties.origin_mysql_database,
            df=df,
            table_name="ods_tushare_board_concept_name_ths",
            merge_on=['ymd', 'board_code']
        )
        logging.info(f"板块列表: {len(df)} 个")


    @timing_decorator
    def download_board_daily(self, start_date=None, end_date=None):
        """下载同花顺板块行情，写入 ods_tushare_stock_board_concept_index_ths"""
        if not end_date:
            end_date = DateUtility.today()
        if not start_date:
            start_date = DateUtility.first_day_of_month()

        logging.info(f"板块行情: {start_date} ~ {end_date}")

        board_df = mysql_utils.data_from_mysql_to_dataframe_latest(
            user=base_properties.origin_mysql_user,
            password=base_properties.origin_mysql_password,
            host=base_properties.origin_mysql_host,
            database=base_properties.origin_mysql_database,
            table_name='ods_tushare_board_concept_name_ths',
            cols=['ymd', 'board_name', 'board_code']
        )
        if board_df.empty:
            logging.warning("板块列表为空，跳过行情下载")
            return pd.DataFrame()

        all_data = []
        failed = []
        total = len(board_df)

        for idx, row in board_df.iterrows():
            ts_code = row['board_code']
            board_name = row['board_name']

            # 进度条，每50个打印一次
            if (idx + 1) % 50 == 0 or idx == 0 or idx == total - 1:
                sys.stdout.write(f"\r板块行情: {idx+1}/{total}")
                sys.stdout.flush()

            try:
                df = self.pro.ths_daily(ts_code=ts_code, start_date=start_date, end_date=end_date)
                if df is not None and not df.empty:
                    all_data.append(df)
                else:
                    failed.append(ts_code)
            except Exception as e:
                error_msg = str(e)
                if "积分" in error_msg or "权限" in error_msg:
                    logging.error(f"积分/权限不足: {error_msg}")
                    raise
                failed.append(ts_code)

            time.sleep(0.25)

        sys.stdout.write("\n")

        if not all_data:
            logging.warning("未获取到行情数据")
            return pd.DataFrame()

        result = pd.concat(all_data, ignore_index=True)

        result['trade_date'] = result['trade_date'].astype(str).str.strip()
        result['ymd'] = result['trade_date']

        result.rename(columns={
            'ts_code': 'board_code',
            'open': 'open',
            'high': 'high',
            'low': 'low',
            'close': 'close',
            'vol': 'trading_volume',
            'amount': 'trading_amount'
        }, inplace=True)

        board_names = dict(zip(board_df['board_code'], board_df['board_name']))
        result['board_name'] = result['board_code'].map(board_names)

        for col in ['open', 'high', 'low', 'close', 'trading_volume', 'trading_amount']:
            if col in result.columns:
                result[col] = pd.to_numeric(result[col], errors='coerce')

        target_cols = ['ymd', 'board_name', 'board_code', 'open', 'high', 'low', 'close', 'trading_volume', 'trading_amount']
        result = result[[c for c in target_cols if c in result.columns]]
        result = result.drop_duplicates(subset=['ymd', 'board_code'], keep='first')

        date_counts = result['ymd'].value_counts().sort_index()
        logging.info(f"行情日期分布: {dict(date_counts)}")

        mysql_utils.data_from_dataframe_to_mysql(
            user=base_properties.origin_mysql_user,
            password=base_properties.origin_mysql_password,
            host=base_properties.origin_mysql_host,
            database=base_properties.origin_mysql_database,
            df=result,
            table_name="ods_tushare_stock_board_concept_index_ths",
            merge_on=['ymd', 'board_code']
        )

        logging.info(f"板块行情: {len(result)} 条, 成功 {total - len(failed)} 个, 失败 {len(failed)} 个")


    @timing_decorator
    def download_board_members(self):
        """下载同花顺板块成分股，写入 ods_tushare_stock_board_concept_maps_ths"""
        board_df = mysql_utils.data_from_mysql_to_dataframe_latest(
            user=base_properties.origin_mysql_user,
            password=base_properties.origin_mysql_password,
            host=base_properties.origin_mysql_host,
            database=base_properties.origin_mysql_database,
            table_name='ods_tushare_board_concept_name_ths',
            cols=['ymd', 'board_name', 'board_code']
        )
        if board_df.empty:
            logging.warning("板块列表为空，跳过成分股下载")
            return pd.DataFrame()

        all_members = []
        failed = []
        ymd = pd.to_datetime(board_df['ymd'].iloc[0]).strftime('%Y%m%d')
        total = len(board_df)

        for idx, row in board_df.iterrows():
            ts_code = row['board_code']
            board_name = row['board_name']

            if (idx + 1) % 100 == 0 or idx == 0 or idx == total - 1:
                sys.stdout.write(f"\r成分股: {idx + 1}/{total}")
                sys.stdout.flush()

            try:
                df = self.pro.ths_member(ts_code=ts_code)
                if df is not None and not df.empty:
                    df['ymd'] = ymd
                    df['board_code'] = ts_code
                    df['board_name'] = board_name

                    df = df.rename(columns={
                        'con_code': 'stock_code',
                        'con_name': 'stock_name'
                    })

                    if 'ts_code' in df.columns:
                        df = df.drop(columns=['ts_code'])

                    if 'stock_code' not in df.columns or 'stock_name' not in df.columns:
                        failed.append(ts_code)
                        continue

                    for col in ['weight', 'in_date', 'out_date', 'is_new']:
                        if col not in df.columns:
                            df[col] = None

                    target_cols = ['ymd', 'board_name', 'board_code', 'stock_code', 'stock_name', 'weight', 'in_date',
                                   'out_date', 'is_new']
                    available_cols = [c for c in target_cols if c in df.columns]
                    all_members.append(df[available_cols])
                else:
                    failed.append(ts_code)
            except Exception as e:
                error_msg = str(e)
                if "积分" in error_msg or "权限" in error_msg:
                    logging.error(f"积分/权限不足: {error_msg}")
                    raise
                failed.append(ts_code)

            time.sleep(0.8)

        sys.stdout.write("\n")

        if not all_members:
            logging.warning("未获取到任何成分股，跳过删除和写入，保留历史数据")
            return pd.DataFrame()

        result = pd.concat(all_members, ignore_index=True)

        for col in ['in_date', 'out_date']:
            if col in result.columns:
                result[col] = pd.to_datetime(result[col], errors='coerce').dt.strftime('%Y%m%d')
                result[col] = result[col].replace('NaT', None)

        result = result.drop_duplicates(subset=['ymd', 'board_code', 'stock_code'], keep='first')

        logging.info(f"成分股: {len(result)} 条, 成功 {total - len(failed)} 个板块, 失败 {len(failed)} 个")

        # ========== 滚动删除：确保删完后表还有数据 ==========
        try:
            from sqlalchemy import create_engine, text
            engine = create_engine(
                f"mysql+pymysql://{base_properties.origin_mysql_user}:{base_properties.origin_mysql_password}"
                f"@{base_properties.origin_mysql_host}/{base_properties.origin_mysql_database}"
            )

            # 先查：表中是否存在 今天之前 的数据
            check_sql = f"SELECT COUNT(*) as cnt FROM ods_tushare_stock_board_concept_maps_ths WHERE ymd < '{ymd}'"
            with engine.connect() as conn:
                check_result = conn.execute(text(check_sql)).fetchone()
                history_count = check_result[0] if check_result else 0

            if history_count > 0:
                # 有历史数据才删除：删10天前的
                cutoff_date = (pd.to_datetime(ymd, format='%Y%m%d') - pd.Timedelta(days=10)).strftime('%Y%m%d')
                delete_sql = f"DELETE FROM ods_tushare_stock_board_concept_maps_ths WHERE ymd < '{cutoff_date}'"
                with engine.connect() as conn:
                    result_delete = conn.execute(text(delete_sql))
                    conn.commit()
                    deleted_rows = result_delete.rowcount
                logging.info(
                    f"滚动删除完成: ymd < {cutoff_date}，删除 {deleted_rows} 条，历史数据 {history_count} 条保留")
            else:
                logging.warning(f"表中无历史数据(ymd < {ymd})，跳过删除，直接写入")

            engine.dispose()
        except Exception as e:
            logging.error(f"滚动删除失败: {e}")
        # ========== 滚动删除结束 ==========

        mysql_utils.data_from_dataframe_to_mysql(
            user=base_properties.origin_mysql_user,
            password=base_properties.origin_mysql_password,
            host=base_properties.origin_mysql_host,
            database=base_properties.origin_mysql_database,
            df=result,
            table_name="ods_tushare_stock_board_concept_maps_ths",
            merge_on=['ymd', 'board_code', 'stock_code']
        )

        logging.info("成分股写入完成")
        return result


    @script_run(script_name="download_tushare_data_afternoon.py")
    def setup(self):
        self.get_stock_kline_tushare()
        self.download_board_list()
        self.download_board_daily()
        self.download_board_members()


if __name__ == '__main__':
    downloader = SaveTushareDailyData()
    downloader.setup()

