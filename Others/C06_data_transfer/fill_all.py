import tushare as ts
import pandas as pd
import sys
import time
import logging
import warnings
import contextlib
import io
import os

import CommonProperties.Base_Properties as base_properties
import CommonProperties.Mysql_Utils as mysql_utils
from CommonProperties.DateUtility import DateUtility
from CommonProperties.Base_utils import timing_decorator, script_run

warnings.filterwarnings('ignore', category=FutureWarning)

# 本地缓存路径：格式出问题时改从这里读，不用重新拉全量
CACHE_PATH = r'F:\Quant\cache\stk_holdernumber_raw.csv'


class SaveTushareDailyData:
    def __init__(self):
        ts.set_token(base_properties.ts_token)
        self.pro = ts.pro_api()

    def _fetch_holdernumber_by_day(self, time_start_date, time_end_date):
        """
        按公告日逐天拉取股东人数（规避单次3000行限制），返回合并后的dataframe
        """
        all_df = pd.DataFrame()
        days = pd.date_range(time_start_date, time_end_date, freq='D')
        total = len(days)

        for i, day in enumerate(days, 1):
            d = day.strftime('%Y%m%d')
            try:
                with contextlib.redirect_stdout(io.StringIO()), contextlib.redirect_stderr(io.StringIO()):
                    df = self.pro.stk_holdernumber(start_date=d, end_date=d)
            except Exception as e:
                logging.warning(f"stk_holdernumber 获取 {d} 失败: {e}")
                time.sleep(1)
                continue

            if df is not None and not df.empty:
                all_df = pd.concat([all_df, df], ignore_index=True)

            sys.stdout.write(f"\r当前执行 stk_holdernumber 第 {i} 天，共 {total} 天，累计 {all_df.shape[0]} 条")
            sys.stdout.flush()
            time.sleep(0.35)   # 5000积分频率限制宽松，0.35s足够

        sys.stdout.write("\n")
        return all_df

    @timing_decorator
    def get_shareholder_num_tushare(self, time_start_date=None, time_end_date=None, use_cache=False):
        """
        获取tushare股东人数(stk_holdernumber)，用于回填 ods_shareholder_num 的披露日 ann_date
        写入 ods_tushare_holdernumber

        首次全量回填: get_shareholder_num_tushare('2020-01-01', DateUtility.today())
        日常增量调度: 不带参数，默认滚动近15天（容忍tushare的T+1~T+2延迟）
        use_cache=True: 跳过API拉取，直接读本地缓存csv调试格式问题
        """
        # 0.缓存模式：直接读上次落盘的原始数据，调格式时不用等20分钟重拉
        if use_cache and os.path.exists(CACHE_PATH):
            logging.info(f"从缓存读取: {CACHE_PATH}")
            holdernumber_df = pd.read_csv(CACHE_PATH, dtype=str)
        else:
            if not time_start_date:
                try:
                    time_start_date = DateUtility.date_delta(-15)   # 若没有此方法，用 DateUtility.first_day_of_month()
                except AttributeError:
                    time_start_date = DateUtility.first_day_of_month()
            if not time_end_date:
                time_end_date = DateUtility.today()

            # 1.按天拉取
            holdernumber_df = self._fetch_holdernumber_by_day(time_start_date, time_end_date)

            # 拉取完成立刻落盘缓存（原始数据，格式未处理）
            if not holdernumber_df.empty:
                os.makedirs(os.path.dirname(CACHE_PATH), exist_ok=True)
                holdernumber_df.to_csv(CACHE_PATH, index=False, encoding='utf-8-sig')
                logging.info(f"原始数据已缓存: {CACHE_PATH}")

        if holdernumber_df.empty:
            logging.info("stk_holdernumber 返回为空，跳过插入")
            return 0

        # 2.清洗 + 日期格式转换 YYYYMMDD -> date
        # 2.1 转字符串、去首尾空格（tushare部分记录带空格或为空）
        for col in ['ann_date', 'end_date']:
            holdernumber_df[col] = holdernumber_df[col].astype(str).str.strip()

        # 2.2 打印脏数据示例（只打一次，确认异常来源）
        bad = holdernumber_df[~holdernumber_df['ann_date'].str.fullmatch(r'\d{8}')]
        if not bad.empty:
            logging.warning(f"ann_date 异常值 {len(bad)} 条，示例: {bad['ann_date'].unique()[:10]}")

        # 2.3 过滤掉无法解析的行（空串、None、'nan'等）
        holdernumber_df = holdernumber_df[
            holdernumber_df['ann_date'].str.fullmatch(r'\d{8}')
            & holdernumber_df['end_date'].str.fullmatch(r'\d{8}')
        ].copy()

        # 2.4 转换格式
        holdernumber_df['ann_date'] = pd.to_datetime(holdernumber_df['ann_date'], format='%Y%m%d').dt.strftime('%Y-%m-%d')
        holdernumber_df['end_date'] = pd.to_datetime(holdernumber_df['end_date'], format='%Y%m%d').dt.strftime('%Y-%m-%d')

        # 3.同一(ts_code, end_date)可能有多条(重复披露)，按ann_date升序后保留首次披露——可知日以最早为准
        holdernumber_df = holdernumber_df.sort_values('ann_date')
        holdernumber_df = holdernumber_df.drop_duplicates(subset=['ts_code', 'end_date'], keep='first')

        # 4.写入 staging 表（mysql_utils 是 INSERT IGNORE 首写不覆盖，
        #    先排序再去重，保证最早ann_date最先落库）
        mysql_utils.data_from_dataframe_to_mysql(
            user=base_properties.origin_mysql_user,
            password=base_properties.origin_mysql_password,
            host=base_properties.origin_mysql_host,
            database=base_properties.origin_mysql_database,
            df=holdernumber_df,
            table_name="ods_tushare_holdernumber",
            merge_on=['ts_code', 'end_date']
        )

        return len(holdernumber_df)


if __name__ == '__main__':
    saver = SaveTushareDailyData()

    # # ---- 首次全量回填（跑一次）： ----
    # saver.get_shareholder_num_tushare('2020-01-01', DateUtility.today())

    saver.get_shareholder_num_tushare(use_cache=True)


    # # ---- 格式调试时从缓存跑，不用重新拉API： ----
    # saver.get_shareholder_num_tushare(use_cache=True)

    # # ---- 日常增量（进你的每日调度）： ----
    # saver.get_shareholder_num_tushare()