# -*- coding: utf-8 -*-
"""
Tushare数据下载器 - 优化版
匹配最新表结构（K线表名保持不变）
"""

import tushare as ts
import pandas as pd
import sys
import time
import logging
import warnings
from datetime import datetime, timedelta
import traceback
from tqdm import tqdm

import CommonProperties.Base_Properties as base_properties
import CommonProperties.Mysql_Utils as mysql_utils
from CommonProperties.DateUtility import DateUtility

warnings.filterwarnings('ignore', category=FutureWarning)


class TushareDataFetcher:
    """Tushare数据下载器 - 匹配最新表结构（K线表名保持不变）"""

    def __init__(self, stock_code_df=None):
        """
        初始化Tushare
        :param stock_code_df: 股票代码DataFrame，可选
        """
        # 设置Tushare Token
        ts.set_token(base_properties.ts_token)
        self.pro = ts.pro_api()
        self.stock_code_df = stock_code_df

        # API频率控制配置（10000积分账号优化）
        self.api_delay = {
            'kline': 0.2,  # K线数据（相对宽松）
            'normal': 0.3,  # 普通板块接口
            'member': 0.5,  # 成分股接口（严格限制）
            'error': 2.0,
            'batch': 3.0  # 批次间延迟
        }

        # 日志配置
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[
                logging.StreamHandler(),
                logging.FileHandler(f'tushare_downloader_{DateUtility.today()}.log')
            ]
        )
        self.logger = logging.getLogger(__name__)

    # ============ 原有的日K线下载功能 ============

    def get_stock_kline_tushare(self):
        """
        使用Tushare获取全部股票的历史日K线数据
        """
        self.logger.info("开始获取股票日K线数据")

        # 1. 获取日期范围
        today = DateUtility.today()

        if int(today[6:8]) > 15:
            time_start_date = DateUtility.next_day(-15)  # 15天前
        else:
            time_start_date = DateUtility.first_day_of_month()  # 当月1号
        time_end_date = today

        # 2. 获取股票代码列表
        stock_code_list = mysql_utils.get_stock_codes_latest(self.stock_code_df)
        if not stock_code_list:
            self.logger.warning("股票代码列表为空")
            return pd.DataFrame()

        self.logger.info(
            f"将获取 {len(stock_code_list)} 只股票的K线数据，日期范围: {time_start_date} 到 {time_end_date}")

        # 3. 分批处理设置
        batch_size = 100
        total_batches = (len(stock_code_list) + batch_size - 1) // batch_size
        kline_total_df = pd.DataFrame()

        # 4. 分批获取K线数据
        for i in range(0, len(stock_code_list), batch_size):
            batch_list = stock_code_list[i:i + batch_size]
            batch_num = i // batch_size + 1

            sys.stdout.write(f"\r当前执行K线数据的第{batch_num}批次，总共{total_batches}个批次")
            sys.stdout.flush()

            # 循环单个股票调用
            for ts_code in batch_list:
                try:
                    df_batch = ts.pro_bar(
                        ts_code=ts_code,
                        start_date=time_start_date,
                        end_date=time_end_date,
                        adj='qfq',  # 前复权
                        freq='D'  # 日线
                    )

                    if df_batch is not None and not df_batch.empty:
                        kline_total_df = pd.concat([kline_total_df, df_batch], ignore_index=True)

                    # 频率控制
                    time.sleep(self.api_delay['kline'])

                except Exception as e:
                    self.logger.warning(f"股票 {ts_code} K线获取失败: {str(e)[:50]}")
                    time.sleep(self.api_delay['error'])
                    continue

            # 批次间延迟
            if i + batch_size < len(stock_code_list):
                time.sleep(1)

        sys.stdout.write("\n")

        # 5. 数据处理 - 匹配表结构
        if not kline_total_df.empty:
            # 重命名列以匹配表结构
            kline_total_df.rename(columns={
                'ts_code': 'stock_code',
                'trade_date': 'ymd',
                'pct_chg': 'change_pct',
                'vol': 'volume',
                'amount': 'trading_amount'
            }, inplace=True)

            # 转换日期格式为YYYYMMDD字符串
            kline_total_df['ymd'] = pd.to_datetime(kline_total_df['ymd']).dt.strftime('%Y%m%d')

            # 确保数据类型正确
            numeric_columns = ['open', 'close', 'high', 'low', 'change_pct', 'volume', 'trading_amount']
            for col in numeric_columns:
                if col in kline_total_df.columns:
                    kline_total_df[col] = pd.to_numeric(kline_total_df[col], errors='coerce')

            # 选择需要的列，完全匹配表结构
            required_columns = ['ymd', 'stock_code', 'open', 'close', 'high', 'low',
                                'change_pct', 'volume', 'trading_amount']

            # 确保所有必需的列都存在
            for col in required_columns:
                if col not in kline_total_df.columns:
                    kline_total_df[col] = None

            kline_total_df = kline_total_df[required_columns]

            # 去除重复
            kline_total_df = kline_total_df.drop_duplicates(subset=['ymd', 'stock_code'], keep='first')

            self.logger.info(f"成功获取 {len(kline_total_df)} 条日K线数据")
            return kline_total_df
        else:
            self.logger.warning('K线数据获取为空')
            return pd.DataFrame()

    def save_kline_to_mysql(self, df=None):
        """
        保存K线数据到MySQL - 保持原表名
        :param df: K线DataFrame，如为None则重新获取
        """
        if df is None:
            df = self.get_stock_kline_tushare()

        if df.empty:
            self.logger.warning("K线数据为空，不保存到数据库")
            return False

        try:
            mysql_utils.data_from_dataframe_to_mysql(
                user=base_properties.origin_mysql_user,
                password=base_properties.origin_mysql_password,
                host=base_properties.origin_mysql_host,
                database=base_properties.origin_mysql_database,
                df=df,
                table_name="ods_stock_kline_daily_ts",  # 保持原表名不变
                merge_on=['ymd', 'stock_code']
            )
            self.logger.info(f"K线数据成功保存到数据库: {len(df)} 条记录")
            return True
        except Exception as e:
            self.logger.error(f"K线数据保存到MySQL失败: {e}")
            return False

    # ============ 新增的6个板块数据接口 ============

    def _safe_api_call(self, api_func, *args, **kwargs):
        """安全调用API，带频率控制和重试"""
        max_retries = 3
        for attempt in range(max_retries):
            try:
                result = api_func(*args, **kwargs)
                return result
            except Exception as e:
                error_msg = str(e)
                if "频繁" in error_msg or "limit" in error_msg.lower() or "429" in error_msg:
                    wait_time = 5 * (attempt + 1)
                    self.logger.warning(f"API限流，等待{wait_time}秒后重试...")
                    time.sleep(wait_time)
                    continue
                elif "积分不够" in error_msg or "permission" in error_msg.lower():
                    self.logger.error(f"积分不足或权限不够: {error_msg}")
                    return None
                elif attempt < max_retries - 1:
                    self.logger.warning(f"API调用失败，第{attempt + 1}次重试: {error_msg[:50]}")
                    time.sleep(self.api_delay['error'])
                    continue
                else:
                    self.logger.error(f"API调用最终失败: {error_msg}")
                    return None
        return None

    # ===== 同花顺板块数据接口 =====

    def get_ths_index(self, exchange='A', index_type=None):
        """同花顺板块指数列表 - 匹配新表结构"""
        try:
            self.logger.info("获取同花顺板块指数列表...")
            params = {'exchange': exchange}
            if index_type:
                params['type'] = index_type

            df = self._safe_api_call(self.pro.ths_index, **params)
            time.sleep(self.api_delay['normal'])

            if df is not None and not df.empty:
                # 添加核心日期维度
                df['ymd'] = DateUtility.today()

                # 日期格式转换
                if 'list_date' in df.columns:
                    df['list_date'] = pd.to_datetime(df['list_date'], format='%Y%m%d', errors='coerce').dt.strftime(
                        '%Y%m%d')

                # 重命名列以匹配新表结构
                df = df.rename(columns={
                    'ts_code': 'board_code',
                    'name': 'board_name',
                    'count': 'component_count',
                    'exchange': 'market'
                })

                # 确保所有必需的列都存在
                required_columns = ['ymd', 'board_name', 'board_code', 'component_count',
                                    'market', 'list_date', 'index_type']

                for col in required_columns:
                    if col not in df.columns:
                        if col == 'index_type' and 'type' in df.columns:
                            df['index_type'] = df['type']
                        elif col == 'index_type':
                            df['index_type'] = 'N'  # 默认值
                        else:
                            df[col] = None

                # 转换数值类型
                if 'component_count' in df.columns:
                    df['component_count'] = pd.to_numeric(df['component_count'], errors='coerce').astype('Int64')

                return df[required_columns]

            return pd.DataFrame()

        except Exception as e:
            self.logger.error(f"获取ths_index失败: {e}")
            return pd.DataFrame()

    def get_ths_daily(self, ts_code=None, start_date=None, end_date=None, limit_days=30):
        """同花顺板块指数行情 - 匹配新表结构"""
        try:
            if not end_date:
                end_date = DateUtility.today()
            if not start_date:
                start_date = (datetime.strptime(end_date, '%Y%m%d') -
                              timedelta(days=limit_days)).strftime('%Y%m%d')

            self.logger.info(f"获取同花顺板块行情 {start_date}-{end_date}")

            params = {'start_date': start_date, 'end_date': end_date}
            if ts_code:
                params['ts_code'] = ts_code

            df = self._safe_api_call(self.pro.ths_daily, **params)
            time.sleep(self.api_delay['normal'])

            if df is not None and not df.empty:
                # 转换日期格式
                df['trade_date'] = pd.to_datetime(df['trade_date'], format='%Y%m%d').dt.strftime('%Y%m%d')

                # 获取板块名称信息
                board_info = {}
                if 'ts_code' in df.columns:
                    unique_codes = df['ts_code'].unique()
                    for code in unique_codes:
                        # 这里需要根据实际情况获取板块名称，暂时用代码替代
                        board_info[code] = code

                # 重命名列以匹配新表结构
                df = df.rename(columns={
                    'ts_code': 'board_code',
                    'trade_date': 'ymd',
                    'pre_close': 'prev_close',
                    'change': 'change_amt',
                    'pct_change': 'change_pct',
                    'vol': 'trading_volume',
                    'amount': 'avg_price',  # 注意：原接口amount字段对应表结构中的avg_price
                    'total_mv': 'total_market_value',
                    'float_mv': 'float_market_value',
                    'turnover_rate': 'turnover_rate'
                })

                # 添加板块名称
                df['board_name'] = df['board_code'].map(board_info)

                # 确保所有必需的列都存在
                required_columns = ['ymd', 'board_name', 'board_code', 'open', 'high', 'low',
                                    'close', 'prev_close', 'avg_price', 'change_amt', 'change_pct',
                                    'trading_volume', 'turnover_rate', 'total_market_value', 'float_market_value']

                for col in required_columns:
                    if col not in df.columns:
                        df[col] = None

                # 转换数值类型
                numeric_columns = ['open', 'high', 'low', 'close', 'prev_close', 'avg_price',
                                   'change_amt', 'change_pct', 'trading_volume', 'turnover_rate',
                                   'total_market_value', 'float_market_value']
                for col in numeric_columns:
                    if col in df.columns:
                        df[col] = pd.to_numeric(df[col], errors='coerce')

                return df[required_columns]

            return pd.DataFrame()

        except Exception as e:
            self.logger.error(f"获取ths_daily失败: {e}")
            return pd.DataFrame()

    def get_ths_member(self, ts_code):
        """同花顺板块成分股 - 匹配新表结构"""
        try:
            self.logger.info(f"获取板块成分股: {ts_code}")

            df = self._safe_api_call(self.pro.ths_member, ts_code=ts_code)
            time.sleep(self.api_delay['member'])

            if df is not None and not df.empty:
                # 添加核心日期维度和板块信息
                df['ymd'] = DateUtility.today()
                df['board_code'] = ts_code

                # 获取板块名称（这里需要根据实际情况补充）
                df['board_name'] = ts_code  # 临时用代码代替，实际应从ths_index获取

                # 重命名列以匹配新表结构
                df = df.rename(columns={
                    'con_code': 'stock_code',
                    'con_name': 'stock_name',
                    'is_new': 'is_new',
                    'weight': 'weight'
                })

                # 日期格式转换
                if 'in_date' in df.columns:
                    df['in_date'] = pd.to_datetime(df['in_date'], errors='coerce').dt.strftime('%Y%m%d')
                if 'out_date' in df.columns:
                    df['out_date'] = pd.to_datetime(df['out_date'], errors='coerce').dt.strftime('%Y%m%d')

                # 确保所有必需的列都存在
                required_columns = ['ymd', 'board_name', 'board_code', 'stock_code',
                                    'stock_name', 'weight', 'in_date', 'out_date', 'is_new']

                for col in required_columns:
                    if col not in df.columns:
                        df[col] = None

                # 转换数值类型
                if 'weight' in df.columns:
                    df['weight'] = pd.to_numeric(df['weight'], errors='coerce')

                return df[required_columns]

            return pd.DataFrame()

        except Exception as e:
            self.logger.error(f"获取ths_member({ts_code})失败: {e}")
            return pd.DataFrame()

    # ===== 东方财富板块数据接口 =====

    def get_dc_index(self, trade_date=None, ts_code=None):
        """东方财富概念板块 - 匹配新表结构"""
        try:
            if not trade_date:
                trade_date = DateUtility.today()

            self.logger.info(f"获取东方财富概念板块 {trade_date}")

            params = {'trade_date': trade_date}
            if ts_code:
                params['ts_code'] = ts_code

            df = self._safe_api_call(self.pro.dc_index, **params)
            time.sleep(self.api_delay['normal'])

            if df is not None and not df.empty:
                # 添加日期维度
                df['ymd'] = trade_date

                # 重命名列以匹配新表结构
                df = df.rename(columns={
                    'ts_code': 'board_code',
                    'name': 'board_name',
                    'leading': 'leading_stock',
                    'leading_code': 'leading_stock_code',
                    'pct_change': 'change_pct',
                    'leading_pct': 'leading_stock_pct',
                    'total_mv': 'total_market_value',
                    'up_num': 'rising_stocks_num',
                    'down_num': 'falling_stocks_num',
                    'turnover_rate': 'turnover_rate'
                })

                # 确保所有必需的列都存在
                required_columns = ['ymd', 'board_name', 'board_code', 'leading_stock',
                                    'leading_stock_code', 'change_pct', 'leading_stock_pct',
                                    'total_market_value', 'turnover_rate', 'rising_stocks_num',
                                    'falling_stocks_num']

                for col in required_columns:
                    if col not in df.columns:
                        df[col] = None

                # 转换数值类型
                numeric_columns = ['change_pct', 'leading_stock_pct', 'total_market_value',
                                   'turnover_rate', 'rising_stocks_num', 'falling_stocks_num']
                for col in numeric_columns:
                    if col in df.columns:
                        if col in ['rising_stocks_num', 'falling_stocks_num']:
                            df[col] = pd.to_numeric(df[col], errors='coerce').astype('Int64')
                        else:
                            df[col] = pd.to_numeric(df[col], errors='coerce')

                return df[required_columns]

            return pd.DataFrame()

        except Exception as e:
            self.logger.error(f"获取dc_index失败: {e}")
            return pd.DataFrame()

    def get_dc_daily(self, trade_date=None):
        """东方财富板块行情 - 匹配新表结构"""
        try:
            if not trade_date:
                trade_date = DateUtility.today()

            self.logger.info(f"获取东方财富板块行情 {trade_date}")

            df = self._safe_api_call(self.pro.dc_daily, trade_date=trade_date)
            time.sleep(self.api_delay['normal'])

            if df is not None and not df.empty:
                # 添加日期维度
                df['ymd'] = trade_date

                # 获取板块名称信息
                board_info = {}
                if 'ts_code' in df.columns:
                    unique_codes = df['ts_code'].unique()
                    for code in unique_codes:
                        board_info[code] = code  # 临时用代码代替

                # 重命名列以匹配新表结构
                df = df.rename(columns={
                    'ts_code': 'board_code',
                    'change': 'change_amt',
                    'pct_change': 'change_pct',
                    'vol': 'trading_volume',
                    'amount': 'trading_amount',
                    'swing': 'amplitude',
                    'turnover_rate': 'turnover_rate'
                })

                # 添加板块名称
                df['board_name'] = df['board_code'].map(board_info)

                # 确保所有必需的列都存在
                required_columns = ['ymd', 'board_name', 'board_code', 'open', 'high', 'low',
                                    'close', 'change_amt', 'change_pct', 'trading_volume',
                                    'trading_amount', 'amplitude', 'turnover_rate', 'category']

                for col in required_columns:
                    if col not in df.columns:
                        if col == 'category':
                            df['category'] = '概念板块'  # 默认值
                        else:
                            df[col] = None

                # 转换数值类型
                numeric_columns = ['open', 'high', 'low', 'close', 'change_amt', 'change_pct',
                                   'trading_volume', 'trading_amount', 'amplitude', 'turnover_rate']
                for col in numeric_columns:
                    if col in df.columns:
                        df[col] = pd.to_numeric(df[col], errors='coerce')

                return df[required_columns]

            return pd.DataFrame()

        except Exception as e:
            self.logger.error(f"获取dc_daily失败: {e}")
            return pd.DataFrame()

    def get_dc_member(self, trade_date=None, ts_code=None):
        """东方财富板块成分股 - 匹配新表结构"""
        try:
            if not trade_date:
                trade_date = DateUtility.today()

            self.logger.info(f"获取东方财富板块成分 {trade_date} {ts_code or ''}")

            params = {'trade_date': trade_date}
            if ts_code:
                params['ts_code'] = ts_code

            df = self._safe_api_call(self.pro.dc_member, **params)
            time.sleep(self.api_delay['member'])

            if df is not None and not df.empty:
                # 添加日期维度
                df['ymd'] = trade_date

                # 获取板块名称信息
                if 'ts_code' in df.columns:
                    df['board_name'] = df['ts_code']  # 临时用代码代替

                # 重命名列以匹配新表结构
                df = df.rename(columns={
                    'trade_date': 'ymd',
                    'ts_code': 'board_code',
                    'con_code': 'stock_code',
                    'name': 'stock_name'
                })

                # 确保所有必需的列都存在
                required_columns = ['ymd', 'board_name', 'board_code', 'stock_code', 'stock_name']

                for col in required_columns:
                    if col not in df.columns:
                        df[col] = None

                return df[required_columns]

            return pd.DataFrame()

        except Exception as e:
            self.logger.error(f"获取dc_member失败: {e}")
            return pd.DataFrame()

    # ============ 批量获取函数 ============

    def batch_get_all_ths_members(self, board_codes=None, batch_size=5, max_boards=None):
        """
        批量获取所有同花顺板块的成分股
        """
        try:
            if board_codes is None:
                index_df = self.get_ths_index()
                if index_df.empty:
                    self.logger.error("无法获取板块列表")
                    return pd.DataFrame()
                board_codes = index_df['board_code'].tolist()

            if max_boards:
                board_codes = board_codes[:max_boards]

            total_boards = len(board_codes)
            self.logger.info(f"批量获取{total_boards}个板块的成分股，批次大小{batch_size}")

            all_members = []

            for i in range(0, total_boards, batch_size):
                batch = board_codes[i:i + batch_size]
                batch_num = i // batch_size + 1
                total_batches = (total_boards + batch_size - 1) // batch_size

                self.logger.info(f"处理批次 {batch_num}/{total_batches}")

                batch_results = []
                for board_code in tqdm(batch, desc=f"批次{batch_num}"):
                    try:
                        df = self.get_ths_member(board_code)
                        if not df.empty:
                            batch_results.append(df)
                    except Exception as e:
                        self.logger.warning(f"板块 {board_code} 失败: {str(e)[:50]}")

                    time.sleep(0.3)

                if batch_results:
                    batch_df = pd.concat(batch_results, ignore_index=True)
                    all_members.append(batch_df)

                    # 实时保存 - 使用新表名
                    self._save_to_mysql(batch_df, "ods_tushare_stock_board_concept_maps_ths",
                                        ['ymd', 'board_code', 'stock_code'])

                # 批次间延迟
                if i + batch_size < total_boards:
                    time.sleep(self.api_delay['batch'])

            if all_members:
                final_df = pd.concat(all_members, ignore_index=True)
                self.logger.info(f"批量获取完成: {len(final_df)}条成分股记录")
                return final_df

            return pd.DataFrame()

        except Exception as e:
            self.logger.error(f"批量获取成分股失败: {e}")
            return pd.DataFrame()

    # ============ 数据保存函数 ============

    def _save_to_mysql(self, df, table_name, merge_on):
        """保存数据到MySQL"""
        if df.empty:
            return

        try:
            # 转换日期格式为MySQL DATE类型
            if 'ymd' in df.columns:
                # 确保ymd是YYYYMMDD格式的字符串
                df['ymd'] = pd.to_datetime(df['ymd'], format='%Y%m%d', errors='coerce').dt.strftime('%Y-%m-%d')

            df = df.drop_duplicates(subset=merge_on, keep='first')

            mysql_utils.data_from_dataframe_to_mysql(
                user=base_properties.origin_mysql_user,
                password=base_properties.origin_mysql_password,
                host=base_properties.origin_mysql_host,
                database=base_properties.origin_mysql_database,
                df=df,
                table_name=table_name,
                merge_on=merge_on
            )
            self.logger.info(f"保存成功: {len(df)}条 -> {table_name}")
        except Exception as e:
            self.logger.error(f"保存到MySQL失败({table_name}): {e}")

    # ============ 完整执行函数 ============

    def run_all_data_tasks(self, include_kline=True, include_board_members=True,
                           max_ths_boards=50, max_dc_boards=30):
        """
        执行所有数据获取任务 - 匹配新表结构
        :param include_kline: 是否包含K线数据
        :param include_board_members: 是否包含板块成分股
        :param max_ths_boards: 最大同花顺板块数
        :param max_dc_boards: 最大东方财富板块数
        :return: 结果统计
        """
        self.logger.info("=" * 70)
        self.logger.info("Tushare数据下载任务 - 匹配新表结构（K线表名保持不变）")
        self.logger.info("=" * 70)

        results = {}
        today = DateUtility.today()

        # ===== 1. 股票日K线数据 =====
        if include_kline:
            self.logger.info("\n1. 下载股票日K线数据")
            kline_success = self.save_kline_to_mysql()
            results['kline'] = kline_success

        # ===== 2. 板块数据 =====
        self.logger.info("\n2. 下载板块数据")

        # 2.1 同花顺数据
        self.logger.info("  → 同花顺板块数据")

        # 板块指数列表 - 新表名
        ths_index_df = self.get_ths_index()
        if not ths_index_df.empty:
            self._save_to_mysql(ths_index_df, "ods_tushare_board_concept_name_ths",
                                ['ymd', 'board_code'])
            results['ths_index'] = len(ths_index_df)

        # 板块行情 - 新表名
        ths_daily_df = self.get_ths_daily(limit_days=7)
        if not ths_daily_df.empty:
            self._save_to_mysql(ths_daily_df, "ods_tushare_stock_board_concept_hist_ths",
                                ['ymd', 'board_code'])
            results['ths_daily'] = len(ths_daily_df)

        # 板块成分股（核心） - 新表名
        if include_board_members and not ths_index_df.empty:
            board_codes = ths_index_df['board_code'].tolist()
            if max_ths_boards:
                board_codes = board_codes[:max_ths_boards]

            ths_members_df = self.batch_get_all_ths_members(board_codes, batch_size=5)
            if not ths_members_df.empty:
                results['ths_member'] = len(ths_members_df)

        # 2.2 东方财富数据
        self.logger.info("  → 东方财富板块数据")

        # 概念板块 - 新表名
        dc_index_df = self.get_dc_index(today)
        if not dc_index_df.empty:
            self._save_to_mysql(dc_index_df, "ods_tushare_stock_board_concept_name_em",
                                ['ymd', 'board_code'])
            results['dc_index'] = len(dc_index_df)

        # 板块行情 - 新表名
        dc_daily_df = self.get_dc_daily(today)
        if not dc_daily_df.empty:
            self._save_to_mysql(dc_daily_df, "ods_tushare_stock_board_concept_hist_em",
                                ['ymd', 'board_code'])
            results['dc_daily'] = len(dc_daily_df)

        # 板块成分股 - 新表名
        if include_board_members:
            dc_member_df = self.get_dc_member(today)
            if not dc_member_df.empty:
                self._save_to_mysql(dc_member_df, "ods_tushare_stock_board_concept_maps_em",
                                    ['ymd', 'board_code', 'stock_code'])
                results['dc_member'] = len(dc_member_df)

        # ===== 3. 任务完成 =====
        self.logger.info("\n" + "=" * 70)
        self.logger.info("🎉 所有数据下载任务完成！")
        self.logger.info("=" * 70)

        # 汇总统计
        self.logger.info("📊 数据下载统计:")
        for key, value in results.items():
            if key == 'kline':
                status = "成功" if value else "失败"
                self.logger.info(f"  {key}: {status}")
            else:
                self.logger.info(f"  {key}: {value:,} 条")

        return results


def main():
    """主函数 - 简洁版"""
    print("=" * 60)
    print("Tushare数据下载器 - 匹配新表结构（K线表名保持不变）")
    print("=" * 60)

    # 创建下载器
    token = base_properties.ts_token

    # 创建Tushare实例并进行自定义配置
    ts.set_token(token)
    pro = ts.pro_api()

    # 设置自定义API代理
    pro._DataApi__token = token
    pro._DataApi__http_url = 'http://lianghua.nanyangqiankun.top'

    # 创建下载器，传入配置好的pro实例
    fetcher = TushareDataFetcher(stock_code_df=None)
    fetcher.pro = pro

    # 简单选择
    print("\n请选择下载模式：")
    print("1. 日K线数据 + 板块数据（全部）")
    print("2. 只下载板块数据（不含K线）")

    choice = input("\n请输入选择 (1/2): ").strip()

    if choice == '1':
        # 模式1：包含K线
        print("\n📊 开始下载：日K线 + 全部板块数据")
        results = fetcher.run_all_data_tasks(
            include_kline=True,
            include_board_members=True,
            max_ths_boards=50,
            max_dc_boards=30
        )
    elif choice == '2':
        # 模式2：只下载板块
        print("\n📦 开始下载：仅板块数据（不含K线）")
        results = fetcher.run_all_data_tasks(
            include_kline=False,
            include_board_members=True,
            max_ths_boards=50,
            max_dc_boards=30
        )
    else:
        print("无效选择，退出")
        return

    print(f"\n✅ 下载任务完成！")


if __name__ == '__main__':
    main()