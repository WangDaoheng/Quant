# -*- coding: utf-8 -*-

import numpy as np
import pandas as pd
from datetime import datetime, timedelta
import json
import logging
from pathlib import Path

import CommonProperties.Base_Properties as base_properties
import CommonProperties.Mysql_Utils as mysql_utils
from CommonProperties.DateUtility import DateUtility
from CommonProperties.Base_utils import timing_decorator, script_run
from CommonProperties import set_config

# ************************************************************************
# 本代码的作用是生成大盘情绪仪表盘的JSON数据文件
# 输出: sentiment.json + plates.json 供前端静态页面读取
# ************************************************************************

# 调用日志配置
set_config.setup_logging_config()

# 数据库配置
local_user = base_properties.local_mysql_user
local_password = base_properties.local_mysql_password
local_database = base_properties.local_mysql_database
local_host = base_properties.local_mysql_host

origin_user = base_properties.origin_mysql_user
origin_password = base_properties.origin_mysql_password
origin_database = base_properties.origin_mysql_database
origin_host = base_properties.origin_mysql_host

# 输出目录
OUTPUT_DIR = Path('./data_jsons')
OUTPUT_DIR.mkdir(exist_ok=True)

# 指数代码映射
INDEX_CODES = {
    'sh': '000001.SH',  # 上证指数（全市场量能）
    'cyb': '399006.SZ',  # 创业板指
    'kc50': '000688.SH',  # 科创50
}

# 情绪得分权重
SCORE_WEIGHTS = {
    'zt_ratio': 0.30,  # 涨跌停比
    'premium': 0.25,  # 打板溢价
    'hl_diff': 0.20,  # 新高新低差
    'vol_pctile': 0.25,  # 量能分位
}

class GenerateSentimentData:
    """生成情绪仪表盘数据"""
    def __init__(self, n_days=60):
        self.n_days = n_days
        self.trading_days = []
        self.limit_df = pd.DataFrame()
        self.index_data = {}
        self.hl_df = pd.DataFrame()
        self.zt_consecutive = pd.Series(dtype=int)

    def get_trading_days(self):
        """获取最近N个交易日"""
        end_date = DateUtility.next_day(-1)  # 昨天（T-1）
        start_date = (datetime.strptime(end_date, '%Y%m%d') - timedelta(days=self.n_days * 2)).strftime('%Y%m%d')

        df = mysql_utils.data_from_mysql_to_dataframe(
            user=origin_user,
            password=origin_password,
            host=origin_host,
            database=origin_database,
            table_name='ods_trading_days_insight',
            start_date=start_date,
            end_date=end_date,
            cols=['ymd']
        )

        if df.empty:
            logging.error("交易日历表为空，无法获取交易日")
            return []

        self.trading_days = sorted(df['ymd'].astype(str).tolist())[-self.n_days:]
        logging.info(f"交易日范围: {self.trading_days[0]} ~ {self.trading_days[-1]}, 共{len(self.trading_days)}天")
        return self.trading_days

    def fetch_limit_summary(self):
        """获取涨跌停数据"""
        start_date = self.trading_days[0]
        end_date = self.trading_days[-1]

        self.limit_df = mysql_utils.data_from_mysql_to_dataframe(
            user=origin_user,
            password=origin_password,
            host=origin_host,
            database=origin_database,
            table_name='ods_stock_limit_summary_insight',
            start_date=start_date,
            end_date=end_date,
            cols=['ymd', 'today_ZT', 'today_DT', 'yesterday_ZT_rate']
        )

        if not self.limit_df.empty:
            self.limit_df['ymd'] = self.limit_df['ymd'].astype(str)
            self.limit_df = self.limit_df.set_index('ymd')
            logging.info(f"涨跌停数据: {len(self.limit_df)}天")
        else:
            logging.warning("涨跌停数据为空")

    def fetch_index_data(self):
        """获取指数K线数据"""
        start_date = self.trading_days[0]
        end_date = self.trading_days[-1]

        all_df = mysql_utils.data_from_mysql_to_dataframe(
            user=origin_user,
            password=origin_password,
            host=origin_host,
            database=origin_database,
            table_name='ods_index_a_share_insight',
            start_date=start_date,
            end_date=end_date,
            cols=['index_code', 'ymd', 'open', 'close', 'high', 'low', 'volume']
        )

        if all_df.empty:
            logging.error("指数数据为空")
            return

        all_df['ymd'] = all_df['ymd'].astype(str)

        for key, code in INDEX_CODES.items():
            sub_df = all_df[all_df['index_code'] == code].copy()
            if not sub_df.empty:
                sub_df = sub_df.set_index('ymd')
                self.index_data[code] = sub_df
                logging.info(f"{code} 数据: {len(sub_df)}天")
            else:
                logging.warning(f"{code} 无数据")

    def fetch_high_low(self):
        """获取新高新低数据"""
        start_date = self.trading_days[0]
        end_date = self.trading_days[-1]

        self.hl_df = mysql_utils.data_from_mysql_to_dataframe(
            user=origin_user,
            password=origin_password,
            host=origin_host,
            database=origin_database,
            table_name='ods_akshare_stock_a_high_low_statistics',
            start_date=start_date,
            end_date=end_date,
            cols=['ymd', 'market', 'high20', 'low20', 'high60', 'low60']
        )

        if not self.hl_df.empty:
            # 只保留全市场数据
            self.hl_df = self.hl_df[self.hl_df['market'] == 'all'].copy()
            self.hl_df['ymd'] = self.hl_df['ymd'].astype(str)
            self.hl_df = self.hl_df.set_index('ymd')
            logging.info(f"新高新低数据: {len(self.hl_df)}天")
        else:
            logging.warning("新高新低数据为空")

    def fetch_zt_list(self):
        """获取涨停清单，计算最高连板"""
        start_date = self.trading_days[0]
        end_date = self.trading_days[-1]

        df = mysql_utils.data_from_mysql_to_dataframe(
            user=origin_user,
            password=origin_password,
            host=origin_host,
            database=origin_database,
            table_name='dwd_stock_zt_list',
            start_date=start_date,
            end_date=end_date,
            cols=['ymd', 'stock_code']
        )

        if df.empty:
            logging.warning("涨停清单为空")
            self.zt_consecutive = pd.Series(dtype=int)
            return

        # 统一日期格式为 YYYYMMDD
        df['ymd'] = df['ymd'].astype(str).str.replace('-', '').str.replace('/', '')
        df = df.sort_values(['stock_code', 'ymd'])

        # 计算连板数
        df['consecutive'] = 1
        for stock in df['stock_code'].unique():
            mask = df['stock_code'] == stock
            stock_df = df[mask]
            consecutive = 1
            prev_ymd = None
            for idx, row in stock_df.iterrows():
                if prev_ymd is not None:
                    curr_date = datetime.strptime(row['ymd'], '%Y%m%d')
                    prev_date = datetime.strptime(prev_ymd, '%Y%m%d')
                    if (curr_date - prev_date).days <= 3:
                        consecutive += 1
                    else:
                        consecutive = 1
                df.loc[idx, 'consecutive'] = consecutive
                prev_ymd = row['ymd']

        self.zt_consecutive = df.groupby('ymd')['consecutive'].max()
        logging.info(f"连板数据: {len(self.zt_consecutive)}天")

    def calc_volume_percentile(self, volume_series):
        """计算量能分位"""
        return volume_series.rank(pct=True) * 100

    def calc_emotion_score(self, row, vol_pctile):
        """计算情绪得分"""
        zt = row.get('today_ZT', 0)
        dt = row.get('today_DT', 0)
        zt_ratio = zt / (zt + dt + 1)
        zt_score = min(zt_ratio * 150, 100)

        premium = row.get('yesterday_ZT_rate', 0) or 0
        premium_score = max(0, min((premium + 3) / 6 * 100, 100))

        high20 = row.get('high20', 0) or 0
        low20 = row.get('low20', 0) or 0
        hl_diff = high20 - low20
        hl_score = max(0, min((hl_diff + 200) / 400 * 100, 100))

        vol_score = vol_pctile if not pd.isna(vol_pctile) else 50

        total = (zt_score * SCORE_WEIGHTS['zt_ratio'] +
                 premium_score * SCORE_WEIGHTS['premium'] +
                 hl_score * SCORE_WEIGHTS['hl_diff'] +
                 vol_score * SCORE_WEIGHTS['vol_pctile'])

        return {
            'score': round(total, 1),
            'zt_score': round(zt_score, 1),
            'premium_score': round(premium_score, 1),
            'hl_score': round(hl_score, 1),
            'vol_score': round(vol_score, 1),
        }

    @timing_decorator
    def generate_sentiment_json(self):
        """生成 sentiment.json"""
        sh_df = self.index_data.get(INDEX_CODES['sh'], pd.DataFrame()).copy()
        if sh_df.empty:
            logging.error("上证指数数据为空，无法生成情绪数据")
            return

        sh_df['amount_yi'] = sh_df['volume'] * 100 / 1e8
        vol_pctile = self.calc_volume_percentile(sh_df['amount_yi'])

        result = {
            'meta': {
                'generated_at': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                'date_range': [self.trading_days[0], self.trading_days[-1]],
                'days': len(self.trading_days),
            },
            'daily': []
        }

        for ymd in self.trading_days:
            date_str = f"{ymd[4:6]}/{ymd[6:8]}"

            limit_row = self.limit_df.loc[ymd] if ymd in self.limit_df.index else pd.Series()
            hl_row = self.hl_df.loc[ymd] if ymd in self.hl_df.index else pd.Series()

            sh_row = sh_df.loc[ymd] if ymd in sh_df.index else pd.Series()
            cyb_df = self.index_data.get(INDEX_CODES['cyb'], pd.DataFrame())
            cyb_row = cyb_df.loc[ymd] if ymd in cyb_df.index else pd.Series()
            kc_df = self.index_data.get(INDEX_CODES['kc50'], pd.DataFrame())
            kc_row = kc_df.loc[ymd] if ymd in kc_df.index else pd.Series()

            emotion = self.calc_emotion_score(
                {**limit_row.to_dict(), **hl_row.to_dict()},
                vol_pctile.get(ymd, 50)
            )

            max_board = int(self.zt_consecutive.get(ymd, 0))

            daily_data = {
                'ymd': ymd,
                'date': date_str,
                'zt': int(limit_row.get('today_ZT', 0)),
                'dt': int(limit_row.get('today_DT', 0)),
                'premium': round(float(limit_row.get('yesterday_ZT_rate', 0) or 0), 2),
                'hl_diff': int(hl_row.get('high20', 0) or 0) - int(hl_row.get('low20', 0) or 0),
                'high20': int(hl_row.get('high20', 0) or 0),
                'low20': int(hl_row.get('low20', 0) or 0),
                'amount_yi': round(float(sh_row.get('amount_yi', 0)), 2),
                'max_board': max_board,
                'emotion': emotion,
                'indices': {
                    'sh': {
                        'open': round(float(sh_row.get('open', 0)), 2),
                        'close': round(float(sh_row.get('close', 0)), 2),
                        'low': round(float(sh_row.get('low', 0)), 2),
                        'high': round(float(sh_row.get('high', 0)), 2),
                        'volume': round(float(sh_row.get('volume', 0)) * 100 / 1e12, 2),
                    },
                    'cyb': {
                        'open': round(float(cyb_row.get('open', 0)), 2),
                        'close': round(float(cyb_row.get('close', 0)), 2),
                        'low': round(float(cyb_row.get('low', 0)), 2),
                        'high': round(float(cyb_row.get('high', 0)), 2),
                        'volume': round(float(cyb_row.get('volume', 0)) * 100 / 1e12, 2),
                    },
                    'kc50': {
                        'open': round(float(kc_row.get('open', 0)), 2),
                        'close': round(float(kc_row.get('close', 0)), 2),
                        'low': round(float(kc_row.get('low', 0)), 2),
                        'high': round(float(kc_row.get('high', 0)), 2),
                        'volume': round(float(kc_row.get('volume', 0)) * 100 / 1e12, 2),
                    },
                }
            }

            result['daily'].append(daily_data)

        output_file = OUTPUT_DIR / 'sentiment.json'
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(result, f, ensure_ascii=False, indent=2)

        logging.info(f"生成 {output_file}，共 {len(result['daily'])} 天数据")

    @timing_decorator
    def generate_plates_json(self):
        """生成 plates.json（板块数据）"""
        result = {
            'meta': {
                'generated_at': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                'date_range': [self.trading_days[0], self.trading_days[-1]] if self.trading_days else [],
            },
            'daily_top20': {}
        }

        output_file = OUTPUT_DIR / 'plates.json'
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(result, f, ensure_ascii=False, indent=2)

        logging.info(f"生成 {output_file}（板块数据待补充）")

    @script_run(script_name="generate_sentiment_data.py")
    def setup(self):
        """主流程"""
        logging.info(f"开始生成情绪数据，最近 {self.n_days} 个交易日...")

        if not self.get_trading_days():
            logging.error("无交易日数据，退出")
            return

        self.fetch_limit_summary()
        self.fetch_index_data()
        self.fetch_high_low()
        self.fetch_zt_list()

        self.generate_sentiment_json()
        self.generate_plates_json()

        logging.info("完成！")


if __name__ == '__main__':
    # 增量：最近60个交易日
    generator = GenerateSentimentData(n_days=60)
    generator.setup()

    # 历史回填：250日（用于量能分位）
    # generator = GenerateSentimentData(n_days=250)
    # generator.setup()
