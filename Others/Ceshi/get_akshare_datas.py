import akshare as ak
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import sqlite3
import time


class AShareFullDataCollector:
    def __init__(self, db_path='stock_full_data.db'):
        self.db_path = db_path
        self.init_database()

    def init_database(self):
        """初始化数据库，包含估值数据和股东人数"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()

        # 创建估值数据表
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS stock_valuation (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            trade_date DATE NOT NULL,
            symbol TEXT NOT NULL,
            name TEXT,
            pe REAL,
            pb REAL,
            pe_ttm REAL,
            total_shares REAL,
            float_shares REAL,
            total_mv REAL,
            circ_mv REAL,
            update_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(trade_date, symbol)
        )
        ''')

        # 创建股东人数数据表
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS stock_holder_number (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            symbol TEXT NOT NULL,
            holder_date DATE,
            holder_number REAL,
            holder_change REAL,
            holder_change_pct REAL,
            avg_hold_amount REAL,
            avg_hold_market_value REAL,
            data_source TEXT,
            update_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(symbol, holder_date)
        )
        ''')

        conn.commit()
        conn.close()

    def get_all_stock_codes(self):
        """获取所有A股股票代码列表"""
        try:
            # 获取A股实时行情数据，包含代码列表
            df = ak.stock_zh_a_spot_em()
            stock_codes = df['代码'].tolist()
            print(f"获取到 {len(stock_codes)} 只股票代码")
            return stock_codes[:100]  # 测试时限制数量，实际使用时去掉限制
        except Exception as e:
            print(f"获取股票代码失败: {e}")
            return []

    def get_daily_valuation_data(self):
        """获取每日估值数据（PE/PB等）"""
        print("开始获取每日估值数据...")

        try:
            # 获取A股实时行情数据
            df = ak.stock_zh_a_spot_em()

            # 选择需要的字段
            columns_mapping = {
                '代码': 'symbol',
                '名称': 'name',
                '市盈率-动态': 'pe',
                '市净率': 'pb',
                '总市值': 'total_mv',
                '流通市值': 'circ_mv',
                '总股本': 'total_shares',
                '流通股本': 'float_shares'
            }

            # 筛选并重命名列
            df_clean = df[list(columns_mapping.keys())].copy()
            df_clean.columns = list(columns_mapping.values())

            # 转换数据类型
            numeric_cols = ['pe', 'pb', 'total_mv', 'circ_mv', 'total_shares', 'float_shares']
            for col in numeric_cols:
                df_clean[col] = pd.to_numeric(df_clean[col], errors='coerce')

            # 添加日期
            trade_date = datetime.now().strftime('%Y-%m-%d')
            df_clean['trade_date'] = trade_date

            # 保存到数据库
            self.save_valuation_to_db(df_clean)

            print(f"估值数据获取完成，共 {len(df_clean)} 条记录")
            return df_clean

        except Exception as e:
            print(f"获取估值数据失败: {e}")
            return None

    def get_holder_data_batch(self, stock_codes, batch_size=50):
        """批量获取股东人数数据"""
        print("开始批量获取股东人数数据...")

        all_holder_data = []

        for i in range(0, len(stock_codes), batch_size):
            batch = stock_codes[i:i + batch_size]
            print(f"处理批次 {i // batch_size + 1}: {len(batch)} 只股票")

            for stock_code in batch:
                try:
                    holder_data = self.get_single_stock_holders(stock_code)
                    if holder_data is not None:
                        all_holder_data.append(holder_data)

                    # 避免请求过于频繁
                    time.sleep(0.5)

                except Exception as e:
                    print(f"股票 {stock_code} 股东数据获取失败: {e}")
                    continue

        if all_holder_data:
            result_df = pd.DataFrame(all_holder_data)
            self.save_holders_to_db(result_df)
            return result_df

        return None

    def get_single_stock_holders(self, stock_code):
        """获取单只股票的股东人数"""
        try:
            # 确定市场前缀
            if stock_code.startswith(('0', '3')):
                symbol = f"sz{stock_code}"
                exchange = "SZ"
            else:
                symbol = f"sh{stock_code}"
                exchange = "SH"

            # 获取股东人数明细
            df = ak.stock_zh_a_gdhs_detail_em(symbol=symbol)

            if df is not None and not df.empty:
                # 获取最新数据
                latest = df.iloc[0]

                holder_info = {
                    'symbol': stock_code,
                    'holder_date': latest.get('日期'),
                    'holder_number': latest.get('股东人数'),
                    'holder_change': latest.get('较上期变化'),
                    'holder_change_pct': latest.get('较上期变化比例'),
                    'avg_hold_amount': latest.get('人均持股数'),
                    'avg_hold_market_value': latest.get('人均持股市值'),
                    'data_source': 'eastmoney'
                }

                # 打印最新股东人数
                print(f"{stock_code}: 股东人数 {holder_info['holder_number']}，"
                      f"较上期变化 {holder_info['holder_change']}")

                return holder_info

        except Exception as e:
            # 如果这个接口失败，尝试其他方法
            try:
                return self.get_holder_backup_method(stock_code)
            except:
                return None

    def get_holder_backup_method(self, stock_code):
        """备用方法获取股东人数"""
        try:
            # 使用股票基本信息接口
            if stock_code.startswith(('0', '3')):
                info_df = ak.stock_individual_info_em(symbol=f"sz{stock_code}")
            else:
                info_df = ak.stock_individual_info_em(symbol=f"sh{stock_code}")

            # 查找股东人数信息
            holder_row = info_df[info_df['item'].str.contains('股东人数|股东总数')]
            if not holder_row.empty:
                holder_number = holder_row.iloc[0]['value']

                return {
                    'symbol': stock_code,
                    'holder_date': datetime.now().strftime('%Y-%m-%d'),
                    'holder_number': float(str(holder_number).replace(',', '')),
                    'data_source': 'backup'
                }
        except:
            return None

        return None

    def save_valuation_to_db(self, df):
        """保存估值数据到数据库"""
        conn = sqlite3.connect(self.db_path)
        df.to_sql('stock_valuation', conn, if_exists='append', index=False)
        conn.close()

    def save_holders_to_db(self, df):
        """保存股东人数数据到数据库"""
        conn = sqlite3.connect(self.db_path)
        df.to_sql('stock_holder_number', conn, if_exists='append', index=False)
        conn.close()

    def run_daily_collection(self):
        """运行每日数据收集"""
        print("=" * 50)
        print(f"开始执行每日数据收集 {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print("=" * 50)

        # 1. 获取股票代码列表
        stock_codes = self.get_all_stock_codes()

        if not stock_codes:
            print("未获取到股票代码，程序退出")
            return

        # 2. 获取每日估值数据
        valuation_data = self.get_daily_valuation_data()

        # 3. 获取股东人数数据（建议分批进行，避免请求过多）
        print("\n开始获取股东人数数据...")
        holder_data = self.get_holder_data_batch(stock_codes[:50])  # 先获取前50只

        print("\n" + "=" * 50)
        print("数据收集完成")

        if valuation_data is not None:
            print(f"估值数据: {len(valuation_data)} 条记录")
            print(f"PE中位数: {valuation_data['pe'].median():.2f}")
            print(f"PB中位数: {valuation_data['pb'].median():.2f}")

        if holder_data is not None:
            print(f"股东人数数据: {len(holder_data)} 条记录")

        print("=" * 50)


# 使用示例
if __name__ == "__main__":
    collector = AShareFullDataCollector()

    # 运行数据收集
    collector.run_daily_collection()

    # 或者分别获取
    # valuation = collector.get_daily_valuation_data()
    # holders = collector.get_holder_data_batch(["000001", "000002"])