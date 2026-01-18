import akshare as ak
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import sqlite3
import time
import traceback


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
        print("数据库初始化完成")

    def get_all_stock_codes(self):
        """获取所有A股股票代码列表"""
        try:
            # 获取A股实时行情数据，包含代码列表
            df = ak.stock_zh_a_spot_em()
            print(f"获取到的数据列名: {df.columns.tolist()}")
            print(f"数据样例:\n{df.head()[['代码', '名称', '最新价', '市盈率-动态']]}")

            stock_codes = df['代码'].astype(str).tolist()
            print(f"获取到 {len(stock_codes)} 只股票代码")
            return stock_codes
        except Exception as e:
            print(f"获取股票代码失败: {e}")
            traceback.print_exc()
            return []

    def get_daily_valuation_data(self):
        """获取每日估值数据（PE/PB等）- 修复版"""
        print("开始获取每日估值数据...")

        try:
            # 获取A股实时行情数据
            df = ak.stock_zh_a_spot_em()

            print("原始数据列名:", df.columns.tolist())
            print(f"数据形状: {df.shape}")

            # 查看实际存在的列名
            available_columns = df.columns.tolist()

            # 定义需要的字段和可能的别名
            required_fields = {
                'symbol': ['代码'],
                'name': ['名称'],
                'pe': ['市盈率-动态', '市盈率(动态)', 'pe'],
                'pb': ['市净率', 'pb'],
                'total_mv': ['总市值', '总市值(元)', '总市值元'],
                'circ_mv': ['流通市值', '流通市值(元)', '流通市值元'],
                'total_shares': ['总股本', '总股数', '总股本(万股)'],
                'float_shares': ['流通股本', '流通股', '流通股本(万股)']
            }

            # 构建最终的字段映射
            column_mapping = {}
            missing_fields = []

            for new_name, possible_names in required_fields.items():
                found = False
                for possible_name in possible_names:
                    if possible_name in available_columns:
                        column_mapping[possible_name] = new_name
                        found = True
                        break

                if not found:
                    missing_fields.append(new_name)
                    column_mapping[new_name] = new_name  # 使用默认值

            print(f"找到的字段映射: {column_mapping}")
            print(f"缺失的字段: {missing_fields}")

            # 只选择存在的列
            existing_columns = [col for col in column_mapping.keys() if col in available_columns]
            df_selected = df[existing_columns].copy()

            # 重命名列
            df_selected.columns = [column_mapping[col] for col in existing_columns]

            # 添加缺失的列
            for missing_field in missing_fields:
                if missing_field not in df_selected.columns:
                    df_selected[missing_field] = None

            # 确保所有需要的列都存在
            for field in required_fields.keys():
                if field not in df_selected.columns:
                    df_selected[field] = None

            # 转换数据类型
            numeric_cols = ['pe', 'pb', 'total_mv', 'circ_mv', 'total_shares', 'float_shares']
            for col in numeric_cols:
                if col in df_selected.columns:
                    df_selected[col] = pd.to_numeric(df_selected[col], errors='coerce')

            # 添加日期
            trade_date = datetime.now().strftime('%Y-%m-%d')
            df_selected['trade_date'] = trade_date

            # 显示数据样例
            print(f"\n处理后的数据样例:")
            print(df_selected.head()[['symbol', 'name', 'pe', 'pb', 'total_mv']])
            print(f"\n数据统计:")
            print(f"PE平均值: {df_selected['pe'].mean():.2f}")
            print(f"PB平均值: {df_selected['pb'].mean():.2f}")
            print(f"总记录数: {len(df_selected)}")

            # 保存到数据库
            if len(df_selected) > 0:
                self.save_valuation_to_db(df_selected)
                print(f"估值数据保存完成，共 {len(df_selected)} 条记录")
            else:
                print("警告: 没有数据可保存")

            return df_selected

        except Exception as e:
            print(f"获取估值数据失败: {e}")
            traceback.print_exc()
            return None

    def save_valuation_to_db(self, df):
        """保存估值数据到数据库 - 修复版"""
        try:
            conn = sqlite3.connect(self.db_path)

            # 确保列顺序与表结构匹配
            table_columns = ['trade_date', 'symbol', 'name', 'pe', 'pb', 'pe_ttm',
                             'total_shares', 'float_shares', 'total_mv', 'circ_mv']

            # 重新排列列顺序，添加缺失的列
            for col in table_columns:
                if col not in df.columns:
                    df[col] = None

            # 只保留表需要的列
            df_to_save = df[table_columns].copy()

            # 保存到数据库
            df_to_save.to_sql('stock_valuation', conn, if_exists='append', index=False)
            conn.close()

            print(f"成功保存 {len(df_to_save)} 条估值数据到数据库")

            # 验证保存的数据
            self.verify_saved_data('stock_valuation')

        except Exception as e:
            print(f"保存估值数据到数据库失败: {e}")
            traceback.print_exc()

    def verify_saved_data(self, table_name):
        """验证数据库中的数据"""
        try:
            conn = sqlite3.connect(self.db_path)
            query = f"SELECT COUNT(*) as count, MAX(trade_date) as latest_date FROM {table_name}"
            result = pd.read_sql_query(query, conn)
            conn.close()

            print(f"数据库验证 - {table_name}:")
            print(f"总记录数: {result['count'].iloc[0]}")
            print(f"最新日期: {result['latest_date'].iloc[0]}")

            # 查看具体数据
            if result['count'].iloc[0] > 0:
                conn = sqlite3.connect(self.db_path)
                sample_query = f"SELECT * FROM {table_name} ORDER BY update_time DESC LIMIT 5"
                sample_data = pd.read_sql_query(sample_query, conn)
                conn.close()
                print(f"最近5条数据:\n{sample_data}")

        except Exception as e:
            print(f"验证数据失败: {e}")

    def get_holder_data_batch(self, stock_codes, batch_size=10):
        """批量获取股东人数数据 - 简化版"""
        print("开始批量获取股东人数数据...")

        all_holder_data = []
        successful_count = 0

        # 先测试少量股票
        test_codes = stock_codes[:batch_size] if len(stock_codes) > batch_size else stock_codes

        for i, stock_code in enumerate(test_codes, 1):
            try:
                print(f"\n[{i}/{len(test_codes)}] 获取股票 {stock_code} 的股东人数...")

                holder_info = self.get_single_stock_holders_debug(stock_code)
                if holder_info:
                    all_holder_data.append(holder_info)
                    successful_count += 1

                # 避免请求过于频繁
                time.sleep(1)

            except Exception as e:
                print(f"股票 {stock_code} 股东数据获取失败: {e}")
                continue

        if all_holder_data:
            result_df = pd.DataFrame(all_holder_data)
            print(f"\n成功获取 {successful_count} 只股票的股东人数")
            print(result_df.head())

            # 保存到数据库
            self.save_holders_to_db(result_df)
            return result_df
        else:
            print("未获取到任何股东人数数据")
            return None

    def get_single_stock_holders_debug(self, stock_code):
        """调试版本的股东人数获取"""
        try:
            # 方法1: 使用股东人数明细接口
            print(f"尝试方法1: 使用股东人数明细接口...")

            # 确定市场前缀
            if stock_code.startswith(('0', '3')):
                symbol = f"sz{stock_code}"
            else:
                symbol = f"sh{stock_code}"

            print(f"尝试获取 {symbol} 的股东人数...")
            df = ak.stock_zh_a_gdhs_detail_em(symbol=symbol)

            if df is not None and not df.empty:
                print(f"成功获取到 {len(df)} 条历史记录")
                print(f"列名: {df.columns.tolist()}")
                print(f"最新记录:\n{df.iloc[0]}")

                latest = df.iloc[0]
                holder_info = {
                    'symbol': stock_code,
                    'holder_date': latest.get('日期'),
                    'holder_number': latest.get('股东人数'),
                    'holder_change': latest.get('较上期变化'),
                    'holder_change_pct': latest.get('较上期变化比例'),
                    'avg_hold_amount': latest.get('人均持股数'),
                    'avg_hold_market_value': latest.get('人均持股市值'),
                    'data_source': 'eastmoney_detail'
                }
                return holder_info
            else:
                print(f"股东人数明细接口返回空数据")

        except Exception as e:
            print(f"方法1失败: {e}")

        # 方法2: 使用备用接口
        try:
            print(f"尝试方法2: 使用备用接口...")
            return self.get_holder_backup_method(stock_code)
        except Exception as e:
            print(f"方法2失败: {e}")
            return None

    def get_holder_backup_method(self, stock_code):
        """备用方法获取股东人数"""
        try:
            # 使用股票基本信息接口
            if stock_code.startswith(('0', '3')):
                symbol = f"sz{stock_code}"
            else:
                symbol = f"sh{stock_code}"

            print(f"尝试获取 {symbol} 的基本信息...")
            info_df = ak.stock_individual_info_em(symbol=symbol)

            if info_df is not None and not info_df.empty:
                print(f"基本信息列名: {info_df.columns.tolist()}")
                print(f"基本信息内容:\n{info_df.head()}")

                # 查找股东人数信息
                for _, row in info_df.iterrows():
                    if '股东人数' in str(row['item']) or '股东总数' in str(row['item']):
                        print(f"找到股东人数信息: {row['item']} = {row['value']}")

                        # 尝试解析数值
                        try:
                            value_str = str(row['value']).replace(',', '').replace('万', '')
                            holder_num = float(value_str)

                            # 如果是万单位，转换为实际人数
                            if '万' in str(row['value']):
                                holder_num = holder_num * 10000

                            return {
                                'symbol': stock_code,
                                'holder_date': datetime.now().strftime('%Y-%m-%d'),
                                'holder_number': holder_num,
                                'data_source': 'backup_info'
                            }
                        except:
                            return {
                                'symbol': stock_code,
                                'holder_date': datetime.now().strftime('%Y-%m-%d'),
                                'holder_number': None,
                                'data_source': 'backup_info_parse_error'
                            }

            print("未在基本信息中找到股东人数")
            return None

        except Exception as e:
            print(f"备用方法失败: {e}")
            return None

    def save_holders_to_db(self, df):
        """保存股东人数数据到数据库"""
        try:
            if df.empty:
                print("没有股东数据可保存")
                return

            conn = sqlite3.connect(self.db_path)

            # 确保列顺序与表结构匹配
            table_columns = ['symbol', 'holder_date', 'holder_number', 'holder_change',
                             'holder_change_pct', 'avg_hold_amount', 'avg_hold_market_value', 'data_source']

            # 重新排列列顺序，添加缺失的列
            for col in table_columns:
                if col not in df.columns:
                    df[col] = None

            # 只保留表需要的列
            df_to_save = df[table_columns].copy()

            # 保存到数据库
            df_to_save.to_sql('stock_holder_number', conn, if_exists='append', index=False)
            conn.close()

            print(f"成功保存 {len(df_to_save)} 条股东数据到数据库")

            # 验证保存的数据
            self.verify_saved_data('stock_holder_number')

        except Exception as e:
            print(f"保存股东数据到数据库失败: {e}")
            traceback.print_exc()

    def run_daily_collection(self):
        """运行每日数据收集"""
        print("=" * 60)
        print(f"开始执行每日数据收集 {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print("=" * 60)

        # 1. 获取股票代码列表
        print("\n[步骤1] 获取股票代码列表...")
        stock_codes = self.get_all_stock_codes()

        if not stock_codes:
            print("未获取到股票代码，程序退出")
            return

        # 2. 获取每日估值数据
        print("\n[步骤2] 获取每日估值数据...")
        valuation_data = self.get_daily_valuation_data()

        # 3. 获取股东人数数据（先获取少量）
        print("\n[步骤3] 获取股东人数数据...")
        test_codes = stock_codes[:20]  # 先测试20只股票
        print(f"测试获取 {len(test_codes)} 只股票的股东人数")
        holder_data = self.get_holder_data_batch(test_codes, batch_size=10)

        # 4. 最终统计
        print("\n" + "=" * 60)
        print("数据收集完成统计:")
        print("=" * 60)

        if valuation_data is not None:
            print(f"估值数据: {len(valuation_data)} 条记录")
            print(f"PE统计: 均值={valuation_data['pe'].mean():.2f}, 中位数={valuation_data['pe'].median():.2f}")
            print(f"PB统计: 均值={valuation_data['pb'].mean():.2f}, 中位数={valuation_data['pb'].median():.2f}")

        if holder_data is not None:
            print(f"股东人数数据: {len(holder_data)} 条记录")
            valid_holders = holder_data['holder_number'].notna().sum()
            print(f"有效股东人数记录: {valid_holders} 条")

        # 5. 导出数据到CSV
        self.export_to_csv(valuation_data, holder_data)

        print("=" * 60)

    def export_to_csv(self, valuation_data, holder_data):
        """导出数据到CSV文件"""
        try:
            trade_date = datetime.now().strftime('%Y%m%d')

            if valuation_data is not None and len(valuation_data) > 0:
                csv_file = f"stock_valuation_{trade_date}.csv"
                valuation_data.to_csv(csv_file, index=False, encoding='utf-8-sig')
                print(f"估值数据已导出到: {csv_file}")

            if holder_data is not None and len(holder_data) > 0:
                csv_file = f"stock_holders_{trade_date}.csv"
                holder_data.to_csv(csv_file, index=False, encoding='utf-8-sig')
                print(f"股东数据已导出到: {csv_file}")

        except Exception as e:
            print(f"导出CSV失败: {e}")


# 独立测试函数
def demo_single_stock():
    """测试单只股票的数据获取"""
    print("测试单只股票数据获取...")

    # 测试平安银行
    test_code = "000001"
    collector = AShareFullDataCollector('test_stock.db')

    print(f"\n测试股票 {test_code}:")

    # 测试估值数据
    print("\n1. 测试估值数据获取...")
    df_spot = ak.stock_zh_a_spot_em()
    stock_data = df_spot[df_spot['代码'] == test_code]
    if not stock_data.empty:
        print(f"估值数据: {stock_data.iloc[0][['代码', '名称', '市盈率-动态', '市净率']]}")

    # 测试股东人数
    print("\n2. 测试股东人数获取...")
    holder_info = collector.get_single_stock_holders_debug(test_code)
    if holder_info:
        print(f"股东人数信息: {holder_info}")


def demo_stock_board_concept_cons_em():
    res0 = ak.stock_board_concept_name_em()
    temp = res0[res0['板块名称']=='存储芯片']
    res = ak.stock_board_concept_cons_em(symbol='存储芯片')
    res2 = ak.stock_board_concept_hist_em(symbol='存储芯片',period='daily',start_date='20251001',end_date='20260201')
    ak.stock_value_em()
    print(res.shape)


# 主程序
if __name__ == "__main__":
    # 可以选择运行测试或完整收集
    # mode = input("选择模式 (1=完整收集, 2=单只测试): ").strip()
    #
    # if mode == "2":
    #     demo_single_stock()
    # else:
    #     # 运行完整数据收集
    #     collector = AShareFullDataCollector()
    #     collector.run_daily_collection()
    #
    #     # 等待用户查看结果
    #     input("\n按回车键退出...")
    demo_stock_board_concept_cons_em()

