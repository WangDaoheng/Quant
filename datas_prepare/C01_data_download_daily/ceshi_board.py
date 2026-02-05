import pandas as pd
import requests
import time
import random
from typing import List, Dict
from bs4 import BeautifulSoup
from datetime import datetime
import logging

# 导入你的配置
import CommonProperties.Base_Properties as base_properties
import CommonProperties.Mysql_Utils as mysql_utils
from CommonProperties.DateUtility import DateUtility


class THSConceptCrawler:
    """同花顺概念板块爬虫（生产级安全版本）"""

    def __init__(self):
        # MySQL配置
        self.mysql_config = {
            'user': base_properties.origin_mysql_user,
            'password': base_properties.origin_mysql_password,
            'host': base_properties.origin_mysql_host,
            'database': base_properties.origin_mysql_database
        }

        # 安全配置
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
            'Accept-Language': 'zh-CN,zh;q=0.9,en;q=0.8',
            'DNT': '1',
            'Connection': 'keep-alive',
            'Referer': 'https://q.10jqka.com.cn/',
        })

        # 延迟配置（安全第一）
        self.page_delay = 5.0  # 页间延迟
        self.concept_delay = 8.0  # 概念间延迟
        self.batch_delay = 25.0  # 批次间延迟

        # 简洁日志
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(message)s',
            datefmt='%H:%M:%S'
        )
        self.logger = logging.getLogger(__name__)

    def run(self, batch_size: int = 15, test_mode: bool = False):
        """主执行函数"""
        print("=" * 70)
        print("同花顺概念板块股票爬虫 - 安全模式")
        print(f"开始时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"延迟配置: 页间{self.page_delay}s | 概念间{self.concept_delay}s | 批次间{self.batch_delay}s")
        print("=" * 70)

        # 1. 获取概念板块
        print("获取概念板块数据...")
        concepts_df = self._get_concepts()

        if concepts_df.empty:
            print("❌ 未获取到概念板块数据")
            return

        if test_mode:
            concepts_df = concepts_df.head(5)  # 测试模式只取5个

        total_concepts = len(concepts_df)
        print(f"📋 共 {total_concepts} 个概念板块")

        # 2. 分批处理
        all_success = 0
        total_records = 0

        for batch_idx, batch_start in enumerate(range(0, total_concepts, batch_size), 1):
            batch_end = min(batch_start + batch_size, total_concepts)
            batch = concepts_df.iloc[batch_start:batch_end]

            print(f"\n📦 批次 {batch_idx} - 概念 {batch_start + 1} 到 {batch_end}")

            batch_data = []
            batch_success = 0

            # 处理批次内的每个概念
            for idx, (_, row) in enumerate(batch.iterrows(), 1):
                code = row['board_code']
                name = row['board_name']

                # 概念间延迟（带随机抖动）
                if idx > 1:
                    delay = self.concept_delay * random.uniform(0.9, 1.1)
                    time.sleep(delay)

                try:
                    stocks = self._crawl_single_concept(code, name)

                    if stocks:
                        batch_data.extend(stocks)
                        batch_success += 1
                        all_success += 1
                        total_records += len(stocks)
                        print(f"  ✓ {name[:18]:18s} ({code}): {len(stocks):3d} 只")
                    else:
                        print(f"  - {name[:18]:18s} ({code}): 无数据")

                except Exception as e:
                    print(f"  ✗ {name[:18]:18s} ({code}): 错误")
                    time.sleep(10)  # 错误后额外等待

            # 3. 写入当前批次数据
            if batch_data:
                self._save_batch_to_mysql(batch_data)
                print(f"  💾 批次写入: {len(batch_data)} 条记录")

            # 4. 批次统计
            print(f"  📊 批次完成: {batch_success}/{len(batch)} 个概念")
            print(f"  📈 累计进度: {all_success}/{total_concepts} 个概念 | {total_records:,} 条记录")

            # 5. 批次间延迟（重要！）
            if batch_end < total_concepts:
                print(f"  ⏳ 批次间隔 {self.batch_delay} 秒...")
                time.sleep(self.batch_delay)

        # 最终统计
        print("\n" + "=" * 70)
        print("🎉 爬取任务完成!")
        print(f"✅ 成功概念: {all_success}/{total_concepts}")
        print(f"📊 总记录数: {total_records:,}")
        print(f"⏰ 结束时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

    def _get_concepts(self):
        """从MySQL获取概念板块"""
        try:
            return mysql_utils.data_from_mysql_to_dataframe_latest(
                user=self.mysql_config['user'],
                password=self.mysql_config['password'],
                host=self.mysql_config['host'],
                database=self.mysql_config['database'],
                table_name="ods_akshare_board_concept_name_ths",
                cols=['board_code', 'board_name']
            )
        except Exception as e:
            print(f"获取概念板块失败: {e}")
            return pd.DataFrame()

    def _crawl_single_concept(self, code: str, name: str) -> List[Dict]:
        """爬取单个概念的所有股票"""
        all_stocks = []

        # 获取总页数
        try:
            total_pages = self._get_total_pages(code)
        except:
            total_pages = 1

        if total_pages == 0:
            return []

        # 逐页爬取
        for page in range(1, total_pages + 1):
            # 页间延迟
            if page > 1:
                delay = self.page_delay * random.uniform(0.8, 1.2)
                time.sleep(delay)

            page_stocks = self._crawl_single_page(code, page)

            if not page_stocks:
                break

            # 格式化数据
            today = datetime.now().strftime('%Y-%m-%d')
            for stock in page_stocks:
                all_stocks.append({
                    'ymd': today,
                    'board_code': code,
                    'board_name': name,
                    'stock_code': stock.get('代码', ''),
                    'stock_name': stock.get('名称', '')
                })

        return all_stocks

    def _get_total_pages(self, code: str) -> int:
        """获取总页数"""
        url = f"https://q.10jqka.com.cn/gn/detail/code/{code}/"

        for attempt in range(2):  # 重试一次
            try:
                response = self.session.get(url, timeout=15)
                response.encoding = 'gbk'

                # 检查限制
                if any(keyword in response.text for keyword in ["访问限制", "请稍后再试", "频率过快"]):
                    print("    ⚠️  检测到限制，等待15秒...")
                    time.sleep(15)
                    continue

                soup = BeautifulSoup(response.text, 'html.parser')

                # 查找分页信息
                page_info = soup.find('span', class_='page_info')
                if page_info:
                    text = page_info.get_text(strip=True)
                    if '/' in text:
                        return int(text.split('/')[1])

                return 1

            except Exception as e:
                if attempt == 0:
                    time.sleep(8)

        return 1

    def _crawl_single_page(self, code: str, page: int) -> List[Dict]:
        """爬取单页"""
        if page == 1:
            url = f"https://q.10jqka.com.cn/gn/detail/code/{code}/"
        else:
            url = f"https://q.10jqka.com.cn/gn/detail/code/{code}/page/{page}/"

        try:
            response = self.session.get(url, timeout=15)
            response.encoding = 'gbk'

            if "暂无成份股数据" in response.text:
                return []

            soup = BeautifulSoup(response.text, 'html.parser')
            table = soup.find('table', class_='m-table m-pager-table')

            if not table:
                return []

            stocks = []
            tbody = table.find('tbody')
            if tbody:
                for row in tbody.find_all('tr'):
                    cols = row.find_all('td')
                    if len(cols) >= 3:
                        stocks.append({
                            '代码': cols[1].get_text(strip=True),
                            '名称': cols[2].get_text(strip=True)
                        })

            return stocks

        except:
            return []

    def _save_batch_to_mysql(self, data: List[Dict]):
        """保存批次数据到MySQL"""
        if not data:
            return

        try:
            df = pd.DataFrame(data)
            df = df.drop_duplicates(['ymd', 'board_code', 'stock_code'])

            mysql_utils.data_from_dataframe_to_mysql(
                user=self.mysql_config['user'],
                password=self.mysql_config['password'],
                host=self.mysql_config['host'],
                database=self.mysql_config['database'],
                df=df,
                table_name="ods_akshare_stock_board_concept_maps_ths",
                merge_on=['ymd', 'board_code']
            )

        except Exception as e:
            print(f"⚠️  写入MySQL失败: {e}")
            # 尝试保存备份
            try:
                backup_file = f"backup_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
                df.to_csv(backup_file, index=False, encoding='utf-8-sig')
                print(f"    数据已备份到: {backup_file}")
            except:
                print("    备份失败")


def main():
    """主函数 - 完整爬取"""
    crawler = THSConceptCrawler()
    crawler.run(batch_size=15, test_mode=False)


def test():
    """测试函数 - 只爬取前几个概念"""
    crawler = THSConceptCrawler()

    # 测试用更短的延迟
    crawler.page_delay = 3.0
    crawler.concept_delay = 5.0
    crawler.batch_delay = 10.0

    print("🧪 测试模式启动（只爬取前5个概念）")
    crawler.run(batch_size=5, test_mode=True)


def custom():
    """自定义配置"""
    crawler = THSConceptCrawler()

    # 自定义延迟（根据网络情况调整）
    crawler.page_delay = 6.0  # 页间延迟
    crawler.concept_delay = 10.0  # 概念间延迟
    crawler.batch_delay = 30.0  # 批次间延迟

    # 自定义批次大小
    batch_size = 12

    print(f"⚙️  自定义模式: 批次{batch_size}个概念 | 延迟{crawler.concept_delay}s")
    crawler.run(batch_size=batch_size, test_mode=False)


if __name__ == "__main__":
    print("请选择运行模式:")
    print("1. 完整爬取（安全模式，推荐）")
    print("2. 测试模式（只爬5个概念）")
    print("3. 自定义模式")
    print("4. 超安全模式（最保守）")

    choice = input("请输入选择 (1/2/3/4): ").strip()

    if choice == '1':
        main()
    elif choice == '2':
        test()
    elif choice == '3':
        custom()
    elif choice == '4':
        # 超安全模式
        crawler = THSConceptCrawler()
        crawler.page_delay = 8.0
        crawler.concept_delay = 15.0
        crawler.batch_delay = 40.0
        print("🛡️  超安全模式启动（最保守配置）")
        crawler.run(batch_size=10, test_mode=False)
    else:
        print("无效选择，使用默认模式")
        main()