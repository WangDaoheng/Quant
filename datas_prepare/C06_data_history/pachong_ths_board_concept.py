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
    """同花顺概念板块爬虫 - 修复分页问题"""

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

        # 延迟配置
        self.page_delay = 5.0  # 页间延迟
        self.concept_delay = 8.0  # 概念间延迟
        self.batch_delay = 25.0  # 批次间延迟

        # 简洁日志
        logging.basicConfig(
            level=logging.INFO,
            format='%(message)s'
        )

    def run(self, batch_size: int = 15, test_mode: bool = False):
        """主执行函数"""
        print("=" * 70)
        print("同花顺概念板块股票爬虫 - 修复分页版")
        print(f"开始时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print("=" * 70)

        # 1. 获取概念板块
        print("获取概念板块数据...")
        concepts_df = self._get_concepts()

        if concepts_df.empty:
            print("❌ 未获取到概念板块数据")
            return

        if test_mode:
            concepts_df = concepts_df.head(3)  # 测试模式

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

            for idx, (_, row) in enumerate(batch.iterrows(), 1):
                code = row['board_code']
                name = row['board_name']

                # 概念间延迟
                if idx > 1:
                    delay = self.concept_delay * random.uniform(0.9, 1.1)
                    time.sleep(delay)

                try:
                    stocks = self._crawl_concept_all_pages(code, name)

                    if stocks:
                        batch_data.extend(stocks)
                        batch_success += 1
                        all_success += 1
                        total_records += len(stocks)
                        print(f"  ✓ {name[:18]:18s} ({code}): {len(stocks):4d} 只")
                    else:
                        print(f"  - {name[:18]:18s} ({code}): 无数据")

                except Exception as e:
                    print(f"  ✗ {name[:18]:18s} ({code}): 错误 - {str(e)[:30]}")
                    time.sleep(10)

            # 3. 写入当前批次数据
            if batch_data:
                self._save_batch_to_mysql(batch_data)
                print(f"  💾 批次写入: {len(batch_data):,} 条记录")

            # 4. 批次统计
            print(f"  📊 批次完成: {batch_success}/{len(batch)} 个概念")
            print(f"  📈 累计进度: {all_success}/{total_concepts} 个概念 | {total_records:,} 条记录")

            # 5. 批次间延迟
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

    def _crawl_concept_all_pages(self, code: str, name: str) -> List[Dict]:
        """爬取单个概念的所有页面股票"""
        all_stocks = []

        # 获取总页数
        total_pages = self._get_total_pages(code)
        if total_pages == 0:
            return []

        print(f"    {name[:15]:15s}: 共 {total_pages} 页")

        # 逐页爬取
        for page in range(1, total_pages + 1):
            # 页间延迟
            if page > 1:
                delay = self.page_delay * random.uniform(0.8, 1.2)
                time.sleep(delay)

            page_stocks = self._crawl_single_page(code, page)

            if not page_stocks:
                print(f"    第 {page} 页无数据，停止爬取")
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

            # 显示进度
            if page % 5 == 0 or page == total_pages:
                print(f"    已爬取 {page}/{total_pages} 页，累计 {len(all_stocks)} 只股票")

        return all_stocks

    def _get_total_pages(self, code: str) -> int:
        """准确获取总页数 - 修复版本"""
        url = f"https://q.10jqka.com.cn/gn/detail/code/{code}/"

        for attempt in range(3):
            try:
                response = self.session.get(url, timeout=20)
                response.encoding = 'gbk'

                # 检查限制
                if any(keyword in response.text for keyword in ["访问限制", "请稍后再试"]):
                    print(f"    ⚠️  检测到访问限制，等待20秒...")
                    time.sleep(20)
                    continue

                soup = BeautifulSoup(response.text, 'html.parser')

                # 方法1：查找分页信息（最可靠）
                page_info = soup.find('span', class_='page_info')
                if page_info:
                    text = page_info.get_text(strip=True)
                    if '/' in text:
                        pages = int(text.split('/')[1])
                        return pages

                # 方法2：查找所有页码链接
                page_links = soup.select('.m-pager a.changePage, .m-pager a[page]')
                max_page = 1
                for link in page_links:
                    try:
                        # 从href或page属性获取页码
                        href = link.get('href', '')
                        page_attr = link.get('page', '')

                        if page_attr and page_attr.isdigit():
                            page_num = int(page_attr)
                        elif '/page/' in href:
                            # 从URL提取页码，如 /page/2/
                            parts = href.split('/page/')
                            if len(parts) > 1:
                                page_part = parts[1].split('/')[0]
                                if page_part.isdigit():
                                    page_num = int(page_part)

                        if page_num > max_page:
                            max_page = page_num
                    except:
                        continue

                return max_page

            except Exception as e:
                if attempt < 2:
                    print(f"    获取页数失败第{attempt + 1}次，等待10秒...")
                    time.sleep(10)
                else:
                    print(f"    无法获取页数，使用默认1页")
                    return 1

        return 1

    def _crawl_single_page(self, code: str, page: int) -> List[Dict]:
        """爬取单页股票数据"""
        if page == 1:
            url = f"https://q.10jqka.com.cn/gn/detail/code/{code}/"
        else:
            url = f"https://q.10jqka.com.cn/gn/detail/code/{code}/page/{page}/"

        for attempt in range(2):
            try:
                response = self.session.get(url, timeout=15)
                response.encoding = 'gbk'

                if "暂无成份股数据" in response.text:
                    return []

                soup = BeautifulSoup(response.text, 'html.parser')

                # 查找股票表格
                table = soup.find('table', class_='m-table m-pager-table')
                if not table:
                    # 尝试其他可能的表格类名
                    tables = soup.find_all('table')
                    for t in tables:
                        if 'tbody' in str(t) and 'tr' in str(t):
                            table = t
                            break

                if not table:
                    return []

                stocks = []
                tbody = table.find('tbody')
                if tbody:
                    rows = tbody.find_all('tr')
                    for row in rows:
                        cols = row.find_all('td')
                        if len(cols) >= 3:
                            # 提取股票代码和名称
                            code_elem = cols[1].find('a')
                            name_elem = cols[2].find('a')

                            if code_elem and name_elem:
                                stock_code = code_elem.get_text(strip=True)
                                stock_name = name_elem.get_text(strip=True)

                                # 验证股票代码格式
                                if stock_code and len(stock_code) >= 6:
                                    stocks.append({
                                        '代码': stock_code,
                                        '名称': stock_name
                                    })

                return stocks

            except Exception as e:
                if attempt == 0:
                    print(f"    第{page}页获取失败，重试...")
                    time.sleep(8)
                else:
                    return []

        return []

    def _save_batch_to_mysql(self, data: List[Dict]):
        """保存批次数据到MySQL"""
        if not data:
            return

        try:
            df = pd.DataFrame(data)

            # 去重（基于关键字段）
            df = df.drop_duplicates(
                subset=['ymd', 'board_code', 'stock_code'],
                keep='first'
            )

            # 验证数据
            print(f"    数据验证: {len(df)} 条，去重后 {len(df)} 条")

            mysql_utils.data_from_dataframe_to_mysql(
                user=self.mysql_config['user'],
                password=self.mysql_config['password'],
                host=self.mysql_config['host'],
                database=self.mysql_config['database'],
                df=df,
                table_name="ods_akshare_stock_board_concept_maps_ths",
                merge_on=['ymd', 'board_code']
            )

            print(f"    ✅ MySQL写入成功")

        except Exception as e:
            print(f"    ⚠️  写入MySQL失败: {e}")
            # 保存备份
            try:
                timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
                backup_file = f"backup_{timestamp}.parquet"
                df.to_parquet(backup_file, index=False)
                print(f"    数据已备份到: {backup_file}")
            except:
                print("    备份失败")


def test_single_concept():
    """测试单个概念的分页爬取"""
    crawler = THSConceptCrawler()

    # 测试几个热门概念
    test_concepts = [
        ("300008", "新能源汽车"),  # 应该有几百只
        ("301558", "阿里巴巴概念"),  # 应该有几百只
        ("301459", "华为概念"),  # 应该有几百只
    ]

    for code, name in test_concepts:
        print(f"\n测试概念: {name} ({code})")
        try:
            stocks = crawler._crawl_concept_all_pages(code, name)
            print(f"  实际获取: {len(stocks)} 只股票")

            # 显示前5只
            if stocks:
                print("  示例股票:")
                for i, stock in enumerate(stocks[:5], 1):
                    print(f"    {i}. {stock['stock_code']} {stock['stock_name']}")
        except Exception as e:
            print(f"  错误: {e}")


def main():
    """主函数 - 完整爬取"""
    crawler = THSConceptCrawler()
    crawler.run(batch_size=10, test_mode=False)


def safe_mode():
    """安全模式 - 更保守的配置"""
    crawler = THSConceptCrawler()

    # 更保守的延迟
    crawler.page_delay = 8.0
    crawler.concept_delay = 12.0
    crawler.batch_delay = 30.0

    print("🛡️  安全模式启动（保守配置）")
    crawler.run(batch_size=8, test_mode=False)


if __name__ == "__main__":
    print("请选择运行模式:")
    print("1. 测试单个概念的分页")
    print("2. 完整爬取（正常模式）")
    print("3. 安全模式（更保守）")

    choice = input("请输入选择 (1/2/3): ").strip()

    if choice == '1':
        test_single_concept()
    elif choice == '2':
        main()
    elif choice == '3':
        safe_mode()
    else:
        print("无效选择，使用测试模式")
        test_single_concept()