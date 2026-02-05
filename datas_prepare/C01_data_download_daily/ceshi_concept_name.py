import requests
import pandas as pd
import time
import json
from typing import List, Dict
from bs4 import BeautifulSoup
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


class THSConceptCrawler:
    """同花顺概念板块完整爬虫（带自动翻页）"""

    def __init__(self):
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
            'Accept-Language': 'zh-CN,zh;q=0.9,en;q=0.8',
            'Accept-Encoding': 'gzip, deflate',
            'DNT': '1',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1',
            'Cache-Control': 'max-age=0',
        }
        self.session = requests.Session()
        self.session.headers.update(self.headers)

    def get_total_pages(self, concept_code: str) -> int:
        """获取概念板块的总页数"""
        url = f"https://q.10jqka.com.cn/gn/detail/code/{concept_code}/"

        try:
            response = self.session.get(url, timeout=10)
            response.encoding = 'gbk'

            # 解析总页数
            soup = BeautifulSoup(response.text, 'html.parser')

            # 查找分页信息
            page_info = soup.find('span', class_='page_info')
            if page_info:
                # 格式如 "1/35"
                page_text = page_info.get_text(strip=True)
                if '/' in page_text:
                    total_pages = int(page_text.split('/')[1])
                    return total_pages

            # 如果没有找到，查找所有页码链接
            page_links = soup.select('.m-pager a.changePage')
            if page_links:
                page_numbers = []
                for link in page_links:
                    try:
                        page_num = int(link.get('page', 0))
                        if page_num > 0:
                            page_numbers.append(page_num)
                    except:
                        continue
                if page_numbers:
                    return max(page_numbers)

            # 默认返回1页
            return 1

        except Exception as e:
            logging.error(f"获取总页数失败: {e}")
            return 1

    def get_concept_stocks_by_page(self, concept_code: str, page: int = 1) -> List[Dict]:
        """获取指定概念板块指定页的股票数据"""
        if page == 1:
            url = f"https://q.10jqka.com.cn/gn/detail/code/{concept_code}/"
        else:
            url = f"https://q.10jqka.com.cn/gn/detail/code/{concept_code}/page/{page}/"

        stocks = []

        try:
            logging.info(f"正在爬取第 {page} 页: {url}")
            response = self.session.get(url, timeout=15)
            response.encoding = 'gbk'

            if "暂无成份股数据" in response.text:
                logging.warning(f"第 {page} 页暂无成份股数据")
                return []

            # 使用BeautifulSoup解析HTML
            soup = BeautifulSoup(response.text, 'html.parser')

            # 找到股票表格
            table = soup.find('table', class_='m-table m-pager-table')
            if not table:
                logging.warning(f"第 {page} 页未找到股票表格")
                return []

            # 提取表头
            headers = []
            thead = table.find('thead')
            if thead:
                ths = thead.find_all('th')
                headers = [th.get_text(strip=True) for th in ths]

            # 提取数据行
            tbody = table.find('tbody')
            if not tbody:
                return []

            rows = tbody.find_all('tr')
            for row in rows:
                cols = row.find_all('td')
                if len(cols) < 3:  # 至少要有代码和名称
                    continue

                stock_data = {}

                # 获取股票代码和名称
                code_td = cols[1]
                code_link = code_td.find('a')
                if code_link:
                    stock_data['代码'] = code_link.get_text(strip=True)

                name_td = cols[2]
                name_link = name_td.find('a')
                if name_link:
                    stock_data['名称'] = name_link.get_text(strip=True)

                # 获取其他字段
                field_mapping = {
                    0: '序号',
                    3: '现价',
                    4: '涨跌幅',
                    5: '涨跌',
                    6: '涨速',
                    7: '换手',
                    8: '量比',
                    9: '振幅',
                    10: '成交额',
                    11: '流通股',
                    12: '流通市值',
                    13: '市盈率'
                }

                for idx, col in enumerate(cols):
                    if idx in field_mapping:
                        value = col.get_text(strip=True)
                        # 处理价格类数据
                        if idx in [3, 4, 5, 6, 7, 8, 9, 13]:  # 数值类字段
                            value = value.replace('%', '')
                        stock_data[field_mapping[idx]] = value

                if stock_data:  # 确保有数据
                    stocks.append(stock_data)

            logging.info(f"第 {page} 页获取到 {len(stocks)} 只股票")

        except Exception as e:
            logging.error(f"爬取第 {page} 页失败: {e}")

        return stocks

    def get_all_concept_stocks(self, concept_code: str, max_pages: int = None) -> pd.DataFrame:
        """获取概念板块的所有股票（自动翻页）"""
        all_stocks = []

        try:
            # 1. 获取总页数
            total_pages = self.get_total_pages(concept_code)
            if max_pages and max_pages < total_pages:
                total_pages = max_pages

            logging.info(f"概念板块 {concept_code} 共有 {total_pages} 页")

            # 2. 逐页爬取
            for page in range(1, total_pages + 1):
                page_stocks = self.get_concept_stocks_by_page(concept_code, page)

                if not page_stocks:
                    logging.warning(f"第 {page} 页没有数据，停止爬取")
                    break

                all_stocks.extend(page_stocks)

                # 显示进度
                logging.info(f"进度: {page}/{total_pages}页，累计获取 {len(all_stocks)} 只股票")

                # 添加延迟避免被封
                if page < total_pages:
                    time.sleep(5)  # 页间延迟

            # 3. 转换为DataFrame
            if all_stocks:
                df = pd.DataFrame(all_stocks)

                # 数据类型转换
                numeric_columns = ['序号', '现价', '涨跌幅', '涨跌', '涨速', '换手', '量比', '振幅', '市盈率']
                for col in numeric_columns:
                    if col in df.columns:
                        df[col] = pd.to_numeric(df[col], errors='coerce')

                # 清理数据
                df = df.drop_duplicates(subset=['代码'], keep='first')
                df = df.reset_index(drop=True)

                return df
            else:
                return pd.DataFrame()

        except Exception as e:
            logging.error(f"获取所有股票失败: {e}")
            return pd.DataFrame()

    def search_concept_code(self, concept_name: str) -> str:
        """根据概念名称搜索概念代码"""
        url = "https://q.10jqka.com.cn/gn/index"

        try:
            response = self.session.get(url, timeout=10)
            response.encoding = 'gbk'

            soup = BeautifulSoup(response.text, 'html.parser')

            # 查找所有概念链接
            concept_links = soup.find_all('a', href=True)

            for link in concept_links:
                href = link['href']
                text = link.get_text(strip=True)

                # 匹配概念详情链接
                if '/gn/detail/code/' in href and concept_name in text:
                    # 从URL中提取代码
                    code = href.split('/gn/detail/code/')[1].rstrip('/')
                    logging.info(f"找到概念: {text}，代码: {code}")
                    return code

            logging.warning(f"未找到概念: {concept_name}")

        except Exception as e:
            logging.error(f"搜索概念代码失败: {e}")

        return ""


# 使用示例
def main():
    crawler = THSConceptCrawler()

    # 方法1: 直接使用概念代码
    concept_code = "301558"  # 阿里巴巴概念

    print(f"开始爬取概念板块: {concept_code}")

    # 获取所有股票
    all_stocks_df = crawler.get_all_concept_stocks(concept_code)

    if not all_stocks_df.empty:
        print(f"\n成功获取 {len(all_stocks_df)} 只股票")

        # 显示前20条
        pd.set_option('display.max_columns', None)
        pd.set_option('display.width', None)
        print("\n股票列表（前20只）:")
        print(all_stocks_df.head(20).to_string(index=False))

        # 保存到CSV
        filename = f"阿里巴巴概念_股票列表_{len(all_stocks_df)}只.csv"
        all_stocks_df.to_csv(filename, index=False, encoding='utf-8-sig')
        print(f"\n数据已保存到: {filename}")

        # 统计信息
        print("\n=== 统计信息 ===")
        print(f"股票总数: {len(all_stocks_df)}")
        print(f"平均涨跌幅: {all_stocks_df['涨跌幅'].mean():.2f}%")
        print(f"最高涨跌幅: {all_stocks_df['涨跌幅'].max():.2f}%")
        print(f"最低涨跌幅: {all_stocks_df['涨跌幅'].min():.2f}%")

        # 按涨跌幅排序
        print("\n涨幅前十:")
        top_gainers = all_stocks_df.nlargest(10, '涨跌幅')[['代码', '名称', '涨跌幅']]
        print(top_gainers.to_string(index=False))

    else:
        print("未能获取股票数据")


def test_single_page():
    """测试单页爬取"""
    crawler = THSConceptCrawler()

    concept_code = "301558"
    page = 1

    print(f"测试爬取第 {page} 页")
    stocks = crawler.get_concept_stocks_by_page(concept_code, page)

    if stocks:
        df = pd.DataFrame(stocks)
        print(f"第{page}页获取到 {len(df)} 只股票")
        print(df[['代码', '名称', '现价', '涨跌幅']].head(10))


def get_concept_by_name(concept_name):
    """根据概念名称爬取"""
    crawler = THSConceptCrawler()

    print(f"搜索概念: {concept_name}")
    concept_code = crawler.search_concept_code(concept_name)

    if concept_code:
        print(f"找到概念代码: {concept_code}")

        # 获取前3页数据（快速测试）
        df = crawler.get_all_concept_stocks(concept_code, max_pages=3)

        if not df.empty:
            print(f"\n获取到 {len(df)} 只股票")
            print(df[['代码', '名称', '涨跌幅']].head(15))

            # 保存
            safe_name = concept_name.replace('/', '_').replace('\\', '_')
            filename = f"{safe_name}_概念股.csv"
            df.to_csv(filename, index=False, encoding='utf-8-sig')
            print(f"\n已保存到: {filename}")
    else:
        print("未找到该概念")


if __name__ == "__main__":
    # 选择你要使用的方式

    # 方式1: 完整爬取指定概念代码
    # main()

    # 方式2: 测试单页
    # test_single_page()

    # 方式3: 根据名称搜索并爬取
    get_concept_by_name("人工智能")

    # 方式4: 批量获取多个概念
    # concepts = ["301558", "301459", "302035"]  # 阿里巴巴、华为、人工智能
    # for code in concepts:
    #     crawler = THSConceptCrawler()
    #     df = crawler.get_all_concept_stocks(code, max_pages=2)
    #     if not df.empty:
    #         print(f"概念 {code}: {len(df)} 只股票")