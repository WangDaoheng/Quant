import requests
import pandas as pd
import re
import json
import time
from typing import List, Dict
from dataclasses import dataclass
from urllib.parse import quote


@dataclass
class StockInfo:
    """股票信息数据类"""
    code: str  # 股票代码
    name: str  # 股票名称
    price: float  # 最新价
    change: str  # 涨跌幅
    market: str  # 市场类型


class THSConceptCrawler:
    """同花顺概念板块爬虫"""

    def __init__(self):
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'zh-CN,zh;q=0.9,en;q=0.8',
            'Accept-Encoding': 'gzip, deflate',
            'DNT': '1',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1',
            'Sec-Fetch-Dest': 'document',
            'Sec-Fetch-Mode': 'navigate',
            'Sec-Fetch-Site': 'none',
            'Sec-Fetch-User': '?1',
            'Cache-Control': 'max-age=0',
        }
        self.session = requests.Session()
        self.session.headers.update(self.headers)

    def get_concept_list(self) -> pd.DataFrame:
        """
        获取所有概念板块列表
        返回包含概念名称和代码的DataFrame
        """
        url = "http://q.10jqka.com.cn/gn/index"

        try:
            response = self.session.get(url, timeout=10)
            response.encoding = 'gb2312'

            # 使用正则表达式提取概念数据
            # 查找类似 <a href="/gn/detail/code/301558/" target="_blank">AI应用</a>
            pattern = r'<a href="/gn/detail/code/(\d+)/"[^>]*>([^<]+)</a>'
            matches = re.findall(pattern, response.text)

            concepts = []
            for code, name in matches:
                concepts.append({
                    '概念代码': code,
                    '概念名称': name.strip()
                })

            return pd.DataFrame(concepts).drop_duplicates()

        except Exception as e:
            print(f"获取概念列表失败: {e}")
            return pd.DataFrame()

    def get_concept_stocks(self, concept_code: str, max_pages: int = 10) -> List[StockInfo]:
        """
        获取指定概念板块的所有股票
        :param concept_code: 概念板块代码，如 '301558'（AI应用）
        :param max_pages: 最大爬取页数（每页通常80只股票）
        :return: 股票信息列表
        """
        base_url = f"http://q.10jqka.com.cn/gn/detail/code/{concept_code}/"
        all_stocks = []

        for page in range(1, max_pages + 1):
            try:
                # 构建每页的URL
                if page == 1:
                    url = base_url
                else:
                    url = f"{base_url}page/{page}/"

                print(f"正在爬取: {url}")
                response = self.session.get(url, timeout=10)
                response.encoding = 'gb2312'

                # 检查是否还有数据
                if "暂无成份股数据" in response.text:
                    if page == 1:
                        print("该概念板块暂无股票数据")
                    break

                # 从HTML中提取股票表格数据
                page_stocks = self._parse_stocks_from_html(response.text)

                if not page_stocks:
                    print(f"第{page}页没有找到股票数据，可能已到末尾")
                    break

                all_stocks.extend(page_stocks)
                print(f"第{page}页获取到 {len(page_stocks)} 只股票")

                # 检查是否还有下一页
                if page == max_pages or not self._has_next_page(response.text):
                    break

                # 礼貌延迟，避免被封IP
                time.sleep(1.5)

            except Exception as e:
                print(f"爬取第{page}页失败: {e}")
                continue

        return all_stocks

    def _parse_stocks_from_html(self, html: str) -> List[StockInfo]:
        """从HTML中解析股票数据"""
        stocks = []

        try:
            # 方法1: 解析HTML表格（更稳定）
            # 查找股票表格
            table_pattern = r'<table[^>]*class="m-table m-pager-table"[^>]*>.*?</table>'
            table_match = re.search(table_pattern, html, re.DOTALL)

            if table_match:
                table_html = table_match.group(0)

                # 提取每一行数据
                row_pattern = r'<tr[^>]*>.*?</tr>'
                rows = re.findall(row_pattern, table_html, re.DOTALL)

                for row in rows[1:]:  # 跳过表头
                    # 提取股票代码
                    code_match = re.search(r'<a[^>]*code=(\d{6})[^>]*>(\d{6})</a>', row)
                    if not code_match:
                        continue

                    stock_code = code_match.group(1)

                    # 提取股票名称
                    name_match = re.search(r'<a[^>]*target="_blank"[^>]*>([^<]+)</a>', row)
                    stock_name = name_match.group(1) if name_match else ""

                    # 提取价格和涨跌幅（简化版，实际需要更复杂的解析）
                    cells = re.findall(r'<td[^>]*>([^<]+)</td>', row)
                    if len(cells) >= 3:
                        stock_price = cells[1] if cells[1] != '--' else '0'
                        stock_change = cells[2] if cells[2] != '--' else '0%'
                    else:
                        stock_price = '0'
                        stock_change = '0%'

                    # 确定市场类型
                    market = 'SZ' if stock_code.startswith(('00', '30')) else 'SH' if stock_code.startswith(
                        ('60', '68')) else 'BJ'

                    try:
                        stock = StockInfo(
                            code=stock_code,
                            name=stock_name,
                            price=float(stock_price),
                            change=stock_change,
                            market=market
                        )
                        stocks.append(stock)
                    except ValueError:
                        continue

            # 方法2: 如果方法1失败，尝试另一种解析方式
            if not stocks:
                stocks = self._parse_stocks_alternative(html)

        except Exception as e:
            print(f"解析HTML失败: {e}")

        return stocks

    def _parse_stocks_alternative(self, html: str) -> List[StockInfo]:
        """备选解析方法"""
        stocks = []

        # 使用更简单的正则匹配
        # 匹配格式: <td><a href="http://stockpage.10jqka.com.cn/000001/" target="_blank">000001</a></td>
        stock_pattern = r'code=(\d{6})[^>]*>(\d{6})</a>.*?target="_blank">([^<]+)</a>'

        matches = re.findall(stock_pattern, html, re.DOTALL)
        for match in matches:
            code, code_check, name = match
            if code == code_check:  # 验证一致性
                market = 'SZ' if code.startswith(('00', '30')) else 'SH' if code.startswith(('60', '68')) else 'BJ'

                stock = StockInfo(
                    code=code,
                    name=name.strip(),
                    price=0.0,
                    change='0%',
                    market=market
                )
                stocks.append(stock)

        return stocks

    def _has_next_page(self, html: str) -> bool:
        """检查是否有下一页"""
        return '下一页' in html and 'disabled' not in html

    def search_concept_by_name(self, concept_name: str) -> str:
        """
        根据概念名称搜索对应的概念代码
        :param concept_name: 概念名称，如'人工智能'
        :return: 概念代码
        """
        concepts_df = self.get_concept_list()
        if concepts_df.empty:
            return ""

        # 精确匹配
        exact_match = concepts_df[concepts_df['概念名称'] == concept_name]
        if not exact_match.empty:
            return exact_match.iloc[0]['概念代码']

        # 模糊匹配
        fuzzy_match = concepts_df[concepts_df['概念名称'].str.contains(concept_name)]
        if not fuzzy_match.empty:
            print(f"找到相关概念: {fuzzy_match.iloc[0]['概念名称']}")
            return fuzzy_match.iloc[0]['概念代码']

        print(f"未找到概念: {concept_name}")
        print("可用的概念有（前10个）:")
        print(concepts_df.head(10)['概念名称'].tolist())
        return ""


# 使用示例
def main():
    crawler = THSConceptCrawler()

    # 1. 获取所有概念列表
    # print("正在获取概念板块列表...")
    # concepts_df = crawler.get_concept_list()
    # if not concepts_df.empty:
    #     print(f"共获取到 {len(concepts_df)} 个概念板块")
    #     print("\n部分概念板块示例:")
    #     print(concepts_df.head(10).to_string(index=False))
    #
    #     # 保存概念列表
    #     concepts_df.to_csv('ths_concepts_list.csv', index=False, encoding='utf-8-sig')
    #     print("\n概念列表已保存到 ths_concepts_list.csv")

    # 2. 获取特定概念板块的股票
    concept_name = "人工智能"  # 可以修改为你想要的概念
    print(f"\n正在搜索概念: {concept_name}")

    concept_code = crawler.search_concept_by_name(concept_name)
    if concept_code:
        print(f"概念代码: {concept_code}")

        # 开始爬取该概念下的股票
        print(f"\n开始爬取 {concept_name} 概念股...")
        stocks = crawler.get_concept_stocks(concept_code, max_pages=3)

        if stocks:
            print(f"\n成功获取 {len(stocks)} 只股票")

            # 转换为DataFrame
            stocks_df = pd.DataFrame([vars(s) for s in stocks])

            # 显示数据
            print("\n股票列表:")
            print(stocks_df.head(20).to_string(index=False))

            # 保存到CSV
            filename = f"{concept_name}_概念股.csv"
            stocks_df.to_csv(filename, index=False, encoding='utf-8-sig')
            print(f"\n数据已保存到 {filename}")

            # 统计信息
            print(f"\n市场分布:")
            market_dist = stocks_df['market'].value_counts()
            print(market_dist.to_string())
        else:
            print("未获取到股票数据")
    else:
        print("请检查概念名称是否正确")


# 简单调用示例（快速测试）
def simple_crawl(concept_name="人工智能"):
    """简化版的爬取函数"""
    crawler = THSConceptCrawler()

    # 获取概念代码
    print(f"搜索概念: {concept_name}")
    concept_code = crawler.search_concept_by_name(concept_name)

    if not concept_code:
        print("概念未找到")
        return

    print(f"开始爬取概念代码: {concept_code}")

    # 爬取第一页数据（快速测试）
    url = f"http://q.10jqka.com.cn/gn/detail/code/{concept_code}/"
    response = requests.get(url, headers=crawler.headers)
    response.encoding = 'gb2312'

    # 简单解析
    stocks = crawler._parse_stocks_from_html(response.text)

    if stocks:
        print(f"\n获取到 {len(stocks)} 只股票:")
        for i, stock in enumerate(stocks[:10], 1):
            print(f"{i:2d}. {stock.code} {stock.name:10s} {stock.market}")
    else:
        print("未解析到股票数据")


if __name__ == "__main__":
    # 完整爬取
    # main()

    # 或快速测试
    simple_crawl("人工智能")
