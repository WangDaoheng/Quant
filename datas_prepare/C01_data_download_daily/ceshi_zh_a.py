import requests
import json
import time


def get_all_stocks_by_page():
    """分页获取所有股票数据"""
    base_url = "https://82.push2.eastmoney.com/api/qt/clist/get"
    all_stocks = []
    page = 1
    page_size = 100  # 每次获取100条
    max_pages = 50  # 防止无限循环

    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
        'Referer': 'http://quote.eastmoney.com/',
        'Accept': 'application/json, text/plain, */*',
        'Accept-Language': 'zh-CN,zh;q=0.9,en;q=0.8',
    }

    while page <= max_pages:
        params = {
            "pn": str(page),
            "pz": str(page_size),
            "po": "1",
            "np": "2",
            "ut": "bd1d9ddb04089700cf9c27f6f7426281",
            "fltt": "2",
            "invt": "2",
            "fid": "f3",
            "fs": "m:0 t:6,m:0 t:80,m:1 t:2,m:1 t:23,m:0 t:81 s:2048",
            "fields": "f1,f2,f3,f4,f5,f6,f7,f8,f9,f10,f12,f13,f14,f15,f16,f17,f18,f20,f21,f23,f24,f25,f22,f11,f62,f128,f136,f115,f152",
            "_": str(int(time.time() * 1000)),  # 动态时间戳
        }

        try:
            print(f"正在获取第 {page} 页数据...")
            response = requests.get(base_url, params=params, headers=headers, timeout=30)
            response.raise_for_status()

            data = response.json()

            if data.get("data"):
                stocks = data["data"].get("diff", [])
                total = data["data"].get("total", 0)

                if not stocks:
                    print(f"第 {page} 页没有数据，停止获取")
                    break

                all_stocks.extend(stocks)
                current_count = len(all_stocks)

                print(f"第 {page} 页获取 {len(stocks)} 条，累计 {current_count} 条，总计 {total} 条")

                # 判断是否获取完成
                if current_count >= total or len(stocks) < page_size:
                    print(f"数据获取完成！总共获取 {current_count} 条记录")
                    break

                page += 1

                # 添加延迟，避免请求过快
                time.sleep(3.1)
            else:
                print(f"第 {page} 页返回数据格式异常")
                break

        except requests.exceptions.RequestException as e:
            print(f"第 {page} 页请求失败: {e}")
            break
        except json.JSONDecodeError as e:
            print(f"第 {page} 页JSON解析失败: {e}")
            break

    return all_stocks


if __name__=='__main__':
    # 使用
    stocks = get_all_stocks_by_page()
    print(f"最终获取股票数量: {len(stocks)}")

    # 保存到文件
    with open('stocks_data.json', 'w', encoding='utf-8') as f:
        json.dump(stocks, f, ensure_ascii=False, indent=2)

