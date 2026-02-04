import requests
import pandas as pd
import time


def stock_board_concept_name_em() -> pd.DataFrame:
    """东方财富概念板块数据（分页获取全部）"""
    all_data = []
    page = 1
    page_size = 100

    while True:
        url = "https://79.push2.eastmoney.com/api/qt/clist/get"
        params = {
            "pn": str(page),
            "pz": str(page_size),
            "po": "1",
            "np": "2",
            "ut": "bd1d9ddb04089700cf9c27f6f7426281",
            "fltt": "2",
            "invt": "2",
            "fid": "f3",
            "fs": "m:90 t:3 f:!50",
            "fields": "f2,f3,f4,f8,f12,f14,f15,f16,f17,f18,f20,f21,f24,f25,f22,f33,f11,f62,f128,f124,f107,f104,f105,f136",
            "_": str(int(time.time() * 1000)),
        }

        try:
            r = requests.get(url, params=params, timeout=10)
            data_json = r.json()

            if not data_json.get("data") or not data_json["data"].get("diff"):
                break

            temp_df = pd.DataFrame(data_json["data"]["diff"]).T
            temp_df.reset_index(inplace=True)
            temp_df["index"] = range((page - 1) * page_size + 1,
                                     (page - 1) * page_size + len(temp_df) + 1)
            all_data.append(temp_df)

            total_num = data_json["data"]["total"]
            if page * page_size >= total_num:
                break

            page += 1
            time.sleep(3.1)

        except Exception:
            break

    if not all_data:
        return pd.DataFrame()

    final_df = pd.concat(all_data, ignore_index=True)

    final_df.columns = [
        "排名", "最新价", "涨跌幅", "涨跌额", "换手率", "_",
        "板块代码", "板块名称", "_", "_", "_", "_",
        "总市值", "_", "_", "_", "_", "_", "_",
        "上涨家数", "下跌家数", "_", "_",
        "领涨股票", "_", "_", "领涨股票-涨跌幅"
    ]

    final_df = final_df[
        [
            "排名", "板块名称", "板块代码", "最新价", "涨跌额",
            "涨跌幅", "总市值", "换手率", "上涨家数", "下跌家数",
            "领涨股票", "领涨股票-涨跌幅"
        ]
    ]

    numeric_cols = ["最新价", "涨跌额", "涨跌幅", "总市值", "换手率",
                    "上涨家数", "下跌家数", "领涨股票-涨跌幅"]

    for col in numeric_cols:
        final_df[col] = pd.to_numeric(final_df[col], errors="coerce")

    return final_df.sort_values("排名").reset_index(drop=True)


def main():
    """测试函数"""
    df = stock_board_concept_name_em()

    if df.empty:
        print("获取数据失败")
        return

    print(f"获取到 {len(df)} 条数据")
    print("\n前5条数据:")
    print(df.head())

    print("\n数据保存到: concept_board.csv")
    df.to_csv("concept_board.csv", index=False, encoding='utf-8-sig')


if __name__ == "__main__":
    main()