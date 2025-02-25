# -*- coding: utf-8 -*-

import os
from datetime import datetime

import pandas as pd
from sqlalchemy import create_engine, text
import time
import platform
import logging
import plotly.express as px
from wordcloud import WordCloud
import matplotlib.pyplot as plt
from pyecharts import options as opts
from pyecharts.charts import WordCloud, Timeline, Page, Grid
from pyecharts.globals import SymbolType

import dash
from dash import dcc, html
from dash.dependencies import Input, Output

# import dataprepare_properties
# import dataprepare_utils
from CommonProperties import Base_Properties
import CommonProperties.Base_utils as base_utils
from CommonProperties.DateUtility import DateUtility
from CommonProperties.Base_utils import timing_decorator

import CommonProperties.Mysql_Utils as mysql_utils

from CommonProperties import set_config

# ************************************************************************
# 本代码的作用是下午收盘后针对 insight 行情源数据的本地保存部分开展merge
# 需要下载的数据:
# 1.上市股票代码
# 2.筹码分布数据   get_chouma_datas()


# ************************************************************************
#  调用日志配置
set_config.setup_logging_config()

######################  mysql 配置信息  本地和远端服务器  ####################
local_user = Base_Properties.local_mysql_user
local_password = Base_Properties.local_mysql_password
local_database = Base_Properties.local_mysql_database
local_host = Base_Properties.local_mysql_host

origin_user = Base_Properties.origin_mysql_user
origin_password = Base_Properties.origin_mysql_password
origin_database = Base_Properties.origin_mysql_database
origin_host = Base_Properties.origin_mysql_host


class ResShow:

    def __init__(self):

        pass


    # @timing_decorator
    def show_zt_details(self):
        """
        涨停股票曲线展示
        Returns:
        """

        #  1.获取日期
        # ymd = DateUtility.today()
        time_start_date = DateUtility.next_day(-45)  # yyyymmdd 格式日期
        time_end_date = DateUtility.next_day(0)      # yyyymmdd 格式日期

        zt_details_df = mysql_utils.data_from_mysql_to_dataframe(
            user=origin_user,
            password=origin_password,
            host=origin_host,
            database='quant',
            table_name='dmart_stock_zt_details',
            start_date=time_start_date,  # 根据需要调整日期范围
            end_date=time_end_date,
            cols=['ymd', 'stock_code', 'stock_name', 'concept_plate', 'index_plate', 'industry_plate', 'style_plate', 'out_plate'])

        # 将空值替换为 'Unknown'
        zt_details_df.fillna('Unknown', inplace=True)

        #####################   对板块字段中的逗号分隔的板块名展开处理    ##################
        # 定义需要处理的板块字段
        plate_columns = ['concept_plate', 'index_plate', 'industry_plate', 'style_plate', 'out_plate']

        # 将逗号分隔的板块名拆分为列表
        for col in plate_columns:
            zt_details_df[col] = zt_details_df[col].str.split(',')

        # 将列表展开为多行
        zt_details_df = zt_details_df.explode(plate_columns[0])  # 先展开第一个板块字段
        for col in plate_columns[1:]:
            zt_details_df = zt_details_df.explode(col)

        # 去除多余的空格
        for col in plate_columns:
            zt_details_df[col] = zt_details_df[col].str.strip()

        #####################   关键指标计算    ##################
        # 统计每日涨停股票数量
        daily_zt_count = zt_details_df.drop_duplicates(subset=['ymd', 'stock_code']).groupby('ymd').size().reset_index(name='zt_count')
        # 统计每日涨停股票的板块分布
        daily_plate_count = (
            zt_details_df.drop_duplicates(subset=['ymd', 'stock_code'])  # 对 stock_code 去重
            .groupby(['ymd'] + plate_columns)  # 按日期和板块字段分组
            .size()  # 统计每组的数量
            .reset_index(name='count')  # 重置索引并命名统计列为 count
        )

        #####################   指标展示    ##################

        ############### 1.绘制曲线图——每日股票涨停数量
        fig_line = px.line(daily_zt_count, x='ymd', y='zt_count', title='每日涨停股票数量')
        fig_line.show()

        ############### 2.绘制热力图——每日股票涨停板块
        # 选择一个板块类型（例如概念板块）
        plate_type = 'concept_plate'
        daily_plate_count_agg = daily_plate_count.groupby(['ymd', plate_type], as_index=False)['count'].sum()

        duplicates = daily_plate_count_agg.duplicated(subset=['ymd', plate_type], keep=False)
        if duplicates.any():
            print("存在重复的 (ymd, plate_type) 组合：")
            print(daily_plate_count_agg[duplicates])
        else:
            print("数据已去重，可以继续 pivot 操作。")

        plate_data = daily_plate_count_agg.pivot(index='ymd', columns=plate_type, values='count').fillna(0)
        # 绘制热力图
        fig_heatmap = px.imshow(plate_data, labels=dict(x=plate_type, y='日期', color='涨停数量'),
                                title=f'{plate_type} 板块涨停股票热力图')
        fig_heatmap.show()

        ############### 3.绘制词云——每日涨停股票板块分布

        # 遍历每个板块类型
        for plate_type in plate_columns:
            # 创建一个时间轴
            timeline = Timeline()

            # 遍历所有日期
            for target_date in zt_details_df['ymd'].unique():
                # 获取当前板块和日期的数据
                plate_data = zt_details_df[(zt_details_df['ymd'] == target_date) &
                                           (zt_details_df[plate_type].notnull())][plate_type].value_counts()
                # 将数据转换为 pyecharts 需要的格式
                data = [(word, freq) for word, freq in plate_data.items()][:50]  # 限制词汇数量

                # 创建词云图
                wordcloud = (
                    WordCloud()
                    .add("", data, word_size_range=[20, 100], shape=SymbolType.DIAMOND)
                    .set_global_opts(
                        title_opts=opts.TitleOpts(title=f"{target_date} - {plate_type} 板块词云"),
                        tooltip_opts=opts.TooltipOpts(is_show=True)
                    )
                )

                # 将词云图添加到时间轴
                timeline.add(wordcloud, time_point=target_date)

            # 为每个板块生成一个独立的 HTML 文件
            file_name = f"wordcloud_timeline_{plate_type}.html"
            timeline.render(file_name)

        print("每个板块的词云时间轴已生成并保存为独立的 HTML 文件。")

        ############### 4.绘制Dash——每日涨停股票板块分布
        # 创建 Dash 应用
        app = dash.Dash(__name__)

        # 布局
        app.layout = html.Div([
            dcc.DatePickerRange(
                id='date-picker',
                start_date=daily_zt_count['ymd'].min(),
                end_date=daily_zt_count['ymd'].max(),
                display_format='YYYY-MM-DD'
            ),
            dcc.Graph(id='line-chart'),
            dcc.Graph(id='heatmap-chart')
        ])

        # 回调函数
        @app.callback(
            [Output('line-chart', 'figure'), Output('heatmap-chart', 'figure')],
            [Input('date-picker', 'start_date'), Input('date-picker', 'end_date')]
        )
        def update_charts(start_date, end_date):
            # 过滤数据
            filtered_zt_count = daily_zt_count[
                (daily_zt_count['ymd'] >= start_date) & (daily_zt_count['ymd'] <= end_date)]
            filtered_plate_count = daily_plate_count[
                (daily_plate_count['ymd'] >= start_date) & (daily_plate_count['ymd'] <= end_date)]

            # 绘制曲线图
            fig_line = px.line(filtered_zt_count, x='ymd', y='zt_count', title='每日涨停股票数量')

            # 绘制热力图
            plate_type = 'concept_plate'
            plate_data = filtered_plate_count.pivot(index='ymd', columns=plate_type, values='count').fillna(0)
            fig_heatmap = px.imshow(plate_data, labels=dict(x=plate_type, y='日期', color='涨停数量'),
                                    title=f'{plate_type} 板块涨停股票热力图')

            return fig_line, fig_heatmap

        ## 调用执行dash
        app.run_server(debug=True)


    def setup(self):

        # 涨停股票的明细
        self.show_zt_details()


if __name__ == '__main__':
    res_show_data = ResShow()
    res_show_data.setup()





