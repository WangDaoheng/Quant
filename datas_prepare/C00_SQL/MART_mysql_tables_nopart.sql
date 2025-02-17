
--1.1
------------------  dmart_stock_zt_details   股票涨停明细

create table quant.dmart_stock_zt_details (
     ymd                DATE                     --日期
    ,stock_code         varchar(50)              --代码
    ,stock_name         varchar(50)              --名称
    ,concept_plate      VARCHAR(500)             --概念板块
    ,index_plate        VARCHAR(500)             --指数板块
    ,industry_plate     VARCHAR(500)             --行业板块
    ,style_plate        VARCHAR(500)             --风格板块
    ,out_plate          VARCHAR(500)             --外部数据板块
    ,UNIQUE KEY unique_ymd_stock_code (ymd, stock_code)
) ;














