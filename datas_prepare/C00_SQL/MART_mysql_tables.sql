--1.1
------------------  dmart_stock_zt_details   股票涨停明细
CREATE TABLE quant.dmart_stock_zt_details (
       ymd            DATE                     COMMENT '日期'
      ,stock_code     varchar(50)              COMMENT '代码'
      ,stock_name     varchar(50)              COMMENT '名称'
      ,concept_plate  VARCHAR(500)             COMMENT '概念板块'
      ,index_plate    VARCHAR(500)             COMMENT '指数板块'
      ,industry_plate VARCHAR(500)             COMMENT '行业板块'
      ,style_plate    VARCHAR(500)             COMMENT '风格板块'
      ,out_plate      VARCHAR(500)             COMMENT '外部数据板块'
      ,UNIQUE KEY unique_ymd_stock_code (ymd, stock_code)
) COMMENT='股票涨停明细';


------------------  dmart_stock_zt_details_expanded   股票涨停明细拆分
CREATE TABLE quant.dmart_stock_zt_details_expanded (
       ymd            DATE                     COMMENT '日期'
      ,stock_code     VARCHAR(20)              COMMENT '股票代码'
      ,stock_name     VARCHAR(50)              COMMENT '股票名称'
      ,concept_plate  VARCHAR(500)             COMMENT '概念板块'
      ,index_plate    VARCHAR(500)             COMMENT '指数板块'
      ,industry_plate VARCHAR(500)             COMMENT '行业板块'
      ,style_plate    VARCHAR(500)             COMMENT '风格板块'
      ,out_plate      VARCHAR(500)             COMMENT '外部数据板块'
) COMMENT='股票涨停明细拆分';

