
--1.1
------------------  dwd_ashare_stock_base_info   股票基本信息大宽表

create table quant.dwd_ashare_stock_base_info (
     ymd              DATE               --日期
    ,stock_code       varchar(50)        --代码
    ,stock_name       varchar(50)        --名称
    ,close            double             --最新收盘价
    ,market_value     double             --流通市值(亿)
    ,total_value      double             --总市值(亿)
    ,total_asset      double             --总资产(亿)
    ,net_asset        double             --净资产(亿)
    ,total_capital    double             --总股本(亿)
    ,float_capital    double             --流通股(亿)
    ,shareholder_num  bigint             --股东人数
    ,pb               varchar(50)        --市净率
    ,pe               varchar(50)        --市盈(动)
    ,market           VARCHAR(50)        --市场特征主板创业板等
    ,plate_names      VARCHAR(500)       --板块名称
    ,UNIQUE KEY unique_ymd_stock_code (ymd, stock_code)
) ;






--1.2
------------------  dwd_stock_ZT_list   涨停股票清单
CREATE TABLE quant.dwd_stock_ZT_list (
     ymd          DATE          NOT NULL   --交易日期
    ,stock_code   VARCHAR(50)   NOT NULL   --股票代码
    ,last_close   FLOAT                    --昨日收盘价
    ,open         FLOAT                    --开盘价
    ,high         FLOAT                    --最高价
    ,low          FLOAT                    --最低价
    ,close        FLOAT                    --收盘价
    ,rate         FLOAT                    --涨幅
    ,UNIQUE KEY unique_ymd_stock_code (ymd, stock_code)
);



--1.3
------------------  dwd_stock_DT_list   跌停股票清单
CREATE TABLE quant.dwd_stock_DT_list (
     ymd          DATE          NOT NULL   --交易日期
    ,stock_code   VARCHAR(50)   NOT NULL   --股票代码
    ,last_close   FLOAT                    --昨日收盘价
    ,open         FLOAT                    --开盘价
    ,high         FLOAT                    --最高价
    ,low          FLOAT                    --最低价
    ,close        FLOAT                    --收盘价
    ,rate         FLOAT                    --涨幅
    ,UNIQUE KEY unique_ymd_stock_code (ymd, stock_code)
);




--4.2        多渠道板块数据 -- 多渠道汇总
------------------  dwd_stock_a_total_plate
CREATE TABLE quant.dwd_stock_a_total_plate (
     ymd          DATE        NOT NULL      --日期
    ,plate_name   VARCHAR(50) NOT NULL      --板块名称
    ,stock_code   VARCHAR(50)               --标的代码
    ,stock_name   VARCHAR(50)               --标的名称
    ,source_table VARCHAR(50)               --来源表
    ,remark       VARCHAR(50)               --备注
    ,UNIQUE KEY unique_ymd_plate_code (ymd, plate_name, stock_code)
) ;











