
--1.1
------------------  dwd_stock_ZT_list   涨停股票清单







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











