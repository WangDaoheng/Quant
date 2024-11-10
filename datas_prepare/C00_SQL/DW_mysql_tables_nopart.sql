
--1.1
------------------  dwd_stock_ZT_list   涨停股票清单
CREATE TABLE quant.dwd_stock_ZT_list (
     ymd          DATE          NOT NULL   --交易日期
    ,htsc_code    VARCHAR(50)   NOT NULL   --股票代码
    ,last_close   FLOAT                    --昨日收盘价
    ,open         FLOAT                    --开盘价
    ,high         FLOAT                    --最高价
    ,low          FLOAT                    --最低价
    ,close        FLOAT                    --收盘价
    ,rate         FLOAT                    --涨幅
    ,UNIQUE KEY unique_ymd_stock_code (ymd, htsc_code)
);



--1.2
------------------  dwd_stock_DT_list   跌停股票清单
CREATE TABLE quant.dwd_stock_DT_list (
     ymd          DATE          NOT NULL   --交易日期
    ,htsc_code    VARCHAR(50)   NOT NULL   --股票代码
    ,last_close   FLOAT                    --昨日收盘价
    ,open         FLOAT                    --开盘价
    ,high         FLOAT                    --最高价
    ,low          FLOAT                    --最低价
    ,close        FLOAT                    --收盘价
    ,rate         FLOAT                    --涨幅
    ,UNIQUE KEY unique_ymd_stock_code (ymd, htsc_code)
);
















