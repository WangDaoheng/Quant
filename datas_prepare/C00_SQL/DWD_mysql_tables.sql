--1.1
------------------  dwd_ashare_stock_base_info   股票基本信息大宽表
CREATE TABLE quant.dwd_ashare_stock_base_info (
       ymd                DATE                     COMMENT '日期'
      ,stock_code         varchar(50)              COMMENT '代码'
      ,stock_name         varchar(50)              COMMENT '名称'
      ,close              double                   COMMENT '最新收盘价'
      ,change_pct         float                    COMMENT '当日涨跌幅(%)'
      ,volume             BIGINT                   COMMENT '成交量'
      ,trading_amount     double                   COMMENT '成交额'
      ,market_value       double                   COMMENT '流通市值(亿)'
      ,total_value        double                   COMMENT '总市值(亿)'
      ,total_asset        double                   COMMENT '总资产(亿)'
      ,net_asset          double                   COMMENT '净资产(亿)'
      ,total_capital      double                   COMMENT '总股本(亿)'
      ,float_capital      double                   COMMENT '流通股(亿)'
      ,shareholder_num    bigint                   COMMENT '股东人数'
      ,pb                 varchar(50)              COMMENT '市净率'
      ,pe                 varchar(50)              COMMENT '市盈(动)'
      ,market             VARCHAR(50)              COMMENT '市场特征主板创业板等'
      ,plate_names        VARCHAR(500)             COMMENT '板块名称'
      ,UNIQUE KEY unique_ymd_stock_code (ymd, stock_code)
) COMMENT='股票基本信息大宽表';


--1.2
------------------  dwd_stock_zt_list   涨停股票清单
CREATE TABLE quant.dwd_stock_zt_list (
       ymd                DATE          NOT NULL   COMMENT '交易日期'
      ,stock_code         VARCHAR(50)   NOT NULL   COMMENT '股票代码'
      ,stock_name         VARCHAR(50)              COMMENT '股票名称'
      ,last_close         FLOAT                    COMMENT '昨日收盘价'
      ,close              FLOAT                    COMMENT '收盘价'
      ,rate               FLOAT                    COMMENT '涨幅'
      ,market_value       double                   COMMENT '流通市值(亿)'
      ,total_value        double                   COMMENT '总市值(亿)'
      ,total_asset        double                   COMMENT '总资产(亿)'
      ,net_asset          double                   COMMENT '净资产(亿)'
      ,total_capital      double                   COMMENT '总股本(亿)'
      ,float_capital      double                   COMMENT '流通股(亿)'
      ,shareholder_num    bigint                   COMMENT '股东人数'
      ,pb                 varchar(50)              COMMENT '市净率'
      ,pe                 varchar(50)              COMMENT '市盈(动)'
      ,market             VARCHAR(50)              COMMENT '市场特征主板创业板等'
      ,plate_names        VARCHAR(500)             COMMENT '板块名称'
      ,UNIQUE KEY unique_ymd_stock_code (ymd, stock_code)
) COMMENT='涨停股票清单';


--1.3
------------------  dwd_stock_dt_list   跌停股票清单
CREATE TABLE quant.dwd_stock_dt_list (
       ymd                DATE          NOT NULL   COMMENT '交易日期'
      ,stock_code         VARCHAR(50)   NOT NULL   COMMENT '股票代码'
      ,stock_name         VARCHAR(50)              COMMENT '股票名称'
      ,last_close         FLOAT                    COMMENT '昨日收盘价'
      ,close              FLOAT                    COMMENT '收盘价'
      ,rate               FLOAT                    COMMENT '涨幅'
      ,market_value       double                   COMMENT '流通市值(亿)'
      ,total_value        double                   COMMENT '总市值(亿)'
      ,total_asset        double                   COMMENT '总资产(亿)'
      ,net_asset          double                   COMMENT '净资产(亿)'
      ,total_capital      double                   COMMENT '总股本(亿)'
      ,float_capital      double                   COMMENT '流通股(亿)'
      ,shareholder_num    bigint                   COMMENT '股东人数'
      ,pct_of_total_sh    DOUBLE(10, 4)            COMMENT '股东数较上期环比波动百分比'
      ,pb                 varchar(50)              COMMENT '市净率'
      ,pe                 varchar(50)              COMMENT '市盈(动)'
      ,market             VARCHAR(50)              COMMENT '市场特征主板创业板等'
      ,plate_names        VARCHAR(500)             COMMENT '板块名称'
      ,UNIQUE KEY unique_ymd_stock_code (ymd, stock_code)
) COMMENT='跌停股票清单';



--1.4
------------------  dwd_shareholder_num_latest   个股的股东数(全量最新数据表)
CREATE TABLE quant.dwd_shareholder_num_latest (
       ymd                    DATE                    COMMENT '交易日期'
      ,stock_code             varchar(100)            COMMENT '股票代码'
      ,stock_name             varchar(50)             COMMENT '股票名称'
      ,total_sh               DOUBLE                  COMMENT '总股东数'
      ,avg_share              DOUBLE(10, 4)           COMMENT '每个股东平均持股数'
      ,pct_of_total_sh        DOUBLE(10, 4)           COMMENT '股东数较上期环比波动百分比'
      ,pct_of_avg_sh          DOUBLE(10, 4)           COMMENT '每个股东平均持股数较上期环比波动百分比'
      ,UNIQUE KEY unique_ymd_stock_code (ymd, stock_code)
) COMMENT='个股的股东数(全量最新数据表)';




--4.2        多渠道板块数据 -- 多渠道汇总
------------------  dwd_stock_a_total_plate
CREATE TABLE quant.dwd_stock_a_total_plate (
       ymd                DATE          NOT NULL   COMMENT '日期'
      ,board_code         VARCHAR(50)   NOT NULL   COMMENT '板块代码'
      ,board_name         VARCHAR(50)   NOT NULL   COMMENT '板块名称'
      ,stock_code         VARCHAR(50)              COMMENT '标的代码'
      ,stock_name         VARCHAR(50)              COMMENT '标的名称'
      ,source_table       VARCHAR(50)              COMMENT '来源表'
      ,remark             VARCHAR(50)              COMMENT '备注'
      ,UNIQUE KEY unique_ymd_plate_code (ymd, plate_name, stock_code)
) COMMENT='多渠道板块数据 -- 多渠道汇总';


