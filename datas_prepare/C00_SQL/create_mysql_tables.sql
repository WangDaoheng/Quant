
--1.1
------------------  ods_stock_code_daily_insight   当日已上市股票码表
CREATE TABLE quant.ods_stock_code_daily_insight (
    ymd DATE NOT NULL,
    htsc_code VARCHAR(50) NOT NULL,
    name VARCHAR(50),
    exchange VARCHAR(50),
    UNIQUE KEY unique_ymd_stock_code (ymd, htsc_code)
);


--1.2
------------------  stock_kline_daily_insight   当日已上市股票的历史日K
CREATE TABLE quant.ods_stock_kline_daily_insight_now (
    htsc_code VARCHAR(50) NOT NULL,
    ymd DATE NOT NULL,
    open FLOAT,
    close FLOAT,
    high FLOAT,
    low FLOAT,
    num_trades BIGINT,
    volume BIGINT,
    UNIQUE KEY unique_ymd_stock_code (ymd, htsc_code)
) ;


CREATE TABLE quant.ods_stock_kline_daily_insight (
    htsc_code VARCHAR(50) NOT NULL,
    ymd DATE NOT NULL,
    open FLOAT,
    close FLOAT,
    high FLOAT,
    low FLOAT,
    num_trades BIGINT,
    volume BIGINT,
    UNIQUE KEY unique_ymd_stock_code (ymd, htsc_code)
) PARTITION BY RANGE (YEAR(ymd) * 100 + MONTH(ymd)) (
    PARTITION p202112 VALUES LESS THAN (202201),
    PARTITION p202212 VALUES LESS THAN (202301),
    PARTITION p202312 VALUES LESS THAN (202401),
    PARTITION p202401 VALUES LESS THAN (202402),
    PARTITION p202402 VALUES LESS THAN (202403),
    PARTITION p202403 VALUES LESS THAN (202404),
    PARTITION p202404 VALUES LESS THAN (202405),
    PARTITION p202405 VALUES LESS THAN (202406),
    PARTITION p202406 VALUES LESS THAN (202407),
    PARTITION p202407 VALUES LESS THAN (202408),
    PARTITION p202408 VALUES LESS THAN (202409),
    -- 添加其他月份的分区
    PARTITION pmax VALUES LESS THAN MAXVALUE
);


--1.3
------------------  index_a_share_insight   大A的主要指数日K
CREATE TABLE quant.ods_index_a_share_insight_now (
    htsc_code VARCHAR(50) NOT NULL,
    name VARCHAR(50) NOT NULL,
    ymd DATE NOT NULL,
    open FLOAT,
    close FLOAT,
    high FLOAT,
    low FLOAT,
    volume BIGINT,
    UNIQUE KEY unique_ymd_stock_code (ymd, htsc_code)
) ;


CREATE TABLE quant.ods_index_a_share_insight (
    htsc_code VARCHAR(50) NOT NULL,
    name VARCHAR(50) NOT NULL,
    ymd DATE NOT NULL,
    open FLOAT,
    close FLOAT,
    high FLOAT,
    low FLOAT,
    volume BIGINT,
    UNIQUE KEY unique_ymd_stock_code (ymd, htsc_code)
) PARTITION BY RANGE (YEAR(ymd) * 100 + MONTH(ymd)) (
    PARTITION p202112 VALUES LESS THAN (202201),
    PARTITION p202212 VALUES LESS THAN (202301),
    PARTITION p202312 VALUES LESS THAN (202401),
    PARTITION p202401 VALUES LESS THAN (202402),
    PARTITION p202402 VALUES LESS THAN (202403),
    PARTITION p202403 VALUES LESS THAN (202404),
    PARTITION p202404 VALUES LESS THAN (202405),
    PARTITION p202405 VALUES LESS THAN (202406),
    PARTITION p202406 VALUES LESS THAN (202407),
    PARTITION p202407 VALUES LESS THAN (202408),
    PARTITION p202408 VALUES LESS THAN (202409),
    -- 添加其他月份的分区
    PARTITION pmax VALUES LESS THAN MAXVALUE
);


--1.4
------------------  stock_limit_summary_insight   当日大A行情温度
CREATE TABLE quant.ods_stock_limit_summary_insight_now (
    ymd DATE NOT NULL,
    name VARCHAR(50) NOT NULL,
    today_ZT INT,
    today_DT INT,
    yesterday_ZT INT,
    yesterday_DT INT,
    yesterday_ZT_rate FLOAT,
    UNIQUE KEY unique_ymd_name (ymd, name)
) ;


CREATE TABLE quant.ods_stock_limit_summary_insight (
    ymd DATE NOT NULL,
    name VARCHAR(50) NOT NULL,
    today_ZT INT,
    today_DT INT,
    yesterday_ZT INT,
    yesterday_DT INT,
    yesterday_ZT_rate FLOAT,
    UNIQUE KEY unique_ymd_name (ymd, name)
) PARTITION BY RANGE (YEAR(ymd) * 100 + MONTH(ymd)) (
    PARTITION p202112 VALUES LESS THAN (202201),
    PARTITION p202212 VALUES LESS THAN (202301),
    PARTITION p202312 VALUES LESS THAN (202401),
    PARTITION p202401 VALUES LESS THAN (202402),
    PARTITION p202402 VALUES LESS THAN (202403),
    PARTITION p202403 VALUES LESS THAN (202404),
    PARTITION p202404 VALUES LESS THAN (202405),
    PARTITION p202405 VALUES LESS THAN (202406),
    PARTITION p202406 VALUES LESS THAN (202407),
    PARTITION p202407 VALUES LESS THAN (202408),
    PARTITION p202408 VALUES LESS THAN (202409),
    -- 添加其他月份的分区
    PARTITION pmax VALUES LESS THAN MAXVALUE
);



--1.5
------------------  future_inside_insight   内盘主要期货数据日K
CREATE TABLE quant.ods_future_inside_insight_now (
    htsc_code VARCHAR(50) NOT NULL,
    ymd DATE NOT NULL,
    open FLOAT,
    close FLOAT,
    high FLOAT,
    low FLOAT,
    volume BIGINT,
    open_interest BIGINT,
    settle BIGINT,
    UNIQUE KEY unique_ymd_stock_code (ymd, htsc_code)
) ;


CREATE TABLE quant.ods_future_inside_insight (
    htsc_code VARCHAR(50) NOT NULL,
    ymd DATE NOT NULL,
    open FLOAT,
    close FLOAT,
    high FLOAT,
    low FLOAT,
    volume BIGINT,
    open_interest BIGINT,
    settle BIGINT,
    UNIQUE KEY unique_ymd_stock_code (ymd, htsc_code)
) PARTITION BY RANGE (YEAR(ymd) * 100 + MONTH(ymd)) (
    PARTITION p202112 VALUES LESS THAN (202201),
    PARTITION p202212 VALUES LESS THAN (202301),
    PARTITION p202312 VALUES LESS THAN (202401),
    PARTITION p202401 VALUES LESS THAN (202402),
    PARTITION p202402 VALUES LESS THAN (202403),
    PARTITION p202403 VALUES LESS THAN (202404),
    PARTITION p202404 VALUES LESS THAN (202405),
    PARTITION p202405 VALUES LESS THAN (202406),
    PARTITION p202406 VALUES LESS THAN (202407),
    PARTITION p202407 VALUES LESS THAN (202408),
    PARTITION p202408 VALUES LESS THAN (202409),
    -- 添加其他月份的分区
    PARTITION pmax VALUES LESS THAN MAXVALUE
);



--1.6
------------------  stock_chouma_insight   A股的筹码分布数据

CREATE TABLE quant.ods_stock_chouma_insight (
    htsc_code                                VARCHAR(50) NOT NULL
   ,ymd                                      DATE NOT NULL
   ,exchange                                 VARCHAR(50)
   ,last                                     FLOAT
   ,prev_close                               FLOAT
   ,total_share                              BIGINT
   ,a_total_share                            BIGINT
   ,a_listed_share                           BIGINT
   ,listed_share                             BIGINT
   ,restricted_share                         BIGINT
   ,cost_5pct                                FLOAT
   ,cost_15pct                               FLOAT
   ,cost_50pct                               FLOAT
   ,cost_85pct                               FLOAT
   ,cost_95pct                               FLOAT
   ,avg_cost                                 FLOAT
   ,max_cost                                 FLOAT
   ,min_cost                                 FLOAT
   ,winner_rate                              FLOAT
   ,diversity                                FLOAT
   ,pre_winner_rate                          FLOAT
   ,restricted_avg_cost                      FLOAT
   ,restricted_max_cost                      FLOAT
   ,restricted_min_cost                      FLOAT
   ,large_shareholders_avg_cost              FLOAT
   ,large_shareholders_total_share           FLOAT
   ,large_shareholders_total_share_pct       FLOAT
   ,UNIQUE KEY unique_ymd_stock_code (ymd, htsc_code)
 ) PARTITION BY RANGE (YEAR(ymd) * 100 + MONTH(ymd)) (
    PARTITION p202112 VALUES LESS THAN (202201),
    PARTITION p202212 VALUES LESS THAN (202301),
    PARTITION p202312 VALUES LESS THAN (202401),
    PARTITION p202401 VALUES LESS THAN (202402),
    PARTITION p202402 VALUES LESS THAN (202403),
    PARTITION p202403 VALUES LESS THAN (202404),
    PARTITION p202404 VALUES LESS THAN (202405),
    PARTITION p202405 VALUES LESS THAN (202406),
    PARTITION p202406 VALUES LESS THAN (202407),
    PARTITION p202407 VALUES LESS THAN (202408),
    PARTITION p202408 VALUES LESS THAN (202409),
    -- 添加其他月份的分区
    PARTITION pmax VALUES LESS THAN MAXVALUE
);



--1.7
------------------  astock_industry_overview   行业分类，申万三级分类
CREATE TABLE quant.ods_astock_industry_overview (
    ymd                  DATE
   ,classified           varchar(100)
   ,industry_name        varchar(100)
   ,industry_code        varchar(100)
   ,l1_code              varchar(100)
   ,l1_name              varchar(100)
   ,l2_code              varchar(100)
   ,l2_name              varchar(100)
   ,l3_code              varchar(100)
   ,l3_name              varchar(100)
   ,UNIQUE KEY unique_industry_code (ymd, industry_code)
 );


--1.8
------------------  astock_industry_detail   股票&行业的关联
CREATE TABLE quant.ods_astock_industry_detail (
    ymd              DATE
   ,htsc_code        varchar(100)
   ,name             varchar(100)
   ,industry_name    varchar(100)
   ,industry_code    varchar(100)
   ,l1_code          varchar(100)
   ,l1_name          varchar(100)
   ,l2_code          varchar(100)
   ,l2_name          varchar(100)
   ,l3_code          varchar(100)
   ,l3_name          varchar(100)
   ,UNIQUE KEY unique_industry_code (ymd, htsc_code)
);


--1.9
------------------  shareholder_num   个股的股东数
CREATE TABLE quant.ods_shareholder_num_now (
      htsc_code              varchar(100)
     ,name                   varchar(100)
     ,ymd                    DATE
     ,total_sh               DOUBLE
     ,avg_share              DOUBLE
     ,pct_of_total_sh        DOUBLE
     ,pct_of_avg_sh          DOUBLE
   ,UNIQUE KEY unique_industry_code (ymd, htsc_code)
 );


CREATE TABLE quant.ods_shareholder_num (
      htsc_code              varchar(100)
     ,name                   varchar(100)
     ,ymd                    DATE
     ,total_sh               DOUBLE
     ,avg_share              DOUBLE
     ,pct_of_total_sh        DOUBLE
     ,pct_of_avg_sh          DOUBLE
   ,UNIQUE KEY unique_industry_code (ymd, htsc_code)
 );


--1.10
------------------  north_bound   北向持仓数据
CREATE TABLE quant.ods_north_bound_daily_now (
      htsc_code            varchar(100)
     ,ymd                  DATE
     ,sh_hkshare_hold      BIGINT
     ,pct_total_share      FLOAT
   ,UNIQUE KEY unique_industry_code (ymd, htsc_code)
 );


CREATE TABLE quant.ods_north_bound_daily (
      htsc_code            varchar(100)
     ,ymd                  DATE
     ,sh_hkshare_hold      BIGINT
     ,pct_total_share      FLOAT
   ,UNIQUE KEY unique_industry_code (ymd, htsc_code)
 );


--2.1
------------------  us_stock_daily_vantage   美股 日K
CREATE TABLE quant.ods_us_stock_daily_vantage (
    name VARCHAR(50) NOT NULL,
    ymd DATE NOT NULL,
    open FLOAT,
    high FLOAT,
    low FLOAT,
    close FLOAT,
    volume BIGINT,
    UNIQUE KEY unique_ymd_stock_code (ymd, name)
) ;



--2.2
------------------  exchange_rate_vantage_detail   汇率&美元指数 日K
CREATE TABLE quant.ods_exchange_rate_vantage_detail (
    name VARCHAR(50) NOT NULL,
    ymd DATE NOT NULL,
    open FLOAT,
    high FLOAT,
    low FLOAT,
    close FLOAT,
    UNIQUE KEY unique_ymd_stock_code (ymd, name)
) ;


CREATE TABLE quant.ods_exchange_dxy_vantage (
    ymd DATE NOT NULL,
    name VARCHAR(50) NOT NULL,
    UNIQUE KEY unique_ymd_stock_code (ymd, name)
) ;

























