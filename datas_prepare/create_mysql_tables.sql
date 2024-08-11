

------------------  stock_code_daily_insight   当日已上市股票码表
CREATE TABLE quant.stock_code_daily_insight (
    ymd DATE NOT NULL,
    htsc_code VARCHAR(50) NOT NULL,
    name VARCHAR(50),
    exchange VARCHAR(50),
    UNIQUE KEY unique_ymd_stock_code (ymd, htsc_code)
);



------------------  stock_kline_daily_insight   当日已上市股票的历史日K
CREATE TABLE quant.stock_kline_daily_insight_now (
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


CREATE TABLE quant.stock_kline_daily_insight (
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



------------------  index_a_share_insight   大A的主要指数日K
CREATE TABLE quant.index_a_share_insight_now (
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


CREATE TABLE quant.index_a_share_insight (
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


------------------  stock_limit_summary_insight   当日大A行情温度
CREATE TABLE quant.stock_limit_summary_insight_now (
    ymd DATE NOT NULL,
    name VARCHAR(50) NOT NULL,
    today_ZT INT,
    today_DT INT,
    yesterday_ZT INT,
    yesterday_DT INT,
    yesterday_ZT_rate FLOAT,
    UNIQUE KEY unique_ymd_name (ymd, name)
) ;


CREATE TABLE quant.stock_limit_summary_insight (
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




------------------  future_inside_insight   内盘主要期货数据日K

CREATE TABLE quant.future_inside_insight_now (
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


CREATE TABLE quant.future_inside_insight (
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




------------------  exchange_rate_vantage_detail   汇率&美元指数 日K

CREATE TABLE quant.exchange_rate_vantage_detail (
    name VARCHAR(50) NOT NULL,
    ymd DATE NOT NULL,
    open FLOAT,
    high FLOAT,
    low FLOAT,
    close FLOAT,
    UNIQUE KEY unique_ymd_stock_code (ymd, name)
) ;


CREATE TABLE quant.exchange_DXY_vantage (
    ymd DATE NOT NULL,
    name VARCHAR(50) NOT NULL,
    UNIQUE KEY unique_ymd_stock_code (ymd, name)
) ;





























