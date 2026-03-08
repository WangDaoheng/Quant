--1.1
------------------  dwd_ashare_stock_base_info   股票基本信息大宽表
CREATE TABLE quant.dwd_ashare_stock_base_info (
       ymd                DATE                     COMMENT '日期'
      ,stock_code         varchar(50)              COMMENT '代码'
      ,stock_name         varchar(50)              COMMENT '名称'
      ,close              double(12,2)             COMMENT '最新收盘价'
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
      ,pb                 float(12,2)              COMMENT '市净率'
      ,pe                 float(12,2)              COMMENT '市盈(动)'
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




--1.5
------------------  dwd_stock_technical_indicators   股票技术指标预计算表（均线等）
CREATE TABLE quant.dwd_stock_technical_indicators (
     ymd                      DATE        NOT NULL    COMMENT '交易日期'
    ,stock_code               VARCHAR(50) NOT NULL    COMMENT '股票代码'
    ,stock_name               varchar(50)             COMMENT '股票名称'
    ,close                    FLOAT                   COMMENT '收盘价'
    ,volume                   BIGINT                  COMMENT '成交量'
    -- 成交量均线
    ,ma5                      FLOAT                   COMMENT '5日均线'
    ,ma10                     FLOAT                   COMMENT '10日均线'
    ,ma20                     FLOAT                   COMMENT '20日均线'
    ,ma60                     FLOAT                   COMMENT '60日均线'
    ,ma120                    FLOAT                   COMMENT '120日均线（半年线）'
    ,ma250                    FLOAT                   COMMENT '250日均线（年线）'
    -- 成交量均线
    ,vol_ma5                  FLOAT                   COMMENT '5日均量'
    ,vol_ma10                 FLOAT                   COMMENT '10日均量'
    ,vol_ma20                 FLOAT                   COMMENT '20日均量'
    ,vol_ma60                 FLOAT                   COMMENT '60日均量'
    ,vol_ma120                FLOAT                   COMMENT '120日均量'
    ,vol_ma250                FLOAT                   COMMENT '250日均量'
    -- 价格偏离度
    ,price_vs_ma5             DECIMAL(10,2)           COMMENT '价格/5日均线-1, 单位%'
    ,price_vs_ma20            DECIMAL(10,2)           COMMENT '价格/20日均线-1, 单位%'
    ,price_vs_ma60            DECIMAL(10,2)           COMMENT '价格/60日均线-1, 单位%'
    -- 成交量偏离度
    ,volume_vs_ma5            DECIMAL(10,2)           COMMENT '成交量/5日均量-1, 单位%'
    ,volume_vs_ma20           DECIMAL(10,2)           COMMENT '成交量/20日均量-1, 单位%'
    ,UNIQUE KEY unique_ymd_stock_code (ymd, stock_code)
    ,INDEX idx_stock_code (stock_code)
    ,INDEX idx_ma5 (ma5)
    ,INDEX idx_vol_ma5 (vol_ma5)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci
COMMENT='股票技术指标预计算表（均线等）';







--2.1
------------------  dwd_factor_volume_shrinkage  缩量下跌因子明细表
CREATE TABLE IF NOT EXISTS quant.dwd_factor_volume_shrinkage (
    ymd                     DATE        NOT NULL COMMENT '交易日期',
    stock_code              VARCHAR(50) NOT NULL COMMENT '股票代码',
    stock_name              VARCHAR(50)          COMMENT '股票名称',
    -- 原始数据             
    close                   FLOAT                COMMENT '收盘价',
    volume                  BIGINT               COMMENT '成交量',
    -- 成交量指标           
    vol_ma5                 FLOAT                COMMENT '5日均量',
    vol_ma60                FLOAT                COMMENT '60日均量',
    volume_vs_ma5           DECIMAL(10,2)        COMMENT '成交量/5日均量-1, 单位%',
    -- 缩量判断             
    is_shrink_today         TINYINT(1)           COMMENT '当日是否缩量(1:是,0:否)',
    consecutive_shrink_days INT                  COMMENT '连续缩量天数',
    -- 阴线判断
    is_down                 TINYINT(1)           COMMENT '当日是否阴线(1:是,0:否)',
    consecutive_down_days   INT                  COMMENT '连续阴线天数',
    -- 因子得分
    volume_score            DECIMAL(5,2)         COMMENT '缩量因子得分(0-60)',
    price_score             DECIMAL(5,2)         COMMENT '下跌因子得分(0-40)',
    composite_score         DECIMAL(5,2)         COMMENT '缩量下跌综合得分(0-100)',
    signal_level            CHAR(1)              COMMENT '信号等级(A/B/C/D/E)',
    UNIQUE KEY unique_ymd_stock (ymd, stock_code)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci
COMMENT='缩量下跌因子明细表';




--2.2
------------------  dwd_factor_summary  因子汇总表（所有因子得分）
CREATE TABLE IF NOT EXISTS quant.dwd_factor_summary (
    ymd               DATE         NOT NULL  COMMENT '交易日期',
    stock_code        VARCHAR(50)  NOT NULL  COMMENT '股票代码',
    stock_name        VARCHAR(50)            COMMENT '股票名称',
    -- 各因子得分
    pb_score          DECIMAL(5,2) DEFAULT 0 COMMENT 'PB因子得分(0-100)',
    zt_score          DECIMAL(5,2) DEFAULT 0 COMMENT '涨停因子得分(0-100)',
    shareholder_score DECIMAL(5,2) DEFAULT 0 COMMENT '股东数因子得分(0-100)',
    -- 缩量下跌因子相关得分
    volume_score      DECIMAL(5,2) DEFAULT 0 COMMENT '缩量因子得分(0-60)',
    price_score       DECIMAL(5,2) DEFAULT 0 COMMENT '下跌因子得分(0-40)',
    composite_score   DECIMAL(5,2) DEFAULT 0 COMMENT '缩量下跌综合得分(0-100)',
    signal_level      CHAR(1)                COMMENT '缩量下跌信号等级(A/B/C/D/E)',
    -- 其他因子可继续添加
    UNIQUE KEY unique_ymd_stock (ymd, stock_code),
    INDEX idx_composite (composite_score),
    INDEX idx_pb (pb_score),
    INDEX idx_zt (zt_score)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci
COMMENT='因子汇总表（所有因子得分）';









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


