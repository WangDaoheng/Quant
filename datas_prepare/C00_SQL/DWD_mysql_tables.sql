--1.1
------------------  dwd_ashare_stock_base_info   股票基本信息大宽表
CREATE TABLE quant.dwd_ashare_stock_base_info (
       ymd                DATE                     COMMENT '日期'
      ,stock_code         VARCHAR(50)              COMMENT '代码'
      ,stock_name         VARCHAR(50)              COMMENT '名称'
      ,close              DOUBLE(12,2)             COMMENT '最新收盘价'
      ,change_pct         FLOAT                    COMMENT '当日涨跌幅(%)'
      ,volume             BIGINT                   COMMENT '成交量'
      ,trading_amount     DOUBLE                   COMMENT '成交额'
      ,market_value       DOUBLE                   COMMENT '流通市值(亿)'
      ,total_value        DOUBLE                   COMMENT '总市值(亿)'
      ,total_capital      DOUBLE                   COMMENT '总股本(亿)'
      ,float_capital      DOUBLE                   COMMENT '流通股(亿)'
      ,shareholder_num    BIGINT                   COMMENT '股东人数'
      ,pct_of_total_sh    DOUBLE(12,4)             COMMENT '股东数较上期环比波动百分比'
      ,pb                 FLOAT(12,2)              COMMENT '市净率'
      ,pe                 FLOAT(12,2)              COMMENT '市盈(动)'
      ,market             VARCHAR(50)              COMMENT '市场特征主板创业板等'
      ,plate_names        VARCHAR(2000)            COMMENT '板块名称'
      ,UNIQUE KEY unique_ymd_stock_code (ymd, stock_code)
) COMMENT='股票基本信息大宽表';


--1.2
------------------  dwd_stock_zt_list   涨停股票清单
CREATE TABLE `dwd_stock_zt_list` (
  `ymd`             date            NOT NULL                COMMENT '交易日期',
  `stock_code`      varchar(20)     NOT NULL                COMMENT '股票代码',
  `stock_name`      varchar(50)     DEFAULT NULL            COMMENT '股票名称',
  `open`            decimal(10,2)   DEFAULT NULL            COMMENT '开盘价',
  `high`            decimal(10,2)   DEFAULT NULL            COMMENT '最高价',
  `low`             decimal(10,2)   DEFAULT NULL            COMMENT '最低价',
  `close`           decimal(10,2)   DEFAULT NULL            COMMENT '收盘价',
  `volume`          bigint          DEFAULT NULL            COMMENT '成交量（手）',
  `last_close`      decimal(10,2)   DEFAULT NULL            COMMENT '昨日收盘价',
  `rate`            decimal(10,2)   DEFAULT NULL            COMMENT '涨跌幅（%）',
  `zt_type`         varchar(10)     DEFAULT NULL            COMMENT '涨停类型：yizi/tzi/shiti/zha',
  `zt_price`        decimal(10,2)   DEFAULT NULL            COMMENT '理论涨停价',
  `turnover_rate`   decimal(10,2)   DEFAULT NULL            COMMENT '换手率（%）',
  `market_value`    decimal(20,2)   DEFAULT NULL            COMMENT '流通市值（亿）',
  `total_value`     decimal(20,2)   DEFAULT NULL            COMMENT '总市值（亿）',
  `total_capital`   decimal(20,2)   DEFAULT NULL            COMMENT '总股本（亿）',
  `float_capital`   decimal(20,2)   DEFAULT NULL            COMMENT '流通股（亿）',
  `shareholder_num` int             DEFAULT NULL            COMMENT '股东人数',
  `pb`              decimal(10,2)   DEFAULT NULL            COMMENT '市净率',
  `pe`              decimal(10,2)   DEFAULT NULL            COMMENT '市盈（动）',
  `market`          varchar(20)     DEFAULT NULL            COMMENT '市场特征（主板/创业板/科创板）',
  `plate_names`     text                                    COMMENT '所属板块（逗号分隔）',
  `created_at`      timestamp       NULL DEFAULT CURRENT_TIMESTAMP COMMENT '写入时间',
  PRIMARY KEY (`ymd`,`stock_code`),
  KEY `idx_ymd` (`ymd`),
  KEY `idx_zt_type` (`zt_type`),
  KEY `idx_stock_code` (`stock_code`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci COMMENT='涨停明细表（含类型分类）';



--1.3
------------------  dwd_stock_dt_list   跌停股票清单
CREATE TABLE `dwd_stock_dt_list` (
  `ymd`             date            NOT NULL                COMMENT '交易日期',
  `stock_code`      varchar(20)     NOT NULL                COMMENT '股票代码',
  `stock_name`      varchar(50)     DEFAULT NULL            COMMENT '股票名称',
  `open`            decimal(10,2)   DEFAULT NULL            COMMENT '开盘价',
  `high`            decimal(10,2)   DEFAULT NULL            COMMENT '最高价',
  `low`             decimal(10,2)   DEFAULT NULL            COMMENT '最低价',
  `close`           decimal(10,2)   DEFAULT NULL            COMMENT '收盘价',
  `volume`          bigint          DEFAULT NULL            COMMENT '成交量（手）',
  `last_close`      decimal(10,2)   DEFAULT NULL            COMMENT '昨日收盘价',
  `rate`            decimal(10,2)   DEFAULT NULL            COMMENT '涨跌幅（%）',
  `dt_price`        decimal(10,2)   DEFAULT NULL            COMMENT '理论跌停价',
  `turnover_rate`   decimal(10,2)   DEFAULT NULL            COMMENT '换手率（%）',
  `market_value`    decimal(20,2)   DEFAULT NULL            COMMENT '流通市值（亿）',
  `total_value`     decimal(20,2)   DEFAULT NULL            COMMENT '总市值（亿）',
  `total_capital`   decimal(20,2)   DEFAULT NULL            COMMENT '总股本（亿）',
  `float_capital`   decimal(20,2)   DEFAULT NULL            COMMENT '流通股（亿）',
  `shareholder_num` int             DEFAULT NULL            COMMENT '股东人数',
  `pb`              decimal(10,2)   DEFAULT NULL            COMMENT '市净率',
  `pe`              decimal(10,2)   DEFAULT NULL            COMMENT '市盈（动）',
  `market`          varchar(20)     DEFAULT NULL            COMMENT '市场特征',
  `plate_names`     text                                    COMMENT '所属板块',
  `created_at`      timestamp       NULL DEFAULT CURRENT_TIMESTAMP COMMENT '写入时间',
  PRIMARY KEY (`ymd`,`stock_code`),
  KEY `idx_ymd` (`ymd`),
  KEY `idx_stock_code` (`stock_code`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci COMMENT='跌停明细表';


--1.5
------------------  dwd_stock_technical_indicators   股票技术指标预计算表（均线等）
CREATE TABLE quant.dwd_stock_technical_indicators (
     ymd                      DATE        NOT NULL    COMMENT '交易日期'
    ,stock_code               VARCHAR(50) NOT NULL    COMMENT '股票代码'
    ,stock_name               varchar(50)             COMMENT '股票名称'
    ,close                    FLOAT                   COMMENT '收盘价'
    ,volume                   BIGINT                  COMMENT '成交量'
    -- 价格均线
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
CREATE TABLE `dwd_stock_a_total_plate` (
  `ymd`             date            NOT NULL                COMMENT '日期',
  `board_code`      varchar(50)     CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci DEFAULT NULL COMMENT '板块代码',
  `board_name`      varchar(50)     CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NOT NULL COMMENT '板块名称',
  `stock_code`      varchar(50)     DEFAULT NULL            COMMENT '标的代码',
  `stock_name`      varchar(50)     DEFAULT NULL            COMMENT '标的名称',
  `source_table`    varchar(50)     DEFAULT NULL            COMMENT '来源表',
  `remark`          varchar(50)     DEFAULT NULL            COMMENT '备注',
  `stock_code_pure` varchar(50)     GENERATED ALWAYS AS (substring_index(`stock_code`,_utf8mb4'.',1)) VIRTUAL,
  UNIQUE KEY `unique_ymd_board_code` (`ymd`,`board_name`,`stock_code`),
  KEY `idx_ymd_stock_board` (`ymd`,`stock_code`,`board_name`),
  KEY `idx_ymd_pure_board` (`ymd`,`stock_code_pure`,`board_name`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci COMMENT='多渠道板块数据 -- 多渠道汇总'


