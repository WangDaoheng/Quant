
--1.0
------------------  ods_trading_days_insight   交易所的交易日历
CREATE TABLE quant.ods_trading_days_insight (
     exchange                 VARCHAR(50)             COMMENT '交易所名称'
    ,ymd                      DATE        NOT NULL    COMMENT '交易日期'
    ,UNIQUE KEY unique_ymd_exchange (exchange, ymd)
) COMMENT='交易所的交易日历';


--1.1
------------------  ods_stock_code_daily_insight   当日已上市股票码表
CREATE TABLE quant.ods_stock_code_daily_insight (
     ymd                      DATE        NOT NULL    COMMENT '交易日期'
    ,stock_code               VARCHAR(50) NOT NULL    COMMENT '股票代码'
    ,stock_name               VARCHAR(50)             COMMENT '股票名'
    ,exchange                 VARCHAR(50)             COMMENT '交易所名称'
    ,UNIQUE KEY unique_ymd_stock_code (ymd, stock_code)
) COMMENT='当日已上市股票码表';


--1.2
------------------  ods_stock_kline_daily_insight   当日已上市股票的历史日K
CREATE TABLE quant.ods_stock_kline_daily_insight_now (
     stock_code               VARCHAR(50) NOT NULL    COMMENT '股票代码'
    ,ymd                      DATE        NOT NULL    COMMENT '交易日期'
    ,open                     FLOAT                   COMMENT '开盘价'
    ,close                    FLOAT                   COMMENT '收盘价'
    ,high                     FLOAT                   COMMENT '最高价'
    ,low                      FLOAT                   COMMENT '最低价'
    ,num_trades               BIGINT                  COMMENT '交易笔数'
    ,volume                   BIGINT                  COMMENT '成交量'
    ,UNIQUE KEY unique_ymd_stock_code (ymd, stock_code)
) COMMENT='当日已上市股票的历史日K(日增量表)';


CREATE TABLE quant.ods_stock_kline_daily_insight (
     stock_code               VARCHAR(50) NOT NULL    COMMENT '股票代码'
    ,ymd                      DATE        NOT NULL    COMMENT '交易日期'
    ,open                     FLOAT                   COMMENT '开盘价'
    ,close                    FLOAT                   COMMENT '收盘价'
    ,high                     FLOAT                   COMMENT '最高价'
    ,low                      FLOAT                   COMMENT '最低价'
    ,num_trades               BIGINT                  COMMENT '交易笔数'
    ,volume                   BIGINT                  COMMENT '成交量'
    ,UNIQUE KEY unique_ymd_stock_code (ymd, stock_code)
) COMMENT='当日已上市股票的历史日K(全量表)';


--1.3
------------------  ods_index_a_share_insight   大A的主要指数日K
CREATE TABLE quant.ods_index_a_share_insight_now (
     index_code               VARCHAR(50) NOT NULL    COMMENT '指数代码'
    ,index_name               VARCHAR(50) NOT NULL    COMMENT '指数名称'
    ,ymd                      DATE        NOT NULL    COMMENT '交易日期'
    ,open                     FLOAT                   COMMENT '开盘价'
    ,close                    FLOAT                   COMMENT '收盘价'
    ,high                     FLOAT                   COMMENT '最高价'
    ,low                      FLOAT                   COMMENT '最低价'
    ,volume                   BIGINT                  COMMENT '成交量'
    ,UNIQUE KEY unique_ymd_stock_code (ymd, index_code)
) COMMENT='大A的主要指数日K(日增量表)';


CREATE TABLE quant.ods_index_a_share_insight (
     index_code               VARCHAR(50) NOT NULL    COMMENT '指数代码'
    ,index_name               VARCHAR(50) NOT NULL    COMMENT '指数名称'
    ,ymd                      DATE        NOT NULL    COMMENT '交易日期'
    ,open                     FLOAT                   COMMENT '开盘价'
    ,close                    FLOAT                   COMMENT '收盘价'
    ,high                     FLOAT                   COMMENT '最高价'
    ,low                      FLOAT                   COMMENT '最低价'
    ,volume                   BIGINT                  COMMENT '成交量'
    ,UNIQUE KEY unique_ymd_stock_code (ymd, index_code)
) COMMENT='大A的主要指数日K(全量表)';


--1.4
------------------  ods_stock_limit_summary_insight   当日大A行情温度
CREATE TABLE quant.ods_stock_limit_summary_insight_now (
     ymd                      DATE        NOT NULL    COMMENT '日期'
    ,name                     VARCHAR(50) NOT NULL    COMMENT '市场名称'
    ,today_ZT                 INT                     COMMENT '今日涨停股票数'
    ,today_DT                 INT                     COMMENT '今日跌停股票数'
    ,yesterday_ZT             INT                     COMMENT '昨日涨停股票数'
    ,yesterday_DT             INT                     COMMENT '昨日跌停股票数'
    ,yesterday_ZT_rate        FLOAT                   COMMENT '昨日涨停股票的今日平均涨幅'
    ,UNIQUE KEY unique_ymd_name (ymd, name)
) COMMENT='当日大A行情温度(日增量表)';


CREATE TABLE quant.ods_stock_limit_summary_insight (
     ymd                      DATE        NOT NULL    COMMENT '日期'
    ,name                     VARCHAR(50) NOT NULL    COMMENT '市场名称'
    ,today_ZT                 INT                     COMMENT '今日涨停股票数'
    ,today_DT                 INT                     COMMENT '今日跌停股票数'
    ,yesterday_ZT             INT                     COMMENT '昨日涨停股票数'
    ,yesterday_DT             INT                     COMMENT '昨日跌停股票数'
    ,yesterday_ZT_rate        FLOAT                   COMMENT '昨日涨停股票的今日平均涨幅'
    ,UNIQUE KEY unique_ymd_name (ymd, name)
) COMMENT='当日大A行情温度(全量表)';


--1.5
------------------  ods_future_inside_insight   内盘主要期货数据日K
CREATE TABLE quant.ods_future_inside_insight_now (
     stock_code               VARCHAR(50) NOT NULL    COMMENT '期货标的代码'
    ,ymd                      DATE        NOT NULL    COMMENT '交易日期'
    ,open                     FLOAT                   COMMENT '开盘价'
    ,close                    FLOAT                   COMMENT '收盘价'
    ,high                     FLOAT                   COMMENT '最高价'
    ,low                      FLOAT                   COMMENT '最低价'
    ,volume                   BIGINT                  COMMENT '成交量'
    ,open_interest            BIGINT                  COMMENT '持仓量'
    ,settle                   BIGINT                  COMMENT '结算价'
    ,UNIQUE KEY unique_ymd_stock_code (ymd, stock_code)
) COMMENT='内盘主要期货数据日K(日增量表)';


CREATE TABLE quant.ods_future_inside_insight (
     stock_code               VARCHAR(50) NOT NULL    COMMENT '期货标的代码'
    ,ymd                      DATE        NOT NULL    COMMENT '交易日期'
    ,open                     FLOAT                   COMMENT '开盘价'
    ,close                    FLOAT                   COMMENT '收盘价'
    ,high                     FLOAT                   COMMENT '最高价'
    ,low                      FLOAT                   COMMENT '最低价'
    ,volume                   BIGINT                  COMMENT '成交量'
    ,open_interest            BIGINT                  COMMENT '持仓量'
    ,settle                   BIGINT                  COMMENT '结算价'
    ,UNIQUE KEY unique_ymd_stock_code (ymd, stock_code)
) COMMENT='内盘主要期货数据日K(全量表)';


--1.6
------------------  ods_stock_chouma_insight   A股的筹码分布数据
CREATE TABLE quant.ods_stock_chouma_insight (
    stock_code                               VARCHAR(50) NOT NULL     COMMENT '证券代码'
   ,ymd                                      DATE NOT NULL            COMMENT '交易日'
   ,exchange                                 VARCHAR(50)              COMMENT '交易所'
   ,close                                    FLOAT                    COMMENT '最新价格'
   ,prev_close                               FLOAT                    COMMENT '昨收价格'
   ,total_shares                             BIGINT                   COMMENT '总股本（股）'
   ,a_total_share                            BIGINT                   COMMENT 'A股总数(股)'
   ,a_listed_share                           BIGINT                   COMMENT '流通a股（万股）'
   ,listed_share                             BIGINT                   COMMENT '流通股总数'
   ,restricted_share                         BIGINT                   COMMENT '限售股总数'
   ,cost_5pct                                FLOAT                    COMMENT '5分位持仓成本（持仓成本最低的 5%的持仓成本）'
   ,cost_15pct                               FLOAT                    COMMENT '15分位持仓成本'
   ,cost_50pct                               FLOAT                    COMMENT '50分位持仓成本'
   ,cost_85pct                               FLOAT                    COMMENT '85分位持仓成本'
   ,cost_95pct                               FLOAT                    COMMENT '95分位持仓成本'
   ,avg_cost                                 FLOAT                    COMMENT '流通股加权平均持仓成本'
   ,max_cost                                 FLOAT                    COMMENT '流通股最大持仓成本'
   ,min_cost                                 FLOAT                    COMMENT '流通股最小持仓成本'
   ,winner_rate                              FLOAT                    COMMENT '流通股获利胜率'
   ,diversity                                FLOAT                    COMMENT '流通股筹码分散程度百分比'
   ,pre_winner_rate                          FLOAT                    COMMENT '流通股昨日获利胜率'
   ,restricted_avg_cost                      FLOAT                    COMMENT '限售股平均持仓成本'
   ,restricted_max_cost                      FLOAT                    COMMENT '限售股最大持仓成本'
   ,restricted_min_cost                      FLOAT                    COMMENT '限售股最小持仓成本'
   ,large_shareholders_avg_cost              FLOAT                    COMMENT '大流通股股东持股平均持仓成本'
   ,large_shareholders_total_share           FLOAT                    COMMENT '大流通股股东持股总数'
   ,large_shareholders_total_share_pct       FLOAT                    COMMENT '大流通股股东持股占总股本的比例'
   ,UNIQUE KEY unique_ymd_stock_code (ymd, stock_code)
) COMMENT='A股的筹码分布数据';


--1.7
------------------  ods_astock_industry_overview   行业分类，申万三级分类
CREATE TABLE quant.ods_astock_industry_overview (
    ymd                       DATE                    COMMENT '交易日期'
   ,classified                varchar(100)            COMMENT '行业分类'
   ,industry_name             varchar(100)            COMMENT '行业名称'
   ,industry_code             varchar(100)            COMMENT '行业代码'
   ,l1_code                   varchar(100)            COMMENT '一级行业代码'
   ,l1_name                   varchar(100)            COMMENT '一级行业名称'
   ,l2_code                   varchar(100)            COMMENT '二级行业代码'
   ,l2_name                   varchar(100)            COMMENT '二级行业名称'
   ,l3_code                   varchar(100)            COMMENT '三级行业代码'
   ,l3_name                   varchar(100)            COMMENT '三级行业名称'
   ,UNIQUE KEY unique_industry_code (ymd, industry_code)
) COMMENT='行业分类，申万三级分类';


--1.8
------------------  ods_astock_industry_detail   股票&行业的关联
CREATE TABLE quant.ods_astock_industry_detail (
    ymd                       DATE                    COMMENT '交易日期'
   ,stock_code                varchar(100)            COMMENT '股票代码'
   ,stock_name                varchar(50)             COMMENT '股票名称'
   ,industry_name             varchar(100)            COMMENT '行业名称'
   ,industry_code             varchar(100)            COMMENT '行业代码'
   ,l1_code                   varchar(100)            COMMENT '一级行业代码'
   ,l1_name                   varchar(100)            COMMENT '一级行业名称'
   ,l2_code                   varchar(100)            COMMENT '二级行业代码'
   ,l2_name                   varchar(100)            COMMENT '二级行业名称'
   ,l3_code                   varchar(100)            COMMENT '三级行业代码'
   ,l3_name                   varchar(100)            COMMENT '三级行业名称'
   ,UNIQUE KEY unique_stock_code (ymd, stock_code)
) COMMENT='股票&行业的关联';


--1.9
------------------  ods_shareholder_num   个股的股东数
CREATE TABLE quant.ods_shareholder_num_now (
       stock_code             varchar(100)            COMMENT '股票代码'
      ,stock_name             varchar(50)             COMMENT '股票名称'
      ,ymd                    DATE                    COMMENT '交易日期'
      ,total_sh               DOUBLE                  COMMENT '总股东数'
      ,avg_share              DOUBLE(10, 4)           COMMENT '每个股东平均持股数'
      ,pct_of_total_sh        DOUBLE(10, 4)           COMMENT '股东数较上期环比波动百分比'
      ,pct_of_avg_sh          DOUBLE(10, 4)           COMMENT '每个股东平均持股数较上期环比波动百分比'
      ,UNIQUE KEY unique_ymd_stock_code (ymd, stock_code)
) COMMENT='个股的股东数(日增量表)';

CREATE TABLE quant.ods_shareholder_num (
       stock_code             varchar(100)            COMMENT '股票代码'
      ,stock_name             varchar(50)             COMMENT '股票名称'
      ,ymd                    DATE                    COMMENT '交易日期'
      ,total_sh               DOUBLE                  COMMENT '总股东数'
      ,avg_share              DOUBLE(10, 4)           COMMENT '每个股东平均持股数'
      ,pct_of_total_sh        DOUBLE(10, 4)           COMMENT '股东数较上期环比波动百分比'
      ,pct_of_avg_sh          DOUBLE(10, 4)           COMMENT '每个股东平均持股数较上期环比波动百分比'
      ,UNIQUE KEY unique_ymd_stock_code (ymd, stock_code)
) COMMENT='个股的股东数(全量表)';


--1.10
------------------  ods_north_bound_daily   北向持仓数据
CREATE TABLE quant.ods_north_bound_daily_now (
      stock_code              varchar(100)            COMMENT '股票代码'
     ,ymd                     DATE                    COMMENT '交易日期'
     ,sh_hkshare_hold         BIGINT                  COMMENT '持股数量'
     ,pct_total_share         FLOAT                   COMMENT '持股占总股本比例'
     ,UNIQUE KEY unique_ymd_stock_code (ymd, stock_code)
) COMMENT='北向持仓数据(日增量表)';


CREATE TABLE quant.ods_north_bound_daily (
      stock_code              varchar(100)            COMMENT '股票代码'
     ,ymd                     DATE                    COMMENT '交易日期'
     ,sh_hkshare_hold         BIGINT                  COMMENT '持股数量'
     ,pct_total_share         FLOAT                   COMMENT '持股占总股本比例'
     ,UNIQUE KEY unique_ymd_stock_code (ymd, stock_code)
) COMMENT='北向持仓数据(全量表)';


--2.1
------------------  ods_us_stock_daily_vantage   美股 日K
CREATE TABLE quant.ods_us_stock_daily_vantage (
     stock_name               VARCHAR(50) NOT NULL    COMMENT '股票名称'
    ,ymd                      DATE        NOT NULL    COMMENT '交易日期'
    ,open                     FLOAT                   COMMENT '开盘价'
    ,high                     FLOAT                   COMMENT '最高价'
    ,low                      FLOAT                   COMMENT '最低价'
    ,close                    FLOAT                   COMMENT '收盘价'
    ,volume                   BIGINT                  COMMENT '成交量'
    ,UNIQUE KEY unique_ymd_name (ymd, stock_name)
) COMMENT='美股 日K';


--2.2
------------------  ods_exchange_rate_vantage_detail   汇率&美元指数 日K
CREATE TABLE quant.ods_exchange_rate_vantage_detail (
     stock_name               VARCHAR(50) NOT NULL    COMMENT '货币对'
    ,ymd                      DATE        NOT NULL    COMMENT '交易日期'
    ,open                     FLOAT                   COMMENT '开盘价'
    ,high                     FLOAT                   COMMENT '最高价'
    ,low                      FLOAT                   COMMENT '最低价'
    ,close                    FLOAT                   COMMENT '收盘价'
    ,UNIQUE KEY unique_ymd_name (ymd, stock_name)
) COMMENT='汇率&美元指数 日K';


--2.3
------------------  ods_exchange_dxy_vantage   美元指数 日K
CREATE TABLE quant.ods_exchange_dxy_vantage (
    ymd                       DATE        NOT NULL    COMMENT '交易日期'
   ,stock_name                VARCHAR(50) NOT NULL    COMMENT '货币对'
   ,UNIQUE KEY unique_ymd_name (ymd, stock_name)
) COMMENT='美元指数 日K';


-------------------------------------------   通达信数据  ---------------------------------
--3.1        
------------------  ods_tdx_stock_concept_plate   通达信概念板块数据
CREATE TABLE quant.ods_tdx_stock_concept_plate (
     ymd                      DATE        NOT NULL    COMMENT '日期'
    ,concept_code             VARCHAR(50) NOT NULL    COMMENT '概念板块代码'
    ,concept_name             VARCHAR(50)             COMMENT '概念板块名称'
    ,stock_code               VARCHAR(50)             COMMENT '股票代码'
    ,stock_name               VARCHAR(50)             COMMENT '股票名称'
) COMMENT='通达信概念板块数据';


--3.2        
------------------  ods_tdx_stock_style_plate   通达信风格板块数据
CREATE TABLE quant.ods_tdx_stock_style_plate (
     ymd                      DATE        NOT NULL    COMMENT '日期'
    ,style_code               VARCHAR(50) NOT NULL    COMMENT '风格板块代码'
    ,style_name               VARCHAR(50)             COMMENT '风格板块名称'
    ,stock_code               VARCHAR(50)             COMMENT '股票代码'
    ,stock_name               VARCHAR(50)             COMMENT '股票名称'
) COMMENT='通达信风格板块数据';


--3.3        
------------------  ods_tdx_stock_industry_plate   通达信行业板块数据
CREATE TABLE quant.ods_tdx_stock_industry_plate (
     ymd                      DATE        NOT NULL    COMMENT '日期'
    ,industry_code            VARCHAR(50) NOT NULL    COMMENT '行业板块代码'
    ,industry_name            VARCHAR(50)             COMMENT '行业板块名称'
    ,stock_code               VARCHAR(50)             COMMENT '股票代码'
    ,stock_name               VARCHAR(50)             COMMENT '股票名称'
) COMMENT='通达信行业板块数据';


--3.4        
------------------  ods_tdx_stock_region_plate   通达信地区板块数据
CREATE TABLE quant.ods_tdx_stock_region_plate (
     ymd                      DATE        NOT NULL    COMMENT '日期'
    ,region_code              VARCHAR(50) NOT NULL    COMMENT '地区板块代码'
    ,region_name              VARCHAR(50)             COMMENT '地区板块名称'
    ,stock_code               VARCHAR(50)             COMMENT '股票代码'
    ,stock_name               VARCHAR(50)             COMMENT '股票名称'
) COMMENT='通达信地区板块数据';


--3.5        
------------------  ods_tdx_stock_index_plate   通达信指数板块数据
CREATE TABLE quant.ods_tdx_stock_index_plate (
     ymd                      DATE        NOT NULL    COMMENT '日期'
    ,index_code               VARCHAR(50) NOT NULL    COMMENT '指数板块代码'
    ,index_name               VARCHAR(50)             COMMENT '指数板块名称'
    ,stock_code               VARCHAR(50)             COMMENT '股票代码'
    ,stock_name               VARCHAR(50)             COMMENT '股票名称'
) COMMENT='通达信指数板块数据';


--3.6        
------------------  ods_tdx_stock_pepb_info   股票基本面数据_资产数据   需手动下载的
CREATE TABLE quant.ods_tdx_stock_pepb_info (
     ymd                      DATE                    COMMENT '日期'
    ,stock_code               varchar(50)             COMMENT '代码'
    ,stock_name               varchar(50)             COMMENT '名称'
    ,market_value             double                  COMMENT '流通市值(亿)'
    ,total_asset              double                  COMMENT '总资产(亿)'
    ,net_asset                double                  COMMENT '净资产(亿)'
    ,total_capital            double                  COMMENT '总股本(亿)'
    ,float_capital            double                  COMMENT '流通股(亿)'
    ,shareholder_num          bigint                  COMMENT '股东人数'
    ,pb                       double                  COMMENT '市净率'
    ,pe                       double                  COMMENT '市盈(动)'
    ,industry                 varchar(50)             COMMENT '细分行业'
    ,UNIQUE KEY unique_ymd_stock_code (ymd, stock_code)
) COMMENT='股票基本面数据_资产数据';



-------------------------------------------   akshare 数据  ---------------------------------
--4.1        
------------------  ods_akshare_stock_value_em   股票基本面数据_估值数据              个股的全量历史数据   不可选定日期
CREATE TABLE quant.ods_akshare_stock_value_em (
     ymd                      DATE                    COMMENT '数据日期'
    ,stock_code               varchar(50)             COMMENT '股票代码'
    ,close                    float                   COMMENT '当日收盘价(元)'
    ,change_pct               float                   COMMENT '当日涨跌幅(%)'
    ,total_market             double                  COMMENT '总市值(元)'
    ,circulation_market       double                  COMMENT '流通市值(元)'
    ,total_shares             double                  COMMENT '总股本(股)'
    ,circulation_shares       double                  COMMENT '流通股本(股)'
    ,pe_ttm                   float(12,2)             COMMENT 'PE(TTM)'
    ,pe_static                float(12,2)             COMMENT 'PE(静)'
    ,pb                       float(12,2)             COMMENT '市净率'
    ,peg                      float(12,2)             COMMENT 'PEG值'
    ,pcf                      float(12,2)             COMMENT '市现率'
    ,ps                       float(12,2)             COMMENT '市销率'
    ,UNIQUE KEY unique_ymd_stock_code (ymd, stock_code)
) COMMENT='股票基本面数据_估值数据';


--4.2
------------------  ods_akshare_stock_zh_a_gdhs_detail_em   股票基本面数据_股东数据    个股的全量历史数据   不可选定日期
CREATE TABLE quant.ods_akshare_stock_zh_a_gdhs_detail_em (
     ymd                      DATE                    COMMENT '股东户数统计截止日（对应核心日期维度）'
    ,stock_code               varchar(50)             COMMENT '股票代码'
    ,stock_name               varchar(50)             COMMENT '股票名称'
    ,range_change_pct         float                   COMMENT '区间涨跌幅(%)'
    ,holder_num_current       bigint                  COMMENT '股东户数-本次'
    ,holder_num_last          bigint                  COMMENT '股东户数-上次'
    ,holder_num_change        bigint                  COMMENT '股东户数-增减'
    ,holder_num_change_pct    float                   COMMENT '股东户数-增减比例(%)'
    ,avg_holder_market        double                  COMMENT '户均持股市值'
    ,avg_holder_share_num     float                   COMMENT '户均持股数量'
    ,total_market             double                  COMMENT '总市值'
    ,total_shares             bigint                  COMMENT '总股本'
    ,share_change             bigint                  COMMENT '股本变动'
    ,share_change_reason      varchar(255)            COMMENT '股本变动原因'
    ,holder_num_announce_date DATE                    COMMENT '股东户数公告日期'
    ,UNIQUE KEY unique_ymd_stock_code (ymd, stock_code)
) COMMENT='股票基本面数据_股东数据';


--4.3
------------------  ods_akshare_stock_cyq_em   股票基本面数据_筹码数据                 个股的全量历史数据   不可选定日期
CREATE TABLE quant.ods_akshare_stock_cyq_em (
     ymd                      DATE                    COMMENT '日期'
    ,stock_code               varchar(50)             COMMENT '股票代码'
    ,stock_name               varchar(50)             COMMENT '股票名称'
    ,profit_ratio             float                   COMMENT '获利比例'
    ,avg_cost                 float                   COMMENT '平均成本'
    ,cost_low_90              float                   COMMENT '90成本-低'
    ,cost_high_90             float                   COMMENT '90成本-高'
    ,concentration_90         float                   COMMENT '90集中度'
    ,cost_low_70              float                   COMMENT '70成本-低'
    ,cost_high_70             float                   COMMENT '70成本-高'
    ,concentration_70         float                   COMMENT '70集中度'
    ,UNIQUE KEY unique_ymd_stock_code (ymd, stock_code)
) COMMENT='股票基本面数据_筹码数据';


--4.4
------------------  ods_akshare_stock_yjkb_em   股票基本面数据_业绩快报数据    全量的每日切片数据 可选定日期
CREATE TABLE quant.ods_akshare_stock_yjkb_em (
     ymd                      DATE                    COMMENT '公告日期（核心日期维度）'
    ,serial_num               varchar(50)             COMMENT '序号'
    ,stock_code               varchar(50)             COMMENT '股票代码'
    ,stock_name               varchar(50)             COMMENT '股票简称'
    ,eps                      double                  COMMENT '每股收益'
    ,income                   double                  COMMENT '营业收入-营业收入'
    ,income_last_year         double                  COMMENT '营业收入-去年同期'
    ,income_yoy               varchar(50)             COMMENT '营业收入-同比增长'
    ,income_qoq               double                  COMMENT '营业收入-季度环比增长'
    ,profit                   double                  COMMENT '净利润-净利润'
    ,profit_last_year         double                  COMMENT '净利润-去年同期'
    ,profit_yoy               varchar(50)             COMMENT '净利润-同比增长'
    ,profit_qoq               double                  COMMENT '净利润-季度环比增长'
    ,asset_per_share          float                   COMMENT '每股净资产'
    ,roe                      double                  COMMENT '净资产收益率'
    ,industry                 varchar(100)            COMMENT '所处行业'
    ,market_board             varchar(50)             COMMENT '市场板块'
    ,securities_type          varchar(50)             COMMENT '证券类型'
    ,UNIQUE KEY unique_ymd_stock_code (ymd, stock_code)
) COMMENT='股票基本面数据_业绩快报数据';


--4.5
------------------  ods_akshare_stock_yjyg_em   股票基本面数据_业绩预告数据    全量的每日切片数据  可选定日期
CREATE TABLE quant.ods_akshare_stock_yjyg_em (
     ymd                      DATE                    COMMENT '公告日期'
    ,serial_num               varchar(50)             COMMENT '序号'
    ,stock_code               varchar(50)             COMMENT '股票代码'
    ,stock_name               varchar(50)             COMMENT '股票简称'
    ,forecast_index           double                  COMMENT '预测指标'
    ,performance_change       double                  COMMENT '业绩变动'
    ,forecast_value           double                  COMMENT '预测数值(元)'
    ,change_pct               double                  COMMENT '业绩变动幅度(%)'
    ,change_reason            varchar(255)            COMMENT '业绩变动原因'
    ,forecast_type            varchar(50)             COMMENT '预告类型'
    ,last_year_value          double                  COMMENT '上年同期值(元)'
    ,UNIQUE KEY unique_ymd_stock_code (ymd, stock_code)
) COMMENT='股票基本面数据_业绩预告数据';


--4.6
------------------  ods_akshare_stock_a_high_low_statistics   大盘情绪数据_大盘区间内的新低新高股票数  全量的每日切片数据 不可指定日期
CREATE TABLE quant.ods_akshare_stock_a_high_low_statistics (
     ymd                      DATE                    COMMENT '交易日'
    ,market                   varchar(50)             COMMENT '市场类型'
    ,close                    float                   COMMENT '相关指数收盘价'
    ,high20                   int                     COMMENT '20日新高'
    ,low20                    int                     COMMENT '20日新低'
    ,high60                   int                     COMMENT '60日新高'
    ,low60                    int                     COMMENT '60日新低'
    ,high120                  int                     COMMENT '120日新高'
    ,low120                   int                     COMMENT '120日新低'
    ,UNIQUE KEY unique_ymd_market (ymd, market)
) COMMENT='大盘情绪数据_大盘区间内的新低新高股票数';


--4.7
------------------  ods_akshare_stock_zh_a_spot_em            行情数据_个股行情数据  全量的每日切片数据 不可指定日期
CREATE TABLE quant.ods_akshare_stock_zh_a_spot_em (
     ymd                      DATE                    COMMENT '数据日期（行情交易日，统一日期维度）'
    ,serial_num               bigint                  COMMENT '序号'
    ,stock_code               varchar(50)             COMMENT '代码'
    ,stock_name               varchar(50)             COMMENT '名称'
    ,close                    float                   COMMENT '最新价格'
    ,change_pct               float                   COMMENT '涨跌幅(%)'
    ,change_amt               float                   COMMENT '涨跌额'
    ,trading_volume           float                   COMMENT '成交量(手)'
    ,trading_amount           double                  COMMENT '成交额(元)'
    ,amplitude                float                   COMMENT '振幅(%)'
    ,high                     float                   COMMENT '最高'
    ,low                      float                   COMMENT '最低'
    ,open                     float                   COMMENT '今开'
    ,prev_close               float                   COMMENT '昨收'
    ,volume_ratio             float                   COMMENT '量比'
    ,turnover_rate            float                   COMMENT '换手率(%)'
    ,pe_dynamic               float                   COMMENT '市盈率-动态'
    ,pb                       float                   COMMENT '市净率'
    ,total_market             double                  COMMENT '总市值(元)'
    ,circulation_market       double                  COMMENT '流通市值(元)'
    ,price_rise_speed         float                   COMMENT '涨速'
    ,five_min_price_change    float                   COMMENT '5分钟涨跌(%)'
    ,sixty_day_price_change   float                   COMMENT '60日涨跌幅(%)'
    ,ytd_price_change         float                   COMMENT '年初至今涨跌幅(%)'
    ,UNIQUE KEY unique_ymd_stock_code (ymd, stock_code)
) COMMENT='行情数据_个股行情数据';


--4.8
------------------  ods_akshare_stock_board_concept_name_em   行情数据_板块行情数据           全量的每日切片数据 不可指定日期   板块三剑客1 
CREATE TABLE quant.ods_akshare_stock_board_concept_name_em (
     ymd                      DATE                    COMMENT '数据日期（核心日期维度，适配量化数据统一归档）'
    ,ranking                  int                     COMMENT '排名'
    ,board_name               varchar(100)            COMMENT '板块名称'
    ,board_code               varchar(50)             COMMENT '板块代码'
    ,close                    float                   COMMENT '最新价'
    ,change_amt               float                   COMMENT '涨跌额'
    ,change_pct               float                   COMMENT '涨跌幅(%)'
    ,total_market             double                  COMMENT '总市值'
    ,turnover_rate            float                   COMMENT '换手率(%)'
    ,rising_stocks_num        int                     COMMENT '上涨家数'
    ,falling_stocks_num       int                     COMMENT '下跌家数'
    ,leading_stock            varchar(100)            COMMENT '领涨股票'
    ,leading_stock_pct        float                   COMMENT '领涨股票-涨跌幅(%)'
    ,UNIQUE KEY unique_ymd_board_code (ymd, board_code)
) COMMENT='行情数据_板块行情数据';


--4.9
------------------  ods_akshare_stock_board_concept_cons_em   行情数据_板块内个股的行情数据    全量的每日切片数据 不可指定日期   板块三剑客2
CREATE TABLE quant.ods_akshare_stock_board_concept_cons_em (
     ymd                      DATE                    COMMENT '数据日期（核心日期维度，用于归档和跨表关联）'
    ,board_name               varchar(100)            COMMENT '板块名称'
    ,board_code               varchar(50)             COMMENT '板块代码'
    ,serial_num               int                     COMMENT '序号'
    ,stock_code               varchar(50)             COMMENT '代码'
    ,stock_name               varchar(50)             COMMENT '名称'
    ,close                    float                   COMMENT '最新价'
    ,change_pct               float                   COMMENT '涨跌幅(%)'
    ,change_amt               float                   COMMENT '涨跌额'
    ,trading_volume           float                   COMMENT '成交量(手)'
    ,trading_amount           float                   COMMENT '成交额'
    ,amplitude                float                   COMMENT '振幅(%)'
    ,high                     float                   COMMENT '最高'
    ,low                      float                   COMMENT '最低'
    ,open                     float                   COMMENT '今开'
    ,prev_close               float                   COMMENT '昨收'
    ,turnover_rate            float                   COMMENT '换手率(%)'
    ,pe_dynamic               float                   COMMENT '市盈率-动态'
    ,pb                       float                   COMMENT '市净率'
    ,UNIQUE KEY unique_ymd_stock_code (ymd, stock_code)
) COMMENT='行情数据_板块内个股的行情数据';


--4.10
------------------  ods_akshare_stock_board_concept_hist_em   行情数据_板块历史行情数据    可指定日期范围   板块三剑客3
CREATE TABLE quant.ods_akshare_stock_board_concept_hist_em (
     ymd                      DATE                    COMMENT '日期（行情交易日）'
    ,board_code               varchar(50)             COMMENT '板块代码（补充字段，关联板块基础信息，适配量化关联分析）'
    ,open                     float                   COMMENT '开盘'
    ,close                    float                   COMMENT '收盘'
    ,high                     float                   COMMENT '最高'
    ,low                      float                   COMMENT '最低'
    ,change_pct               float                   COMMENT '涨跌幅(%)'
    ,change_amt               float                   COMMENT '涨跌额'
    ,trading_volume           bigint                  COMMENT '成交量'
    ,trading_amount           double                  COMMENT '成交额'
    ,amplitude                float                   COMMENT '振幅(%)'
    ,turnover_rate            float                   COMMENT '换手率(%)'
    ,UNIQUE KEY unique_ymd_board_code (ymd, board_code)
) COMMENT='行情数据_板块历史行情数据';




--4.11
------------------  ods_akshare_board_concept_name_ths          行情数据_同花顺板块码值       全量的每日切片数据 不可指定日期   同花顺板块三剑客1 
CREATE TABLE quant.ods_akshare_board_concept_name_ths (
     ymd                      DATE                    COMMENT '数据日期（核心日期维度，适配量化数据统一归档）'
    ,board_name               varchar(100)            COMMENT '板块名称'
    ,board_code               varchar(50)             COMMENT '板块代码'
    ,UNIQUE KEY unique_ymd_board_code (ymd, board_code)
) COMMENT='板块数据_同花顺板块码值';


--4.12
------------------  ods_akshare_stock_board_concept_index_ths   行情数据_同花顺板块行情数据    全量的每日切片数据 不可指定日期   板块三剑客2
CREATE TABLE quant.ods_akshare_stock_board_concept_index_ths (
     ymd                      DATE                    COMMENT '数据日期（核心日期维度，用于归档和跨表关联）'
    ,board_name               varchar(100)            COMMENT '板块名称'
    ,board_code               varchar(50)             COMMENT '板块代码'
    ,open                     float                   COMMENT '今开'
    ,high                     float                   COMMENT '最高'
    ,low                      float                   COMMENT '最低'
    ,close                    float                   COMMENT '最新价'
    ,trading_volume           float                   COMMENT '成交量(手)'
    ,trading_amount           float                   COMMENT '成交额'
    ,UNIQUE KEY unique_ymd_board_code (ymd, board_code)
) COMMENT='行情数据_板块内个股的行情数据';



--4.13
------------------  ods_akshare_stock_board_concept_index_ths   行情数据_同花顺板块内含股票    手动跑爬虫获取
CREATE TABLE quant.ods_akshare_stock_board_concept_maps_ths (
     ymd                      DATE                    COMMENT '数据日期（核心日期维度，用于归档和跨表关联）'
    ,board_name               varchar(100)            COMMENT '板块名称'
    ,board_code               varchar(50)             COMMENT '板块代码'
    ,stock_code               varchar(50)             COMMENT '代码'
    ,stock_name               varchar(50)             COMMENT '名称'
    ,UNIQUE KEY unique_ymd_board_code (ymd, board_code)
) COMMENT='行情数据_同花顺板块内含股票';



----------------------------------------- tushare 数据  ----------------------------------------------
-- ============ 1. 同花顺板块指数列表 (ths_index) ============
-- ============ 1. 同花顺板块码值（基础信息） ============
-- 对应接口：ths_index
CREATE TABLE quant.ods_tushare_board_concept_name_ths (
     ymd                      DATE                    COMMENT '数据日期（核心日期维度，适配量化数据统一归档）'
    ,board_name               varchar(100)            COMMENT '板块名称'
    ,board_code               varchar(50)             COMMENT '板块代码'
    ,component_count          int                     COMMENT '成分个数'
    ,market                   varchar(10)             COMMENT '交易所（A-沪深 HK-港股 US-美股）'
    ,list_date                varchar(8)              COMMENT '上市日期YYYYMMDD'
    ,index_type               varchar(10)             COMMENT '指数类型（N-概念 I-行业 R-地域 S-特色 ST-风格 TH-主题 BB-宽基）'
    ,UNIQUE KEY unique_ymd_board_code (ymd, board_code)
) COMMENT='板块数据_同花顺板块码值_Tushare';


-- ============ 2. 同花顺板块历史行情数据 ============
-- 对应接口：ths_daily
CREATE TABLE quant.ods_tushare_stock_board_concept_hist_ths (
     ymd                      DATE                    COMMENT '日期（行情交易日）'
    ,board_name               varchar(100)            COMMENT '板块名称'
    ,board_code               varchar(50)             COMMENT '板块代码'
    ,open                     float                   COMMENT '开盘点位'
    ,high                     float                   COMMENT '最高点位'
    ,low                      float                   COMMENT '最低点位'
    ,close                    float                   COMMENT '收盘点位'
    ,prev_close               float                   COMMENT '昨日收盘点'
    ,avg_price                float                   COMMENT '平均价'
    ,change_amt               float                   COMMENT '涨跌点位'
    ,change_pct               float                   COMMENT '涨跌幅(%)'
    ,trading_volume           float                   COMMENT '成交量'
    ,turnover_rate            float                   COMMENT '换手率(%)'
    ,total_market_value       float                   COMMENT '总市值'
    ,float_market_value       float                   COMMENT '流通市值'
    ,UNIQUE KEY unique_ymd_board_code (ymd, board_code)
) COMMENT='行情数据_同花顺板块历史行情_Tushare';


-- ============ 3. 同花顺板块内含股票（核心） ============
-- 对应接口：ths_member
CREATE TABLE quant.ods_tushare_stock_board_concept_maps_ths (
     ymd                      DATE                    COMMENT '数据日期（核心日期维度，用于归档和跨表关联）'
    ,board_name               varchar(100)            COMMENT '板块名称'
    ,board_code               varchar(50)             COMMENT '板块代码'
    ,stock_code               varchar(50)             COMMENT '股票代码'
    ,stock_name               varchar(100)            COMMENT '股票名称'
    ,weight                   float                   COMMENT '权重'
    ,in_date                  varchar(8)              COMMENT '纳入日期YYYYMMDD'
    ,out_date                 varchar(8)              COMMENT '剔除日期YYYYMMDD'
    ,is_new                   varchar(1)              COMMENT '是否最新(Y/N)'
    ,UNIQUE KEY unique_ymd_board_stock (ymd, board_code, stock_code)
) COMMENT='行情数据_同花顺板块内含股票_Tushare';


-- ============ 4. 东方财富概念板块行情数据 ============
-- 对应接口：dc_index
CREATE TABLE quant.ods_tushare_stock_board_concept_name_em (
     ymd                      DATE                    COMMENT '数据日期（核心日期维度，适配量化数据统一归档）'
    ,board_name               varchar(100)            COMMENT '板块名称'
    ,board_code               varchar(50)             COMMENT '板块代码'
    ,leading_stock            varchar(100)            COMMENT '领涨股票名称'
    ,leading_stock_code       varchar(50)             COMMENT '领涨股票代码'
    ,change_pct               float                   COMMENT '涨跌幅(%)'
    ,leading_stock_pct        float                   COMMENT '领涨股票涨跌幅(%)'
    ,total_market_value       float                   COMMENT '总市值（万元）'
    ,turnover_rate            float                   COMMENT '换手率(%)'
    ,rising_stocks_num        int                     COMMENT '上涨家数'
    ,falling_stocks_num       int                     COMMENT '下降家数'
    ,UNIQUE KEY unique_ymd_board_code (ymd, board_code)
) COMMENT='行情数据_东方财富概念板块行情_Tushare';


-- ============ 5. 东方财富板块历史行情数据 ============
-- 对应接口：dc_daily
CREATE TABLE quant.ods_tushare_stock_board_concept_hist_em (
     ymd                      DATE                    COMMENT '日期（行情交易日）'
    ,board_name               varchar(100)            COMMENT '板块名称'
    ,board_code               varchar(50)             COMMENT '板块代码'
    ,open                     float                   COMMENT '开盘点位'
    ,high                     float                   COMMENT '最高点位'
    ,low                      float                   COMMENT '最低点位'
    ,close                    float                   COMMENT '收盘点位'
    ,change_amt               float                   COMMENT '涨跌点位'
    ,change_pct               float                   COMMENT '涨跌幅(%)'
    ,trading_volume           float                   COMMENT '成交量(股)'
    ,trading_amount           float                   COMMENT '成交额(元)'
    ,amplitude                float                   COMMENT '振幅(%)'
    ,turnover_rate            float                   COMMENT '换手率(%)'
    ,category                 varchar(20)             COMMENT '板块类型（概念板块/行业板块/地域板块）'
    ,UNIQUE KEY unique_ymd_board_code (ymd, board_code)
) COMMENT='行情数据_东方财富板块历史行情_Tushare';


-- ============ 6. 东方财富板块内含股票 ============
-- 对应接口：dc_member
CREATE TABLE quant.ods_tushare_stock_board_concept_maps_em (
     ymd                      DATE                    COMMENT '数据日期（核心日期维度，用于归档和跨表关联）'
    ,board_name               varchar(100)            COMMENT '板块名称'
    ,board_code               varchar(50)             COMMENT '板块代码'
    ,stock_code               varchar(50)             COMMENT '成分股票代码'
    ,stock_name               varchar(100)            COMMENT '成分股名称'
    ,UNIQUE KEY unique_ymd_board_stock (ymd, board_code, stock_code)
) COMMENT='行情数据_东方财富板块内含股票_Tushare';





--6.1
------------------  ods_stock_kline_daily_ts   行情数据_A股历史日K线的tushare数据
CREATE TABLE quant.ods_stock_kline_daily_ts (
     stock_code               VARCHAR(50) NOT NULL    COMMENT '股票代码'
    ,ymd                      DATE        NOT NULL    COMMENT '交易日期'
    ,open                     FLOAT                   COMMENT '开盘价'
    ,close                    FLOAT                   COMMENT '收盘价'
    ,high                     FLOAT                   COMMENT '最高价'
    ,low                      FLOAT                   COMMENT '最低价'
    ,change_pct               FLOAT                   COMMENT '当日涨跌幅(%)'
    ,volume                   BIGINT                  COMMENT '成交量'
    ,trading_amount           double                  COMMENT '成交额'
    ,UNIQUE KEY unique_ymd_stock_code (ymd, stock_code)
) COMMENT='行情数据_A股历史日K线的tushare数据';









--6.1        多渠道板块数据 -- 小红书
------------------  ods_stock_plate_redbook
CREATE TABLE quant.ods_stock_plate_redbook (
     ymd                      DATE        NOT NULL    COMMENT '日期'
    ,board_name               VARCHAR(50) NOT NULL    COMMENT '板块名称'
    ,stock_code               VARCHAR(50)             COMMENT '标的代码'
    ,stock_name               VARCHAR(50)             COMMENT '标的名称'
    ,remark                   VARCHAR(50)             COMMENT '备注'
) COMMENT='多渠道板块数据 -- 小红书';


--6.2        股票基本面数据_所属交易所，主板/创业板/科创板/北证
------------------  ods_stock_exchange_market
CREATE TABLE quant.ods_stock_exchange_market (
     ymd                      DATE        NOT NULL    COMMENT '日期'
    ,stock_code               VARCHAR(50)             COMMENT '标的代码'
    ,stock_name               VARCHAR(50)             COMMENT '标的名称'
    ,market                   VARCHAR(50)             COMMENT '市场特征主板创业板等'
    ,UNIQUE KEY unique_ymd_stock_code (ymd, stock_code)
) COMMENT='股票基本面数据_所属交易所，主板/创业板/科创板/北证';







