
--1.1
------------------  ods_stock_code_daily_insight   当日已上市股票码表
CREATE TABLE quant.ods_stock_code_daily_insight (
     ymd          DATE NOT NULL            --交易日期
    ,htsc_code    VARCHAR(50) NOT NULL     --股票代码
    ,name         VARCHAR(50)              --股票名
    ,exchange     VARCHAR(50)              --交易所名称
    ,UNIQUE KEY unique_ymd_stock_code (ymd, htsc_code)
);


--1.2
------------------  ods_stock_kline_daily_insight   当日已上市股票的历史日K
CREATE TABLE quant.ods_stock_kline_daily_insight_now (
     htsc_code    VARCHAR(50) NOT NULL    --股票代码
    ,ymd          DATE NOT NULL           --交易日期
    ,open         FLOAT                   --开盘价
    ,close        FLOAT                   --收盘价
    ,high         FLOAT                   --最高价
    ,low          FLOAT                   --最低价
    ,num_trades   BIGINT                  --交易笔数
    ,volume       BIGINT                  --成交量
    ,UNIQUE KEY unique_ymd_stock_code (ymd, htsc_code)
) ;


CREATE TABLE quant.ods_stock_kline_daily_insight (
     htsc_code    VARCHAR(50) NOT NULL    --股票代码
    ,ymd          DATE NOT NULL           --交易日期
    ,open         FLOAT                   --开盘价
    ,close        FLOAT                   --收盘价
    ,high         FLOAT                   --最高价
    ,low          FLOAT                   --最低价
    ,num_trades   BIGINT                  --交易笔数
    ,volume       BIGINT                  --成交量
    ,UNIQUE KEY unique_ymd_stock_code (ymd, htsc_code)
);


--1.3
------------------  ods_index_a_share_insight   大A的主要指数日K
CREATE TABLE quant.ods_index_a_share_insight_now (
     htsc_code    VARCHAR(50) NOT NULL    --指数代码
    ,name         VARCHAR(50) NOT NULL    --指数名称
    ,ymd          DATE NOT NULL           --交易日期
    ,open         FLOAT                   --开盘价
    ,close        FLOAT                   --收盘价
    ,high         FLOAT                   --最高价
    ,low          FLOAT                   --最低价
    ,volume       BIGINT                  --成交量
    ,UNIQUE KEY unique_ymd_stock_code (ymd, htsc_code)
) ;


CREATE TABLE quant.ods_index_a_share_insight (
     htsc_code    VARCHAR(50) NOT NULL    --指数代码
    ,name         VARCHAR(50) NOT NULL    --指数名称
    ,ymd          DATE NOT NULL           --交易日期
    ,open         FLOAT                   --开盘价
    ,close        FLOAT                   --收盘价
    ,high         FLOAT                   --最高价
    ,low          FLOAT                   --最低价
    ,volume       BIGINT                  --成交量
    ,UNIQUE KEY unique_ymd_stock_code (ymd, htsc_code)
);


--1.4
------------------  ods_stock_limit_summary_insight   当日大A行情温度
CREATE TABLE quant.ods_stock_limit_summary_insight_now (
     ymd          DATE NOT NULL           --日期
    ,name         VARCHAR(50) NOT NULL    --市场名称
    ,today_ZT     INT                     --今日涨停股票数
    ,today_DT     INT                     --今日跌停股票数
    ,yesterday_ZT INT                     --昨日涨停股票数
    ,yesterday_DT INT                     --昨日跌停股票数
    ,yesterday_ZT_rate FLOAT              --昨日涨停股票的今日平均涨幅
    ,UNIQUE KEY unique_ymd_name (ymd, name)
) ;


CREATE TABLE quant.ods_stock_limit_summary_insight (
     ymd          DATE NOT NULL           --日期
    ,name         VARCHAR(50) NOT NULL    --市场名称
    ,today_ZT     INT                     --今日涨停股票数
    ,today_DT     INT                     --今日跌停股票数
    ,yesterday_ZT INT                     --昨日涨停股票数
    ,yesterday_DT INT                     --昨日跌停股票数
    ,yesterday_ZT_rate FLOAT              --昨日涨停股票的今日平均涨幅
    ,UNIQUE KEY unique_ymd_name (ymd, name)
) ;


--1.5
------------------  ods_future_inside_insight   内盘主要期货数据日K
CREATE TABLE quant.ods_future_inside_insight_now (
     htsc_code      VARCHAR(50) NOT NULL  --期货标的代码
    ,ymd            DATE NOT NULL         --交易日期
    ,open           FLOAT                 --开盘价
    ,close          FLOAT                 --收盘价
    ,high           FLOAT                 --最高价
    ,low            FLOAT                 --最低价
    ,volume         BIGINT                --成交量
    ,open_interest  BIGINT
    ,settle         BIGINT
    ,UNIQUE KEY unique_ymd_stock_code (ymd, htsc_code)
) ;


CREATE TABLE quant.ods_future_inside_insight (
     htsc_code      VARCHAR(50) NOT NULL  --期货标的代码
    ,ymd            DATE NOT NULL         --交易日期
    ,open           FLOAT                 --开盘价
    ,close          FLOAT                 --收盘价
    ,high           FLOAT                 --最高价
    ,low            FLOAT                 --最低价
    ,volume         BIGINT                --成交量
    ,open_interest  BIGINT
    ,settle         BIGINT
    ,UNIQUE KEY unique_ymd_stock_code (ymd, htsc_code)
) ;



--1.6
------------------  ods_stock_chouma_insight   A股的筹码分布数据

CREATE TABLE quant.ods_stock_chouma_insight (
    htsc_code                                VARCHAR(50) NOT NULL     --证券代码
   ,ymd                                      DATE NOT NULL            --交易日
   ,exchange                                 VARCHAR(50)              --交易所
   ,last                                     FLOAT                    --最新价格
   ,prev_close                               FLOAT                    --昨收价格
   ,total_share                              BIGINT                   --总股本（股）
   ,a_total_share                            BIGINT                   --A股总数(股)
   ,a_listed_share                           BIGINT                   --流通a股（万股）
   ,listed_share                             BIGINT                   --流通股总数
   ,restricted_share                         BIGINT                   --限售股总数
   ,cost_5pct                                FLOAT                    --5分位持仓成本（持仓成本最低的 5%的持仓成本）
   ,cost_15pct                               FLOAT                    --15分位持仓成本
   ,cost_50pct                               FLOAT                    --50分位持仓成本
   ,cost_85pct                               FLOAT                    --85分位持仓成本
   ,cost_95pct                               FLOAT                    --95分位持仓成本
   ,avg_cost                                 FLOAT                    --流通股加权平均持仓成本
   ,max_cost                                 FLOAT                    --流通股最大持仓成本
   ,min_cost                                 FLOAT                    --流通股最小持仓成本
   ,winner_rate                              FLOAT                    --流通股获利胜率
   ,diversity                                FLOAT                    --流通股筹码分散程度百分比
   ,pre_winner_rate                          FLOAT                    --流通股昨日获利胜率
   ,restricted_avg_cost                      FLOAT                    --限售股平均持仓成本
   ,restricted_max_cost                      FLOAT                    --限售股最大持仓成本
   ,restricted_min_cost                      FLOAT                    --限售股最小持仓成本
   ,large_shareholders_avg_cost              FLOAT                    --大流通股股东持股平均持仓成本
   ,large_shareholders_total_share           FLOAT                    --大流通股股东持股总数
   ,large_shareholders_total_share_pct       FLOAT                    --大流通股股东持股占总股本的比例
   ,UNIQUE KEY unique_ymd_stock_code (ymd, htsc_code)
 );





--1.7
------------------  ods_astock_industry_overview   行业分类，申万三级分类
CREATE TABLE quant.ods_astock_industry_overview (
    ymd                  DATE                  --交易日期
   ,classified           varchar(100)          --行业分类
   ,industry_name        varchar(100)          --行业名称
   ,industry_code        varchar(100)          --行业代码
   ,l1_code              varchar(100)          --一级行业代码
   ,l1_name              varchar(100)          --一级行业名称
   ,l2_code              varchar(100)          --二级行业代码
   ,l2_name              varchar(100)          --二级行业名称
   ,l3_code              varchar(100)          --三级行业代码
   ,l3_name              varchar(100)          --三级行业名称
   ,UNIQUE KEY unique_industry_code (ymd, industry_code)
 );


--1.8
------------------  ods_astock_industry_detail   股票&行业的关联
CREATE TABLE quant.ods_astock_industry_detail (
    ymd              DATE                      --交易日期
   ,htsc_code        varchar(100)              --股票代码
   ,name             varchar(100)              --股票名称
   ,industry_name    varchar(100)              --行业名称
   ,industry_code    varchar(100)              --行业代码
   ,l1_code          varchar(100)              --一级行业代码
   ,l1_name          varchar(100)              --一级行业名称
   ,l2_code          varchar(100)              --二级行业代码
   ,l2_name          varchar(100)              --二级行业名称
   ,l3_code          varchar(100)              --三级行业代码
   ,l3_name          varchar(100)              --三级行业名称
   ,UNIQUE KEY unique_industry_code (ymd, htsc_code)
);


--1.9
------------------  ods_shareholder_num   个股的股东数

CREATE TABLE quant.ods_shareholder_num_now (
       htsc_code              varchar(100)            --股票代码
      ,name                   varchar(100)            --股票名称
      ,ymd                    DATE                    --交易日期
      ,total_sh               DOUBLE                  --总股东数
      ,avg_share              DOUBLE(10, 4)           --每个股东平均持股数
      ,pct_of_total_sh        DOUBLE(10, 4)           --股东数较上期环比波动百分比
      ,pct_of_avg_sh          DOUBLE(10, 4)           --每个股东平均持股数较上期环比波动百分比
      ,UNIQUE KEY unique_ymd_stock_code (ymd, htsc_code)
);

CREATE TABLE quant.ods_shareholder_num (
       htsc_code              varchar(100)            --股票代码
      ,name                   varchar(100)            --股票名称
      ,ymd                    DATE                    --交易日期
      ,total_sh               DOUBLE                  --总股东数
      ,avg_share              DOUBLE(10, 4)           --每个股东平均持股数
      ,pct_of_total_sh        DOUBLE(10, 4)           --股东数较上期环比波动百分比
      ,pct_of_avg_sh          DOUBLE(10, 4)           --每个股东平均持股数较上期环比波动百分比
      ,UNIQUE KEY unique_ymd_stock_code (ymd, htsc_code)
);



--1.10
------------------  ods_north_bound_daily   北向持仓数据
CREATE TABLE quant.ods_north_bound_daily_now (
      htsc_code            varchar(100)
     ,ymd                  DATE
     ,sh_hkshare_hold      BIGINT
     ,pct_total_share      FLOAT
     ,UNIQUE KEY unique_ymd_stock_code (ymd, htsc_code)
 );


CREATE TABLE quant.ods_north_bound_daily (
      htsc_code            varchar(100)
     ,ymd                  DATE
     ,sh_hkshare_hold      BIGINT
     ,pct_total_share      FLOAT
     ,UNIQUE KEY unique_ymd_stock_code (ymd, htsc_code)
 );



--2.1
------------------  ods_us_stock_daily_vantage   美股 日K
CREATE TABLE quant.ods_us_stock_daily_vantage (
     name     VARCHAR(50) NOT NULL          --股票名称
    ,ymd      DATE        NOT NULL          --交易日期
    ,open     FLOAT                         --开盘价
    ,high     FLOAT                         --最高价
    ,low      FLOAT                         --最低价
    ,close    FLOAT                         --收盘价
    ,volume   BIGINT                        --成交量
    ,UNIQUE KEY unique_ymd_name (ymd, name)
) ;



--2.2
------------------  ods_exchange_rate_vantage_detail   汇率&美元指数 日K
CREATE TABLE quant.ods_exchange_rate_vantage_detail (
     name      VARCHAR(50) NOT NULL         --货币对
    ,ymd       DATE        NOT NULL         --交易日期
    ,open      FLOAT                        --开盘价
    ,high      FLOAT                        --最高价
    ,low       FLOAT                        --最低价
    ,close     FLOAT                        --收盘价
    ,UNIQUE KEY unique_ymd_name (ymd, name)
) ;


--2.3
------------------  ods_exchange_dxy_vantage   美元指数 日K
CREATE TABLE quant.ods_exchange_dxy_vantage (
    ymd DATE NOT NULL,
    name VARCHAR(50) NOT NULL,
    UNIQUE KEY unique_ymd_name (ymd, name)
) ;




--3.1        通达信数据
------------------  ods_tdx_stock_concept_plate   通达信概念板块数据
CREATE TABLE quant.ods_tdx_stock_concept_plate (
     ymd DATE NOT NULL                    --日期
    ,concept_code VARCHAR(50) NOT NULL    --概念板块代码
    ,concept_name VARCHAR(50)             --概念板块名称
    ,stock_code VARCHAR(50)               --股票代码
    ,stock_name VARCHAR(50)               --股票名称
) ;


--3.2        通达信数据
------------------  ods_tdx_stock_style_plate   通达信风格板块数据
CREATE TABLE quant.ods_tdx_stock_style_plate (
     ymd DATE NOT NULL                    --日期
    ,style_code VARCHAR(50) NOT NULL    --概念板块代码
    ,style_name VARCHAR(50)             --概念板块名称
    ,stock_code VARCHAR(50)               --股票代码
    ,stock_name VARCHAR(50)               --股票名称
) ;


--3.3        通达信数据
------------------  ods_tdx_stock_industry_plate   通达信行业板块数据
CREATE TABLE quant.ods_tdx_stock_industry_plate (
     ymd DATE NOT NULL                    --日期
    ,industry_code VARCHAR(50) NOT NULL   --行业板块代码
    ,industry_name VARCHAR(50)            --行业板块名称
    ,stock_code VARCHAR(50)               --股票代码
    ,stock_name VARCHAR(50)               --股票名称
) ;


--3.4        通达信数据
------------------  ods_tdx_stock_region_plate   通达信地区板块数据
CREATE TABLE quant.ods_tdx_stock_region_plate (
     ymd         DATE NOT NULL            --日期
    ,region_code VARCHAR(50) NOT NULL     --地区板块代码
    ,region_name VARCHAR(50)              --地区板块名称
    ,stock_code  VARCHAR(50)              --股票代码
    ,stock_name  VARCHAR(50)              --股票名称
) ;


--3.5        通达信数据
------------------  ods_tdx_stock_index_plate   通达信指数板块数据
CREATE TABLE quant.ods_tdx_stock_index_plate (
     ymd         DATE NOT NULL            --日期
    ,index_code  VARCHAR(50) NOT NULL     --指数板块代码
    ,index_name  VARCHAR(50)              --指数板块名称
    ,stock_code  VARCHAR(50)              --股票代码
    ,stock_name  VARCHAR(50)              --股票名称
) ;


--3.6        通达信数据
------------------  ods_tdx_stock_pepb_info   股票基本面数据_资产数据
CREATE TABLE quant.ods_tdx_stock_pepb_info (
     ymd              DATE               --日期
    ,stock_code       varchar(50)        --代码
    ,stock_name       varchar(50)        --名称
    ,market_value     double             --流通市值(亿)
    ,total_asset      double             --总资产(亿)
    ,net_asset        double             --净资产(亿)
    ,total_capital    double             --总股本(亿)
    ,float_capital    double             --流通股(亿)
    ,shareholder_num  bigint             --股东人数
    ,pb               double             --市净率
    ,pe               double             --市盈(动)
    ,industry         varchar(50)        --细分行业
    ,UNIQUE KEY unique_ymd_stock_code (ymd, stock_code)
) ;





--4.1        多渠道板块数据 -- 小红书
------------------  ods_stock_plate_redbook
CREATE TABLE quant.ods_stock_plate_redbook (
     ymd          DATE        NOT NULL      --日期
    ,plate_name   VARCHAR(50) NOT NULL      --板块名称
    ,stock_code   VARCHAR(50)               --标的代码
    ,stock_name   VARCHAR(50)               --标的名称
    ,remark       VARCHAR(50)               --备注
) ;


--4.2        多渠道板块数据 -- 多渠道汇总
------------------  dwd_stock_a_total_plate
CREATE TABLE quant.dwd_stock_a_total_plate (
     ymd          DATE        NOT NULL      --日期
    ,plate_name   VARCHAR(50) NOT NULL      --板块名称
    ,stock_code   VARCHAR(50)               --标的代码
    ,stock_name   VARCHAR(50)               --标的名称
    ,source_table VARCHAR(50)               --来源表
    ,remark       VARCHAR(50)               --备注
) ;


--5.1        股票基本面数据_所属交易所，主板/创业板/科创板/北证
------------------  ods_stock_exchange_market
CREATE TABLE quant.ods_stock_exchange_market (
     ymd          DATE        NOT NULL      --日期
    ,stock_code   VARCHAR(50)               --标的代码
    ,stock_name   VARCHAR(50)               --标的名称
    ,market       VARCHAR(50)               --市场特征主板创业板等
    ,UNIQUE KEY unique_ymd_stock_code (ymd, stock_code)
) ;

















