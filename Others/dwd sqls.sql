DELETE  FROM quant.dwd_ashare_stock_base_info WHERE  ymd = '{ymd}';


            insert IGNORE  into quant.dwd_ashare_stock_base_info 
            select 
                  tkline.ymd       
                 ,tkline.stock_code
                 ,tcode.stock_name 
                 ,tkline.close      
                 ,tkline.change_pct
                 ,tkline.volume
                 ,tkline.trading_amount
                 ,tpepb.circulation_market                 AS  market_value
                 ,tpepb.total_market                       AS  total_value
                 ,tpepb.total_shares                       AS  total_capital
                 ,tpepb.circulation_shares                 AS  float_capital
                 ,tshare.total_sh                          AS  shareholder_num
                 ,tshare.pct_of_total_sh                   AS  pct_of_total_sh
                 ,tpepb.pb                                 AS  pb
                 ,tpepb.pe_ttm                             AS  pe
                 ,texchange.market                         AS  market
                 ,tplate.plate_names                       AS  plate_names
            from  
            ( select
                  stock_code
                 ,ymd
                 ,close
                 ,change_pct
                 ,volume
                 ,trading_amount
              from  quant.ods_stock_kline_daily_ts
              where  ymd={ymd}
            ) tkline
            left join
            ( select
                  ymd
                 ,stock_code
                 ,stock_name
                 ,exchange
              from quant.ods_stock_code_daily_insight
              where ymd=(select max(ymd) from quant.ods_stock_code_daily_insight)
            ) tcode
            on tkline.stock_code = tcode.stock_code
            left join
            ( select
                  ymd                  
                 ,stock_code           
                 ,total_market         --总市值
                 ,circulation_market   --流通市值
                 ,total_shares         --总股本
                 ,circulation_shares   --流通股本
                 ,pe_ttm               --PE_TTM
                 ,pb                   --市净率
                 ,peg                  --PEG值
              from  quant.ods_akshare_stock_value_em
            ) tpepb
            ON SUBSTRING_INDEX(tkline.stock_code, '.', 1) = tpepb.stock_code
            left join 
            ( select
                  ymd            
                 ,stock_code     
                 ,stock_name     
                 ,total_sh       
                 ,avg_share      
                 ,pct_of_total_sh
                 ,pct_of_avg_sh  
              from  quant.dwd_shareholder_num_latest 
            ) tshare
            on tkline.stock_code = tshare.stock_code
            left join 
            ( select 
                  ymd                                               
                 ,stock_code                                        
                 ,stock_name                                        
                 ,market                                            
              from  quant.ods_stock_exchange_market 
              where ymd = (SELECT MAX(ymd) FROM quant.ods_stock_exchange_market)
            ) texchange 
            on tkline.stock_code = texchange.stock_code
            left join 
            (
              select 
                  ymd                                              
                 ,stock_code                                       
                 ,stock_name                                       
                 ,GROUP_CONCAT(plate_name ORDER BY plate_name SEPARATOR ',') AS plate_names   
              from  quant.dwd_stock_a_total_plate  
              where ymd = (SELECT MAX(ymd) FROM quant.dwd_stock_a_total_plate)
              group by ymd, stock_code, stock_name 
            ) tplate
            ON SUBSTRING_INDEX(tkline.stock_code, '.', 1)=SUBSTRING_INDEX(tplate.stock_code, '.', 1);











































