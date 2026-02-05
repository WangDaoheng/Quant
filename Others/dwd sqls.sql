DELETE  FROM quant.dwd_ashare_stock_base_info WHERE  ymd = '{ymd}';


insert IGNORE  into quant.dwd_ashare_stock_base_info 
select 
      tkline.ymd                                         
     ,tpbe.stock_code                                    
     ,tpbe.stock_name                                    
     ,tkline.close                                       
     ,tpbe.circulation_market                                  
     ,tpbe.total_market  
     ,''                                 
     ,''                                     
     ,''                                 
     ,''                                 
     ,tpbe.shareholder_num                               
     ,tpbe.pb                                            
     ,tpbe.pe_ttm                                            
     ,texchange.market                                   
     ,tplate.plate_names         
     ,tconcept.plate_names             as concept_plate
     ,tindex.plate_names               as index_plate
     ,tindustry.plate_names            as industry_plate
     ,tstyle.plate_names               as style_plate
     ,tout.plate_names                 as out_plate
from  

( select
      stock_code
     ,ymd
     ,open
     ,close
     ,high
     ,low
     ,change_pct
     ,volume
     ,trading_amount
  from  quant.ods_stock_kline_daily_ts
  where  ymd={ymd}
) tkline


( select
      ymd                 --数据日期
     ,stock_code          --股票代码
     ,close               --当日收盘价(元)
     ,change_pct          --当日涨跌幅(%)
     ,total_market        --总市值(元)
     ,circulation_market  --流通市值(元)
     ,total_shares        --总股本(股)
     ,circulation_shares  --流通股本(股)
     ,pe_ttm              --PE(TTM)
     ,pe_static           --PE(静)
     ,pb                  --市净率
     ,peg                 --PEG值
     ,pcf                 --市现率
     ,ps                  --市销率
  from  quant.ods_akshare_stock_value_em
  where ymd=SELECT MAX(ymd) FROM quant.ods_akshare_stock_value_em)
) tpbe













left join 
( select 
      ymd                                                
     ,stock_code                                         
     ,stock_name                                         
     ,market_value                                       
     ,total_asset                                        
     ,net_asset                                          
     ,total_capital                                      
     ,float_capital                                      
     ,shareholder_num                                    
     ,pb                                                 
     ,pe                                                 
     ,industry                                           
  from  quant.ods_tdx_stock_pepb_info 
  WHERE ymd = (SELECT MAX(ymd) FROM quant.ods_tdx_stock_pepb_info)
) tpbe
ON SUBSTRING_INDEX(tkline.stock_code, '.', 1) = tpbe.stock_code
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
ON SUBSTRING_INDEX(tkline.stock_code, '.', 1) = tplate.stock_code
LEFT JOIN 
    (
        SELECT 
            ymd,                                              
            stock_code,                                       
            GROUP_CONCAT(plate_name ORDER BY plate_name SEPARATOR ',') AS plate_names   
        FROM quant.dwd_stock_a_total_plate  
        WHERE ymd = (SELECT MAX(ymd) FROM quant.dwd_stock_a_total_plate)
          AND source_table = 'ods_tdx_stock_concept_plate'
        GROUP BY ymd, stock_code
    ) tconcept
ON SUBSTRING_INDEX(tkline.stock_code, '.', 1) = tconcept.stock_code
LEFT JOIN 
    (
        SELECT 
            ymd,                                              
            stock_code,                                       
            GROUP_CONCAT(plate_name ORDER BY plate_name SEPARATOR ',') AS plate_names   
        FROM quant.dwd_stock_a_total_plate  
        WHERE ymd = (SELECT MAX(ymd) FROM quant.dwd_stock_a_total_plate)
          AND source_table = 'ods_tdx_stock_index_plate'
        GROUP BY ymd, stock_code
    ) tindex
ON SUBSTRING_INDEX(tkline.stock_code, '.', 1) = tindex.stock_code
LEFT JOIN 
    (
        SELECT 
            ymd,                                              
            stock_code,                                       
            GROUP_CONCAT(plate_name ORDER BY plate_name SEPARATOR ',') AS plate_names   
        FROM quant.dwd_stock_a_total_plate  
        WHERE ymd = (SELECT MAX(ymd) FROM quant.dwd_stock_a_total_plate)
          AND source_table = 'ods_tdx_stock_industry_plate'
        GROUP BY ymd, stock_code
    ) tindustry
ON SUBSTRING_INDEX(tkline.stock_code, '.', 1) = tindustry.stock_code
LEFT JOIN 
    (
        SELECT 
            ymd,                                              
            stock_code,                                       
            GROUP_CONCAT(plate_name ORDER BY plate_name SEPARATOR ',') AS plate_names   
        FROM quant.dwd_stock_a_total_plate  
        WHERE ymd = (SELECT MAX(ymd) FROM quant.dwd_stock_a_total_plate)
          AND source_table = 'ods_tdx_stock_style_plate'
        GROUP BY ymd, stock_code
    ) tstyle
ON SUBSTRING_INDEX(tkline.stock_code, '.', 1) = tstyle.stock_code
LEFT JOIN 
    (
        SELECT 
            ymd,                                              
            stock_code,                                       
            GROUP_CONCAT(plate_name ORDER BY plate_name SEPARATOR ',') AS plate_names   
        FROM quant.dwd_stock_a_total_plate  
        WHERE ymd = (SELECT MAX(ymd) FROM quant.dwd_stock_a_total_plate)
          AND source_table = 'ods_stock_plate_redbook'
        GROUP BY ymd, stock_code
    ) tout
ON SUBSTRING_INDEX(tkline.stock_code, '.', 1) = tout.stock_code;























