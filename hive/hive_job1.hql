INSERT OVERWRITE LOCAL DIRECTORY '/Users/filippofrillici/Desktop/APPUNTIUNI/BigData/PrimoProgetto/HiveQL/output'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
LINES TERMINATED BY '\n'
STORED AS TEXTFILE

SELECT ticker, MAX(close) as max_close, MIN(close) as min_close, AVG(volume) as avg_volume,((MAX(struct(stockdate,close)).col2 - MIN(struct(stockdate,close)).col2)/MIN(struct(stockdate,close)).col2 )*100 as percent_var 
FROM historical_stock_prices 
WHERE stockdate >= '2008-01-01' AND stockdate <= '2019-01-01' 
GROUP BY ticker
ORDER BY percent_var DESC;