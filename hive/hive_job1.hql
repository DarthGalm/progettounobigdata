INSERT OVERWRITE LOCAL DIRECTORY '~/Your/Dir'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ' '
LINES TERMINATED BY '\n'
STORED AS TEXTFILE

SELECT ticker, ROUND(((MAX(struct(stockdate,close)).col2 - MIN(struct(stockdate,close)).col2)/MIN(struct(stockdate,close)).col2 )*100) as percent_var, ',', concat_ws(", ", cast(ROUND(MIN(close),3) as string), cast(ROUND(MAX(close)) as string), cast(ROUND(AVG(volume)) as string))
FROM historical_stock_prices 
WHERE stockdate >= '2008-01-01' AND stockdate <= '2019-01-01' 
GROUP BY ticker
ORDER BY percent_var DESC
LIMIT 10;