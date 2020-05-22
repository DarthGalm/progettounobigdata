INSERT OVERWRITE LOCAL DIRECTORY '~/Your/Dir'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
LINES TERMINATED BY '\n'
STORED AS TEXTFILE

SELECT result.sector, concat_ws(", ",collect_list(concat_ws(': ',cast(result.year as string),cast(result.avgVolume as string),cast(result.avgVariation as string),cast(result.avgClose as string))))
FROM(
SELECT grouped_by_ticker.sector, grouped_by_ticker.year, ROUND((sum(grouped_by_ticker.sumVolume) / sum(grouped_by_ticker.avgCounter)),3) as avgVolume, ROUND(avg(grouped_by_ticker.percent_variation),3) as avgVariation, ROUND(sum(grouped_by_ticker.sumClose) / sum(grouped_by_ticker.avgCounter)),3) as avgClose 
FROM(
SELECT ticker, sector, sum(volume) as sumVolume, sum(close) as sumClose, count(close) as avgCounter, YEAR(hsp.stockdate) as year, (((max(struct(hsp.stockdate,close)).col2) - (min(struct(hsp.stockdate,close)).col2))  / (min(struct(hsp.stockdate,close)).col2) * 100) as percent_variation
FROM historical_stocks AS hs JOIN historical_stock_prices AS hsp 
ON hs.ticker = hsp.ticker
WHERE stockdate >= '2008-01-01' AND stockdate <= '2018-12-31'
GROUP BY hsp.ticker, sector, YEAR(hsp.stockdate)
) grouped_by_ticker
GROUP BY grouped_by_ticker.sector, grouped_by_ticker.year
) result
GROUP BY sector

LIMIT 10;


