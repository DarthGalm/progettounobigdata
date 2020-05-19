INSERT OVERWRITE LOCAL DIRECTORY '/home/danilo/Desktop/out/'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
LINES TERMINATED BY '\n'
STORED AS TEXTFILE

SELECT concat_ws(" -- ",collect_list(company)), trend
FROM(
SELECT company, concat_ws(", ",collect_list(concat_ws(': ',cast(year as string), cast(percent_variation as string)))) as trend
FROM(
SELECT grouped_by_ticker.name as company, grouped_by_ticker.year as year, round(avg(grouped_by_ticker.percent_variation)) as percent_variation
FROM(
SELECT ticker, name as name, YEAR(hsp.stockdate) as year, (((max(struct(hsp.stockdate,close)).col2) - (min(struct(hsp.stockdate,close)).col2))  / (min(struct(hsp.stockdate,close)).col2) * 100) as percent_variation
FROM historical_stocks AS hs JOIN historical_stock_prices AS hsp 
ON hs.ticker = hsp.ticker
WHERE stockdate >= '2016-01-01' AND stockdate <= '2018-12-31'
GROUP BY hsp.ticker, hs.name, YEAR(hsp.stockdate)
) grouped_by_ticker
GROUP BY grouped_by_ticker.name, grouped_by_ticker.year
) grouped_by_company
GROUP BY company
) result
WHERE collect_list(company) > 1
GROUP BY trend;

LIMIT 10;


