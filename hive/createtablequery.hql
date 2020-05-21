CREATE TABLE if not exists historical_stock_prices (ticker STRING, open DOUBLE, close DOUBLE, adj_close DOUBLE, low DOUBLE, high DOUBLE, volume BIGINT, stockdate DATE) row format delimited fields terminated by ',' lines terminated by '\n';

CREATE TABLE if not exists historical_stocks (ticker STRING, stockexchange STRING, name STRING, sector STRING, industry STRING) ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde' WITH SERDEPROPERTIES ("separatorChar" = ",", "quoteChar" = "\""); 

LOAD DATA LOCAL INPATH '~/Path/To/file.csv' OVERWRITE INTO TABLE historical_stock_prices;

LOAD DATA LOCAL INPATH '~/Path/To/file.csv' OVERWRITE INTO TABLE historical_stocks;

