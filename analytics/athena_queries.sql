-- 1) Find Top 5 Stocks with the Highest Price Change:
SELECT symbol, price, previous_close,
       (price - previous_close) AS price_change
FROM stock_data_table
ORDER BY price_change DESC
LIMIT 5;

-- 2) Get Average Trading Volume Per Stock:
SELECT symbol, AVG(volume) AS avg_volume
FROM stock_data_table
GROUP BY symbol;

-- 3) Find Anomalous Stocks (Price Change > 5%):
SELECT symbol, price, previous_close,
       ROUND(((price - previous_close) / previous_close) * 100, 2) AS change_percent
FROM stock_data_table
WHERE ABS(((price - previous_close) / previous_close) * 100) > 5;

-- 4) Most recent record timestamp per stock:
SELECT symbol, MAX(timestamp) AS latest_time
FROM stock_data_table
GROUP BY symbol;

-- 5) Average price per stock:
SELECT symbol, AVG(price) AS avg_price
FROM stock_data_table
GROUP BY symbol;

-- 6) Price history for one stock:
SELECT timestamp, price
FROM stock_data_table
WHERE symbol = 'MSFT'
ORDER BY timestamp;

-- 7) Biggest movers (who moved most today):
SELECT
    symbol,
    MAX(ABS((price - previous_close) / previous_close * 100)) AS biggest_move_percent
FROM stock_data_table
GROUP BY symbol
ORDER BY biggest_move_percent DESC;

-- 8) Latest price snapshot (one row per stock):
SELECT t.symbol, t.price, t.timestamp
FROM stock_data_table t
JOIN (
    SELECT symbol, MAX(timestamp) AS latest_time
    FROM stock_data_table
    GROUP BY symbol
) latest
ON t.symbol = latest.symbol
AND t.timestamp = latest.latest_time
ORDER BY t.symbol;

