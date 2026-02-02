CREATE TABLE IF NOT EXISTS daily_crypto_summary AS
SELECT
    symbol,
    DATE(timestamp) AS date,
    AVG(current_price) AS avg_price,
    MAX(current_price) AS max_price,
    MIN(current_price) AS min_price
FROM fact_crypto_prices
GROUP BY symbol, DATE(timestamp);
