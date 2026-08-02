-- Demo 1 - Crypto Dashboard Queries
-- Catalog: dbr_dev
-- Schema: parvinbadalov

-- ============================================================
-- 1. KPI cards: latest price and 24-hour change
-- Dataset: Latest Crypto Prices
-- Recommended visualization: Table or KPI cards
-- ============================================================

SELECT
    symbol,
    ROUND(latest_live_price, 2) AS latest_live_price,
    ROUND(change_pct_24h, 2) AS change_pct_24h,
    ROUND(high_price_24h, 2) AS high_price_24h,
    ROUND(low_price_24h, 2) AS low_price_24h,
    latest_live_event_time
FROM dbr_dev.parvinbadalov.demo1_crypto_latest_prices_gold
ORDER BY symbol;


-- ============================================================
-- 2. Latest live price versus latest historical close
-- Dataset: Live vs Historical Price
-- Recommended visualization: Grouped bar chart
-- X-axis: symbol
-- Y-axis: latest_live_price, latest_historical_close
-- ============================================================

SELECT
    symbol,
    ROUND(latest_live_price, 2) AS latest_live_price,
    ROUND(latest_historical_close, 2) AS latest_historical_close,
    ROUND(price_difference, 2) AS price_difference,
    ROUND(price_difference_pct, 2) AS price_difference_pct
FROM dbr_dev.parvinbadalov.demo1_crypto_latest_prices_gold
ORDER BY symbol;


-- ============================================================
-- 3. Historical daily closing-price trend
-- Dataset: Historical Daily Close
-- Recommended visualization: Line chart
-- X-axis: trade_date
-- Y-axis: close
-- Series: symbol
-- ============================================================

SELECT
    trade_date,
    symbol,
    close,
    volume,
    price_change_pct,
    candle_direction
FROM dbr_dev.parvinbadalov.demo1_crypto_ohlcv_silver
ORDER BY trade_date, symbol;


-- ============================================================
-- 4. Historical return by cryptocurrency
-- Dataset: Historical Return Summary
-- Recommended visualization: Bar chart
-- X-axis: symbol
-- Y-axis: historical_return_pct
-- ============================================================

SELECT
    symbol,
    ROUND(historical_return_pct, 2) AS historical_return_pct,
    ROUND(first_open_price, 2) AS first_open_price,
    ROUND(last_close_price, 2) AS last_close_price,
    historical_start_date,
    historical_end_date
FROM dbr_dev.parvinbadalov.demo1_crypto_market_summary_gold
ORDER BY historical_return_pct DESC;


-- ============================================================
-- 5. Historical high-low range
-- Dataset: Historical Price Range
-- Recommended visualization: Bar chart or table
-- ============================================================

SELECT
    symbol,
    ROUND(period_minimum_low, 2) AS period_minimum_low,
    ROUND(period_maximum_high, 2) AS period_maximum_high,
    ROUND(historical_price_range, 2) AS historical_price_range,
    ROUND(latest_live_price, 2) AS latest_live_price
FROM dbr_dev.parvinbadalov.demo1_crypto_market_summary_gold
ORDER BY symbol;


-- ============================================================
-- 6. Bullish versus bearish days
-- Dataset: Candle Direction Summary
-- Recommended visualization: Stacked bar chart
-- X-axis: symbol
-- Y-axis: bullish_days, bearish_days, neutral_days
-- ============================================================

SELECT
    symbol,
    bullish_days,
    bearish_days,
    neutral_days
FROM dbr_dev.parvinbadalov.demo1_crypto_market_summary_gold
ORDER BY symbol;


-- ============================================================
-- 7. Streaming price trend
-- Dataset: Live Streaming Price Trend
-- Recommended visualization: Line chart
-- X-axis: event_time
-- Y-axis: price_usd
-- Series: symbol
-- ============================================================

SELECT
    event_time,
    symbol,
    price_usd,
    change_pct_24h,
    producer_id
FROM dbr_dev.parvinbadalov.demo1_crypto_ticks_silver
ORDER BY event_time, symbol;


-- ============================================================
-- 8. Streaming operational metrics
-- Dataset: Streaming Metrics
-- Recommended visualization: Table or bar chart
-- ============================================================

SELECT
    symbol,
    streaming_event_count,
    evolved_event_count,
    ROUND(streaming_minimum_price, 2) AS streaming_minimum_price,
    ROUND(streaming_maximum_price, 2) AS streaming_maximum_price,
    ROUND(streaming_average_price, 2) AS streaming_average_price,
    ROUND(average_ingestion_delay_seconds, 2) AS average_ingestion_delay_seconds
FROM dbr_dev.parvinbadalov.demo1_crypto_market_summary_gold
ORDER BY symbol;


-- ============================================================
-- 9. Latest evolved 24-hour market statistics
-- Dataset: Latest 24-Hour Market Statistics
-- Recommended visualization: Table
-- ============================================================

SELECT
    symbol,
    ROUND(latest_live_price, 2) AS latest_live_price,
    ROUND(change_pct_24h, 2) AS change_pct_24h,
    ROUND(high_price_24h, 2) AS high_price_24h,
    ROUND(low_price_24h, 2) AS low_price_24h,
    ROUND(volume_24h, 2) AS volume_24h,
    latest_live_event_time
FROM dbr_dev.parvinbadalov.demo1_crypto_latest_prices_gold
ORDER BY symbol;


-- ============================================================
-- 10. Dashboard refresh timestamp
-- Dataset: Dashboard Metadata
-- Recommended visualization: Single-value text/KPI
-- ============================================================

SELECT
    MAX(gold_updated_at) AS dashboard_last_updated_at
FROM dbr_dev.parvinbadalov.demo1_crypto_latest_prices_gold;
