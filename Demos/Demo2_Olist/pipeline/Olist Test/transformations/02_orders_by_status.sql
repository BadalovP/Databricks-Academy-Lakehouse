-- Purpose:
-- Aggregate the Python-created materialized view by order status.

CREATE OR REFRESH MATERIALIZED VIEW learning_orders_by_status
COMMENT 'Olist order metrics grouped by order status'
AS
SELECT
    order_status,
    COUNT(*) AS order_item_rows,
    COUNT(DISTINCT order_id) AS distinct_orders,
    ROUND(SUM(price), 2) AS total_price,
    ROUND(SUM(freight_value), 2) AS total_freight,
    ROUND(SUM(item_total_value), 2) AS total_value
FROM learning_orders_base
GROUP BY order_status;