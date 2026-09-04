-- Purpose:
-- Compare the current pipeline output with the established project baseline.

CREATE OR REFRESH MATERIALIZED VIEW learning_quality_status
COMMENT 'Validation status for the mixed-language learning pipeline'
AS
SELECT
    order_item_rows,
    distinct_orders,
    total_price,
    total_freight,
    total_value,

    CASE
        WHEN order_item_rows = 112650
         AND distinct_orders = 98666
         AND total_price = 13591643.70
         AND total_freight = 2251909.54
         AND total_value = 15843553.24
        THEN 'PASS'
        ELSE 'REVIEW'
    END AS quality_status

FROM learning_pipeline_summary;