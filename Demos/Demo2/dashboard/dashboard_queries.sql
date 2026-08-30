-- Business KPI source. Dashboard filters operate on this trusted governed view.
SELECT
  order_date,
  country,
  city,
  loyalty_tier,
  category,
  product_name,
  sales_channel,
  order_status,
  customer_key,
  order_id,
  quantity,
  gross_amount,
  discount_amount,
  net_amount
FROM dbr_dev.parvinbadalov.demo2_sales_governed;

-- DQ batch trend and latest-batch counters.
SELECT *
FROM dbr_dev.parvinbadalov.demo2_dq_summary_gold
ORDER BY _batch_loaded_at, _source_batch_id;

-- DQ failures and warnings by concrete rule.
SELECT *
FROM dbr_dev.parvinbadalov.demo2_dq_failures_by_rule_gold
ORDER BY _batch_loaded_at, rule_name;
