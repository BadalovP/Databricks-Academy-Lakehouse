-- Metadata and serving checks for the fail-closed dynamic-view fallback.
DESCRIBE TABLE EXTENDED dbr_dev.parvinbadalov.demo2_sales_governed;

SHOW GRANTS ON VIEW dbr_dev.parvinbadalov.demo2_sales_governed;

SELECT
  SESSION_USER() AS session_user,
  COUNT(*) AS visible_rows,
  COUNT_IF(customer_name = '***MASKED***') AS masked_customer_rows,
  COUNT_IF(email = '***MASKED***') AS masked_email_rows
FROM dbr_dev.parvinbadalov.demo2_sales_governed;
