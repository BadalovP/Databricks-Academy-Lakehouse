SELECT 'bookings_bronze' AS object_name, COUNT(*) AS row_count FROM dbr_dev.parvinbadalov_lab08_prod.bookings_bronze
UNION ALL SELECT 'booking_updates_bronze', COUNT(*) FROM dbr_dev.parvinbadalov_lab08_prod.booking_updates_bronze
UNION ALL SELECT 'users_bronze', COUNT(*) FROM dbr_dev.parvinbadalov_lab08_prod.users_bronze
UNION ALL SELECT 'payments_bronze', COUNT(*) FROM dbr_dev.parvinbadalov_lab08_prod.payments_bronze
UNION ALL SELECT 'gold_production_health', COUNT(*) FROM dbr_dev.parvinbadalov_lab08_prod.gold_production_health
UNION ALL SELECT 'gold_payment_reconciliation_mismatches', COUNT(*) FROM dbr_dev.parvinbadalov_lab08_prod.gold_payment_reconciliation WHERE reconciliation_status <> 'matched'
UNION ALL SELECT 'health_failed_checks', COUNT(*) FROM dbr_dev.parvinbadalov_lab08_prod.gold_production_health WHERE NOT health_passed
