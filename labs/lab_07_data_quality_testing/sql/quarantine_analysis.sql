SELECT reason, COUNT(*) affected_rows FROM (SELECT EXPLODE(_dq_quarantine_reasons) reason FROM dbr_dev.parvinbadalov.business_license_quarantine) GROUP BY reason ORDER BY affected_rows DESC;
