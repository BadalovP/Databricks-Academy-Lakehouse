# Lab 06 External V2 Governance Update

Add these files to:

```text
labs/lab_06_gold_analytics_external/
├── notebooks/
│   └── lab06e_08_governance.ipynb
├── tests/
│   └── test_governance.py
└── sql/
    └── governance_policies_external.sql
```

Do NOT add governance as an eighth task to the recurring Gold Job.

Recommended order:

1. Run the normal 7-task Gold Job.
2. Open `lab06e_08_governance`.
3. Use:
   - catalog = `dbr_dev`
   - target_schema = `parvinbadalov_lab06_ext`
   - run_demo = `true`
4. Run all cells.
5. Capture:
   - `09_governance_rls.png`
   - `10_governance_cls.png`
6. Confirm final governance validation is PASS.
7. Leave the policies attached.

The notebook restores the invoking user to full RLS access and privileged CLS
access at the end so future Job runs under the same identity are not restricted.

For another non-privileged user to experience the policies, that user/group must
first receive normal SELECT permission. If they have SELECT but no RLS mapping,
`fact_encounters` returns zero rows; if they are not in the privileged mapping,
the sensitive `dim_patient` columns are masked.
