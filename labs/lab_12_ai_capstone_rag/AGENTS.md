# Lab 12 agent guardrails

- Work only in this Lab 12 directory and `.github/workflows/lab12_ci.yml`.
- Never deploy a bundle, create or delete workspace resources, run model inference, or trigger AI Search synchronization without explicit human approval.
- Treat `config/default.yml` as non-secret configuration. Never add tokens, client secrets, or credentials.
- Keep external systems behind interfaces and preserve deterministic offline tests.
- Use fictional data only. Do not paste employer incidents, customer data, or production credentials.
- Mark live evidence as unverified until a dated execution artifact is captured in `evidence/`.

