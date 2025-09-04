# ADR-001: Athena vs DuckDB
**Decision:** Use Athena in prod; keep DuckDB for local/offline dev.
**Why:** Serverless scale + Glue catalog; DuckDB = fast local iteration.
**Trade-offs:** Two backends → define QueryBackend interface; shared SQL templates.
