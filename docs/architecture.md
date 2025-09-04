# Architecture

Client (MapLibre) → FastAPI → Athena → S3/Glue. FastAPI exposes /health, /events, /events/summary, /metrics with caching, logging, and CORS.

`mermaid
flowchart LR
A[MapLibre Web UI] -->|filters,bbox| B[FastAPI API]
B -->|cached?| B
B -->|SQL| C[Athena]
C -->|schema| D[Glue Catalog]
C -->|scan| E[S3 Parquet (partitioned)]
B -->|GeoJSON/CSV| A
B --> F[CloudWatch Logs/Metrics]
G[GitHub Actions] --> H[GHCR/ECR]
G --> I[IaC Deploy (ECS or Lambda+APIGW)]
