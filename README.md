# Project A — StormEvents (FastAPI + DuckDB + GeoParquet)

Containerized API to explore NOAA StormEvents stored as GeoParquet.
- **ETL:** CSV → (Geo)Parquet via Python
- **Query:** DuckDB in-place over local Parquet
- **API:** FastAPI with time + bbox filters and a groupby summary
- **UI:** Minimal MapLibre page to plot events

## Quickstart
```bash
# 1) Generate Parquet (writes to data/parquet/stormevents/)
python etl/noaa_etl.py

# 2) Run the API
docker compose up --build

# 3) Try it
http://localhost:8000/docs
web/index.html  (open in your browser)
$rootReadme = @'
# Project A — StormEvents (FastAPI + DuckDB + GeoParquet)

Containerized API to explore NOAA StormEvents stored as GeoParquet.

- **ETL:** CSV → (Geo)Parquet via Python  
- **Query:** DuckDB in-place over local Parquet  
- **API:** FastAPI with time/bbox filters + summary  
- **UI:** Minimal MapLibre page to plot events

## Quickstart
1. Generate Parquet (writes to `data/parquet/stormevents/`)
2. Run the API
3. Try it:
- Swagger: http://localhost:8000/docs
- Map: open `web/index.html` in your browser

## Endpoints
- `GET /health` → status + `parquet_dir` + file count  
- `GET /events?start=&end=&bbox=&limit=` → `event_id, type, magnitude, lon, lat, date`  
- `GET /events/summary?groupby=type&start=&end=&bbox=` → `{ key, n }`

## Data
Parquet files are ignored by git. See `data/README.md`.  
Outputs to: `data/parquet/stormevents/*.parquet`

## Screenshots
Drop screenshots in `assets/` (map, /docs).

## Next
- S3/Glue/Athena wired summary
- Tests (pytest + httpx), CI
