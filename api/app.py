# api/app.py
from fastapi import FastAPI, Query, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from typing import Optional, List, Dict, Any
from pathlib import Path
from datetime import date
import os, glob, math, time

# Optional local engine
import duckdb  # present even if we use Athena

# -------------------------
# App + CORS
# -------------------------
app = FastAPI(title="StormEvents API", version="0.3.0")
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # dev-only; tighten for prod
    allow_methods=["*"],
    allow_headers=["*"],
)

# -------------------------
# Config
# -------------------------
# Backend: "athena" (your current mode) or "duckdb"
BACKEND = os.getenv("BACKEND", "athena").lower()

# DuckDB (for local mode)
PARQUET_DIR  = os.getenv("PARQUET_DIR", "/app/data/parquet/stormevents")
PARQUET_GLOB = str(Path(PARQUET_DIR).joinpath("*.parquet").as_posix())

# Athena (env provided via docker compose .env)
AWS_REGION        = os.getenv("AWS_REGION") or os.getenv("AWS_DEFAULT_REGION") or "us-east-2"
ATHENA_WORKGROUP  = os.getenv("ATHENA_WORKGROUP", "primary")
ATHENA_DATABASE   = os.getenv("ATHENA_DATABASE", "stormevents")
ATHENA_TABLE      = os.getenv("ATHENA_TABLE", "stormevents_v")  # your view
ATHENA_OUTPUT_S3  = os.getenv("ATHENA_OUTPUT_S3")  # e.g. s3://.../athena-results/

# -------------------------
# Helpers (shared)
# -------------------------
def _parse_iso_date(s: str) -> date:
    try:
        return date.fromisoformat(s)
    except Exception:
        raise HTTPException(status_code=400, detail=f"Invalid date '{s}'. Use YYYY-MM-DD.")

def _build_where(start: Optional[str], end: Optional[str], bbox: Optional[str]) -> str:
    clauses: List[str] = []
    # Always keep lon/lat sane
    clauses.append("lon BETWEEN -180 AND 180")
    clauses.append("lat BETWEEN -90 AND 90")

    if start:
        d0 = _parse_iso_date(start)
        clauses.append(f"date >= DATE '{d0.isoformat()}'")
    if end:
        d1 = _parse_iso_date(end)
        clauses.append(f"date <= DATE '{d1.isoformat()}'")

    if bbox:
        try:
            minx, miny, maxx, maxy = map(float, bbox.split(","))
        except Exception:
            raise HTTPException(status_code=400, detail="bbox must be 'minx,miny,maxx,maxy' (lon/lat)")
        if minx >= maxx or miny >= maxy:
            raise HTTPException(status_code=400, detail="bbox min must be < max for both lon and lat")
        clauses.append(f"lon BETWEEN {minx} AND {maxx}")
        clauses.append(f"lat BETWEEN {miny} AND {maxy}")

    return " AND ".join(clauses) if clauses else "1=1"

def _sanitize_records(records: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Replace NaN/Inf with None; stringify dates."""
    out: List[Dict[str, Any]] = []
    for r in records:
        rr: Dict[str, Any] = {}
        for k, v in r.items():
            if isinstance(v, float):
                rr[k] = v if (math.isfinite(v) and not math.isnan(v)) else None
            elif isinstance(v, (date,)):
                rr[k] = v.isoformat()
            else:
                rr[k] = v
        out.append(rr)
    return out

# -------------------------
# Athena helpers
# -------------------------
def _athena_client():
    import boto3
    return boto3.client("athena", region_name=AWS_REGION)

def _build_filters_sql(start: Optional[str], end: Optional[str], bbox: Optional[str]) -> str:
    # Athena SQL uses same textual WHERE we build for DuckDB
    return _build_where(start, end, bbox)

def _run_athena(sql: str, cast: str = "events") -> List[Dict[str, Any]]:
    client = _athena_client()
    start = client.start_query_execution(
        QueryString=sql,
        QueryExecutionContext={"Database": ATHENA_DATABASE},
        WorkGroup=ATHENA_WORKGROUP,
        ResultConfiguration={"OutputLocation": ATHENA_OUTPUT_S3} if ATHENA_OUTPUT_S3 else {},
    )
    qid = start["QueryExecutionId"]

    # Poll until finished
    while True:
        qe = client.get_query_execution(QueryExecutionId=qid)["QueryExecution"]
        state = qe["Status"]["State"]
        if state in ("SUCCEEDED", "FAILED", "CANCELLED"):
            break
        time.sleep(0.5)

    if state != "SUCCEEDED":
        reason = qe["Status"].get("StateChangeReason", "")
        raise HTTPException(status_code=502, detail=f"Athena query {state}: {reason}")

    rs = client.get_query_results(QueryExecutionId=qid)["ResultSet"]
    rows = rs.get("Rows", [])
    if not rows:
        return []

    headers = [c["VarCharValue"] for c in rows[0]["Data"]]
    items: List[Dict[str, Any]] = []

    def f2(x):
        try:
            return float(x) if x not in (None, "", "NaN", "nan", "inf", "-inf") else None
        except Exception:
            return None

    def i2(x):
        try:
            return int(float(x)) if x not in (None, "") else None
        except Exception:
            return None

    for row in rows[1:]:
        vals = [cell.get("VarCharValue") if isinstance(cell, dict) else None for cell in row["Data"]]
        rec = dict(zip(headers, vals))

        if cast == "events":
            rec = {
                "event_id": i2(rec.get("event_id")),
                "type": rec.get("type"),
                "magnitude": f2(rec.get("magnitude")),
                "lon": f2(rec.get("lon")),
                "lat": f2(rec.get("lat")),
                "date": rec.get("date"),
            }
        elif cast == "summary":
            rec = {"key": rec.get("key"), "n": i2(rec.get("n"))}
        items.append(rec)
    return items

def _events_athena(start: Optional[str], end: Optional[str], bbox: Optional[str], limit: int):
    where = _build_filters_sql(start, end, bbox)
    sql = f"""
      SELECT event_id, type, magnitude, lon, lat, date
      FROM {ATHENA_TABLE}
      WHERE {where}
      ORDER BY date DESC
      LIMIT {limit}
    """
    return _run_athena(sql, cast="events")

def _summary_athena(groupby: str, start: Optional[str], end: Optional[str], bbox: Optional[str]):
    allowed = {"type"}
    if groupby not in allowed:
        raise HTTPException(status_code=400, detail=f"Unsupported groupby '{groupby}'. Allowed: {sorted(allowed)}")
    where = _build_filters_sql(start, end, bbox)
    sql = f"""
      SELECT {groupby} AS key, COUNT(*) AS n
      FROM {ATHENA_TABLE}
      WHERE {where}
      GROUP BY {groupby}
      ORDER BY n DESC
    """
    return _run_athena(sql, cast="summary")

# -------------------------
# DuckDB helpers (optional local mode)
# -------------------------
def _ensure_parquet_present() -> List[str]:
    files = glob.glob(PARQUET_GLOB)
    if not files:
        raise HTTPException(
            status_code=500,
            detail=f"No Parquet files found in {PARQUET_DIR}. (Looked for {PARQUET_GLOB})"
        )
    return files

def _events_duckdb(start: Optional[str], end: Optional[str], bbox: Optional[str], limit: int):
    _ensure_parquet_present()
    where = _build_where(start, end, bbox)
    q = f"""
      SELECT event_id, type,
             CASE WHEN isfinite(magnitude) THEN magnitude ELSE NULL END AS magnitude,
             CAST(lon AS DOUBLE) AS lon,
             CAST(lat AS DOUBLE) AS lat,
             CAST(date AS DATE)  AS date
      FROM read_parquet('{PARQUET_GLOB}')
      WHERE {where}
      ORDER BY date DESC
      LIMIT {limit}
    """
    con = duckdb.connect()
    try:
        rows = con.sql(q).to_df().to_dict(orient="records")
    finally:
        con.close()
    return _sanitize_records(rows)

def _summary_duckdb(groupby: str, start: Optional[str], end: Optional[str], bbox: Optional[str]):
    _ensure_parquet_present()
    allowed = {"type"}
    if groupby not in allowed:
        raise HTTPException(status_code=400, detail=f"Unsupported groupby '{groupby}'. Allowed: {sorted(allowed)}")
    where = _build_where(start, end, bbox)
    q = f"""
      SELECT {groupby} AS key, COUNT(*) AS n
      FROM read_parquet('{PARQUET_GLOB}')
      WHERE {where}
      GROUP BY {groupby}
      ORDER BY n DESC
    """
    con = duckdb.connect()
    try:
        rows = con.sql(q).to_df().to_dict(orient="records")
    finally:
        con.close()
    return _sanitize_records(rows)

# -------------------------
# Routes
# -------------------------
@app.get("/health")
def health():
    return {
        "status": "ok",
        "backend": BACKEND,
        "workgroup": ATHENA_WORKGROUP,
        "database": ATHENA_DATABASE,
        "table": ATHENA_TABLE,
        "output_s3": ATHENA_OUTPUT_S3,
        "region": AWS_REGION,
    }

@app.get("/events")
def events(
    start: Optional[str] = Query(None, description="Start date, YYYY-MM-DD"),
    end:   Optional[str] = Query(None, description="End date, YYYY-MM-DD"),
    bbox:  Optional[str] = Query(None, description="minx,miny,maxx,maxy (lon/lat)"),
    limit: int = Query(100, description="Max rows")
):
    if BACKEND == "athena":
        items = _events_athena(start, end, bbox, limit)
    else:
        items = _events_duckdb(start, end, bbox, limit)
    return {"count": len(items), "items": items}

@app.get("/events/summary")
def events_summary(
    groupby: str = Query("type", description="Column to group by (currently: 'type')"),
    start:   Optional[str] = Query(None),
    end:     Optional[str] = Query(None),
    bbox:    Optional[str] = Query(None),
):
    if BACKEND == "athena":
        items = _summary_athena(groupby, start, end, bbox)
    else:
        items = _summary_duckdb(groupby, start, end, bbox)
    return {"count": len(items), "items": items}
