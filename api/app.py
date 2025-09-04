# api/app.py
from fastapi import FastAPI, Query, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from typing import Optional, List, Dict, Any
from pathlib import Path
from datetime import date
import os
import glob
import math
import time

# -------------------------------------------------
# FastAPI app + CORS
# -------------------------------------------------
app = FastAPI(title="StormEvents API", version="0.4.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # dev-only; restrict in prod
    allow_methods=["*"],
    allow_headers=["*"],
)

# -------------------------------------------------
# Config (via env vars / .env from docker-compose)
# -------------------------------------------------
BACKEND = (os.getenv("BACKEND") or "athena").lower()  # "athena" or "duckdb"

# DuckDB (local mode – optional)
PARQUET_DIR = os.getenv("PARQUET_DIR", "/app/data/parquet/stormevents")
PARQUET_GLOB = str(Path(PARQUET_DIR) / "*.parquet")

# Athena (cloud mode)
AWS_REGION = os.getenv("AWS_REGION") or os.getenv("AWS_DEFAULT_REGION") or "us-east-2"
ATHENA_WORKGROUP = os.getenv("ATHENA_WORKGROUP", "primary")
ATHENA_DATABASE = os.getenv("ATHENA_DATABASE", "stormevents")
ATHENA_TABLE = os.getenv("ATHENA_TABLE", "stormevents_v")  # your view
ATHENA_OUTPUT_S3 = os.getenv("ATHENA_OUTPUT_S3")  # s3://.../athena-results/


# -------------------------------------------------
# Shared helpers
# -------------------------------------------------
def _parse_iso_date(s: str) -> date:
    try:
        return date.fromisoformat(s)
    except Exception:
        raise HTTPException(
            status_code=400, detail=f"Invalid date '{s}'. Use YYYY-MM-DD."
        )


def _build_where(start: Optional[str], end: Optional[str], bbox: Optional[str]) -> str:
    parts: List[str] = [
        "lon BETWEEN -180 AND 180",
        "lat BETWEEN -90 AND 90",
    ]
    if start:
        parts.append(f"date >= DATE '{_parse_iso_date(start).isoformat()}'")
    if end:
        parts.append(f"date <= DATE '{_parse_iso_date(end).isoformat()}'")

    if bbox:
        try:
            minx, miny, maxx, maxy = map(float, bbox.split(","))
        except Exception:
            raise HTTPException(
                status_code=400, detail="bbox must be 'minx,miny,maxx,maxy' (lon/lat)"
            )
        if minx >= maxx or miny >= maxy:
            raise HTTPException(
                status_code=400, detail="bbox min must be < max for both lon and lat"
            )
        parts += [f"lon BETWEEN {minx} AND {maxx}", f"lat BETWEEN {miny} AND {maxy}"]

    return " AND ".join(parts) if parts else "1=1"


def _clean_rows(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    for r in rows:
        rr: Dict[str, Any] = {}
        for k, v in r.items():
            if isinstance(v, float):
                rr[k] = v if (math.isfinite(v) and not math.isnan(v)) else None
            elif isinstance(v, date):
                rr[k] = v.isoformat()
            else:
                rr[k] = v
        out.append(rr)
    return out


# -------------------------------------------------
# Athena helpers
# -------------------------------------------------
def _athena_client():
    import boto3

    return boto3.client("athena", region_name=AWS_REGION)


def _run_athena(sql: str, cast: str) -> List[Dict[str, Any]]:
    client = _athena_client()
    start = client.start_query_execution(
        QueryString=sql,
        QueryExecutionContext={"Database": ATHENA_DATABASE},
        WorkGroup=ATHENA_WORKGROUP,
        ResultConfiguration=(
            {"OutputLocation": ATHENA_OUTPUT_S3} if ATHENA_OUTPUT_S3 else {}
        ),
    )
    qid = start["QueryExecutionId"]

    # poll
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

    def f2(x):
        try:
            return (
                float(x) if x not in (None, "", "NaN", "nan", "inf", "-inf") else None
            )
        except Exception:
            return None

    def i2(x):
        try:
            return int(float(x)) if x not in (None, "") else None
        except Exception:
            return None

    items: List[Dict[str, Any]] = []
    for row in rows[1:]:
        vals = [
            cell.get("VarCharValue") if isinstance(cell, dict) else None
            for cell in row["Data"]
        ]
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


def _events_athena(
    start: Optional[str], end: Optional[str], bbox: Optional[str], limit: int
):
    where = _build_where(start, end, bbox)
    sql = f"""
      SELECT event_id, type, magnitude, lon, lat, date
      FROM {ATHENA_TABLE}
      WHERE {where}
      ORDER BY date DESC
      LIMIT {limit}
    """
    return _run_athena(sql, "events")


def _summary_athena(
    groupby: str, start: Optional[str], end: Optional[str], bbox: Optional[str]
):
    if groupby != "type":
        raise HTTPException(status_code=400, detail="Only groupby=type is supported.")
    where = _build_where(start, end, bbox)
    sql = f"""
      SELECT {groupby} AS key, COUNT(*) AS n
      FROM {ATHENA_TABLE}
      WHERE {where}
      GROUP BY {groupby}
      ORDER BY n DESC
    """
    return _run_athena(sql, "summary")


# -------------------------------------------------
# DuckDB helpers (lazy import so the container doesn't need it unless used)
# -------------------------------------------------
def _ensure_parquet_present() -> List[str]:
    files = glob.glob(PARQUET_GLOB)
    if not files:
        raise HTTPException(
            status_code=500, detail=f"No Parquet files found in {PARQUET_DIR}"
        )
    return files


def _events_duckdb(
    start: Optional[str], end: Optional[str], bbox: Optional[str], limit: int
):
    import duckdb

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
    return _clean_rows(rows)


def _summary_duckdb(
    groupby: str, start: Optional[str], end: Optional[str], bbox: Optional[str]
):
    import duckdb

    _ensure_parquet_present()
    if groupby != "type":
        raise HTTPException(status_code=400, detail="Only groupby=type is supported.")
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
    return _clean_rows(rows)


# -------------------------------------------------
# Routes
# -------------------------------------------------
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
    start: Optional[str] = Query(None, description="YYYY-MM-DD"),
    end: Optional[str] = Query(None, description="YYYY-MM-DD"),
    bbox: Optional[str] = Query(None, description="minx,miny,maxx,maxy (lon/lat)"),
    limit: int = Query(100, ge=1, le=5000),
):
    if BACKEND == "athena":
        items = _events_athena(start, end, bbox, limit)
    else:
        items = _events_duckdb(start, end, bbox, limit)
    return {"count": len(items), "items": items}


@app.get("/events/summary")
def events_summary(
    groupby: str = Query("type"),
    start: Optional[str] = Query(None),
    end: Optional[str] = Query(None),
    bbox: Optional[str] = Query(None),
):
    if BACKEND == "athena":
        items = _summary_athena(groupby, start, end, bbox)
    else:
        items = _summary_duckdb(groupby, start, end, bbox)
    return {"count": len(items), "items": items}


# -------------------------------------------------
# Static site (Option B): serve /app/static at /
# Mount this LAST so API routes take precedence.
# -------------------------------------------------
app.mount("/", StaticFiles(directory="static", html=True), name="static")

# Optional: allow `python app.py` locally (not used in Docker)
if __name__ == "__main__":
    import uvicorn

    uvicorn.run("app:app", host="0.0.0.0", port=8000)
