from fastapi import FastAPI, Query, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from typing import Optional, List, Dict, Any
from pathlib import Path
import os, glob, math, duckdb
from datetime import date

# -----------------------------------------------------------------------------
# App + CORS
# -----------------------------------------------------------------------------
app = FastAPI(title="StormEvents API", version="0.2.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],   # dev-only; tighten for production
    allow_methods=["*"],
    allow_headers=["*"],
)

# -----------------------------------------------------------------------------
# Config: where Parquet lives (env override supported)
# Use POSIX paths so DuckDB globs work on Windows too.
# -----------------------------------------------------------------------------
PARQUET_DIR  = os.getenv("PARQUET_DIR", "/app/data/parquet/stormevents")
PARQUET_GLOB = str(Path(PARQUET_DIR).joinpath("*.parquet").as_posix())

# -----------------------------------------------------------------------------
# Helpers
# -----------------------------------------------------------------------------
def _ensure_parquet_present() -> List[str]:
    files = glob.glob(PARQUET_GLOB)
    if not files:
        raise HTTPException(
            status_code=500,
            detail=f"No Parquet files found in {PARQUET_DIR}. (Looked for {PARQUET_GLOB})"
        )
    return files

def _parse_iso_date(s: str) -> date:
    try:
        return date.fromisoformat(s)
    except Exception:
        raise HTTPException(status_code=400, detail=f"Invalid date '{s}'. Use YYYY-MM-DD.")

def _build_where(start: Optional[str], end: Optional[str], bbox: Optional[str]) -> str:
    clauses: List[str] = []
    # keep lon/lat sane regardless of filters
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
            raise HTTPException(
                status_code=400,
                detail="bbox must be 'minx,miny,maxx,maxy' (lon/lat)"
            )
        if minx >= maxx or miny >= maxy:
            raise HTTPException(status_code=400, detail="bbox min must be < max for both lon and lat")
        clauses.append(f"lon BETWEEN {minx} AND {maxx}")
        clauses.append(f"lat BETWEEN {miny} AND {maxy}")

    return " AND ".join(clauses) if clauses else "1=1"

def _sanitize_records(records: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Replace NaN/Inf with None, stringify date/datetime for JSON safety."""
    out: List[Dict[str, Any]] = []
    for r in records:
        rr: Dict[str, Any] = {}
        for k, v in r.items():
            if isinstance(v, float):
                if (not math.isfinite(v)) or math.isnan(v):
                    rr[k] = None
                else:
                    rr[k] = v
            elif isinstance(v, (date,)):  # date to 'YYYY-MM-DD'
                rr[k] = v.isoformat()
            else:
                rr[k] = v
        out.append(rr)
    return out

# -----------------------------------------------------------------------------
# Routes
# -----------------------------------------------------------------------------
@app.get("/health")
def health():
    files = glob.glob(PARQUET_GLOB)
    return {"status": "ok", "parquet_dir": PARQUET_DIR, "files": len(files)}

@app.get("/events")
def events(
    start: Optional[str] = Query(None, description="Start date, YYYY-MM-DD"),
    end:   Optional[str] = Query(None, description="End date, YYYY-MM-DD"),
    bbox:  Optional[str] = Query(None, description="minx,miny,maxx,maxy (lon/lat)"),
    limit: int = Query(100, ge=1, le=10000, description="Max rows to return"),
):
    _ensure_parquet_present()
    where = _build_where(start, end, bbox)

    q = f"""
    SELECT
        event_id,
        type,
        magnitude,
        lon,
        lat,
        date
    FROM read_parquet('{PARQUET_GLOB}')
    WHERE {where}
    ORDER BY date DESC
    LIMIT {int(limit)}
    """

    con = duckdb.connect()
    try:
        df = con.sql(q).to_df()
    finally:
        con.close()

    records = df.to_dict(orient="records")
    items = _sanitize_records(records)
    return {"count": len(items), "items": items}

@app.get("/events/summary")
def events_summary(
    groupby: str = Query("type", description="Column to group by (currently: 'type')"),
    start: Optional[str] = Query(None),
    end:   Optional[str] = Query(None),
    bbox:  Optional[str] = Query(None),
):
    _ensure_parquet_present()

    # restrict to supported group-bys to avoid SQL injection & weirdness
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
        df = con.sql(q).to_df()
    finally:
        con.close()

    items = _sanitize_records(df.to_dict(orient="records"))
    return {"count": len(items), "items": items}

