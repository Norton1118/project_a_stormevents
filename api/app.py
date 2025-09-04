# api/app.py
from __future__ import annotations

from datetime import date
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles

# -------------------------------------------------
# FastAPI app + CORS
# -------------------------------------------------
app = FastAPI(title="StormEvents API", version="0.4.0")

# Dev CORS (lock this down in prod)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # dev-only; restrict in prod
    allow_methods=["*"],
    allow_headers=["*"],
)

# -------------------------------------------------
# Static hosting (safe)
# Serve packaged assets from api/static if it exists.
# This prevents CI/test failures when the folder isn't present.
# -------------------------------------------------
STATIC_DIR: Path = (Path(__file__).parent / "static").resolve()
if STATIC_DIR.exists():
    app.mount("/", StaticFiles(directory=str(STATIC_DIR), html=True), name="static")

# -------------------------------------------------
# Helpers
# -------------------------------------------------


def _parse_bbox(bbox: str) -> Tuple[float, float, float, float]:
    """Parse 'minx,miny,maxx,maxy' into floats."""
    parts = bbox.split(",")
    if len(parts) != 4:
        raise HTTPException(status_code=422, detail="bbox must be 'minx,miny,maxx,maxy'")
    try:
        mnx, mny, mxx, mxy = (float(p) for p in parts)
    except ValueError as exc:  # pragma: no cover - defensive
        raise HTTPException(status_code=422, detail="bbox values must be numbers") from exc
    if mnx >= mxx or mny >= mxy:
        raise HTTPException(status_code=422, detail="bbox must be min<max for x and y")
    return mnx, mny, mxx, mxy


def _to_feature_collection(rows: List[Dict[str, Any]]) -> Dict[str, Any]:
    """Convert rows with lon/lat (or geometry WKT) to a GeoJSON FeatureCollection.
    This is a placeholder; adapt to your schema.
    """
    features: List[Dict[str, Any]] = []
    for r in rows:
        # Expecting keys: longitude, latitude, and properties...
        if "longitude" in r and "latitude" in r:
            geom = {"type": "Point", "coordinates": [r["longitude"], r["latitude"]]}
        else:  # pragma: no cover - fallback
            geom = None
        props = {k: v for k, v in r.items() if k not in {"longitude", "latitude"}}
        features.append({"type": "Feature", "geometry": geom, "properties": props})
    return {"type": "FeatureCollection", "features": features}


# -------------------------------------------------
# Routes
# -------------------------------------------------


@app.get("/health")
def health() -> Dict[str, str]:
    return {"status": "ok"}


@app.get("/events")
def events(
    start: date = Query(..., description="YYYY-MM-DD"),
    end: date = Query(..., description="YYYY-MM-DD"),
    bbox: Optional[str] = Query(
        None, description="minx,miny,maxx,maxy (WGS84 lon/lat order)"
    ),
    limit: int = Query(1000, ge=1, le=100_000),
    types: Optional[List[str]] = Query(
        default=None, alias="types", description="Optional event types filter"
    ),
) -> Dict[str, Any]:
    """Return GeoJSON features for StormEvents filtered by date range, bbox, and type."""
    if start > end:
        raise HTTPException(status_code=422, detail="start must be <= end")
    bbox_tuple: Optional[Tuple[float, float, float, float]] = None
    if bbox:
        bbox_tuple = _parse_bbox(bbox)

    # TODO: Replace this stub with your Athena/DuckDB query.
    # rows = query_backend.fetch_events(start, end, bbox_tuple, types, limit)
    rows: List[Dict[str, Any]] = []  # placeholder so the endpoint is valid

    return _to_feature_collection(rows)


@app.get("/events/summary")
def events_summary(
    start: date = Query(..., description="YYYY-MM-DD"),
    end: date = Query(..., description="YYYY-MM-DD"),
    groupby: str = Query("type", description="Field to group by (e.g., 'type' or 'state')"),
    bbox: Optional[str] = Query(None),
) -> Dict[str, Any]:
    """Return aggregated counts grouped by a field."""
    if start > end:
        raise HTTPException(status_code=422, detail="start must be <= end")
    bbox_tuple: Optional[Tuple[float, float, float, float]] = None
    if bbox:
        bbox_tuple = _parse_bbox(bbox)

    # TODO: Replace this stub with your Athena/DuckDB aggregation.
    # summary_rows = query_backend.summary(start, end, groupby, bbox_tuple)
    summary_rows: List[Dict[str, Any]] = []  # placeholder

    return {"groupby": groupby, "rows": summary_rows}
