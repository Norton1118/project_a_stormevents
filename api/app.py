# api/app.py
from __future__ import annotations

import os
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
# Allow CI/tests to skip via DISABLE_STATIC=1.
# -------------------------------------------------
STATIC_DIR: Path = (Path(__file__).parent / "static").resolve()
if STATIC_DIR.is_dir() and os.getenv("DISABLE_STATIC", "0") != "1":
    app.mount("/", StaticFiles(directory=str(STATIC_DIR), html=True), name="static")


# -------------------------------------------------
# Helpers
# -------------------------------------------------
def _parse_bbox(bbox: str) -> Tuple[float, float, float, float]:
    """Parse 'minx,miny,maxx,maxy' into floats."""
    parts = bbox.split(",")
    if len(parts) != 4:
        raise HTTPException(
            status_code=422, detail="bbox must be 'minx,miny,maxx,maxy'"
        )
    try:
        mnx, mny, mxx, mxy = (float(p) for p in parts)
    except ValueError as exc:  # pragma: no cover
        raise HTTPException(
            status_code=422, detail="bbox values must be numbers"
        ) from exc
    if mnx >= mxx or mny >= mxy:
        raise HTTPException(status_code=422, detail="bbox must be min<max for x and y")
    return mnx, mny, mxx, mxy


def _to_feature_collection(rows: List[Dict[str, Any]]) -> Dict[str, Any]:
    """Convert rows with lon/lat to GeoJSON FeatureCollection (placeholder)."""
    features: List[Dict[str, Any]] = []
    for r in rows:
        geom = None
        if "longitude" in r and "latitude" in r:
            geom = {"type": "Point", "coordinates": [r["longitude"], r["latitude"]]}
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
    bbox: Optional[str] = Query(None, description="minx,miny,maxx,maxy (lon/lat)"),
    limit: int = Query(1000, ge=1, le=100_000),
    types: Optional[List[str]] = Query(default=None, alias="types"),
) -> Dict[str, Any]:
    """Return GeoJSON features filtered by date/bbox/type."""
    if start > end:
        raise HTTPException(status_code=422, detail="start must be <= end")
    _bbox: Optional[Tuple[float, float, float, float]] = (
        _parse_bbox(bbox) if bbox else None
    )

    # TODO: Replace with Athena/DuckDB query using `_bbox`, `types`, `limit`.
    rows: List[Dict[str, Any]] = []

    return _to_feature_collection(rows)


@app.get("/events/summary")
def events_summary(
    start: date = Query(..., description="YYYY-MM-DD"),
    end: date = Query(..., description="YYYY-MM-DD"),
    groupby: str = Query("type", description="Group field (e.g., 'type' or 'state')"),
    bbox: Optional[str] = Query(None),
) -> Dict[str, Any]:
    """Return aggregated counts grouped by a field."""
    if start > end:
        raise HTTPException(status_code=422, detail="start must be <= end")
    _bbox: Optional[Tuple[float, float, float, float]] = (
        _parse_bbox(bbox) if bbox else None
    )

    # TODO: Replace with Athena/DuckDB aggregation using `_bbox` and `groupby`.
    summary_rows: List[Dict[str, Any]] = []

    return {"groupby": groupby, "rows": summary_rows}
