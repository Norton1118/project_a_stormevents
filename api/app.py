# api/app.py
from __future__ import annotations

import os
import time
import uuid
import logging
from datetime import date
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import structlog
from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request
from starlette.responses import Response
from starlette_exporter import PrometheusMiddleware, handle_metrics


# ----------------------------
# Logging / Request tracing
# ----------------------------
def configure_logging() -> None:
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    structlog.configure(
        processors=[
            structlog.contextvars.merge_contextvars,
            structlog.processors.add_log_level,
            structlog.processors.TimeStamper(fmt="iso"),
            structlog.processors.dict_tracebacks,
            structlog.processors.JSONRenderer(),
        ],
        wrapper_class=structlog.make_filtering_bound_logger(logging.INFO),
        cache_logger_on_first_use=True,
    )


class RequestIDMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next):
        rid = request.headers.get("x-request-id") or uuid.uuid4().hex
        structlog.contextvars.bind_contextvars(request_id=rid)
        start = time.perf_counter()
        try:
            response: Response = await call_next(request)
            return response
        finally:
            dur_ms = (time.perf_counter() - start) * 1000
            structlog.get_logger().info(
                "request",
                method=request.method,
                path=request.url.path,
                status_code=getattr(response, "status_code", 0),
                duration_ms=round(dur_ms, 2),
            )
            structlog.contextvars.clear_contextvars()


# ----------------------------
# FastAPI app + middleware
# ----------------------------
app = FastAPI(title="StormEvents API", version="0.4.0")

# CORS: dev-open (tighten in prod)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

configure_logging()
app.add_middleware(RequestIDMiddleware)

# Prometheus metrics
app.add_middleware(PrometheusMiddleware, app_name="stormevents_api", group_paths=True)
app.add_route("/metrics", handle_metrics)


# ----------------------------
# Helpers
# ----------------------------
def _parse_bbox(bbox: str) -> Tuple[float, float, float, float]:
    """Parse 'minx,miny,maxx,maxy' into floats."""
    parts = bbox.split(",")
    if len(parts) != 4:
        raise HTTPException(
            status_code=422, detail="bbox must be 'minx,miny,maxx,maxy'"
        )
    try:
        mnx, mny, mxx, mxy = (float(p) for p in parts)
    except ValueError as exc:
        raise HTTPException(
            status_code=422, detail="bbox values must be numbers"
        ) from exc
    if mnx >= mxx or mny >= mxy:
        raise HTTPException(status_code=422, detail="bbox must be min<max for x and y")
    return mnx, mny, mxx, mxy


def _to_feature_collection(rows: List[Dict[str, Any]]) -> Dict[str, Any]:
    """Convert rows with lon/lat to a GeoJSON FeatureCollection (placeholder)."""
    features: List[Dict[str, Any]] = []
    for r in rows:
        geom = None
        if "longitude" in r and "latitude" in r:
            geom = {"type": "Point", "coordinates": [r["longitude"], r["latitude"]]}
        props = {k: v for k, v in r.items() if k not in {"longitude", "latitude"}}
        features.append({"type": "Feature", "geometry": geom, "properties": props})
    return {"type": "FeatureCollection", "features": features}


# ----------------------------
# Routes
# ----------------------------
@app.get("/health")
def health() -> Dict[str, str]:
    return {"status": "ok"}


@app.get("/events")
def events(
    start: date = Query(..., description="YYYY-MM-DD"),
    end: date = Query(..., description="YYYY-MM-DD"),
    bbox: Optional[str] = Query(None, description="minx,miny,maxx,maxy (lon,lat)"),
    limit: int = Query(1000, ge=1, le=100_000),
    types: Optional[List[str]] = Query(default=None, alias="types"),
) -> Dict[str, Any]:
    """Return GeoJSON features (stubbed for now)."""
    if start > end:
        raise HTTPException(status_code=422, detail="start must be <= end")
    _bbox: Optional[Tuple[float, float, float, float]] = (
        _parse_bbox(bbox) if bbox else None
    )

    # TODO: replace with Athena/DuckDB query using start/end/_bbox/limit/types
    rows: List[Dict[str, Any]] = []
    return _to_feature_collection(rows)


@app.get("/events/summary")
def events_summary(
    start: date = Query(..., description="YYYY-MM-DD"),
    end: date = Query(..., description="YYYY-MM-DD"),
    groupby: str = Query("type", description="Group field (e.g., 'type' or 'state')"),
    bbox: Optional[str] = Query(None),
) -> Dict[str, Any]:
    """Return aggregated counts (stub)."""
    if start > end:
        raise HTTPException(status_code=422, detail="start must be <= end")
    _bbox: Optional[Tuple[float, float, float, float]] = (
        _parse_bbox(bbox) if bbox else None
    )

    # TODO: replace with Athena/DuckDB aggregation
    summary_rows: List[Dict[str, Any]] = []
    return {"groupby": groupby, "rows": summary_rows}


# ----------------------------
# Static UI (guarded)
# Put this LAST so API routes take precedence.
# Skip in CI by setting DISABLE_STATIC=1.
# ----------------------------
STATIC_DIR: Path = (Path(__file__).parent / "static").resolve()
if STATIC_DIR.is_dir() and os.getenv("DISABLE_STATIC", "0") != "1":
    app.mount("/", StaticFiles(directory=str(STATIC_DIR), html=True), name="static")
