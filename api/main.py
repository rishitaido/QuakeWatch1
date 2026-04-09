"""
QuakeWatch — REST API
FastAPI server that reads from DynamoDB and exposes earthquake,
alert, and stats data to the dashboard.

Endpoints:
  GET /health          — liveness check
  GET /earthquakes     — list earthquakes (filters: min_mag, limit)
  GET /alerts          — list active alerts
  GET /stats           — summary statistics

Owner: Rishi
"""

import logging
import os
from decimal import Decimal
from typing import Optional

import boto3
from boto3.dynamodb.conditions import Attr
from botocore.exceptions import ClientError
from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware

# ── Configuration ────────────────────────────────────────────────────────────
EARTHQUAKES_TABLE = os.environ.get("EARTHQUAKES_TABLE", "earthquakes")
ALERTS_TABLE = os.environ.get("ALERTS_TABLE", "alerts")
AWS_REGION = os.environ.get("AWS_REGION", "us-east-1")
API_PORT = int(os.environ.get("API_PORT", 8000))

# ── Logging ──────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [API] %(levelname)s %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("api")

# ── AWS ──────────────────────────────────────────────────────────────────────
dynamodb = boto3.resource("dynamodb", region_name=AWS_REGION)
eq_table = dynamodb.Table(EARTHQUAKES_TABLE)
alert_table = dynamodb.Table(ALERTS_TABLE)

# ── FastAPI app ───────────────────────────────────────────────────────────────
app = FastAPI(
    title="QuakeWatch API",
    description="Real-time earthquake data served from AWS DynamoDB.",
    version="1.0.0",
)

# Allow the Nginx-proxied dashboard (and local dev) to call the API
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["GET"],
    allow_headers=["*"],
)


# ── Helpers ──────────────────────────────────────────────────────────────────

def decimal_to_float(obj):
    """
    Recursively convert Decimal values (returned by boto3) to float/int
    so FastAPI can serialise them as JSON.
    """
    if isinstance(obj, list):
        return [decimal_to_float(i) for i in obj]
    if isinstance(obj, dict):
        return {k: decimal_to_float(v) for k, v in obj.items()}
    if isinstance(obj, Decimal):
        # Keep whole numbers as int for cleaner JSON
        return int(obj) if obj % 1 == 0 else float(obj)
    return obj


def scan_table(table, filter_expression=None) -> list[dict]:
    """
    Perform a full DynamoDB table scan with automatic pagination.
    Use sparingly — scans consume RCUs proportional to table size.
    """
    kwargs = {}
    if filter_expression is not None:
        kwargs["FilterExpression"] = filter_expression

    items = []
    try:
        while True:
            response = table.scan(**kwargs)
            items.extend(response.get("Items", []))
            last_key = response.get("LastEvaluatedKey")
            if not last_key:
                break
            kwargs["ExclusiveStartKey"] = last_key
    except ClientError as exc:
        logger.error(f"DynamoDB scan error: {exc}")
        raise HTTPException(status_code=502, detail="Database error")

    return decimal_to_float(items)


# ── Routes ───────────────────────────────────────────────────────────────────

@app.get("/health", tags=["Meta"])
def health():
    """Liveness check — returns 200 if the API is running."""
    return {"status": "ok"}


@app.get("/earthquakes", tags=["Earthquakes"])
def get_earthquakes(
    min_mag: Optional[float] = Query(
        default=None,
        description="Minimum magnitude filter (e.g. 4.5)",
        ge=0.0,
        le=10.0,
    ),
    limit: int = Query(
        default=50,
        description="Maximum number of results to return",
        ge=1,
        le=500,
    ),
):
    """
    Return a list of earthquakes from DynamoDB, newest first.

    - **min_mag**: optional lower bound on magnitude
    - **limit**: cap on result count (default 50, max 500)
    """
    filter_expr = None
    if min_mag is not None:
        filter_expr = Attr("magnitude").gte(Decimal(str(min_mag)))

    items = scan_table(eq_table, filter_expr)

    # Sort by time descending (most recent first); handle missing time gracefully
    items.sort(key=lambda x: x.get("time") or 0, reverse=True)

    return {"count": len(items[:limit]), "earthquakes": items[:limit]}


@app.get("/alerts", tags=["Alerts"])
def get_alerts(
    severity: Optional[str] = Query(
        default=None,
        description="Filter by severity: HIGH or MEDIUM",
    ),
    limit: int = Query(default=50, ge=1, le=500),
):
    """
    Return a list of alert records, newest first.

    - **severity**: optional filter — `HIGH` or `MEDIUM`
    - **limit**: cap on result count
    """
    filter_expr = None
    if severity:
        sev = severity.upper()
        if sev not in ("HIGH", "MEDIUM"):
            raise HTTPException(
                status_code=400,
                detail="severity must be HIGH or MEDIUM",
            )
        filter_expr = Attr("severity").eq(sev)

    items = scan_table(alert_table, filter_expr)
    items.sort(key=lambda x: x.get("created_at") or "", reverse=True)

    return {"count": len(items[:limit]), "alerts": items[:limit]}


@app.get("/stats", tags=["Stats"])
def get_stats():
    """
    Return summary statistics computed from the earthquakes table:
    - total event count
    - average magnitude
    - highest magnitude event
    - count of HIGH and MEDIUM alerts
    """
    earthquakes = scan_table(eq_table)
    alerts = scan_table(alert_table)

    total = len(earthquakes)
    magnitudes = [
        e["magnitude"] for e in earthquakes if e.get("magnitude") is not None
    ]

    avg_mag = round(sum(magnitudes) / len(magnitudes), 2) if magnitudes else None
    max_mag_event = (
        max(earthquakes, key=lambda e: e.get("magnitude") or 0)
        if earthquakes
        else None
    )

    high_alerts = sum(1 for a in alerts if a.get("severity") == "HIGH")
    medium_alerts = sum(1 for a in alerts if a.get("severity") == "MEDIUM")

    return {
        "total_earthquakes": total,
        "average_magnitude": avg_mag,
        "max_magnitude_event": max_mag_event,
        "total_alerts": len(alerts),
        "high_alerts": high_alerts,
        "medium_alerts": medium_alerts,
    }
