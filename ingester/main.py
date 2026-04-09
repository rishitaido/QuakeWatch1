"""
QuakeWatch — Seismic Ingester
Polls the USGS GeoJSON feed every POLL_INTERVAL_SECONDS seconds.
Deduplicates against DynamoDB (survives restarts) and publishes new
events to SQS for the processor to consume.

Owner: Rishi
"""

import json
import logging
import os
import time
from decimal import Decimal

import boto3
import requests
from botocore.exceptions import ClientError

# ── Configuration ────────────────────────────────────────────────────────────
USGS_FEED_URL = os.environ.get(
    "USGS_FEED_URL",
    "https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/all_hour.geojson",
)
POLL_INTERVAL = int(os.environ.get("POLL_INTERVAL_SECONDS", 60))
SQS_QUEUE_URL = os.environ["SQS_QUEUE_URL"]
EARTHQUAKES_TABLE = os.environ.get("EARTHQUAKES_TABLE", "earthquakes")
AWS_REGION = os.environ.get("AWS_REGION", "us-east-1")

# ── Logging ──────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [INGESTER] %(levelname)s %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("ingester")

# ── AWS clients ──────────────────────────────────────────────────────────────
sqs = boto3.client("sqs", region_name=AWS_REGION)
dynamodb = boto3.resource("dynamodb", region_name=AWS_REGION)
table = dynamodb.Table(EARTHQUAKES_TABLE)


def fetch_feed() -> list[dict]:
    """Fetch the USGS GeoJSON feed and return a list of feature dicts."""
    try:
        resp = requests.get(USGS_FEED_URL, timeout=15)
        resp.raise_for_status()
        return resp.json().get("features", [])
    except requests.RequestException as exc:
        logger.error(f"Failed to fetch USGS feed: {exc}")
        return []


def already_seen(event_id: str) -> bool:
    """
    Check DynamoDB to see if this event_id has already been processed.
    Using a lightweight get_item (partition key only) to keep reads cheap.
    """
    try:
        response = table.get_item(
            Key={"event_id": event_id},
            ProjectionExpression="event_id",  # only fetch the key — minimal RCU
        )
        return "Item" in response
    except ClientError as exc:
        logger.warning(f"DynamoDB get_item error for {event_id}: {exc}")
        # Fail open — treat as unseen so we don't silently drop events
        return False


def publish_to_sqs(event_id: str, payload: dict) -> None:
    """Publish a single earthquake event to SQS as a JSON message."""
    try:
        sqs.send_message(
            QueueUrl=SQS_QUEUE_URL,
            MessageBody=json.dumps(payload),
            # MessageGroupId not needed for standard queues; add if using FIFO
        )
        logger.info(
            f"Published  event_id={event_id}  mag={payload.get('magnitude')}  "
            f"place={payload.get('place')!r}"
        )
    except ClientError as exc:
        logger.error(f"SQS send_message failed for {event_id}: {exc}")


def parse_feature(feature: dict) -> dict | None:
    """
    Extract the fields we need from a USGS GeoJSON feature.
    Returns None if required fields are missing.
    """
    try:
        props = feature["properties"]
        coords = feature["geometry"]["coordinates"]  # [lon, lat, depth]
        event_id = feature["id"]

        return {
            "event_id": event_id,
            "magnitude": props.get("mag"),          # float or None
            "place": props.get("place", "Unknown"),
            "time": props.get("time"),               # Unix ms timestamp
            "updated": props.get("updated"),
            "lat": round(coords[1], 4),
            "lon": round(coords[0], 4),
            "depth_km": round(coords[2], 2) if coords[2] is not None else None,
            "status": props.get("status", "automatic"),
            "tsunami": props.get("tsunami", 0),
            "url": props.get("url", ""),
        }
    except (KeyError, IndexError, TypeError) as exc:
        logger.debug(f"Skipping malformed feature: {exc}")
        return None


def poll_once() -> int:
    """
    Fetch the feed, deduplicate, and publish new events.
    Returns the number of new events published.
    """
    features = fetch_feed()
    if not features:
        return 0

    new_count = 0
    for feature in features:
        payload = parse_feature(feature)
        if payload is None:
            continue

        event_id = payload["event_id"]
        if already_seen(event_id):
            continue

        publish_to_sqs(event_id, payload)
        new_count += 1

    return new_count


def main() -> None:
    logger.info("=" * 60)
    logger.info("QuakeWatch Seismic Ingester starting")
    logger.info(f"  Feed URL : {USGS_FEED_URL}")
    logger.info(f"  Interval : {POLL_INTERVAL}s")
    logger.info(f"  Queue    : {SQS_QUEUE_URL}")
    logger.info(f"  Table    : {EARTHQUAKES_TABLE}")
    logger.info("=" * 60)

    while True:
        logger.info("Polling USGS feed...")
        new = poll_once()
        logger.info(f"Poll complete — {new} new event(s) published to SQS")
        time.sleep(POLL_INTERVAL)


if __name__ == "__main__":
    main()
