"""
QuakeWatch - Impact Processor Service
Consumes earthquake events from SQS, calculates impact scores by correlating
epicenters with population density, and writes enriched data to DynamoDB.
Owner: Asha
"""

import os
import sys
import json
import math
import time
import logging
from decimal import Decimal
from datetime import datetime, timezone

import boto3
from botocore.exceptions import ClientError

# ── Configuration ──────────────────────────────────────────
SQS_QUEUE_URL = os.environ["SQS_QUEUE_URL"]
EARTHQUAKES_TABLE = os.environ.get("EARTHQUAKES_TABLE", "earthquakes")
CITIES_TABLE = os.environ.get("CITIES_TABLE", "cities")
AWS_REGION = os.environ.get("AWS_REGION", "us-east-1")

HIGH_SEVERITY_MAG = float(os.environ.get("HIGH_SEVERITY_MAG", "6.0"))
HIGH_SEVERITY_IMPACT = float(os.environ.get("HIGH_SEVERITY_IMPACT", "80"))
MEDIUM_SEVERITY_MAG = float(os.environ.get("MEDIUM_SEVERITY_MAG", "4.5"))
MEDIUM_SEVERITY_IMPACT = float(os.environ.get("MEDIUM_SEVERITY_IMPACT", "40"))

IMPACT_RADIUS_KM = 300  # Only consider cities within this radius
IMPACT_LOG_SCALE = 15   # Tuning factor for 0-100 normalization

# ── Logging ────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [PROCESSOR] %(levelname)s %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    stream=sys.stdout,
)
logger = logging.getLogger("processor")

# ── AWS Clients ────────────────────────────────────────────
sqs = boto3.client("sqs", region_name=AWS_REGION)
dynamodb = boto3.resource("dynamodb", region_name=AWS_REGION)
earthquakes_table = dynamodb.Table(EARTHQUAKES_TABLE)
cities_table = dynamodb.Table(CITIES_TABLE)

# ── In-memory city cache ───────────────────────────────────
cities_cache = []


def load_cities():
    """
    Load all cities from DynamoDB into memory for fast lookups.
    Called once at startup. Cities don't change during runtime.
    """
    global cities_cache
    logger.info("Loading cities from DynamoDB...")

    try:
        cities = []
        response = cities_table.scan()
        cities.extend(response.get("Items", []))

        # Handle pagination for large datasets
        while "LastEvaluatedKey" in response:
            response = cities_table.scan(
                ExclusiveStartKey=response["LastEvaluatedKey"]
            )
            cities.extend(response.get("Items", []))

        cities_cache = [
            {
                "name": c.get("name", "Unknown"),
                "country": c.get("country", ""),
                "lat": float(c.get("lat", 0)),
                "lon": float(c.get("lon", 0)),
                "population": int(c.get("population", 0)),
            }
            for c in cities
        ]

        logger.info(f"Loaded {len(cities_cache)} cities into cache")

    except ClientError as e:
        logger.error(f"Failed to load cities: {e}")
        cities_cache = []


def haversine(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """
    Calculate the great-circle distance between two points on Earth
    using the Haversine formula.

    Args:
        lat1, lon1: Coordinates of point 1 (degrees)
        lat2, lon2: Coordinates of point 2 (degrees)

    Returns:
        Distance in kilometers
    """
    R = 6371.0  # Earth's radius in km

    lat1_r = math.radians(lat1)
    lat2_r = math.radians(lat2)
    dlat = math.radians(lat2 - lat1)
    dlon = math.radians(lon2 - lon1)

    a = (
        math.sin(dlat / 2) ** 2
        + math.cos(lat1_r) * math.cos(lat2_r) * math.sin(dlon / 2) ** 2
    )
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))

    return R * c


def calculate_impact_score(magnitude: float, lat: float, lon: float) -> dict:
    """
    Calculate composite impact score for an earthquake by correlating
    its epicenter with nearby population centers.

    Formula per city: city_impact = (mag^2) * (population/1000) / (distance_km^2 + 1)
    Overall score: normalized sum on a 0-100 logarithmic scale.

    Returns dict with: impact_score, nearest_city, nearest_city_dist_km, nearby_cities_count
    """
    if not cities_cache:
        return {
            "impact_score": 0,
            "nearest_city": "Unknown",
            "nearest_city_dist_km": -1,
            "nearby_cities_count": 0,
        }

    raw_impact_sum = 0.0
    nearest_city = None
    nearest_distance = float("inf")
    nearby_count = 0

    for city in cities_cache:
        dist = haversine(lat, lon, city["lat"], city["lon"])

        # Track nearest city regardless of radius
        if dist < nearest_distance:
            nearest_distance = dist
            nearest_city = city

        # Only count cities within impact radius for scoring
        if dist <= IMPACT_RADIUS_KM:
            nearby_count += 1
            city_impact = (magnitude ** 2) * (city["population"] / 1000) / (dist ** 2 + 1)
            raw_impact_sum += city_impact

    # Normalize to 0-100 using logarithmic scale
    if raw_impact_sum > 0:
        impact_score = min(100.0, IMPACT_LOG_SCALE * math.log10(1 + raw_impact_sum))
    else:
        impact_score = 0.0

    return {
        "impact_score": round(impact_score, 1),
        "nearest_city": nearest_city["name"] if nearest_city else "Unknown",
        "nearest_city_country": nearest_city["country"] if nearest_city else "",
        "nearest_city_dist_km": round(nearest_distance, 1) if nearest_city else -1,
        "nearby_cities_count": nearby_count,
    }


def determine_severity(magnitude: float, impact_score: float) -> str:
    """
    Assign severity level based on magnitude and impact score thresholds.
    """
    if magnitude >= HIGH_SEVERITY_MAG or impact_score >= HIGH_SEVERITY_IMPACT:
        return "high"
    elif magnitude >= MEDIUM_SEVERITY_MAG or impact_score >= MEDIUM_SEVERITY_IMPACT:
        return "medium"
    else:
        return "low"


def write_enriched_event(earthquake: dict, impact: dict, severity: str):
    """
    Write the enriched earthquake event to DynamoDB.
    Uses Decimal for DynamoDB number compatibility.
    """
    try:
        item = {
            "event_id": earthquake["event_id"],
            "timestamp": int(earthquake["time"]),
            "magnitude": Decimal(str(earthquake.get("magnitude", 0))),
            "depth": Decimal(str(earthquake.get("depth", 0))),
            "lat": Decimal(str(earthquake.get("lat", 0))),
            "lon": Decimal(str(earthquake.get("lon", 0))),
            "place": earthquake.get("place", "Unknown"),
            "url": earthquake.get("url", ""),
            "tsunami": int(earthquake.get("tsunami", 0)),
            "impact_score": Decimal(str(impact["impact_score"])),
            "nearest_city": impact["nearest_city"],
            "nearest_city_country": impact.get("nearest_city_country", ""),
            "nearest_city_dist_km": Decimal(str(impact["nearest_city_dist_km"])),
            "nearby_cities_count": int(impact["nearby_cities_count"]),
            "severity": severity,
            "processed_at": int(datetime.now(timezone.utc).timestamp() * 1000),
        }

        earthquakes_table.put_item(Item=item)
        return True

    except ClientError as e:
        logger.error(f"Failed to write event {earthquake['event_id']}: {e}")
        return False


def process_message(message: dict):
    """
    Process a single SQS message: parse, enrich with impact score, write to DynamoDB.
    """
    try:
        body = json.loads(message["Body"])
        event_id = body.get("event_id", "unknown")
        magnitude = float(body.get("magnitude", 0))
        lat = float(body.get("lat", 0))
        lon = float(body.get("lon", 0))

        # Calculate impact score
        impact = calculate_impact_score(magnitude, lat, lon)
        severity = determine_severity(magnitude, impact["impact_score"])

        # Write enriched event to DynamoDB
        if write_enriched_event(body, impact, severity):
            # Delete message from SQS only after successful write
            sqs.delete_message(
                QueueUrl=SQS_QUEUE_URL,
                ReceiptHandle=message["ReceiptHandle"],
            )

            logger.info(
                f"PROCESSED: {event_id} | mag={magnitude} | "
                f"impact={impact['impact_score']} | severity={severity} | "
                f"nearest={impact['nearest_city']} ({impact['nearest_city_dist_km']}km) | "
                f"{impact['nearby_cities_count']} cities in range"
            )
        else:
            logger.warning(f"Failed to write {event_id}, message will retry via SQS")

    except (json.JSONDecodeError, KeyError, ValueError) as e:
        logger.error(f"Bad message format: {e}. Message body: {message.get('Body', '')[:200]}")
        # Delete malformed messages to prevent infinite retry
        sqs.delete_message(
            QueueUrl=SQS_QUEUE_URL,
            ReceiptHandle=message["ReceiptHandle"],
        )


def poll_sqs():
    """
    Long-poll SQS for new messages and process them.
    """
    try:
        response = sqs.receive_message(
            QueueUrl=SQS_QUEUE_URL,
            MaxNumberOfMessages=10,
            WaitTimeSeconds=20,  # Long polling
            MessageAttributeNames=["All"],
        )

        messages = response.get("Messages", [])
        for message in messages:
            process_message(message)

        return len(messages)

    except ClientError as e:
        logger.error(f"SQS poll failed: {e}")
        return 0


def main():
    """
    Main loop: load cities, then continuously poll SQS for earthquake events.
    """
    logger.info("=" * 60)
    logger.info("QuakeWatch Impact Processor starting")
    logger.info(f"SQS Queue: {SQS_QUEUE_URL}")
    logger.info(f"Impact radius: {IMPACT_RADIUS_KM} km")
    logger.info("=" * 60)

    # Load cities into memory cache
    while not cities_cache:
        load_cities()
        if not cities_cache:
            logger.warning("No cities loaded. Retrying in 10s...")
            time.sleep(10)

    # Main processing loop
    logger.info("Starting SQS polling loop...")
    while True:
        try:
            poll_sqs()
        except Exception as e:
            logger.error(f"Poll cycle error: {e}")
            time.sleep(5)


if __name__ == "__main__":
    main()
