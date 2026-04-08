"""
QuakeWatch - City Seed Data Script
Downloads the GeoNames cities dataset and loads the top 1,000 cities
by population into the DynamoDB cities table.

Run once before starting the system:
    python seed_cities.py

Owner: Asha
"""

import os
import sys
import json
import csv
import logging
import zipfile
import io
from decimal import Decimal

import boto3
import requests
from botocore.exceptions import ClientError

# ── Configuration ──────────────────────────────────────────
CITIES_TABLE = os.environ.get("CITIES_TABLE", "cities")
AWS_REGION = os.environ.get("AWS_REGION", "us-east-1")
GEONAMES_URL = "https://download.geonames.org/export/dump/cities15000.zip"
TOP_N_CITIES = 1000

# ── Logging ────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [SEED] %(levelname)s %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("seed")

# GeoNames TSV column indices (from readme.txt)
# 0: geonameid, 1: name, 2: asciiname, 3: alternatenames,
# 4: latitude, 5: longitude, 6: feature class, 7: feature code,
# 8: country code, 9: cc2, 10: admin1, 11: admin2,
# 12: admin3, 13: admin4, 14: population, 15: elevation,
# 16: dem, 17: timezone, 18: modification date
COL_ID = 0
COL_NAME = 1
COL_LAT = 4
COL_LON = 5
COL_COUNTRY = 8
COL_POPULATION = 14


def download_and_parse_cities() -> list[dict]:
    """
    Download the GeoNames cities15000 dataset and extract the top N cities
    by population.
    """
    logger.info(f"Downloading GeoNames dataset from {GEONAMES_URL}...")
    response = requests.get(GEONAMES_URL, timeout=60)
    response.raise_for_status()

    logger.info("Extracting ZIP file...")
    z = zipfile.ZipFile(io.BytesIO(response.content))

    # The ZIP contains cities15000.txt
    txt_filename = "cities15000.txt"
    cities = []

    with z.open(txt_filename) as f:
        reader = csv.reader(io.TextIOWrapper(f, encoding="utf-8"), delimiter="\t")
        for row in reader:
            try:
                population = int(row[COL_POPULATION])
                if population <= 0:
                    continue

                cities.append({
                    "city_id": row[COL_ID],
                    "name": row[COL_NAME],
                    "country": row[COL_COUNTRY],
                    "lat": float(row[COL_LAT]),
                    "lon": float(row[COL_LON]),
                    "population": population,
                })
            except (ValueError, IndexError):
                continue

    # Sort by population descending and take top N
    cities.sort(key=lambda c: c["population"], reverse=True)
    top_cities = cities[:TOP_N_CITIES]

    logger.info(
        f"Parsed {len(cities)} cities total, selected top {len(top_cities)} by population"
    )
    logger.info(
        f"Largest: {top_cities[0]['name']} ({top_cities[0]['population']:,}), "
        f"Smallest in set: {top_cities[-1]['name']} ({top_cities[-1]['population']:,})"
    )

    return top_cities


def save_cities_json(cities: list[dict], filepath: str = "cities.json"):
    """Save cities to a JSON file for reference / debugging."""
    with open(filepath, "w") as f:
        json.dump(cities, f, indent=2)
    logger.info(f"Saved {len(cities)} cities to {filepath}")


def upload_to_dynamodb(cities: list[dict]):
    """
    Batch-write cities to DynamoDB. Uses batch_writer which handles
    batching and retries automatically.
    """
    dynamodb = boto3.resource("dynamodb", region_name=AWS_REGION)
    table = dynamodb.Table(CITIES_TABLE)

    logger.info(f"Uploading {len(cities)} cities to DynamoDB table '{CITIES_TABLE}'...")

    with table.batch_writer() as batch:
        for i, city in enumerate(cities):
            batch.put_item(
                Item={
                    "city_id": city["city_id"],
                    "name": city["name"],
                    "country": city["country"],
                    "lat": Decimal(str(round(city["lat"], 4))),
                    "lon": Decimal(str(round(city["lon"], 4))),
                    "population": city["population"],
                }
            )

            if (i + 1) % 100 == 0:
                logger.info(f"  Uploaded {i + 1}/{len(cities)} cities...")

    logger.info(f"Successfully uploaded all {len(cities)} cities to DynamoDB")


def main():
    logger.info("=" * 60)
    logger.info("QuakeWatch City Seed Script")
    logger.info("=" * 60)

    # Step 1: Download and parse
    cities = download_and_parse_cities()

    # Step 2: Save local JSON copy
    save_cities_json(cities)

    # Step 3: Upload to DynamoDB
    upload_to_dynamodb(cities)

    logger.info("Seeding complete!")


if __name__ == "__main__":
    main()