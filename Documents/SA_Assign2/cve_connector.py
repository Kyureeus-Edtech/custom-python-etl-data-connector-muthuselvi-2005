#!/usr/bin/env python3
import os
import datetime as dt
import logging
import sys
import requests
from pymongo import MongoClient
from pymongo.errors import PyMongoError
from dotenv import load_dotenv

# ------------------------------
# Logging Configuration
# ------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)
logger = logging.getLogger("cve_full_etl")

# ------------------------------
# Base URL for CVE Search API
# ------------------------------
BASE_URL = "https://cve.circl.lu/api"

# ------------------------------
# Endpoints to Call (4–5 modules)
# ------------------------------
ENDPOINTS = {
    "last": {
        "path": "/last",
        "method": "GET",
        "params": None
    },
    "browse_vendor": {
        "path": "/browse/microsoft",
        "method": "GET",
        "params": None
    },
    "product_cves": {
        "path": "/search/microsoft/windows_10",
        "method": "GET",
        "params": None
    },
    "cve_details": {
        "path": "/cve/CVE-2024-12345",
        "method": "GET",
        "params": None
    },
    "cwe": {
        "path": "/cwe",
        "method": "GET",
        "params": None
    }
}

# ------------------------------
# Extract Function
# ------------------------------
def extract(module: str, ep_def: dict):
    path = ep_def["path"]
    url = BASE_URL + path
    method = ep_def.get("method", "GET")
    params = ep_def.get("params")

    logger.info("Calling %s -> %s", module, url)
    try:
        if method == "GET":
            resp = requests.get(url, params=params, timeout=20)
        else:
            resp = requests.request(method, url, params=params, timeout=20)
        resp.raise_for_status()
    except requests.exceptions.RequestException as e:
        logger.error("HTTP error on %s: %s", module, e)
        return None, None

    try:
        data = resp.json()
    except ValueError:
        logger.error("Invalid JSON on %s: %s", module, resp.text)
        return None, None

    meta = {
        "module": module,
        "path": path,
        "status_code": resp.status_code,
        "fetched_at": dt.datetime.now(dt.timezone.utc),
        "params": params
    }
    return data, meta

# ------------------------------
# Transform Function (Fixed)
# ------------------------------
def transform(data):
    """Simplify the data based on its structure (list or dict)."""

    # Case 1: List of CVE dictionaries or strings
    if isinstance(data, list):
        simplified = []
        for item in data[:10]:  # limit to 10 for demo
            if isinstance(item, dict):
                simplified.append({
                    "id": item.get("id"),
                    "cvss": item.get("cvss"),
                    "summary": item.get("summary")
                })
            else:
                simplified.append({"value": str(item)})
        return simplified

    # Case 2: Dictionary response
    elif isinstance(data, dict):
        if "product" in data:
            # e.g., /browse/microsoft
            return {
                "vendor": data.get("vendor"),
                "products": data.get("product")[:10]
            }
        elif "id" in data and "summary" in data:
            # e.g., /cve/CVE-XXXX-XXXX
            return {
                "id": data.get("id"),
                "cvss": data.get("cvss"),
                "summary": data.get("summary")
            }
        else:
            return data

    # Case 3: Unknown (string, number, etc.)
    else:
        return {"value": str(data)}

# ------------------------------
# Load Function (to MongoDB)
# ------------------------------
def load_mongo(doc, meta, db_name, coll_name, mongo_uri):
    client = MongoClient(mongo_uri)
    try:
        coll = client[db_name][coll_name]
        rec = {
            "data": doc,
            "meta": meta,
            "etl": {
                "source": "cve_full",
                "ingested_at": dt.datetime.now(dt.timezone.utc),
                "version": 1
            }
        }
        coll.insert_one(rec)
        logger.info("Inserted module %s record", meta.get("module"))
    except PyMongoError as e:
        logger.error("MongoDB error: %s", e)
    finally:
        client.close()

# ------------------------------
# Main ETL Execution
# ------------------------------
def main():
    load_dotenv()
    mongo_uri = os.getenv("MONGO_URI", "mongodb://localhost:27017")
    db = os.getenv("MONGO_DB", "threat_intel")
    coll = os.getenv("COLLECTION_NAME", "cve_full")

    for module, ep_def in ENDPOINTS.items():
        data, meta = extract(module, ep_def)
        if data is None:
            continue
        transformed = transform(data)
        load_mongo(transformed, meta, db, coll, mongo_uri)

    logger.info("✅ All modules processed successfully.")

# ------------------------------
# Script Entry Point
# ------------------------------
if __name__ == "__main__":
    main()
