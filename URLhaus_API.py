import os
import json
import requests
from pymongo import MongoClient
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

API_URL = "https://urlhaus.abuse.ch/downloads/json_recent/"
MONGO_URI = os.getenv("MONGO_URI")
DB_NAME = os.getenv("DB_NAME")
COLLECTION = os.getenv("COLLECTION")

print("Config:")
print(" API_URL:", API_URL)
print(" MONGO_URI:", MONGO_URI)
print(" DB_NAME:", DB_NAME, "COLLECTION:", COLLECTION)


def extract_data():
    print("Starting ETL...")
    try:
        print(f"Fetching JSON data from: {API_URL}")
        response = requests.get(API_URL, timeout=30, headers={
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
            'Accept': 'application/json'
        })
        response.raise_for_status()

        print("Response status:", response.status_code)

        data = response.json()

        # If the JSON is a dict of lists
        all_records = []
        for id_key, record_list in data.items():
            if isinstance(record_list, list):
                for record in record_list:
                    record["id"] = id_key
                    all_records.append(record)

        print(f"Extracted {len(all_records)} records from JSON.")
        if all_records:
            print("Sample record:", json.dumps(all_records[0], indent=2))

        return all_records

    except Exception as e:
        print("Error fetching data:", str(e))
        return []


def transform_data(records):
    print("Transforming data...")
    if not records:
        return []

    transformed = []
    for rec in records:
        transformed.append({
            "url": rec.get("url", ""),
            "date_added": rec.get("date_added") or rec.get("dateadded", ""),
            "threat": rec.get("threat", ""),
            "tags": rec.get("tags", []),
            "reporter": rec.get("reporter", "unknown"),
            "url_status": rec.get("url_status", ""),
            "last_online": rec.get("last_online", ""),
            "urlhaus_reference": rec.get("urlhaus_reference") or rec.get("urlhaus_link", "")
        })

    return transformed


def load_data(records):
    if not records:
        print("No data to load into MongoDB.")
        return

    try:
        client = MongoClient(MONGO_URI)
        db = client[DB_NAME]
        collection = db[COLLECTION]

        result = collection.insert_many(records)
        print(f"Inserted {len(result.inserted_ids)} records into MongoDB.")

    except Exception as e:
        print("Error loading data into MongoDB:", str(e))
    finally:
        client.close()


if __name__ == "__main__":
    extracted = extract_data()
    transformed = transform_data(extracted)
    load_data(transformed)
    print("ETL completed.")
