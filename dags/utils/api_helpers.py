import requests
from datetime import datetime
from pymongo import MongoClient


def fetch_api_data(url, headers=None, params=None, timeout=10):
    """
    Generic function to fetch and parse JSON data from an API.
    Returns the parsed JSON or raises a RuntimeError.
    """
    try:
        response = requests.get(url, headers=headers, params=params, timeout=timeout)
        response.raise_for_status()
        return response.json()
    except requests.RequestException as e:
        raise RuntimeError(f"API request failed: {e}")

def log_ingestion_metadata(data, source_name):
    """
    Logs useful ingestion info and returns metadata.
    """
    record_count = len(data) if isinstance(data, list) else 1
    metadata = {
        "source": source_name,
        "record_count": record_count,
        "ingested_at": datetime.utcnow()
    }
    print(f"[{source_name.upper()}] Ingested {record_count} records at {metadata['ingested_at']}")
    return metadata

def store_raw_data(data, collection_name, db_name='massive_data_db', mongo_uri='mongodb://mongo:27017/'):
    """
    Store raw data into MongoDB with ingestion timestamp.
    """
    client = MongoClient(mongo_uri)
    db = client[db_name]
    collection = db[collection_name]

    # Add timestamp
    timestamp = datetime.utcnow()
    if isinstance(data, list):
        for item in data:
            item["ingested_at"] = timestamp
        collection.insert_many(data)
    else:
        data["ingested_at"] = timestamp
        collection.insert_one(data)

    print(f"[{collection_name}] Inserted {len(data) if isinstance(data, list) else 1} records at {timestamp}")
    client.close()

    return {"collection": collection_name, "record_count": len(data) if isinstance(data, list) else 1, "ingested_at": timestamp}
