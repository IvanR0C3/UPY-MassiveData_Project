from pymongo import MongoClient
from datetime import datetime
import os

# Connection function
def get_mongo_client():
    mongo_uri = os.getenv("MONGO_URI", "mongodb://mongo:27017/")
    return MongoClient(mongo_uri)

# Function to insert raw data
def insert_raw_data(db_name, collection_name, data):
    client = get_mongo_client()
    db = client[db_name]
    collection = db[f"raw_{collection_name}"]

    if isinstance(data, list):
        result = collection.insert_many(data)
        count = len(result.inserted_ids)
    else:
        result = collection.insert_one(data)
        count = 1

    print(f"[Mongo] Inserted {count} raw document(s) into '{collection.name}'")
    return count

# Function to insert processed data
def insert_processed_data(db_name, collection_name, data):
    client = get_mongo_client()
    db = client[db_name]
    collection = db[f"processed_{collection_name}"]

    if isinstance(data, list):
        result = collection.insert_many(data)
        count = len(result.inserted_ids)
    else:
        result = collection.insert_one(data)
        count = 1

    print(f"[Mongo] Inserted {count} processed document(s) into '{collection.name}'")
    return count

# Optional: log metadata
def log_metadata(db_name, source, stage, record_count):
    client = get_mongo_client()
    db = client[db_name]
    log_collection = db["etl_logs"]

    log_collection.insert_one({
        "source": source,
        "stage": stage,
        "record_count": record_count,
        "timestamp": datetime.utcnow()
    })

    print(f"[Mongo] Logged metadata for {source} at stage '{stage}'")
