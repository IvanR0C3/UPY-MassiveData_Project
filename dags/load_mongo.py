from utils.mongo_utils import get_mongo_client
from datetime import datetime
import logging

def load_processed_data():
    """
    Loads processed data from processed collections and simulates
    loading it into a data warehouse (logs it here).
    """
    client = get_mongo_client()
    db = client["etl_db"]

    collections = ["processed_stock", "processed_weather", "processed_mobility"]
    loaded_counts = {}

    for collection_name in collections:
        collection = db[collection_name]
        records = list(collection.find())

        if not records:
            logging.warning(f"[LOAD] Collection '{collection_name}' is empty. Skipping.")
            continue

        logging.info(f"[LOAD] Loading {len(records)} records from '{collection_name}'")
        for record in records:
            logging.debug(f"[{collection_name}] {record}")

        loaded_counts[collection_name] = len(records)

    logging.info(f"[LOAD] Completed loading for: {loaded_counts}")
    return f"Loaded data from: {', '.join(loaded_counts.keys())}"
