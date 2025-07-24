from airflow.models import Variable
from utils.mongo_utils import insert_processed_data

def transform_mobility_data(**context):
    """
    Transforms bike-sharing mobility data from CityBikes API.
    Extracts metadata and enriches with location tags.
    """
    ti = context['ti']
    raw_data = ti.xcom_pull(task_ids='ingest_mobility_data', key='ingestion_metadata')

    if not raw_data:
        print("No mobility metadata received.")
        return

    # Now fetch the actual data again via direct access to Mongo (you can also push via XCom if you prefer)
    from utils.mongo_utils import get_mongo_client
    client = get_mongo_client()
    db = client["massive_data_db"]
    raw_collection = db["raw_mobility"]

    # Get the last inserted document (the full API response)
    last_entry = raw_collection.find_one(sort=[("ingested_at", -1)])

    if not last_entry or "networks" not in last_entry:
        print("No valid networks found in raw data.")
        return

    processed = []
    for network in last_entry["networks"][:10]:  # Only transform 10 entries to keep it light
        try:
            location = network.get("location", {})
            record = {
                "network_id": network.get("id"),
                "name": network.get("name"),
                "city": location.get("city"),
                "country": location.get("country"),
                "coordinates": {
                    "lat": location.get("latitude"),
                    "lon": location.get("longitude")
                },
                "is_mexico": location.get("country") == "MX"
            }
            processed.append(record)
        except Exception as e:
            print(f"Skipping record due to error: {e}")
            continue

    if processed:
        insert_processed_data("massive_data_db", "mobility", processed)
        print(f"Inserted {len(processed)} mobility records.")
    else:
        print("No mobility records to transform.")
