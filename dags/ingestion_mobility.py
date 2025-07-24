from datetime import datetime
from airflow.models import Variable
from utils.api_helpers import fetch_api_data, store_raw_data

def ingest_mobility_data(**kwargs):
    """
    Fetch and store urban mobility data from CityBikes API.
    Grabs list of bike-sharing networks globally.
    """
    url = "https://api.citybik.es/v2/networks"

    # 1. Fetch mobility data
    data = fetch_api_data(url)

    # 2. Store raw data into MongoDB
    metadata = store_raw_data(data, collection_name="raw_mobility")

    # 3. Push metadata via XCom
    metadata["source"] = "mobility"
    kwargs["ti"].xcom_push(key="ingestion_metadata", value=metadata)

    return "Mobility data ingested successfully."
