from utils.mongo_utils import get_mongo_client
from datetime import datetime
import logging

def transform_weather_data(**context):
    """
    Transforms raw weather data from MongoDB and stores the processed version.
    Skips records that do not contain forecast data or critical temperature fields.
    """
    client = get_mongo_client()
    db = client["massive_data_db"]
    raw_collection = db["raw_weather"]
    processed_collection = db["processed_weather"]

    raw_docs = list(raw_collection.find())
    transformed_docs = []

    for doc in raw_docs:
        try:
            location = doc.get("location", {})
            forecast = doc.get("forecast", {}).get("forecastday", [])

            if not forecast:
                logging.warning("Skipping weather record: forecastday is empty.")
                continue

            forecast_data = forecast[0].get("day", {})
            if forecast_data.get("avgtemp_c") is None or not location.get("name"):
                logging.warning("Skipping weather record: missing critical temperature or location data.")
                continue

            transformed = {
                "location": location.get("name"),
                "region": location.get("region"),
                "country": location.get("country"),
                "avg_temp_c": forecast_data.get("avgtemp_c"),
                "max_temp_c": forecast_data.get("maxtemp_c"),
                "min_temp_c": forecast_data.get("mintemp_c"),
                "condition": forecast_data.get("condition", {}).get("text", "Unknown"),
                "transformed_at": datetime.utcnow()
            }

            transformed_docs.append(transformed)

        except Exception as e:
            logging.warning(f"[weather] Skipping record due to error: {e}")

    if transformed_docs:
        processed_collection.insert_many(transformed_docs)
        logging.info(f"[processed_weather] Inserted {len(transformed_docs)} records at {datetime.utcnow()}")

    return "Weather data transformed successfully."
    
    print("Forecast field:", forecast)
    print("Forecast day:", forecast_data)
    
