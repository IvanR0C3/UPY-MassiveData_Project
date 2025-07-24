from airflow.models import Variable
from datetime import datetime, timedelta
from utils.api_helpers import fetch_api_data, store_raw_data

def ingest_weather_data(**context):
    """
    Ingests weather data for Mérida from WeatherAPI and stores it raw in MongoDB.
    """
    base_url = "http://api.weatherapi.com/v1/forecast.json"
    api_key = Variable.get("WEATHER_API_KEY")

    # Use today if after 6 AM UTC, else use yesterday
    today = datetime.utcnow()
    query_date = today - timedelta(days=1) if today.hour < 6 else today
    date_str = query_date.strftime('%Y-%m-%d')

    full_url = f"{base_url}?key={api_key}&q=Merida&days=1&dt={date_str}"

    json_data = fetch_api_data(full_url)

    # Store raw data to MongoDB
    store_raw_data(json_data, collection_name="raw_weather")

    # Remove ObjectId if added by MongoDB
    if "_id" in json_data:
        del json_data["_id"]

    context['ti'].xcom_push(key='weather_data', value=json_data)
    return "Weather data ingested successfully."
