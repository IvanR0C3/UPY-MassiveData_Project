from airflow.models import Variable
from datetime import datetime
from utils.api_helpers import fetch_api_data, store_raw_data
from utils.mongo_utils import get_mongo_client

def ingest_stock_data(**context):
    """
    Ingests Tesla stock data from the Financial Modeling Prep API and stores it raw in MongoDB.
    """
    url = "https://financialmodelingprep.com/api/v3/historical-price-full/TSLA?serietype=line"
    api_key = Variable.get("FMP_API_KEY")
    full_url = f"{url}&apikey={api_key}"

    # 1. Fetch
    json_data = fetch_api_data(full_url)
    stock_data = json_data.get("historical", [])[:5]

    # 2. Store raw in MongoDB
    store_raw_data(stock_data, collection_name="raw_stock")

    # 3. Prepare XCom-safe version (remove ObjectId)
    xcom_data = [{k: v for k, v in doc.items() if k != "_id"} for doc in stock_data]

    # 4. Push to XCom
    context['ti'].xcom_push(key='stock_data', value=xcom_data)

    return "Stock data ingested successfully."
