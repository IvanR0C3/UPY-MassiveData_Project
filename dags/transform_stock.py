from airflow.models import Variable
from utils.mongo_utils import insert_processed_data
from datetime import datetime

def transform_stock_data(**context):
    """
    Transforms Tesla stock data pulled from XCom.
    Cleans and enriches with daily percentage change.
    """
    ti = context["ti"]
    stock_data = ti.xcom_pull(task_ids='ingest_stock_data', key='stock_data')

    if not stock_data or not isinstance(stock_data, list):
        print("No stock data received or wrong type.")
        return

    transformed = []

    for record in stock_data:
        if not isinstance(record, dict):
            print("Skipping record due to invalid format (not dict):", record)
            continue
        try:
            date = record.get("date")
            close = float(record.get("close", 0))
            open_price = float(record.get("open", 0))

            percent_change = ((close - open_price) / open_price) * 100 if open_price != 0 else 0

            transformed_record = {
                "date": datetime.strptime(date, "%Y-%m-%d").isoformat(),
                "open": open_price,
                "close": close,
                "high": float(record.get("high", 0)),
                "low": float(record.get("low", 0)),
                "volume": int(record.get("volume", 0)),
                "percent_change": round(percent_change, 2),
                "is_gain": close > open_price,
                "symbol": record.get("symbol", "TSLA")
            }

            transformed.append(transformed_record)

        except Exception as e:
            print(f"Error transforming record {record}: {e}")
            continue

    if transformed:
        insert_processed_data("massive_data_db", "stock", transformed)
        print(f"Inserted {len(transformed)} transformed stock records.")
    else:
        print("No valid stock records to transform.")
