# UPY-MassiveData_Project
Final project for Massive Data Management 5B – analyzing how weather and urban mobility impact the financial markets using Airflow, MongoDB, Streamlit, and Docker.


## 🧠 Project Overview

This project presents a complete batch data pipeline developed for the *Massive Data Management* course at Universidad Politécnica de Yucatán (UPY). It demonstrates the implementation of an automated ETL (Extract, Transform, Load) process using Apache Airflow, MongoDB, and Streamlit.

The objective is to integrate and process data from **three different public APIs** related to:
- Financial stock market (Tesla stock)
- Urban mobility indicators
- Weather conditions in Mérida, Yucatán

The pipeline orchestrates the extraction of raw data, its transformation into structured and comparable formats, and its storage in MongoDB for later visualization through a dashboard. The Streamlit app allows users to explore correlations between market behavior, traffic trends, and weather conditions on a given date.

This repository serves as a foundation for working with real-world data, asynchronous pipelines, containerized environments (Docker), and basic interactive visualization.

## 🌐 APIs Used

This project integrates three public APIs to extract diverse data types for analysis and visualization:

### 1. 📈 Financial Modeling Prep API
- **Purpose**: Provides historical stock data.
- **Entity Example**: Tesla (TSLA)
- **Endpoint**: [`https://financialmodelingprep.com/api/v3/historical-price-full/TSLA`](https://financialmodelingprep.com/api/v3/historical-price-full/TSLA)
- **Authentication**: Requires API key (stored as an Airflow variable `FMP_API_KEY`)

---

### 2. 🌦️ WeatherAPI
- **Purpose**: Supplies weather forecast and historical weather data for specified cities.
- **City Used**: Mérida, Yucatán
- **Endpoint**: [`http://api.weatherapi.com/v1/forecast.json`](https://www.weatherapi.com/docs/)
- **Authentication**: Requires API key (stored as an Airflow variable `WEATHER_API_KEY`)

---

### 3. 🚦 Mobility Data API
- **Purpose**: Simulated urban mobility data, such as traffic congestion or movement trends.
- **Endpoint**: [`https://api.citymobility.org/mobility`](https://api.citymobility.org/mobility)
- **Note**: This is a fictional or placeholder API used for educational purposes. The data mimics real-time traffic patterns by date.

## 🆚 Why MongoDB Instead of PostgreSQL?

Although PostgreSQL is commonly used in production-grade ETL systems, **MongoDB** was chosen for this academic project for the following reasons:

### ✅ Simplicity & Flexibility
- MongoDB stores documents in a flexible, JSON-like format, which matches the structure of data retrieved directly from public APIs (especially weather, mobility, and stock APIs).
- No need for rigid schema definitions or table normalization, making ingestion faster.

### ✅ Faster Prototyping
- Since this project involves fast batch ingestion and transformation of external API data, MongoDB allows quicker storage and inspection without upfront schema setup.

### ✅ Educational Fit
- The focus of the project is to understand **ETL orchestration and pipeline design**, not relational database modeling.
- MongoDB aligns better with exploratory data workflows in data engineering classrooms.

### 🧩 PostgreSQL Role
- PostgreSQL **is still used** by Apache Airflow internally as its metadata database.
- In a real-world pipeline, PostgreSQL (or other relational DBs) would typically be used for **data marts**, reporting layers, or after transformations are complete.

## 🚀 How to Launch the Project with Docker Compose

This project uses Docker Compose to orchestrate all required services, including Airflow (for orchestration), MongoDB (for raw and processed data), and Streamlit (for data visualization). Follow the steps below to build and run the complete stack.

### 📦 Prerequisites

Before launching, ensure the following are installed on your machine:

- [Docker](https://docs.docker.com/get-docker/)
- [Docker Compose](https://docs.docker.com/compose/)

---

### 🐳 Launch All Services

Navigate to the root of the project directory and run:

```bash
docker compose up --build
```

This command will:

Build and run the Streamlit app from streamlit_app/Dockerfile.

Start the MongoDB container and bind port 27018 to your local machine.

Launch the Airflow stack with Webserver, Scheduler, and PostgreSQL.

## 🏃 Triggering the DAG and Checking Logs in Airflow

Once your services are running and Airflow is accessible at [http://localhost:8080](http://localhost:8080), follow these steps to manually trigger your DAG and inspect task execution:

### 🔔 Trigger the DAG Manually

1. **Log into the Airflow UI**:
   Open your browser and go to:  
   [http://localhost:8080](http://localhost:8080)

2. **Activate the DAG**:
   - In the **DAGs list**, locate `main_etl_pipeline`.
   - Toggle the switch on the left to **activate** it.

3. **Trigger a DAG run**:
   - Click the **Play button ▶️** on the right side of the DAG row.
   - Confirm to manually trigger the DAG.

---

### 📄 View Task Logs

To view logs for any task in the pipeline:

1. In the DAG UI, click on **main_etl_pipeline**.
2. On the **Graph View** or **Tree View**, click on any task (e.g., `ingest_weather_data`).
3. In the popup window, click on **View Log**.

This will open the full logs for that specific task execution, which helps in debugging and confirming successful completion.

---

### ✅ Expected DAG Flow

The pipeline executes the following tasks:

- `ingest_stock_data`
- `ingest_weather_data`
- `ingest_mobility_data`
- `transform_stock_data`
- `transform_weather_data`
- `transform_mobility_data`
- `load_processed_data`

Each task logs its progress, and any error messages can be inspected through the **Logs** tab.

## 📊 Viewing the Streamlit Dashboard

The Streamlit dashboard provides a visual interface to explore the processed data from all three APIs (Stock, Weather, Mobility).

### 🚀 How to Access the Dashboard

Once Docker Compose is running and services are up, Streamlit will be served automatically at:

http://localhost:8501


Open that URL in your browser.

### 📂 Dashboard Features

The dashboard will display the following:

- **Stock Prices (Tesla)**:  
  Historical data showing opening, closing, high, and low prices with volume and date information.

- **Weather Data (Mérida)**:  
  Includes average, max, and min temperature, as well as the condition for a specific date.

- **Mobility Data**:  
  Illustrates mobility patterns such as traffic flow or public transportation usage.

### 🧠 What to Look For

The dashboard attempts to draw correlations between weather patterns, mobility behavior, and Tesla’s stock price for a specific date or range.

Use it to answer questions like:

- Does rainy or hot weather correlate with a drop in mobility?
- Do certain mobility trends align with stock price increases or decreases?
- How does overall environmental behavior affect company performance?

### 🛠 Troubleshooting

If the dashboard doesn't appear:

1. Make sure the **MongoDB container is running**.
2. Check that the database contains **processed_*** collections (`processed_stock`, `processed_weather`, `processed_mobility`).
3. Rebuild the Streamlit container if necessary:

```bash
docker compose build streamlit
docker compose up
```
## 🔁 Understanding XCom Usage

In Apache Airflow, **XCom** (short for _"cross-communication"_) allows tasks to share data between each other during DAG execution.

### 🧩 How XCom Works in This Project

In this ETL pipeline, each **Ingest** task pulls data from an external API and pushes it to an XCom so that the corresponding **Transform** task can access it.

This helps maintain a clean separation between tasks and ensures a modular and traceable data pipeline.

---

### 🔄 Flow of XCom Data

Each `ingest_*_data` function uses the following command to push API data:

```python
context['ti'].xcom_push(key='weather_data', value=json_data)

Then, in the corresponding transform task, the data is retrieved with:

weather_data = context['ti'].xcom_pull(key='weather_data', task_ids='ingest_weather_data')

This pattern is used for:

stock_data (from ingest_stock_data → transform_stock_data)

weather_data (from ingest_weather_data → transform_weather_data)

mobility_data (from ingest_mobility_data → transform_mobility_data)

Why Use XCom Here?
It avoids the need to re-query APIs in the transform stage.

It reduces I/O overhead.

It enhances data lineage and debuggability, allowing you to inspect what each task passed to the next.

It allows the load task to pull data from all transform stages cleanly.

How to Inspect XCom in the Airflow UI
You can inspect the stored XCom values in the Airflow Web UI:

Go to the DAG graph view.

Click on a task (e.g., ingest_weather_data).

Select the "XCom" tab.

View the pushed data (if serializable).

Note: Complex objects like MongoDB _id fields (ObjectId) must be excluded or converted to avoid serialization errors.