import streamlit as st
import pandas as pd
from pymongo import MongoClient
from datetime import datetime
import plotly.express as px

# Mongo connection
@st.cache_resource
def get_mongo_client():
    return MongoClient("mongodb://mongo:27017/")

# Load dataset
@st.cache_data
def load_data(collection_name):
    db = get_mongo_client()["massive_data_db"]
    collection = db[collection_name]
    data = list(collection.find({}, {"_id": 0}))
    return pd.DataFrame(data)

# Filter by date
def filter_by_date(df, date_column, start_date, end_date):
    df[date_column] = pd.to_datetime(df[date_column])
    return df[(df[date_column] >= start_date) & (df[date_column] <= end_date)]

# UI
st.sidebar.title("📊 Dashboard Filters")
dataset = st.sidebar.selectbox("Select dataset", ["Stock", "Weather", "Mobility"])
start_date = st.sidebar.date_input("Start date", datetime(2025, 7, 22))
end_date = st.sidebar.date_input("End date", datetime(2025, 7, 25))

# Collection map
collection_map = {
    "Stock": "processed_stock",
    "Weather": "processed_weather",
    "Mobility": "processed_mobility"
}

# Load data
df = load_data(collection_map[dataset])

# Tabs
tab1, tab2 = st.tabs(["📋 Data Overview", "📈 Visualizations"])

with tab1:
    st.header(f"{dataset} Data Preview")
    if dataset == "Stock":
        df_filtered = filter_by_date(df, "date", pd.to_datetime(start_date), pd.to_datetime(end_date))
    elif dataset == "Weather":
        df_filtered = filter_by_date(df, "transformed_at", pd.to_datetime(start_date), pd.to_datetime(end_date))
    else:
        df_filtered = df.copy()
    st.dataframe(df_filtered)

with tab2:
    st.header(f"{dataset} Visualizations")
    if dataset == "Stock":
        fig = px.line(df_filtered, x="date", y="close", title="Stock Closing Prices")
        st.plotly_chart(fig, use_container_width=True)

    elif dataset == "Weather":
        fig = px.bar(df_filtered, x="transformed_at", y=["min_temp_c", "avg_temp_c", "max_temp_c"],
                     title="Daily Temperatures in Merida", barmode="group")
        st.plotly_chart(fig, use_container_width=True)

    elif dataset == "Mobility":
        country_counts = df_filtered["country"].value_counts().reset_index()
        country_counts.columns = ["country", "count"]
        fig = px.pie(country_counts, names="country", values="count", title="Mobility Networks by Country")
        st.plotly_chart(fig, use_container_width=True)

