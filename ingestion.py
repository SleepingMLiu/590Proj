import json
import requests
from datetime import datetime, timezone, timedelta
from google.cloud import pubsub_v1
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.hooks.gcs import GCSHook
import psycopg2

# -----------------------------
# Constants and Helper Functions
# -----------------------------

PROJECT_ID = "prime-agency-456202-b7"
RAW_BUCKET_NAME = "ece-590-group2-raw"
PROCESSED_BUCKET_NAME = "ece-590-group2-processed"
PUBSUB_TOPIC_ID = "data-upload-complete-topic"

NCEI_ALL_DATA_TYPES = [
    "TMAX", "TMIN", "TAVG", "PRCP", "SNOW", "SNWD",
    "AWND", "WDF2", "WSF2",
    "WT01", "WT02", "WT03", "WT04", "WT05", "WT06",
    "WT07", "WT08", "WT09", "WT10", "WT11", "WT13",
    "WT14", "WT15", "WT16", "WT17", "WT18", "WT19",
    "WT21", "WT22"
]

DB_CONFIG = {
    'dbname': "noaa",
    'user': "postgres",
    'password': "final-project",
    'host': "35.202.11.58",
    'connect_timeout': 10
}

def publish_to_pubsub(message, topic_name):
    """Publish a message to a Pub/Sub topic."""
    publisher = pubsub_v1.PublisherClient()
    topic_path = publisher.topic_path(PROJECT_ID, topic_name)
    publisher.publish(topic_path, json.dumps(message).encode('utf-8'))

def fetch_from_noaa(station_id, start_date, end_date):
    """Fetch data from NOAA API."""
    url = f"https://api.weather.gov/stations/{station_id}/observations?start={start_date}T00:00:00Z&end={end_date}T23:59:59Z"
    try:
        response = requests.get(url, timeout=30)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.HTTPError as e:
        raise ValueError(f"HTTP error when fetching from NOAA: {e}")
    except Exception as e:
        raise ValueError(f"Error fetching NOAA data: {e}")

def fetch_from_ncei(station_id, start_date, end_date):
    """Fetch data from NCEI API."""
    params = {
        "dataset": "daily-summaries",
        "stations": station_id,
        "startDate": start_date,
        "endDate": end_date,
        "dataTypes": ",".join(NCEI_ALL_DATA_TYPES),
        "units": "metric",
        "format": "json",
        "includeStationName": "true"
    }
    try:
        response = requests.get(
            "https://www.ncei.noaa.gov/access/services/data/v1",
            params=params,
            headers={"User-Agent": "WeatherDataIngestion/1.0"},
            timeout=30
        )
        response.raise_for_status()
        return response.json()
    except Exception as e:
        raise ValueError(f"Error fetching NCEI data: {e}")

def save_raw_to_gcs(data, conf):
    """Save raw fetched data into GCS."""
    gcs_hook = GCSHook(gcp_conn_id="google_cloud_default")
    sanitized_date_range = conf['date_range'].replace(" ", "_").replace("/", "-")
    file_path = f"noaa/{conf['location']}/{sanitized_date_range}.json"
    gcs_hook.upload(
        bucket_name=RAW_BUCKET_NAME,
        object_name=file_path,
        data=json.dumps(data, indent=2),
        mime_type='application/json'
    )
    print(f"[INFO] Raw data saved to GCS: {file_path}")

def insert_data_into_postgres(data, conf):
    """Insert parsed weather data into PostgreSQL."""
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cursor = conn.cursor()

        if 'features' in data:
            observations = data.get('features', [])
            for obs in observations:
                props = obs.get('properties', {})
                station_id = props.get('station', '').split('/')[-1]
                timestamp = props.get('timestamp')

                cursor.execute(
                    """
                    INSERT INTO readings (station_id, observation_time, temperature_value)
                    VALUES (%s, %s, %s)
                    ON CONFLICT (station_id, observation_time) DO NOTHING
                    """,
                    (
                        station_id,
                        timestamp,
                        props.get('temperature', {}).get('value')
                    )
                )
        else:
            for record in data:
                cursor.execute(
                    """
                    INSERT INTO ncei_daily_data (station_id, date, tmax, tmin, tavg, prcp)
                    VALUES (%s, %s, %s, %s, %s, %s)
                    ON CONFLICT (station_id, date) DO UPDATE SET
                    tmax=EXCLUDED.tmax, tmin=EXCLUDED.tmin, tavg=EXCLUDED.tavg, prcp=EXCLUDED.prcp
                    """,
                    (
                        record['STATION'],
                        record['DATE'],
                        record.get('TMAX'),
                        record.get('TMIN'),
                        record.get('TAVG'),
                        record.get('PRCP')
                    )
                )

        conn.commit()
        print("[INFO] Data inserted into PostgreSQL.")
    except Exception as e:
        print(f"[ERROR] Failed inserting into PostgreSQL: {e}")
        if conn:
            conn.rollback()
        raise
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

def check_postgres(**kwargs):
    """Check if requested data already exists."""
    conf = kwargs['dag_run'].conf
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cursor = conn.cursor()
        start_date, end_date = conf['date_range'].split(" to ")

        cursor.execute(
            """
            SELECT 1 FROM readings 
            WHERE station_id=%s 
            AND observation_time BETWEEN %s AND %s 
            LIMIT 1
            """,
            (conf['location'], start_date, end_date)
        )
        data_exists = cursor.fetchone()
        return "write_processed_to_gcs" if data_exists else "fetch_from_api"

    except Exception as e:
        print(f"[ERROR] Postgres check failed: {e}")
        raise
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

def fetch_from_api(**kwargs):
    """Fetch weather data if missing."""
    conf = kwargs['dag_run'].conf
    station_id = conf['location']
    start_date, end_date = conf['date_range'].split(" to ")

    data = fetch_from_noaa(station_id, start_date, end_date)
    save_raw_to_gcs(data, conf)
    insert_data_into_postgres(data, conf)

def write_processed_to_gcs(**kwargs):
    """Write processed weather data to GCS."""
    conf = kwargs['dag_run'].conf
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cursor = conn.cursor()

        start_date, end_date = conf['date_range'].split(" to ")

        cursor.execute(
            """
            SELECT station_id, observation_time, temperature_value
            FROM readings
            WHERE station_id=%s
            AND observation_time BETWEEN %s AND %s
            """,
            (conf['location'], start_date, end_date)
        )
        rows = cursor.fetchall()
        columns = [desc[0] for desc in cursor.description]

        if not rows:
            raise ValueError("No data found to write.")

        data = [dict(zip(columns, row)) for row in rows]
        gcs_hook = GCSHook(gcp_conn_id="google_cloud_default")

        sanitized_user = conf['user_id'].replace(" ", "_").replace("/", "-")
        sanitized_location = conf['location'].replace(" ", "_").replace("/", "-")
        sanitized_date_range = conf['date_range'].replace(" ", "_").replace("/", "-")
        output_file = f"weather_data/{sanitized_user}_{sanitized_location}_{sanitized_date_range}.json"

        gcs_hook.upload(
            bucket_name=PROCESSED_BUCKET_NAME,
            object_name=output_file,
            data=json.dumps(data, indent=2),
            mime_type='application/json'
        )

        publish_to_pubsub({
            "user_id": conf["user_id"],
            "location": conf["location"],
            "date_range": conf["date_range"],
            "status": "complete",
            "message": "Data ingestion completed."
        }, PUBSUB_TOPIC_ID)

        print("[INFO] Processed data uploaded and notification sent.")

    except Exception as e:
        print(f"[ERROR] Writing processed data failed: {e}")
        raise
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

# -----------------------------
# Define Airflow DAG
# -----------------------------

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id="data_request_pipeline",
    default_args=default_args,
    start_date=datetime(2024, 1, 1, tzinfo=timezone.utc),
    schedule_interval=None,
    catchup=False,
    tags=["weather_data"]
) as dag:

    check_task = PythonOperator(
        task_id="check_postgres",
        python_callable=check_postgres
    )

    fetch_task = PythonOperator(
        task_id="fetch_from_api",
        python_callable=fetch_from_api
    )

    write_task = PythonOperator(
        task_id="write_processed_to_gcs",
        python_callable=write_processed_to_gcs
    )

    # DAG Dependencies
    check_task >> fetch_task >> write_task
