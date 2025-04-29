import json
import requests
from datetime import datetime, timezone, timedelta
from google.cloud import pubsub_v1
from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.providers.google.cloud.hooks.gcs import GCSHook
import psycopg2

# -----------------------------
# Constants and Helper Functions
# -----------------------------

PROJECT_ID = "prime-agency-456202-b7"
RAW_BUCKET_NAME = "ece-590-group2-raw"
PROCESSED_BUCKET_NAME = "ece-590-group2-processed"
PUBSUB_TOPIC_ID = "data-upload-complete-topic"

DB_CONFIG = {
    'dbname': "noaa",
    'user': "postgres",
    'password': "final-project",
    'host': "35.202.11.58",
    'connect_timeout': 10
}

NCEI_ALL_DATA_TYPES = [
    "TMAX", "TMIN", "TAVG", "PRCP", "SNOW", "SNWD",
    "AWND", "WDF2", "WSF2",
    "WT01", "WT02", "WT03", "WT04", "WT05", "WT06",
    "WT07", "WT08", "WT09", "WT10", "WT11", "WT13",
    "WT14", "WT15", "WT16", "WT17", "WT18", "WT19",
    "WT21", "WT22"
]

def publish_to_pubsub(message, topic_name):
    publisher = pubsub_v1.PublisherClient()
    topic_path = publisher.topic_path(PROJECT_ID, topic_name)
    publisher.publish(topic_path, json.dumps(message).encode('utf-8'))

def fetch_from_ncei(station_id, start_date, end_date):
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

def insert_metadata_log(station_id, user_id, action):
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cursor = conn.cursor()
        now = datetime.now(timezone.utc)
        cursor.execute(
            """
            INSERT INTO data_metadata (station_id, data_inserted_at, user_accessed, access_timestamp, action, timestamp)
            VALUES (%s, %s, %s, %s, %s, %s)
            """,
            (station_id, now, user_id, now, action, now)
        )
        conn.commit()
        print(f"[INFO] Metadata log inserted: {action}")
    except Exception as e:
        print(f"[ERROR] Metadata logging failed: {e}")
        if conn:
            conn.rollback()
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

# -----------------------------
# Task Definitions
# -----------------------------

def check_postgres(**kwargs):
    conf = kwargs['dag_run'].conf
    conn = None
    cursor = None
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cursor = conn.cursor()
        start_date, end_date = conf['date_range'].split(" to ")
        start_date = start_date.strip()
        end_date = end_date.strip()

        cursor.execute(
            """
            SELECT COUNT(DISTINCT date)
            FROM readings
            WHERE station_id = %s
            AND date BETWEEN %s AND %s
            """,
            (conf['location'], start_date, end_date)
        )

        existing_days = cursor.fetchone()[0]
        start_dt = datetime.strptime(start_date, "%Y-%m-%d")
        end_dt = datetime.strptime(end_date, "%Y-%m-%d")
        requested_days = (end_dt - start_dt).days + 1

        if existing_days == requested_days:
            print("[INFO] Full data already exists.")
            return "write_processed_to_gcs"
        else:
            print("[INFO] Data missing, need to fetch.")
            return "fetch_from_api"

    except Exception as e:
        print(f"[ERROR] Database check failed: {e}")
        raise
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

def fetch_from_api(**kwargs):
    conf = kwargs['dag_run'].conf
    station_id = conf['location']
    start_date, end_date = conf['date_range'].split(" to ")

    data = fetch_from_ncei(station_id, start_date.strip(), end_date.strip())
    save_raw_to_gcs(data, conf)
    insert_data_into_postgres(data, conf)
    insert_metadata_log(station_id, conf['user_id'], "insert")

def save_raw_to_gcs(data, conf):
    gcs_hook = GCSHook(gcp_conn_id="google_cloud_default")
    sanitized_date_range = conf['date_range'].replace(" ", "_").replace("/", "-")
    file_path = f"ncei/{conf['location']}/{sanitized_date_range}.json"
    gcs_hook.upload(
        bucket_name=RAW_BUCKET_NAME,
        object_name=file_path,
        data=json.dumps(data, indent=2),
        mime_type='application/json'
    )
    print(f"[INFO] Raw data saved: {file_path}")

def insert_data_into_postgres(data, conf):
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cursor = conn.cursor()
        for record in data:
            cursor.execute(
                """
                INSERT INTO readings (station_id, date, tmax, tmin, tavg, prcp, snow, snwd, awnd, wdf2, wsf2, fog, thunder, smoke_haze, station_name)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (station_id, date) DO UPDATE SET
                    tmax=EXCLUDED.tmax,
                    tmin=EXCLUDED.tmin,
                    tavg=EXCLUDED.tavg,
                    prcp=EXCLUDED.prcp,
                    snow=EXCLUDED.snow,
                    snwd=EXCLUDED.snwd,
                    awnd=EXCLUDED.awnd,
                    wdf2=EXCLUDED.wdf2,
                    wsf2=EXCLUDED.wsf2,
                    fog=EXCLUDED.fog,
                    thunder=EXCLUDED.thunder,
                    smoke_haze=EXCLUDED.smoke_haze,
                    station_name=EXCLUDED.station_name
                """,
                (
                    record['STATION'],
                    record['DATE'],
                    record.get('TMAX'),
                    record.get('TMIN'),
                    record.get('TAVG'),
                    record.get('PRCP'),
                    record.get('SNOW'),
                    record.get('SNWD'),
                    record.get('AWND'),
                    record.get('WDF2'),
                    record.get('WSF2'),
                    bool(int(record.get('WT01', 0))),
                    bool(int(record.get('WT03', 0))),
                    bool(int(record.get('WT08', 0))),
                    record.get('NAME')
                )
            )
        conn.commit()
        print("[INFO] Data inserted into Postgres.")

    except Exception as e:
        print(f"[ERROR] Insert failed: {e}")
        if conn:
            conn.rollback()
        raise
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

def write_processed_to_gcs(**kwargs):
    from datetime import date  # 🔥 Important: import date class

    conf = kwargs['dag_run'].conf
    station_id = conf['location']
    user_id = conf['user_id']
    conn = None
    cursor = None
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cursor = conn.cursor()

        start_date, end_date = conf['date_range'].split(" to ")
        start_date = start_date.strip()
        end_date = end_date.strip()
        cursor.execute(
            """
            SELECT station_id, date, tmax, tmin, tavg, prcp, snow, snwd, awnd, wdf2, wsf2, fog, thunder, smoke_haze, station_name
            FROM readings
            WHERE station_id = %s
            AND date BETWEEN %s AND %s
            """,
            (station_id, start_date, end_date)
        )
        rows = cursor.fetchall()

        if not rows:
            raise ValueError("No data found.")

        columns = [desc[0] for desc in cursor.description]
        data = [dict(zip(columns, row)) for row in rows]

        # 🔥 Fix: Convert 'date' object to string
        for record in data:
            if isinstance(record['date'], date):
                record['date'] = record['date'].isoformat()

        gcs_hook = GCSHook(gcp_conn_id="google_cloud_default")
        sanitized_user = user_id.replace(" ", "_")
        sanitized_location = station_id.replace(" ", "_")
        sanitized_date_range = conf['date_range'].replace(" ", "_").replace("/", "-")
        output_file = f"weather_data/{sanitized_user}_{sanitized_location}_{sanitized_date_range}.json"

        gcs_hook.upload(
            bucket_name=PROCESSED_BUCKET_NAME,
            object_name=output_file,
            data=json.dumps(data, indent=2, default=float),
            mime_type='application/json'
        )

        insert_metadata_log(station_id, user_id, "download")

        publish_to_pubsub({
            "user_id": user_id,
            "location": station_id,
            "date_range": conf["date_range"],
            "status": "complete",
            "message": "Data ingestion completed.",
            "gcs_path": f"gs://{PROCESSED_BUCKET_NAME}/{output_file}"
        }, PUBSUB_TOPIC_ID)

        print("[INFO] Processed data uploaded and notification sent.")

    except Exception as e:
        print(f"[ERROR] Writing to GCS failed: {e}")
        raise
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

# -----------------------------
# DAG Definition
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

    check_task = BranchPythonOperator(
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

    check_task >> [fetch_task, write_task]
    fetch_task >> write_task