import json
import requests
from datetime import datetime, timezone, timedelta
from google.cloud import pubsub_v1
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.hooks.gcs import GCSHook
import psycopg2

# -----------------------------
# Constants and Helper functions
# -----------------------------

NCEI_ALL_DATA_TYPES = [
    "TMAX","TMIN","TAVG","PRCP","SNOW","SNWD",
    "AWND","WDF2","WSF2",
    "WT01","WT02","WT03","WT04","WT05","WT06",
    "WT07","WT08","WT09","WT10","WT11","WT13",
    "WT14","WT15","WT16","WT17","WT18","WT19",
    "WT21","WT22"
]

def publish_failure_to_pubsub(message, topic_name):
    """Publish failure message to Pub/Sub topic."""
    publisher = pubsub_v1.PublisherClient()
    topic_path = publisher.topic_path('prime-agency-456202-b7', topic_name)
    publisher.publish(topic_path, message.encode('utf-8'))

def fetch_from_noaa(station_id, start_date, end_date):
    """Fetch data from NOAA API."""
    url = f"https://api.weather.gov/stations/{station_id}/observations?start={start_date}T00:00:00Z&end={end_date}T23:59:59Z"
    try:
        response = requests.get(url, timeout=30)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.HTTPError as e:
        if response.status_code == 404:
            failure_message = f"Station {station_id} not found at NOAA."
            publish_failure_to_pubsub(failure_message, 'data-upload-complete')
            raise ValueError(failure_message)
        else:
            raise ValueError(f"Failed to fetch from NOAA: {e}")
    except Exception as e:
        raise ValueError(f"Error fetching NOAA data: {e}")

def fetch_from_ncei(station_id, start_date, end_date):
    """Fetch comprehensive data from NCEI API"""
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
            headers={"User-Agent": "MyWeatherApp/1.0 (contact@example.com)"},
            timeout=30
        )
        response.raise_for_status()
        return response.json()
    except Exception as e:
        publish_failure_to_pubsub(f"NCEI API failed: {str(e)}", 'data-upload-complete')
        raise ValueError(f"NCEI API error: {e}")

def save_raw_to_gcs(data, conf):
    """Save raw data to GCS with source distinction."""
    raw_bucket_name = "ece-590-group2-raw"
    sanitized_date_range = conf['date_range'].replace(" ", "_").replace("/", "-")
    
    # Different path for NCEI data
    if conf.get('source') == 'ncei':
        raw_file_name = f"noaa/ncei/{conf['location']}/{sanitized_date_range}_full.json"
    else:
        raw_file_name = f"noaa/{conf['location']}/{sanitized_date_range}.json"
    
    try:
        gcs_hook = GCSHook(gcp_conn_id="google_cloud_default")
        gcs_hook.upload(
            bucket_name=raw_bucket_name,
            object_name=raw_file_name,
            data=json.dumps(data, indent=2),
            mime_type='application/json'
        )
        print(f"📤 Raw data saved to {raw_bucket_name}/{raw_file_name}")
        return True
    except Exception as e:
        print(f"❌ Failed to save raw data to GCS: {e}")
        raise

def insert_data_into_postgres(data, conf):
    """Insert data into PostgreSQL database, handling both NOAA and NCEI formats."""
    conn = None
    cursor = None
    try:
        conn = psycopg2.connect(
            dbname="noaa",
            user="postgres",
            password="final-project",
            host="35.202.11.58",
            connect_timeout=10
        )
        cursor = conn.cursor()

        # Check data format (NOAA vs NCEI)
        if 'features' in data:  # NOAA format
            observations = data.get('features', [])
            station_id = None
            inserted_count = 0

            for obs in observations:
                properties = obs.get('properties', {})
                if not properties:
                    continue

                # Extract station ID
                station_url = properties.get('station', '')
                station_id = station_url.split('/')[-1] if station_url else 'unknown'

                # Insert station if not exists
                cursor.execute("""
                    INSERT INTO stations (station_id, elevation, elevation_unit) 
                    VALUES (%s, %s, %s)
                    ON CONFLICT (station_id) DO NOTHING
                    """,
                    (
                        station_id,
                        properties.get('elevation', {}).get('value', None),
                        properties.get('elevation', {}).get('unitCode', None)
                    )
                )

                # Insert readings
                cursor.execute("""
                    INSERT INTO readings (
                        station_id, timestamp,
                        temperature_value, temperature_unit,
                        dewpoint_value, dewpoint_unit,
                        wind_speed_value, wind_speed_unit,
                        wind_direction_value, wind_direction_unit,
                        barometric_pressure_value, barometric_pressure_unit,
                        sea_level_pressure_value, sea_level_pressure_unit,
                        visibility_value, visibility_unit,
                        relative_humidity_value, relative_humidity_unit
                    ) VALUES (
                        %s, %s,
                        %s, %s,
                        %s, %s,
                        %s, %s,
                        %s, %s,
                        %s, %s,
                        %s, %s,
                        %s, %s,
                        %s, %s
                    )
                    ON CONFLICT (station_id, timestamp) DO NOTHING
                    """,
                    (
                        station_id,
                        properties.get('timestamp', None),
                        properties.get('temperature', {}).get('value', None),
                        properties.get('temperature', {}).get('unitCode', None),
                        properties.get('dewpoint', {}).get('value', None),
                        properties.get('dewpoint', {}).get('unitCode', None),
                        properties.get('windSpeed', {}).get('value', None),
                        properties.get('windSpeed', {}).get('unitCode', None),
                        properties.get('windDirection', {}).get('value', None),
                        properties.get('windDirection', {}).get('unitCode', None),
                        properties.get('barometricPressure', {}).get('value', None),
                        properties.get('barometricPressure', {}).get('unitCode', None),
                        properties.get('seaLevelPressure', {}).get('value', None),
                        properties.get('seaLevelPressure', {}).get('unitCode', None),
                        properties.get('visibility', {}).get('value', None),
                        properties.get('visibility', {}).get('unitCode', None),
                        properties.get('relativeHumidity', {}).get('value', None),
                        properties.get('relativeHumidity', {}).get('unitCode', None)
                    )
                )
                inserted_count += 1

            print(f"✅ Inserted {inserted_count} NOAA records for station {station_id}")

        else:  # NCEI format
            # Create table if not exists for NCEI data
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS ncei_daily_data (
                    station_id VARCHAR(20),
                    date DATE,
                    tmax DECIMAL(5,2),
                    tmin DECIMAL(5,2),
                    tavg DECIMAL(5,2),
                    prcp DECIMAL(7,2),
                    snow DECIMAL(7,2),
                    snwd DECIMAL(7,2),
                    awnd DECIMAL(5,2),
                    wdf2 SMALLINT,
                    wsf2 DECIMAL(5,2),
                    fog BOOLEAN,
                    thunder BOOLEAN,
                    smoke_haze BOOLEAN,
                    station_name TEXT,
                    PRIMARY KEY (station_id, date)
                )""")

            inserted_count = 0
            for record in data:
                cursor.execute("""
                    INSERT INTO ncei_daily_data
                    (station_id, date, tmax, tmin, tavg, prcp, snow, snwd, 
                     awnd, wdf2, wsf2, fog, thunder, smoke_haze, station_name)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (station_id, date) DO UPDATE SET
                        tmax = EXCLUDED.tmax,
                        tmin = EXCLUDED.tmin,
                        tavg = EXCLUDED.tavg,
                        prcp = EXCLUDED.prcp,
                        snow = EXCLUDED.snow,
                        snwd = EXCLUDED.snwd,
                        awnd = EXCLUDED.awnd,
                        wdf2 = EXCLUDED.wdf2,
                        wsf2 = EXCLUDED.wsf2,
                        fog = EXCLUDED.fog,
                        thunder = EXCLUDED.thunder,
                        smoke_haze = EXCLUDED.smoke_haze,
                        station_name = EXCLUDED.station_name
                """, (
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
                ))
                inserted_count += 1

            print(f"✅ Inserted {inserted_count} NCEI records")

        # Insert metadata (for both sources)
        start_date, end_date = conf['date_range'].split(" to ")
        source = conf.get('source', 'noaa')
        station_id = station_id if 'features' in data else data[0]['STATION'] if data else 'unknown'
        
        cursor.execute(
            "SELECT 1 FROM data_metadata WHERE station_id=%s AND date_range=%s AND source=%s LIMIT 1",
            (station_id, conf['date_range'], source)
        )
        data_exists = cursor.fetchone() is not None

        action = "DOWNLOAD" if data_exists else "INSERT"
        timestamp = datetime.now(timezone.utc)

        cursor.execute(
            """
            INSERT INTO data_metadata (
                station_id, date_range, source, data_inserted_at, 
                user_accessed, access_timestamp, action, timestamp
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (station_id, date_range, source) DO UPDATE SET
                data_inserted_at = EXCLUDED.data_inserted_at,
                access_timestamp = EXCLUDED.access_timestamp,
                action = EXCLUDED.action,
                timestamp = EXCLUDED.timestamp
            """,
            (
                station_id,
                conf['date_range'],
                source,
                timestamp,
                conf['user_id'],
                timestamp,
                action,
                timestamp
            )
        )

        conn.commit()

    except Exception as e:
        print(f"❌ Error inserting data: {e}")
        if conn:
            conn.rollback()
        raise
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

def datetime_converter(o):
    """Convert datetime to ISO format."""
    if isinstance(o, datetime):
        return o.isoformat()

# -----------------------------
# Main Airflow tasks
# -----------------------------

def check_postgres(**kwargs):
    """Check if data is already in PostgreSQL."""
    conf = kwargs['dag_run'].conf
    conn = None
    cursor = None
    try:
        conn = psycopg2.connect(
            dbname="noaa",
            user="postgres",
            password="final-project",
            host="35.202.11.58",
            connect_timeout=10
        )
        cursor = conn.cursor()

        start_date, end_date = conf['date_range'].split(" to ")
        
        # Updated query to match your actual schema
        cursor.execute(
            """
            SELECT 1 FROM readings 
            WHERE station_id=%s 
            AND observation_time BETWEEN %s AND %s 
            LIMIT 1
            """,
            (conf['location'], start_date, end_date)
        )
        data_exists = cursor.fetchone() is not None

        if data_exists:
            print("✅ Data found in PostgreSQL")
            return "write_processed_to_gcs"
        else:
            print("ℹ️ Data not found, will fetch from API")
            return "fetch_from_api"

    except psycopg2.Error as e:
        print(f"❌ PostgreSQL error: {e}")
        raise
    except Exception as e:
        print(f"❌ Unexpected error: {e}")
        raise
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

def fetch_from_api(**kwargs):
    """Fetch data from NOAA API."""
    conf = kwargs['dag_run'].conf
    station_id = conf['location']
    start_date, end_date = conf['date_range'].split(" to ")

    print(f"ℹ️ Fetching NOAA data for {station_id} from {start_date} to {end_date}")
    api_data = fetch_from_noaa(station_id, start_date, end_date)

    if api_data:
        save_raw_to_gcs(api_data, conf)
        insert_data_into_postgres(api_data, conf)
    else:
        raise Exception("❌ Failed to fetch data from NOAA API")

def fetch_ncei_data(**kwargs):
    """Fetch comprehensive data from NCEI API."""
    conf = kwargs['dag_run'].conf
    station_id = conf['location']
    start_date, end_date = conf['date_range'].split(" to ")
    
    print(f"ℹ️ Fetching NCEI data for {station_id} from {start_date} to {end_date}")
    ncei_data = fetch_from_ncei(station_id, start_date, end_date)
    
    if ncei_data:
        ncei_conf = conf.copy()
        ncei_conf['source'] = 'ncei'
        save_raw_to_gcs(ncei_data, ncei_conf)
        insert_data_into_postgres(ncei_data, ncei_conf)
    else:
        raise Exception("❌ Failed to fetch data from NCEI API")

def fetch_from_api_and_ncei(**kwargs):
    """Fetch from both APIs in parallel."""
    fetch_from_api(**kwargs)
    fetch_ncei_data(**kwargs)

def notify_data_upload_complete(conf):
    """Notify that data upload is complete."""
    project_id = "prime-agency-456202-b7"
    topic_id = "data-upload-complete-topic" 

    try:
        publisher = pubsub_v1.PublisherClient()
        topic_path = publisher.topic_path(project_id, topic_id)

        message = {
            "user_id": conf["user_id"],
            "location": conf["location"],
            "date_range": conf["date_range"],
            "status": "complete",
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "message": "The processed data has been uploaded to the processed bucket."
        }

        future = publisher.publish(topic_path, json.dumps(message).encode("utf-8"))
        print(f"✅ Published confirmation message to Pub/Sub: {message}")
        return future.result()
    except Exception as e:
        print(f"❌ Failed to publish to Pub/Sub: {e}")
        raise

def write_processed_to_gcs(**kwargs):
    """Write processed data to GCS, combining both NOAA and NCEI data."""
    conf = kwargs['dag_run'].conf
    conn = None
    cursor = None
    try:
        processed_bucket_name = "ece-590-group2-processed"
        sanitized_user = conf['user_id'].replace(" ", "_").replace("/", "-")
        sanitized_location = conf['location'].replace(" ", "_").replace("/", "-")
        sanitized_date_range = conf['date_range'].replace(" ", "_").replace("/", "-")
        processed_file_name = f"weather_data/{sanitized_user}_{sanitized_location}_{sanitized_date_range}.json"

        conn = psycopg2.connect(
            dbname="noaa",
            user="postgres",
            password="final-project",
            host="35.202.11.58",
            connect_timeout=10
        )
        cursor = conn.cursor()

        start_date, end_date = conf['date_range'].split(" to ")
        
        # Combine data from both tables
        cursor.execute("""
            SELECT 
                r.station_id, 
                r.timestamp as date,
                r.temperature_value as temp,
                'noaa' as source,
                NULL as tmax,
                NULL as tmin,
                NULL as tavg,
                NULL as prcp
            FROM readings r
            WHERE r.station_id=%s AND date(r.timestamp) BETWEEN %s AND %s
            
            UNION ALL
            
            SELECT 
                n.station_id,
                n.date,
                NULL as temp,
                'ncei' as source,
                n.tmax,
                n.tmin,
                n.tavg,
                n.prcp
            FROM ncei_daily_data n
            WHERE n.station_id=%s AND n.date BETWEEN %s AND %s
            
            ORDER BY date
        """, (conf['location'], start_date, end_date, conf['location'], start_date, end_date))

        records = cursor.fetchall()
        columns = [desc[0] for desc in cursor.description]

        if not records:
            raise ValueError("❌ No records found in PostgreSQL for the given criteria")

        data = [dict(zip(columns, row)) for row in records]
        data_json = json.dumps(data, default=datetime_converter, indent=2)

        gcs_hook = GCSHook(gcp_conn_id="google_cloud_default")
        gcs_hook.upload(
            bucket_name=processed_bucket_name,
            object_name=processed_file_name,
            data=data_json,
            mime_type='application/json'
        )

        print(f"✅ Processed data uploaded to {processed_bucket_name}/{processed_file_name}")
        notify_data_upload_complete(conf)

    except Exception as e:
        print(f"❌ Error writing processed data to GCS: {e}")
        raise
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

# -----------------------------
# Define DAG
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
        python_callable=check_postgres,
        provide_context=True
    )

    fetch_noaa_task = PythonOperator(
        task_id="fetch_from_api",
        python_callable=fetch_from_api,
        provide_context=True
    )

    fetch_ncei_task = PythonOperator(
        task_id="fetch_ncei_data",
        python_callable=fetch_ncei_data,
        provide_context=True
    )

    fetch_both_task = PythonOperator(
        task_id="fetch_from_api_and_ncei",
        python_callable=fetch_from_api_and_ncei,
        provide_context=True
    )

    write_processed_task = PythonOperator(
        task_id="write_processed_to_gcs",
        python_callable=write_processed_to_gcs,
        provide_context=True
    )

    # Define the workflow
    check_task >> [fetch_noaa_task, fetch_ncei_task, fetch_both_task]
    fetch_both_task >> write_processed_task
    [fetch_noaa_task, fetch_ncei_task] >> write_processed_task

    # Set up trigger rules to handle all scenarios
    fetch_noaa_task.trigger_rule = 'none_failed_or_skipped'
    fetch_ncei_task.trigger_rule = 'none_failed_or_skipped'
    fetch_both_task.trigger_rule = 'none_failed_or_skipped'