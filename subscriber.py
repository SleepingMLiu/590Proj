# subscriber.py

from google.cloud import pubsub_v1
import requests
import time
import json
from requests.auth import HTTPBasicAuth
import psycopg2

# -----------------------------
# Configuration
# -----------------------------

PROJECT_ID = "prime-agency-456202-b7"
USER_REQUEST_SUBSCRIPTION_ID = "topic1-sub"
CONFIRMATION_SUBSCRIPTION_ID = "data-upload-complete-sub"
DAG_ID = "data_request_pipeline"
AIRFLOW_TRIGGER_URL = "http://34.30.255.233:8080/api/v1/dags/{dag_id}/dagRuns"
PROCESSED_BUCKET_NAME = "ece-590-group2-processed"  # Used for displaying download link

auth = HTTPBasicAuth("airflow", "airflow")

DB_CONFIG = {
    "dbname": "noaa",
    "user": "postgres",
    "password": "final-project",
    "host": "35.202.11.58",
    "connect_timeout": 10
}

subscriber = pubsub_v1.SubscriberClient()
publisher = pubsub_v1.PublisherClient()

user_request_subscription_path = subscriber.subscription_path(PROJECT_ID, USER_REQUEST_SUBSCRIPTION_ID)
confirmation_subscription_path = subscriber.subscription_path(PROJECT_ID, CONFIRMATION_SUBSCRIPTION_ID)

# -----------------------------
# Helper Functions
# -----------------------------
def data_range_exists_in_readings(station_id, date_range):
    """
    Check if ALL requested dates exist in the 'readings' table.
    """
    from datetime import datetime

    conn = None
    cursor = None
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cursor = conn.cursor()

        start_date, end_date = date_range.split("to")
        start_date = start_date.strip()
        end_date = end_date.strip()

        cursor.execute(
            """
            SELECT COUNT(DISTINCT date)
            FROM readings
            WHERE station_id = %s
            AND date BETWEEN %s AND %s
            """,
            (station_id, start_date, end_date)
        )
        existing_days = cursor.fetchone()[0]

        start_dt = datetime.strptime(start_date, "%Y-%m-%d")
        end_dt = datetime.strptime(end_date, "%Y-%m-%d")
        requested_days = (end_dt - start_dt).days + 1

        return existing_days == requested_days

    except Exception as e:
        print(f"[ERROR] Failed checking readings table: {e}")
        return False
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()


def publish_already_exists_confirmation(user_id, station_id, date_range):
    """
    Publish a confirmation message if data already exists.
    """
    confirmation_topic_path = publisher.topic_path(PROJECT_ID, "data-upload-complete-topic")

    confirmation_message = {
        "user_id": user_id,
        "location": station_id,
        "date_range": date_range,
        "status": "already_exists",
        "message": "Requested data already exists. No re-ingestion necessary.",
        "gcs_path": f"gs://{PROCESSED_BUCKET_NAME}/{user_id}_{station_id}_{date_range.replace(' ', '_').replace('/', '-')}.json"
    }

    publisher.publish(
        confirmation_topic_path,
        json.dumps(confirmation_message).encode("utf-8")
    )
    print("[INFO] Published 'already_exists' confirmation.")

def trigger_airflow_dag(data):
    """
    Trigger the Airflow DAG with the provided data payload.
    """
    try:
        payload = {"conf": data}
        response = requests.post(
            AIRFLOW_TRIGGER_URL.format(dag_id=DAG_ID),
            json=payload,
            auth=auth,
            timeout=30
        )
        if response.status_code == 200:
            print("[INFO] Successfully triggered Airflow DAG.")
            return True
        else:
            print(f"[ERROR] Failed to trigger DAG: {response.status_code} - {response.text}")
            return False
    except Exception as e:
        print(f"[ERROR] Error triggering Airflow DAG: {e}")
        return False

# -----------------------------
# Callback Functions
# -----------------------------

def user_request_callback(message):
    """
    Handles incoming user request messages.
    """
    print(f"\nReceived user request message: {message.data}")
    try:
        data = json.loads(message.data.decode("utf-8"))

        user_id = data.get("user_id", "unknown")
        location = data.get("location")
        date_range = data.get("date_range")
        request_type = data.get("request_type")

        if request_type not in ["data", "metadata"]:
            raise ValueError(f"Invalid request_type: {request_type}")

        if not location or not date_range:
            raise ValueError("Missing required fields: location or date_range.")

        # Step 1: Check if data already exists in readings table
        if data_range_exists_in_readings(location, date_range):
            print(f"[INFO] Full data already exists for {location} {date_range}.")
            publish_already_exists_confirmation(user_id, location, date_range)
            message.ack()
            return  # No need to trigger DAG

        # Step 2: If data not fully exists, trigger ingestion pipeline
        print(f"[INFO] Data missing for {location} {date_range}. Triggering DAG.")
        if trigger_airflow_dag(data):
            message.ack()
        else:
            message.nack()

    except Exception as e:
        print(f"[ERROR] Error handling user request: {e}")
        message.nack()

def confirmation_callback(message):
    """
    Handles incoming confirmation messages from DAG.
    """
    print(f"\nReceived confirmation message: {message.data}")
    try:
        data = json.loads(message.data.decode("utf-8"))
        
        status = data.get("status")
        user_message = data.get("message", "No message provided.")

        print(f"Status: {status}")
        print(f"Message: {user_message}")

        if "gcs_path" in data:
            print(f"Download your file at: {data['gcs_path']}")

        message.ack()

    except Exception as e:
        print(f"[ERROR] Error handling confirmation message: {e}")
        message.nack()

# -----------------------------
# Main
# -----------------------------

def main():
    print(f"Starting subscriber for user requests: {user_request_subscription_path}")
    user_stream = subscriber.subscribe(user_request_subscription_path, callback=user_request_callback)

    print(f"Starting subscriber for confirmations: {confirmation_subscription_path}")
    confirmation_stream = subscriber.subscribe(confirmation_subscription_path, callback=confirmation_callback)

    try:
        while True:
            time.sleep(60)
    except KeyboardInterrupt:
        user_stream.cancel()
        confirmation_stream.cancel()
        print("\nSubscribers stopped.")

if __name__ == "__main__":
    from datetime import datetime  # Only needed here
    main()

