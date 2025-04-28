# subscriber.py

from google.cloud import pubsub_v1
import requests
import time
import json
from requests.auth import HTTPBasicAuth
import psycopg2  # (Make sure this import is already at the top!)

# Database connection info
DB_CONFIG = {
    "dbname": "noaa",
    "user": "postgres",
    "password": "final-project",
    "host": "35.202.11.58",   # <-- your Cloud SQL IP address
    "connect_timeout": 10
}

def data_already_exists(location, date_range, source):
    """
    Check if the requested data already exists in PostgreSQL.
    """
    conn = None
    cursor = None
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cursor = conn.cursor()

        query = """
            SELECT 1 FROM data_metadata
            WHERE station_id = %s AND date_range = %s AND source = %s
            LIMIT 1
        """
        cursor.execute(query, (location, date_range, source))
        result = cursor.fetchone()
        return result is not None

    except Exception as e:
        print(f"[ERROR] Checking existing data failed: {e}")
        return False
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

# Project and subscription details
PROJECT_ID = "prime-agency-456202-b7"
USER_REQUEST_SUBSCRIPTION_ID = "topic1-sub"
CONFIRMATION_SUBSCRIPTION_ID = "data-upload-complete-sub"
DAG_ID = "data_request_pipeline"
AIRFLOW_TRIGGER_URL = "http://34.30.255.233:8080/api/v1/dags/{dag_id}/dagRuns"

# Airflow credentials
auth = HTTPBasicAuth("airflow", "airflow")

# Initialize Pub/Sub client
subscriber = pubsub_v1.SubscriberClient()
user_request_subscription_path = subscriber.subscription_path(PROJECT_ID, USER_REQUEST_SUBSCRIPTION_ID)
confirmation_subscription_path = subscriber.subscription_path(PROJECT_ID, CONFIRMATION_SUBSCRIPTION_ID)

def user_request_callback(message):
    """
    Handles user data request messages by triggering Airflow DAGs.
    """
    print(f"\nReceived user request message: {message.data}")
    try:
        data = json.loads(message.data.decode("utf-8"))
        location = data.get('location')
        date_range = data.get('date_range')
        source = data.get('source')

        # Check if data already exists
        if data_already_exists(location, date_range, source):
            print(f"[INFO] Data already exists for {location} {date_range}. Skipping ingestion.")

            # Publish "already_exists" confirmation
            publisher = pubsub_v1.PublisherClient()
            confirmation_topic_path = publisher.topic_path(PROJECT_ID, "data-upload-complete-topic")

            confirmation_message = {
                "user_id": data.get("user_id", "unknown"),
                "location": location,
                "date_range": date_range,
                "status": "already_exists",
                "message": "Requested data already exists. No re-ingestion necessary."
            }

            publisher.publish(
                confirmation_topic_path,
                json.dumps(confirmation_message).encode("utf-8")
            )
            print("[INFO] Published 'already_exists' message.")
            message.ack()
            return  # Exit early so DAG is not triggered
        request_type = data.get('request_type')

        if request_type == "data":
            required_fields = ['user_id', 'source', 'location', 'date_range']
        elif request_type == "metadata":
            required_fields = ['source']
        else:
            raise ValueError(f"Unknown request_type: {request_type}")

        if not all(field in data for field in required_fields):
            raise ValueError("Missing required fields in the user request")

        # Trigger Airflow DAG
        payload = {"conf": data}
        trigger_response = requests.post(
            AIRFLOW_TRIGGER_URL.format(dag_id=DAG_ID),
            json=payload,
            auth=auth,
            timeout=10  # Added timeout
        )

        if trigger_response.status_code == 200:
            print("Successfully triggered Airflow DAG.")
            message.ack()
        else:
            print(f"Failed to trigger DAG: {trigger_response.status_code} - {trigger_response.text}")
            message.nack()

    except Exception as e:
        print(f"Error handling user request: {e}")
        message.nack()

def confirmation_callback(message):
    """
    Handles confirmation messages after data processing is complete.
    """
    print(f"\nReceived confirmation message: {message.data}")
    try:
        data = json.loads(message.data.decode("utf-8"))
        
        # Basic check for common fields
        status = data.get("status")
        user_message = data.get("message", "No message provided.")

        print(f"Status: {status}")
        print(f"Message: {user_message}")

        # Optional: Show download location if included
        if "location" in data and "date_range" in data:
            print(f"Data available at: gs://your-bucket-name/{data['location']}/{data['date_range']}/")

        message.ack()

    except Exception as e:
        print(f"Error handling confirmation message: {e}")
        message.nack()

def main():
    """
    Main function to start subscribers.
    """
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
    main()
