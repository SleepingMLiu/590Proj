# publisher.py

import json
import threading
import requests
import re
import sys
import time
from datetime import datetime
from google.cloud import pubsub_v1
from mongo_logger import log_user_action  # Assuming you have a MongoDB logger already set up

# GCP project and topic details
PROJECT_ID = "prime-agency-456202-b7"
USER_REQUEST_TOPIC = "topic1"
DATA_COMPLETE_SUBSCRIPTION = "data-upload-complete-topic-sub"

# Mapping common location names to NOAA/NCEI station IDs
STATION_MAPPING = {
    "new york": "USW00094728",
    "nyc": "USW00094728",
    "central park": "USW00094728",
    "jfk": "USW00094789",
    "la guardia": "USW00014732",
    "los angeles": "USW00023174",
    "la": "USW00023174",
    "chicago": "USW00094846",
    "houston": "USW00012918",
    "phoenix": "USW00023183",
}

# Global Pub/Sub publisher and subscriber clients
publisher = pubsub_v1.PublisherClient()
subscriber = pubsub_v1.SubscriberClient()

user_request_topic_path = publisher.topic_path(PROJECT_ID, USER_REQUEST_TOPIC)
data_complete_subscription_path = subscriber.subscription_path(PROJECT_ID, DATA_COMPLETE_SUBSCRIPTION)

# Global event for waiting until upload is done
confirmation_event = threading.Event()
download_link = None

def resolve_station_id(location):
    """
    Resolve user-friendly location to official station ID.
    """
    clean_loc = location.lower().strip()
    if clean_loc in STATION_MAPPING:
        return STATION_MAPPING[clean_loc]
    raise ValueError(f"Location '{location}' not recognized. Try a nearby major city or airport code.")

def validate_date_range(date_range: str) -> bool:
    # Flexible pattern with optional commas/spaces
    pattern = r"^\s*\d{4}-\d{2}-\d{2}\s*,?\s*to\s*,?\s*\d{4}-\d{2}-\d{2}\s*$"

    if not re.match(pattern, date_range):
        return False

    try:
        start_str, end_str = [d.strip().strip(',') for d in date_range.split("to")]
        start_date = datetime.strptime(start_str, "%Y-%m-%d")
        end_date = datetime.strptime(end_str, "%Y-%m-%d")
        return start_date <= end_date  # You can use < if you want strictly increasing
    except ValueError:
        return False  # Invalid calendar date



def get_user_input():
    """
    Prompt user for weather or metadata request input.
    """
    print("\n" + "=" * 50)
    print("Enter your request details")
    print("=" * 50)

    user_id = input("Enter your user ID: ").strip()
    request_type = input("Request type (type data for now): ").strip().lower()

    message = {"user_id": user_id, "request_type": request_type}

    if request_type == "data":
        source = input("Enter source (noaa, mtbs, nifc, landsat): ").strip().lower()
        while source not in ['noaa','mtbs','nifc','landsat']:
            print("Invalid source. Please try again")
            source = input("Enter source (noaa, mtbs, nifc, landsat): ").strip().lower()

        location = input("Enter location (e.g., 'New York', 'JFK', 'LA'): ").strip()
        while location not in STATION_MAPPING.keys():
            print("The location is either not supported or invalid.Please try again.")
            location = input("Enter location (e.g., 'New York', 'JFK', 'LA'): ").strip()

        original_location = location

        date_range = input("Enter date range (yyyy-mm-dd to yyyy-mm-dd): ").strip()
        while not validate_date_range(date_range):
            date_range = input("Enter date range (yyyy-mm-dd to yyyy-mm-dd): ").strip()
            
        if source == "noaa":
            location = resolve_station_id(location)

        if not validate_date_range(date_range):
            raise ValueError("Invalid date format. Expected yyyy-mm-dd to yyyy-mm-dd.")

        message.update({
            "source": source,
            "original_location": original_location,
            "location": location,
            "date_range": date_range.strip()
        })

    elif request_type == "metadata":
        metadata_source = input("Enter metadata source (noaa, mtbs, nifc, landsat): ").strip().lower()
        message["source"] = metadata_source

    else:
        raise ValueError("Request type must be 'data' or 'metadata'.")

    return message

def publish_user_request(message):
    """
    Publish user request to Pub/Sub topic and log the action to MongoDB.
    """
    try:
        future = publisher.publish(user_request_topic_path, json.dumps(message).encode("utf-8"))
        print(f"Published request to 'topic1'. Message ID: {future.result()}")

        # Log action to MongoDB
        if message["request_type"] == "data":
            log_user_action(
                user_id=message["user_id"],
                action="submit_query",
                details={
                    "source": message["source"],
                    "original_location": message["original_location"],
                    "resolved_station_id": message["location"],
                    "date_range": message["date_range"]
                }
            )
        else:
            log_user_action(
                user_id=message["user_id"],
                action="submit_metadata_request",
                details={"source": message["source"]}
            )
        return True

    except Exception as e:
        print(f"Failed to publish message: {str(e)}")
        return False

def listen_for_completion(user_id):
    """
    Listen for data upload completion confirmation via Pub/Sub subscription.
    """
    def callback(message):
        global download_link
        payload = json.loads(message.data.decode("utf-8"))

        if payload.get("user_id") == user_id:
            status = payload.get("status", "unknown")
            print(f"\nData upload status for user {user_id}: {status}")

            if status == "complete":
                print("New data ingestion completed!")
            elif status == "already_exists":
                print("Data already existed. No new ingestion performed.")
            else:
                print("Unknown status received.")

            download_link = payload.get("gcs_path")
            confirmation_event.set()
            message.ack()
        else:
            message.nack()

    streaming_pull_future = subscriber.subscribe(data_complete_subscription_path, callback=callback)
    print(f"Listening for data completion on '{data_complete_subscription_path}'...")

    threading.Thread(target=streaming_pull_future.result, daemon=True).start()

def pretty_wait(event, timeout=600):
    """
    Wait in a user-friendly way with timeout.
    """
    start_time = time.time()
    while not event.is_set():
        if time.time() - start_time > timeout:
            break
        sys.stdout.write("\rWaiting for data upload...")
        sys.stdout.flush()
        time.sleep(0.5)
    print()

def main():
    """
    Main function to drive the publisher program.
    """
    print("Weather Data Request Publisher")
    print("Press Ctrl+C to exit\n")
    global download_link

    try:
        while True:
            message = get_user_input()
            success = publish_user_request(message)
            if success:
                listen_for_completion(message["user_id"])
                print("Waiting for data upload to complete...")
                pretty_wait(confirmation_event, timeout=600)

                if download_link:
                    print(f"Your data is ready! Download it from: {download_link}")
                else:
                    print("Timed out waiting for data upload confirmation.")

                confirmation_event.clear()
                download_link = None
            else:
                print("Skipping waiting for confirmation due to publish failure.")

    except KeyboardInterrupt:
        print("\nPublisher stopped.")

if __name__ == "__main__":
    main()
