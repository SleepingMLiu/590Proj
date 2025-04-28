import json
import threading
import requests
from google.cloud import pubsub_v1
import re
from datetime import datetime
import sys
import time
from mongo_logger import log_user_action



# Project details
project_id = "prime-agency-456202-b7"

# Station mapping dictionary (common locations to NCEI station IDs)

STATION_MAPPING = {
    # New York Area
    "new york": "USW00094728",  # Central Park
    "nyc": "USW00094728",
    "central park": "USW00094728",
    "jfk": "USW00094789",  # JFK Airport
    "la guardia": "USW00014732",
    
    # Other major cities
    "los angeles": "USW00023174",
    "la": "USW00023174",
    "chicago": "USW00094846",
    "houston": "USW00012918",
    "phoenix": "USW00023183",
    
    # Add more as needed
}

# Topics
user_request_topic_id = "user-requests-topic"
data_complete_subscription_id = "data-upload-complete-topic-sub"

# Initialize Publisher and Subscriber
publisher = pubsub_v1.PublisherClient()
subscriber = pubsub_v1.SubscriberClient()

user_request_topic_path = publisher.topic_path(project_id, user_request_topic_id)
data_complete_subscription_path = subscriber.subscription_path(project_id, data_complete_subscription_id)

# Global event to wait for confirmation
confirmation_event = threading.Event()

# Global variable to store the download link
download_link = None

def resolve_station_id(location):
    """
    Convert user-friendly location to NCEI station ID.
    First checks local dictionary, then falls back to API search.
    """
    # Clean the input
    clean_loc = location.lower().strip()
    
    # Check our local mapping first
    if clean_loc in STATION_MAPPING:
        return STATION_MAPPING[clean_loc]
    
    # If not found, try to search via API
    try:
        response = requests.get(
            f"{NCEI_STATION_SEARCH_URL}&startDate=2020-01-01&endDate=2020-01-02",
            headers={"User-Agent": "MyWeatherApp/1.0 (contact@example.com)"}
        )
        response.raise_for_status()
        
        stations = response.json()
        for station in stations:
            if clean_loc in station['NAME'].lower():
                return station['STATION']
        
        # If we get here, no match found
        raise ValueError(f"No station found matching '{location}'. Try a nearby major city or airport code.")
    
    except Exception as e:
        raise ValueError(f"Station lookup failed: {str(e)}")
    

def pretty_wait(event, timeout=600):
    start_time = time.time()
    while not event.is_set():
        if time.time() - start_time > timeout:
            break
        sys.stdout.write("\rWaiting for data upload...")
        sys.stdout.flush()
        time.sleep(0.5)
    print()  # Newline after done

def validate_date_range(date_range):
    pattern = r"^\d{4}-\d{2}-\d{2}\s+to\s+\d{4}-\d{2}-\d{2}$"
    if not re.match(pattern, date_range):
        return False
    try:
        start, end = date_range.split("to")
        datetime.strptime(start.strip(), "%Y-%m-%d")
        datetime.strptime(end.strip(), "%Y-%m-%d")
        return True
    except ValueError:
        return False

def get_user_input():
    """Prompt user for input for weather data or metadata request."""
    print("\n" + "=" * 50)
    print("Enter your request details")
    print("=" * 50)
    user_id = input("Enter your user ID: ")
    
    allowed_request_types = ["data", "metadata"]
    while True:
        request_type = input("Are you requesting 'data' or 'metadata'?: ").lower()
        if request_type in allowed_request_types:
            break
        else:
            print("Invalid input. Please enter 'data' or 'metadata'.")

    message = {
        "user_id": user_id,
        "request_type": request_type
    }

    if request_type == "data":
        
        allowed_sources = ["noaa", "mtbs", "nifc", "landsat"]
        while True:
            source = input("Enter source (noaa, mtbs, nifc, landsat): ").lower()
            if source in allowed_sources:
                break
            else:
                print("Invalid source. Please choose from noaa, mtbs, nifc, or landsat.")

        while True:
            location = input("Enter location (e.g., 'New York', 'JFK', 'LA'): ")
            original_location = location
            try:
                # Resolve to station ID if NOAA source
                if source == "noaa":
                    station_id = resolve_station_id(location)
                    print(f"Using station ID: {station_id}")
                    location = station_id  # Use the resolved station ID
                break
            except ValueError as e:
                print(f"Error: {str(e)}")

        while True:
            date_range = input("Enter date range (yyyy-mm-dd to yyyy-mm-dd): ")
            if validate_date_range(date_range):
                break
            else:
                print("Invalid date format. Example: 2022-01-01 to 2022-01-10")
                
        start_date, end_date = map(str.strip, date_range.split("to"))

        message.update({
            "source": source,
            "original_location": original_location,
            "location": location,
            "start_date": start_date.strip(),
            "end_date": end_date.strip()
        })

    elif request_type == "metadata":
        allowed_metadata_sources = ["noaa", "mtbs", "nifc", "landsat"]
        while True:
            metadata_type = input("Enter metadata source you want (noaa, mtbs, nifc, landsat): ").lower()
            if metadata_type in allowed_metadata_sources:
                break
            else:
                print("Invalid metadata source. Please choose from noaa, mtbs, nifc, or landsat.")

        message.update({
            "source": metadata_type  # We use 'source' for metadata type
        })

    return message

def publish_user_request(message, user_id, source=None, location=None, start_date=None, end_date=None):
    """Publish user request to the 'user-requests-topic'."""
    future = publisher.publish(user_request_topic_path, json.dumps(message).encode("utf-8"))
    print(f"Published request to 'user-requests-topic'. Message ID: {future.result()}")
    if message.get("request_type") == "data":
        log_user_action(
            user_id=user_id,
            action="submit_query",
            details={
                "source": source,
                "original_location": message.get("original_location"),
                "station_id": location,
                "date_range": [start_date, end_date]
            }
        )
    else:
        log_user_action(
            user_id=user_id,
            action="submit_metadata_request",
            details={
                "source": message.get("source")
            }
        )
    

def listen_for_completion(user_id):
    """Listen for confirmation that data upload is complete."""
    def callback(message):
        global download_link
        payload = json.loads(message.data.decode("utf-8"))

        # Match based on user_id
        if payload.get("user_id") == user_id:
            print(f" Data upload complete for user {user_id}!")
            download_link = payload.get("gcs_path")
            confirmation_event.set()
            message.ack()
        else:
            # Not our message, don't ack yet
            message.nack()

    streaming_pull_future = subscriber.subscribe(data_complete_subscription_path, callback=callback)
    print(f" Listening for data completion on '{data_complete_subscription_path}'...")

    # Run the listener in background thread
    threading.Thread(target=streaming_pull_future.result, daemon=True).start()

def main():
    print("Weather Data Request Publisher")
    print("Press Ctrl+C to exit\n")
    global download_link

    try:
        while True:
            # Get user request input
            message = get_user_input()
            user_id = message["user_id"]

            # Start listening for completion before publishing
            listen_for_completion(user_id)

            # Publish the message
            # If it's a data request
            if message["request_type"] == "data":
                publish_user_request(
                    message,
                    user_id=message["user_id"],
                    source=message["source"],
                    location=message["location"],
                    start_date=message["start_date"],
                    end_date=message["end_date"]
                )

            # If it's a metadata request
            else:
                publish_user_request(
                    message,
                    user_id=message["user_id"]
                )

            # Wait until the event is set by subscriber
            print("Waiting for data upload to complete...")
            pretty_wait(confirmation_event, timeout=600)

            if download_link:
                print(f"Your data is ready! Download it from: {download_link}")
            else:
                print("Timed out waiting for data upload confirmation.")

            # Reset event and link for next round
            confirmation_event.clear()
            download_link = None

    except KeyboardInterrupt:
        print("\n Publisher stopped.")

if __name__ == "__main__":
    main()
