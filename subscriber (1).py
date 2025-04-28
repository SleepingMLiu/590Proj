from google.cloud import pubsub_v1
import requests
import time
import json
from requests.auth import HTTPBasicAuth

# Project and subscription details
project_id = "prime-agency-456202-b7"
subscription_id = "user-requests-sub"
dag_id = "data_request_pipeline"
dag_trigger_url = "http://34.30.255.233:8080/api/v1/dags/{dag_id}/dagRuns"

# Airflow credentials
auth = HTTPBasicAuth("airflow", "airflow")

# Initialize Pub/Sub client
subscriber = pubsub_v1.SubscriberClient()
subscription_path = subscriber.subscription_path(project_id, subscription_id)

# Create a Pub/Sub client to listen for confirmation messages from the data-upload-complete-topic
confirmation_sub_id = "data-upload-complete-sub"  # New subscriber for confirmation topic
confirmation_subscription_path = subscriber.subscription_path(project_id, confirmation_sub_id)

# Callback function to handle user data request and trigger DAG
def callback(message):
    print(f"\nReceived message: {message.data}")
    try:
        data = json.loads(message.data.decode("utf-8"))

        request_type = data.get('request_type')  # Check if it's 'data' or 'metadata'

        if request_type == "data":
            # Validate required fields for data request
            required_fields = ['user_id', 'source', 'location', 'date_range']
            if not all(field in data for field in required_fields):
                raise ValueError("Missing required fields for data request")

        elif request_type == "metadata":
            # Validate required fields for metadata request
            required_fields = ['source']
            if not all(field in data for field in required_fields):
                raise ValueError("Missing required fields for metadata request")

        else:
            raise ValueError(f"Unknown request_type: {request_type}")

        payload = {"conf": data}

        # Trigger the DAG
        response = requests.post(
            dag_trigger_url.format(dag_id=dag_id),
            json=payload,
            auth=auth
        )

        if response.status_code == 200:
            print(f"🚀 Successfully triggered DAG: {response.json()}")
            message.ack()

            # After triggering the DAG, publish a confirmation message
            confirmation_publisher = pubsub_v1.PublisherClient()
            confirmation_topic_id = "data-upload-complete-topic"
            confirmation_topic_path = confirmation_publisher.topic_path(project_id, confirmation_topic_id)

            if request_type == "data":
                confirmation_message = {
                    "user_id": data["user_id"],
                    "location": data["location"],
                    "date_range": data["date_range"],
                    "status": "complete",
                    "message": f"The processed data for {data['location']} ({data['date_range']}) has been successfully uploaded to GCS."
                }
            elif request_type == "metadata":
                confirmation_message = {
                    "source": data["source"],
                    "status": "complete",
                    "message": f"The metadata for {data['source']} has been successfully retrieved and uploaded to GCS."
                }

            confirmation_future = confirmation_publisher.publish(
                confirmation_topic_path,
                json.dumps(confirmation_message).encode("utf-8")
            )
            print(f"Confirmation message sent to Pub/Sub: {confirmation_future.result()}")

        else:
            print(f"Failed to trigger DAG: {response.status_code} - {response.text}")
            message.nack()

    except json.JSONDecodeError as e:
        print(f"Invalid JSON format: {e}")
        message.nack()
    except Exception as e:
        print(f"Unexpected error: {e}")
        message.nack()



# Callback function to handle the confirmation message when data is uploaded to GCS
def confirmation_callback(message):
    print(f"\n📨 Confirmation message received: {message.data}")
    try:
        data = json.loads(message.data.decode("utf-8"))

        # Validate required fields in the confirmation message
        required_fields = ['user_id', 'location', 'date_range', 'status', 'message']
        if not all(field in data for field in required_fields):
            raise ValueError("Missing required fields in confirmation message")

        # Display the confirmation message to the user
        print(f"Data upload complete!")
        print(f"{data['message']}")

        # Here you can print additional info like the GCS bucket and file path
        print(f"Your data is now available in GCS at: gs://your-bucket-name/{data['location']}/{data['date_range']}/")

        message.ack()
        
    except json.JSONDecodeError as e:
        print(f"Invalid JSON format: {e}")
        message.nack()
    except Exception as e:
        print(f"Unexpected error: {e}")
        message.nack()


def main():
    # Start listening for user data requests
    print(f"Starting subscriber on {subscription_path}...")
    streaming_pull_future = subscriber.subscribe(subscription_path, callback=callback)

    # Start listening for data upload confirmation
    print(f"Starting confirmation subscriber on {confirmation_subscription_path}...")
    confirmation_streaming_pull_future = subscriber.subscribe(confirmation_subscription_path, callback=confirmation_callback)

    try:
        while True:
            time.sleep(60)
    except KeyboardInterrupt:
        streaming_pull_future.cancel()
        confirmation_streaming_pull_future.cancel()
        print("\nSubscriber stopped.")

if __name__ == "__main__":
    main()