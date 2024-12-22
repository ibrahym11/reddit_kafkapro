import praw
from kafka import KafkaProducer
import json
import time
import requests
from requests.auth import HTTPBasicAuth
from requests.exceptions import RequestException


# Reddit API credentials
client_id = 'cPnEHD7927xNdVC_0dxZHw'
client_secret = 'R6wb4h4TphQi71jWLTonglANOZtdSQ'
username = 'Professional-Lead544'
password = 'password10'
user_agent = 'reddit_producer'


# Initialize Reddit API client
def initialize_reddit():
    try:
        reddit = praw.Reddit(
            client_id=client_id,
            client_secret=client_secret,
            username=username,
            password=password,
            user_agent=user_agent
        )
        print("Successfully connected to Reddit API")
        return reddit
    except Exception as e:
        print(f"Error initializing Reddit client: {e}")
        return None


# Kafka producer configuration
def initialize_kafka_producer():
    try:
        producer = KafkaProducer(
            bootstrap_servers='localhost:9092',
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            request_timeout_ms=120000,  # Increase timeout to 120 seconds
            retries=5,  # Retry the request a few more times
            retry_backoff_ms=5000  # Increase time between retries
        )
        print("Successfully connected to Kafka")
        return producer
    except Exception as e:
        print(f"Error initializing Kafka producer: {e}")
        return None


# Test Reddit API authentication
def get_access_token():
    url = "https://www.reddit.com/api/v1/access_token"
    auth = HTTPBasicAuth(client_id, client_secret)

    data = {
        'grant_type': 'client_credentials',
    }

    headers = {
        'User-Agent': user_agent,
    }

    try:
        response = requests.post(url, data=data, auth=auth, headers=headers)
        if response.status_code == 200:
            print("Access token:", response.json())
            return True
        else:
            print(f"Error: {response.status_code}, {response.text}")
            return False
    except RequestException as e:
        print(f"Error while getting access token: {e}")
        return False


# Stream Reddit comments and send to Kafka
def fetch_comments(reddit, producer):
    subreddit = reddit.subreddit("politics")
    print("Listening to subreddit 'politics' comments...")

    try:
        for comment in subreddit.stream.comments(skip_existing=True):
            message = {
                'comment_id': comment.id,
                'author': str(comment.author),
                'body': comment.body,
                'created_utc': comment.created_utc
            }
            # Send to Kafka topic
            producer.send('reddit-comments', value=message)
            print(f"Sent: {message}")
    except Exception as e:
        print(f"Error occurred while streaming comments: {e}")


# Retry logic for connecting to Reddit API
def retry_logic_for_reddit(max_retries=5):
    for attempt in range(max_retries):
        if get_access_token():
            return True
        print(f"Attempt {attempt + 1} failed. Retrying...")
        time.sleep(10)
    print("Max retries reached. Exiting.")
    return False


# Main function to start the producer
def main():
    # Retry connection to Reddit API
    if not retry_logic_for_reddit():
        return  # Exit if Reddit API authentication fails

    reddit = initialize_reddit()
    if reddit is None:
        return  # Exit if Reddit client initialization fails

    producer = initialize_kafka_producer()
    if producer is None:
        return  # Exit if Kafka producer initialization fails

    # Start streaming comments
    try:
        fetch_comments(reddit, producer)
    except KeyboardInterrupt:
        print("Process interrupted by user.")
    finally:
        producer.close()


if __name__ == "__main__":
    main()


