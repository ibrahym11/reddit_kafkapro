from kafka import KafkaConsumer
from pymongo import MongoClient
import json

# MongoDB configuration
MONGO_URI = "mongodb+srv://ibrooloye1:G1omjbQ0JsfrAEQv@ibrahymcluster.aiuvo.mongodb.net/reddit?retryWrites=true&w=majority"
DATABASE_NAME = "reddit"
COLLECTION_NAME = "comments"

# Kafka configuration
KAFKA_TOPIC = "reddit-comments"
KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"

# Connect to MongoDB
client = MongoClient(MONGO_URI)
db = client[DATABASE_NAME]
collection = db[COLLECTION_NAME]

# Connect to Kafka
consumer = KafkaConsumer(
    KAFKA_TOPIC,
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
    value_deserializer=lambda v: json.loads(v.decode("utf-8")),
    auto_offset_reset="earliest",  # Start reading from the beginning of the topic
    enable_auto_commit=True
)

print("Listening to Kafka topic and inserting data into MongoDB...")

# Consume messages and insert them into MongoDB
try:
    for message in consumer:
        comment = message.value
        collection.insert_one(comment)  # Insert data into MongoDB
        print(f"Inserted into MongoDB: {comment}")
except Exception as e:
    print(f"Error: {e}")
finally:
    consumer.close()
    client.close()


