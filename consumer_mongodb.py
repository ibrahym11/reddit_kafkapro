from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from kafka import KafkaConsumer
from pymongo import MongoClient
import json

# Create Spark session
spark = SparkSession.builder \
    .appName("KafkaToMongo") \
    .config("spark.mongodb.input.uri", "mongodb://localhost:27017/reddit.comments") \
    .config("spark.mongodb.output.uri", "mongodb://localhost:27017/reddit.comments") \
    .getOrCreate()

# Initialize MongoDB client
client = MongoClient("mongodb://localhost:27017/")
db = client.reddit
collection = db.comments

# Kafka Consumer setup
consumer = KafkaConsumer(
    'reddit-comments',
    bootstrap_servers='localhost:9092',
    group_id='consumer_group',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

# Process Kafka messages and save to MongoDB
for message in consumer:
    comment = message.value
    collection.insert_one(comment)  # Insert data into MongoDB

print("Data insertion complete.")
