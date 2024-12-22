import json
from kafka import KafkaConsumer
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Kafka configuration
KAFKA_SERVER = 'localhost:9092'
KAFKA_TOPIC = 'reddit-comments'

# MongoDB configuration
MONGO_URI = "mongodb+srv://ibrooloye1:G1omjbQ0JsfrAEQv@ibrahymcluster.aiuvo.mongodb.net/"
MONGO_CONNECTOR_JAR = "/path/to/mongo-spark-connector_2.12-3.0.0.jar"  # Path to your MongoDB Spark Connector JAR

# Initialize Spark session with MongoDB connector JAR
spark = SparkSession.builder \
    .appName("KafkaMongoIntegration") \
    .config("spark.mongodb.input.uri", MONGO_URI) \
    .config("spark.mongodb.output.uri", MONGO_URI) \
    .config("spark.jars", MONGO_CONNECTOR_JAR) \
    .getOrCreate()

# Kafka consumer configuration
consumer = KafkaConsumer(
    KAFKA_TOPIC,
    bootstrap_servers=KAFKA_SERVER,
    value_deserializer=lambda x: json.loads(x.decode('utf-8')),
    auto_offset_reset='earliest',
    enable_auto_commit=True
)

# Process the Kafka messages using Spark and insert into MongoDB
def process_and_store(message):
    # Convert the message to a Spark DataFrame
    df = spark.createDataFrame([message])

    # You can perform any Spark processing on the DataFrame here if needed
    # For example, filter the comments based on some criteria
    processed_df = df.filter(col('body').isNotNull())

    # Write the processed data to MongoDB
    processed_df.write \
        .format("mongo") \
        .mode("append") \
        .option("spark.mongodb.output.uri", MONGO_URI) \
        .save()

    print(f"Stored comment: {message['comment_id']}")

# Consume messages from Kafka and process them
try:
    print("Listening to Kafka topic 'reddit-comments' and storing data in MongoDB...")

    for message in consumer:
        process_and_store(message.value)

except Exception as e:
    print(f"Error: {e}")

finally:
    consumer.close()
    spark.stop()

