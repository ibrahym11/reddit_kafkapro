# Reddit Kafka Spark MongoDB Project

This project demonstrates how to integrate Reddit, Apache Kafka, Apache Spark, and MongoDB to build a data pipeline. The pipeline collects real-time comments from a subreddit, processes them using Kafka and Spark, and stores the results in a MongoDB database.

## Components Used

### 1. Reddit API
- The Reddit API is used to fetch live comments from a specific subreddit.
- `PRAW` (Python Reddit API Wrapper) is used to interact with the Reddit API.

### 2. Apache Kafka
- Kafka is used as a message broker to stream Reddit comments in real-time.

### 3. Apache Spark
- Spark processes the streamed data and transforms it for further storage or analysis.

### 4. MongoDB
- MongoDB is used to store the processed data for querying and visualization.

## Architecture
1. Reddit comments are fetched in real-time using the Reddit API.
2. The comments are sent to a Kafka topic.
3. Spark consumes messages from the Kafka topic, processes them, and writes the output to MongoDB.

## Prerequisites
- Docker and Docker Compose
- Python 3.x
- MongoDB Atlas account or local MongoDB setup

## Installation and Setup

### Step 1: Clone the Repository
```bash
git clone <repository-url>
cd reddit_kafka_project
```

### Step 2: Set Up Environment Variables
Create a `.env` file with the following:
```
REDDIT_CLIENT_ID=<your_client_id>
REDDIT_CLIENT_SECRET=<your_client_secret>
REDDIT_USERNAME=<your_username>
REDDIT_PASSWORD=<your_password>
MONGO_URI=<your_mongo_uri>
```

### Step 3: Start Services
Use Docker Compose to start Kafka, Zookeeper, and MongoDB:
```bash
docker-compose up -d
```

### Step 4: Run the Producer
Start the Reddit Kafka producer to stream comments into Kafka:
```bash
python producer.py
```

### Step 5: Run the Spark Consumer
Start the Spark job to process the Kafka stream and write data to MongoDB:
```bash
spark-submit spark_consumer.py
```

## Files in the Repository

- `producer.py`: Fetches Reddit comments and sends them to a Kafka topic.
- `spark_consumer.py`: Consumes messages from Kafka, processes them, and writes to MongoDB.
- `docker-compose.yml`: Defines the services for Kafka, Zookeeper, and MongoDB.
- `README.md`: Documentation for the project.

## Usage
- Modify the subreddit in `producer.py` to change the source of the comments.
- Run analytics queries on MongoDB to analyze the stored data.

## Troubleshooting
- Ensure that all services are running with Docker Compose.
- Check your Reddit API credentials and MongoDB URI.
- Use `docker logs <container_name>` to debug service issues.


