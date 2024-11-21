from kafka import KafkaProducer
import json
import random
from datetime import datetime, timedelta

# Kafka producer configuration
KAFKA_BROKER = 'localhost:9092'  # Replace with your Kafka broker
TOPIC = 'your_topic'             # Replace with your Kafka topic

# Initialize Kafka producer
producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# Function to generate random JSON data
def generate_data():
    names = ["John Doe", "Jane Doe", "Alice Smith", "Bob Brown", "Charlie Johnson"]
    for i in range(1, 101):
        yield {
            "id": f"user-{i}",
            "name": random.choice(names),
            "age": random.randint(20, 60),
            "created_at": (datetime.now() - timedelta(seconds=random.randint(0, 3600))).strftime("%Y-%m-%dT%H:%M:%SZ")
        }

# Send 100 records to Kafka
for record in generate_data():
    producer.send(TOPIC, record)
    print(f"Produced: {record}")

# Close producer
producer.flush()
producer.close()
