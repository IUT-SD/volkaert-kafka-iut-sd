from kafka import KafkaConsumer
import json

# Define the Kafka broker and topic
broker = 'my-kafka.kylianvkr-dev.svc.cluster.local:9092'
topic = 'my-first-topic'

# Create a Kafka consumer
consumer = KafkaConsumer(
    topic,
    bootstrap_servers=[broker],
    sasl_mechanism='SCRAM-SHA-256',
    security_protocol='SASL_PLAINTEXT',
    sasl_plain_username='user1',
    sasl_plain_password='fyNJYEYf9G',
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    group_id='erp'
)

print(f"Listening to topic {topic}")

# Poll messages from the Kafka topic
for message in consumer:
    print(f"Received message: {message.value}")