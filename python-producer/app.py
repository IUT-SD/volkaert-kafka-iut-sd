from kafka import KafkaProducer
import time
import json

# Define the Kafka broker and topic
broker = 'my-kafka.kylianvkr-dev.svc.cluster.local:9092'
topic = 'partitionned'

# Create a Kafka producer
producer = KafkaProducer(
    bootstrap_servers=[broker],
    sasl_mechanism='SCRAM-SHA-256',
    security_protocol='SASL_PLAINTEXT',
    sasl_plain_username='user1',
    sasl_plain_password='fyNJYEYf9G',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)
i = 0
while True:
    # Define the message to send
    message = {
        'key': f'oh-!-une-cle {time.time()}',
        'value': i
    }

    # Send the message to the Kafka topic
    producer.send(topic, value=message)

    # Ensure all messages are sent before closing the producer
    producer.flush()

    print(f"Message sent to topic {topic}")
    i += 1