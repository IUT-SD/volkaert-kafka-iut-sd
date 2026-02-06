from kafka import KafkaConsumer
import json
import mariadb
import sys
import uuid

try:
    conn = mariadb.connect(
        user="crmuser",
        password="IjEs0mBeTDB3cqSp",
        host="mariadb",
        port=3306,
        database="crm"

    )
except mariadb.Error as e:
    print(f"Error connecting to MariaDB Platform: {e}")
    sys.exit(1)

cur = conn.cursor()

cur.execute(
            """
            CREATE TABLE IF NOT EXIST clients (
            id VARCHAR(255) PRIMARY KEY
            nom VARCHAR(255)
            prenom VARCHAR(255)
            email VARCHAR(255)
            source_id INT
            )
            """ )

# Define the Kafka broker and topic
broker = 'my-kafka.kylianvkr-dev.svc.cluster.local:9092'
topic = 'dbserver1.inventory.customers'

# Create a Kafka consumer
consumer = KafkaConsumer(
    topic,
    bootstrap_servers=[broker],
    sasl_mechanism='SCRAM-SHA-256',
    security_protocol='SASL_PLAINTEXT',
    sasl_plain_username='user1',
    sasl_plain_password='k5kAwbuULJ',
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    group_id='crm'
)

print(f"Listening to topic {topic}")

# Poll messages from the Kafka topic
for message in consumer:
    valeur = message.value['payload']
    if valeur['before'] is None :
        source_id = valeur['after']['id']
        prenom = valeur['after']['first_name']
        nom = valeur['after']['last_name']
        email = valeur['after']['email']
        base_id = uuid.uuid4()
        cur.execute(
            "INSERT INTO clients (id, nom, prenom, email, source_id) VALUES (?, ?, ?, ?, ?)", 
            (base_id, nom, prenom, email, source_id))
    elif valeur['after'] is None :
        source_id = valeur['before']['id']
        cur.execute(
            "DELETE * FROM clients WHERE source_id LIKE (?)",
            (source_id))
    else :
        source_id = valeur['after']['id']
        prenom = valeur['after']['first_name']
        nom = valeur['after']['last_name']
        email = valeur['after']['email']

        requete = sql = """
                        UPDATE clients
                        SET nom = %s, prenom = %s, email = %s
                        WHERE source_id = %s
                        """

        cur.execute(requete , (nom, prenom, email, source_id))

