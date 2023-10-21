from kafka import KafkaProducer
import json
import time

bootstrap_servers = 'localhost:9092'
topic = 'test2'

# Create a Kafka producer
producer = KafkaProducer(bootstrap_servers=bootstrap_servers,
                         value_serializer=lambda v: json.dumps(v).encode('utf-8'))

for i in range(30):
    producer.send(topic, value=f"Message: {i}")
    print("Message sent to Kafka:", i)
    producer.flush()
    time.sleep(5)

producer.close()
