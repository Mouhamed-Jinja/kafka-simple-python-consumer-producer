from kafka import KafkaConsumer
import json
import logging

bootstrap_servers = 'localhost:9092'
topic = 'test2'
group_id = 'my-consumer-group'


logging.basicConfig(level=logging.INFO)
consumer = KafkaConsumer(topic,
                         bootstrap_servers=bootstrap_servers,
                         group_id=group_id,
                         value_deserializer=lambda v: json.loads(v.decode('utf-8')))


consumer.subscribe(topics=[topic])

try:
    for message in consumer:
        print("Received message from Kafka:", message.value)
except Exception as e:
    logging.error("Error while consuming messages from Kafka: %s", str(e))
finally:
    # Close the consumer
    consumer.close()
