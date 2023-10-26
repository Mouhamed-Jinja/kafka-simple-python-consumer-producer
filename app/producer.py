from kafka import KafkaProducer
import json
import time

#pramerters
topic = "test"
bootstrap_server = "localhost:9092"

producser = KafkaProducer(
                        bootstrap_servers= bootstrap_server,
                        value_serializer = lambda v: json.dumps(v).encode("utf-8")
)

for i in range(100000):
    producser.send(topic=topic, value=f"Message to kafka No: {i}")
    producser.flush()
    print(f"Message to kafka No: {i}")
    time.sleep(1)
    
producser.close()