import socket
import uuid

from confluent_kafka import Producer

conf = {
    'bootstrap.servers': 'kafka:9092',
    'client.id': socket.gethostname()
}

producer = Producer(conf)

topic = "task-topic"

for m in range(100):
    partition = m % 10
    key = str(uuid.uuid4())
    producer.produce(topic, key=key, value=f"message {m}", partition=partition)
    print(f"Produced message: {m} to partition {partition}")

task_count = str(100)
producer.produce("control-topic", key="count", value=task_count)
print(f"Produced control message: {task_count}")

producer.flush()


