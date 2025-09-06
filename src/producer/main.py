import socket
import uuid

from confluent_kafka import Producer

conf = {
    'bootstrap.servers': 'kafka:9092',
    'client.id': socket.gethostname()
}

producer = Producer(conf)

topic = "task-topic"
tasks = ["task1", "task2", "task3"]

for m in tasks:
    key = str(uuid.uuid4())
    producer.produce(topic, key=key, value=m)
    print(f"Produced message: {m}")

task_count = str(len(tasks))
producer.produce("control-topic", key="count", value=task_count)
print(f"Produced control message: {task_count}")

producer.flush()
