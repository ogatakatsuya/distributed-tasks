import socket
import uuid
import logging

from confluent_kafka import Producer

# ログ設定
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

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
    logger.info(f"Produced message: {m} to partition {partition}")

task_count = str(100)
producer.produce("control-topic", key="count", value=task_count)
logger.info(f"Produced control message: {task_count}")

producer.flush()


