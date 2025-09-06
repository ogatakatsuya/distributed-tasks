import time
import random
import socket
import logging
import json

from confluent_kafka import Consumer, Producer

# ログ設定
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Kafka Consumer 設定
consumer_conf = {
    'bootstrap.servers': 'kafka:9092',
    'group.id': 'worker-group',
    'auto.offset.reset': 'earliest'
}
consumer = Consumer(consumer_conf)
consumer.subscribe(["task-topic"])

# Kafka Producer 設定
producer_conf = {
    'bootstrap.servers': 'kafka:9092',
    'client.id': socket.gethostname()
}
producer = Producer(producer_conf)

logger.info("Worker started, waiting for tasks...")

try:
    while True:
        msg = consumer.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            logger.error(f"Consumer error: {msg.error()}")
            continue

        key = msg.key().decode() if msg.key() else None
        value = msg.value().decode()
        logger.info(f"Received task: key={key}, value={value}")

        # ランダムに sleep して処理を擬似化
        sleep_time = random.uniform(0.5, 2.0)
        logger.info(f"Processing task for {sleep_time:.2f} seconds...")
        time.sleep(sleep_time)

        # 結果をJSON形式で作成
        result = {
            "task_id": key,
            "message": value,
            "processed_at": time.time(),
            "worker_id": socket.gethostname()
        }
        
        # result-topic に送信
        producer.produce("result-topic", key=key, value=json.dumps(result))
        producer.flush()
        logger.info(f"Sent result: key={key}, result={result}")

except KeyboardInterrupt:
    logger.info("Worker stopped by user")
finally:
    logger.info("Closing consumer")
    consumer.close()
