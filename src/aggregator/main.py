import json
import logging

from confluent_kafka import Consumer

# ログ設定
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

consumer_conf = {
    'bootstrap.servers': 'kafka:9092',
    'group.id': 'aggregator-group',
    'auto.offset.reset': 'earliest'
}

consumer = Consumer(consumer_conf)
consumer.subscribe(["control-topic", "result-topic"])

total_tasks = None
results = {}

logger.info("Aggregator started, waiting for control message...")

while True:
    msg = consumer.poll(1.0)
    if msg is None:
        continue
    if msg.error():
        logger.error(f"Consumer error: {msg.error()}")
        continue

    topic = msg.topic()
    value = msg.value().decode() if msg.value() else ""

    logger.info(f"Received message from topic '{topic}': value='{value}'")

    if topic == "control-topic":
        if not value:
            logger.warning("Empty control message received")
            continue
        try:
            total_tasks = int(value)
            logger.info(f"Total tasks to wait for: {total_tasks}")
        except ValueError as e:
            logger.error(f"Error parsing control message '{value}': {e}")
            continue
    elif topic == "result-topic":
        if not value:
            logger.warning("Empty result message received")
            continue
        try:
            task_result = json.loads(value)
            key = msg.key().decode() if msg.key() else None
            results[key] = task_result
            logger.info(f"Received result for key={key}: {task_result}")
        except json.JSONDecodeError as e:
            logger.error(f"Error parsing JSON from result message '{value}': {e}")
            continue

    # 全タスク受信したら集約して終了
    if total_tasks and len(results) >= total_tasks:
        with open("aggregated_results.json", "w") as f:
            json.dump(results, f, indent=2)
        logger.info(f"All {total_tasks} tasks aggregated. Saved to aggregated_results.json")
        break

consumer.close()
logger.info("Aggregator closed")
