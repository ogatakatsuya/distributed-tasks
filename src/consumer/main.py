import time
import random
import socket

from confluent_kafka import Consumer, Producer

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

print("Worker started, waiting for tasks...", flush=True)

try:
    while True:
        msg = consumer.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            print("Consumer error:", msg.error(), flush=True)
            continue

        key = msg.key().decode() if msg.key() else None
        value = msg.value().decode()
        print(f"Received task: key={key}, value={value}", flush=True)

        # ランダムに sleep して処理を擬似化
        sleep_time = random.uniform(0.5, 2.0)
        print(f"Processing task for {sleep_time:.2f} seconds...", flush=True)
        time.sleep(sleep_time)

        # result-topic に送信
        producer.produce("result-topic", key=key, value=value)
        producer.flush()
        print(f"Sent result: key={key}, value={value}", flush=True)

except KeyboardInterrupt:
    print("Worker stopped by user", flush=True)
finally:
    print("Closing consumer", flush=True)
    consumer.close()
