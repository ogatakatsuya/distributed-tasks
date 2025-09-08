import socket
import logging
import sys
import os

# プロジェクトルートをパスに追加
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from confluent_kafka import Consumer, Producer
from schemas import PixelPosition, PixelPositionResult
from raytracer import RayTracer

# ログ設定
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Kafka Consumer 設定
consumer_conf = {
    'bootstrap.servers': 'kafka:9092',
    'group.id': 'raytracer-worker-group',
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

# レイトレーサーを初期化
raytracer = RayTracer()
worker_id = socket.gethostname()

logger.info(f"Ray-tracing worker {worker_id} started, waiting for pixel tasks...")

try:
    processed_count = 0
    while True:
        msg = consumer.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            logger.error(f"Consumer error: {msg.error()}")
            continue

        key = msg.key().decode() if msg.key() else None
        value = msg.value().decode()
        
        try:
            # 空のメッセージをスキップ
            if not value or not value.strip():
                logger.debug("Skipping empty message")
                continue
                
            # ピクセルタスクをデシリアライズ
            pixel_task = PixelPosition.from_json(value)
            logger.debug(f"Processing pixel ({pixel_task.row}, {pixel_task.col}) with {pixel_task.count} rays")

            # レイトレーシングを実行
            red, green, blue = raytracer.render_pixel(
                pixel_task.row, 
                pixel_task.col, 
                pixel_task.count
            )

            # 結果を作成
            result = PixelPositionResult(
                count=pixel_task.count,
                row=pixel_task.row,
                col=pixel_task.col,
                red=red,
                green=green,
                blue=blue
            )
            
            # result-topic に送信
            producer.produce("result-topic", key=key, value=result.to_json())
            producer.flush()
            
            processed_count += 1
            if processed_count % 100 == 0:
                logger.info(f"Worker {worker_id}: processed {processed_count} pixels")
        
        except KeyError as e:
            logger.error(f"Missing required field in pixel task: {e}. Message: {value[:100]}")
            continue
        except ValueError as e:
            logger.error(f"Invalid JSON format in pixel task: {e}. Message: {value[:100]}")
            continue
        except Exception as e:
            logger.error(f"Error processing pixel task: {e}. Message: {value[:100]}")
            continue

except KeyboardInterrupt:
    logger.info("Ray-tracing worker stopped by user")
finally:
    logger.info(f"Worker {worker_id} processed {processed_count} pixels total")
    logger.info("Closing consumer")
    consumer.close()
