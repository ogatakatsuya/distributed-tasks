import socket
import logging
import sys
import os

# プロジェクトルートをパスに追加
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from confluent_kafka import Producer
from schemas import PixelPosition

# ログ設定
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# 画像設定
IMAGE_WIDTH = 800
IMAGE_HEIGHT = 800
RAYS_PER_PIXEL = 50

conf = {
    'bootstrap.servers': 'kafka:9092',
    'client.id': socket.gethostname(),
    'queue.buffering.max.messages': 1000000,
    'queue.buffering.max.kbytes': 1048576,
    'batch.size': 16384,
    'linger.ms': 5
}

producer = Producer(conf)

logger.info(f"Starting ray-tracing task generation for {IMAGE_WIDTH}x{IMAGE_HEIGHT} image with {RAYS_PER_PIXEL} rays per pixel")

# 各ピクセルに対してタスクを生成
total_tasks = 0
for row in range(IMAGE_HEIGHT):
    for col in range(IMAGE_WIDTH):
        # ピクセルポジションタスクを作成
        pixel_task = PixelPosition(
            count=RAYS_PER_PIXEL,
            row=row,
            col=col
        )
        
        # パーティショニング: 行番号でパーティション分散
        partition = row % 10
        key = f"pixel_{row}_{col}"
        
        producer.produce(
            "task-topic", 
            key=key, 
            value=pixel_task.to_json(), 
            partition=partition
        )
        
        total_tasks += 1
        
        # 定期的にflushしてバッファオーバーフローを防ぐ
        if total_tasks % 5000 == 0:
            producer.flush()
            logger.info(f"Produced {total_tasks} pixel tasks...")

logger.info(f"Total pixel tasks produced: {total_tasks}")

# 制御メッセージを送信（総タスク数）
producer.produce("control-topic", key="count", value=str(total_tasks))
logger.info(f"Produced control message: total_tasks={total_tasks}")

# 画像情報も制御メッセージとして送信
image_info = f"{IMAGE_WIDTH},{IMAGE_HEIGHT}"
producer.produce("control-topic", key="image_info", value=image_info)
logger.info(f"Produced image info: {image_info}")

producer.flush()
logger.info("All messages flushed to Kafka")


