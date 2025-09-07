import json
import logging
import os
from datetime import datetime
from typing import Dict, Any

import boto3
from botocore.exceptions import ClientError
from confluent_kafka import Consumer
from dotenv import load_dotenv

# 環境変数の読み込み
load_dotenv()

# ログ設定
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# S3設定
S3_BUCKET_NAME = os.getenv('S3_BUCKET_NAME', 'kafka-aggregated-images')
AWS_REGION = os.getenv('AWS_DEFAULT_REGION', 'ap-northeast-3')

# S3クライアントの初期化
s3_client = boto3.client(
    's3',
    region_name=AWS_REGION,
    aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
    aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY')
)

consumer_conf = {
    'bootstrap.servers': 'kafka:9092',
    'group.id': 'aggregator-group',
    'auto.offset.reset': 'earliest'
}

consumer = Consumer(consumer_conf)
consumer.subscribe(["control-topic", "result-topic"])

total_tasks = None
results = {}

def upload_to_s3(data: Dict[Any, Any], filename: str) -> bool:
    """
    JSONデータをS3にアップロードする
    
    Args:
        data: アップロードするデータ
        filename: S3でのファイル名
    
    Returns:
        bool: アップロード成功時True、失敗時False
    """
    try:
        # JSONデータを文字列に変換
        json_data = json.dumps(data, indent=2, ensure_ascii=False)
        
        # S3にアップロード
        s3_client.put_object(
            Bucket=S3_BUCKET_NAME,
            Key=f"results/{filename}",
            Body=json_data.encode('utf-8'),
            ContentType='application/json'
        )
        
        logger.info(f"Successfully uploaded {filename} to S3 bucket {S3_BUCKET_NAME}")
        return True
        
    except ClientError as e:
        logger.error(f"Failed to upload to S3: {e}")
        return False
    except Exception as e:
        logger.error(f"Unexpected error during S3 upload: {e}")
        return False

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
        # ローカルファイルに保存
        with open("aggregated_results.json", "w") as f:
            json.dump(results, f, indent=2)
        logger.info(f"All {total_tasks} tasks aggregated. Saved to aggregated_results.json")
        
        # S3にアップロード
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        s3_filename = f"aggregated_results_{timestamp}.json"
        
        if upload_to_s3(results, s3_filename):
            logger.info(f"Results successfully uploaded to S3: s3://{S3_BUCKET_NAME}/{s3_filename}")
        else:
            logger.warning("Failed to upload results to S3, but local file is saved")
        
        break

consumer.close()
logger.info("Aggregator closed")
