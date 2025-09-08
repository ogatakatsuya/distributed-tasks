import json
import logging
import os
import sys
from datetime import datetime
from typing import Dict, Optional
import numpy as np
from PIL import Image

# プロジェクトルートをパスに追加
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import boto3
from botocore.exceptions import ClientError
from confluent_kafka import Consumer
from dotenv import load_dotenv
from schemas import PixelPositionResult

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
    'group.id': 'raytracer-aggregator-group',
    'auto.offset.reset': 'earliest'
}

consumer = Consumer(consumer_conf)
consumer.subscribe(["control-topic", "result-topic"])

total_tasks: Optional[int] = None
image_width: Optional[int] = None
image_height: Optional[int] = None
pixel_results: Dict[str, PixelPositionResult] = {}

def upload_file_to_s3(file_path: str, s3_key: str) -> bool:
    """
    ファイルをS3にアップロードする
    
    Args:
        file_path: ローカルファイルパス
        s3_key: S3でのキー
    
    Returns:
        bool: アップロード成功時True、失敗時False
    """
    try:
        s3_client.upload_file(file_path, S3_BUCKET_NAME, s3_key)
        logger.info(f"Successfully uploaded {file_path} to S3: s3://{S3_BUCKET_NAME}/{s3_key}")
        return True
        
    except ClientError as e:
        logger.error(f"Failed to upload {file_path} to S3: {e}")
        return False
    except Exception as e:
        logger.error(f"Unexpected error during S3 upload: {e}")
        return False

def create_image_from_pixels(pixels: Dict[str, PixelPositionResult], width: int, height: int) -> str:
    """
    ピクセル結果から画像を作成
    
    Args:
        pixels: ピクセル結果のディクショナリ
        width: 画像幅
        height: 画像高さ
    
    Returns:
        str: 作成された画像ファイルのパス
    """
    # 3チャンネル（RGB）の画像配列を初期化
    image_array = np.zeros((height, width, 3), dtype=np.float32)
    
    # 各ピクセル結果を画像配列に配置
    for pixel_result in pixels.values():
        row = pixel_result.row
        col = pixel_result.col
        if 0 <= row < height and 0 <= col < width:
            image_array[row, col, 0] = pixel_result.red
            image_array[row, col, 1] = pixel_result.green
            image_array[row, col, 2] = pixel_result.blue
    
    # [0,1] の範囲を [0,255] に変換
    image_array = np.clip(image_array * 255, 0, 255).astype(np.uint8)
    
    # PIL Imageに変換して保存
    image = Image.fromarray(image_array, 'RGB')
    
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"raytraced_image_{timestamp}.png"
    image.save(filename)
    
    logger.info(f"Ray-traced image saved as {filename} ({width}x{height})")
    return filename

logger.info("Ray-tracing aggregator started, waiting for control messages...")

while True:
    msg = consumer.poll(1.0)
    if msg is None:
        continue
    if msg.error():
        logger.error(f"Consumer error: {msg.error()}")
        continue

    topic = msg.topic()
    key = msg.key().decode() if msg.key() else None
    value = msg.value().decode() if msg.value() else ""

    if topic == "control-topic":
        if not value:
            logger.warning("Empty control message received")
            continue
        
        if key == "count":
            try:
                total_tasks = int(value)
                logger.info(f"Total pixel tasks to wait for: {total_tasks}")
            except ValueError as e:
                logger.error(f"Error parsing control message '{value}': {e}")
                continue
        elif key == "image_info":
            try:
                width_str, height_str = value.split(",")
                image_width = int(width_str)
                image_height = int(height_str)
                logger.info(f"Image dimensions: {image_width}x{image_height}")
            except ValueError as e:
                logger.error(f"Error parsing image info '{value}': {e}")
                continue
                
    elif topic == "result-topic":
        if not value:
            logger.warning("Empty result message received")
            continue
        try:
            pixel_result = PixelPositionResult.from_json(value)
            pixel_results[key] = pixel_result
            
            if len(pixel_results) % 1000 == 0:
                logger.info(f"Received {len(pixel_results)} pixel results...")
                
        except Exception as e:
            logger.error(f"Error parsing pixel result message '{value}': {e}")
            continue

    # 全タスク受信したら画像を生成して終了
    if total_tasks and image_width and image_height and len(pixel_results) >= total_tasks:
        logger.info(f"All {total_tasks} pixel results received. Creating final image...")
        
        # 画像を生成
        image_filename = create_image_from_pixels(pixel_results, image_width, image_height)
        
        # S3にアップロード
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        s3_key = f"raytraced_images/{image_filename}"
        
        if upload_file_to_s3(image_filename, s3_key):
            logger.info(f"Ray-traced image successfully uploaded to S3: s3://{S3_BUCKET_NAME}/{s3_key}")
        else:
            logger.warning("Failed to upload image to S3, but local file is saved")
        
        # 結果データもJSONで保存・アップロード
        results_data = {
            "image_info": {"width": image_width, "height": image_height},
            "total_pixels": len(pixel_results),
            "pixel_results": {k: {
                "row": v.row, "col": v.col, "count": v.count,
                "red": v.red, "green": v.green, "blue": v.blue
            } for k, v in pixel_results.items()}
        }
        
        json_filename = f"raytracing_results_{timestamp}.json"
        with open(json_filename, "w") as f:
            json.dump(results_data, f, indent=2)
        
        json_s3_key = f"results/{json_filename}"
        upload_file_to_s3(json_filename, json_s3_key)
        
        logger.info("Ray-tracing aggregation completed successfully!")
        break

consumer.close()
logger.info("Ray-tracing aggregator closed")
