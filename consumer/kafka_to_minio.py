import boto3
from confluent_kafka import Consumer, KafkaError
import json
import pandas as pd
from datetime import datetime
import os
from dotenv import load_dotenv
from pathlib import Path
import logging
import traceback
import sys
import tempfile

# -----------------------------
# Load secrets from .env (file next to this script) and basic logging
# -----------------------------
env_path = Path(__file__).parent / ".env"
logger = logging.getLogger(__name__)
print(f"Loading environment from: {env_path}")
load_dotenv(env_path)
logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
logger = logging.getLogger(__name__)

# Required env vars
required = [
    'KAFKA_BOOTSTRAP', 'KAFKA_GROUP',
    'MINIO_ENDPOINT', 'MINIO_ACCESS_KEY', 'MINIO_SECRET_KEY', 'MINIO_BUCKET'
]
missing = [v for v in required if not os.getenv(v)]
if missing:
    logger.error('Missing required environment variables: %s', missing)
    sys.exit(1)

# -----------------------------
# Kafka consumer settings (Confluent Kafka)
# -----------------------------
try:
    conf = {
        'bootstrap.servers': os.getenv('KAFKA_BOOTSTRAP'),
        'group.id': os.getenv('KAFKA_GROUP'),
        'auto.offset.reset': 'earliest',
        'enable.auto.commit': True
    }
    consumer = Consumer(conf)
    consumer.subscribe([
        'banking_server.public.customers',
        'banking_server.public.accounts',
        'banking_server.public.transactions'
    ])
    logger.info('✅ Kafka consumer created, subscribing to topics')
except Exception:
    logger.error('Failed to create KafkaConsumer:\n%s', traceback.format_exc())
    raise

# -----------------------------
# MinIO (S3) client
# -----------------------------
bucket = os.getenv('MINIO_BUCKET')
try:
    s3 = boto3.client(
        's3',
        endpoint_url=os.getenv('MINIO_ENDPOINT'),
        aws_access_key_id=os.getenv('MINIO_ACCESS_KEY'),
        aws_secret_access_key=os.getenv('MINIO_SECRET_KEY')
    )
    # ensure bucket exists
    existing = [b['Name'] for b in s3.list_buckets().get('Buckets', [])]
    if bucket not in existing:
        logger.info('Bucket "%s" not found. Creating...', bucket)
        s3.create_bucket(Bucket=bucket)
    logger.info('✅ Connected to MinIO (bucket=%s)', bucket)
except Exception:
    logger.error('Failed to connect to MinIO or ensure bucket:\n%s', traceback.format_exc())
    raise

# -----------------------------
# Consume and write function with robust error handling
# -----------------------------
batch_size = 10 
buffer = {
    'banking_server.public.customers': [],
    'banking_server.public.accounts': [],
    'banking_server.public.transactions': []
}

def write_to_minio(table_name, records):
    if not records:
        return
    try:
        df = pd.DataFrame(records)
    except Exception:
        logger.exception('Failed to create DataFrame from records')
        return

    date_str = datetime.now().strftime('%Y-%m-%d')
    s3_key = f'{table_name}/date={date_str}/{table_name}_{datetime.now().strftime("%H%M%S%f")}.parquet'

    # write to a temporary file and upload
    try:
        with tempfile.NamedTemporaryFile(suffix='.parquet', delete=False) as tmp:
            temp_path = tmp.name
        # try fastparquet then pyarrow
        try:
            df.to_parquet(temp_path, engine='fastparquet', index=False)
        except Exception:
            try:
                df.to_parquet(temp_path, engine='pyarrow', index=False)
            except Exception:
                logger.error('Failed to write parquet. Install fastparquet or pyarrow.\n%s', traceback.format_exc())
                if os.path.exists(temp_path):
                    os.remove(temp_path)
                return

        s3.upload_file(temp_path, bucket, s3_key)
        logger.info('✅ Uploaded %d records to s3://%s/%s', len(records), bucket, s3_key)
    except Exception:
        logger.exception('Failed to upload file to MinIO')
    finally:
        try:
            if 'temp_path' in locals() and os.path.exists(temp_path):
                os.remove(temp_path)
        except Exception:
            logger.debug('Failed to remove temp file %s', temp_path)


logger.info('Consumer ready — listening for messages...')

try:
    while True:
        message = consumer.poll(1.0)

        if message is None:
            continue
        if message.error():
            if message.error().code() == KafkaError._PARTITION_EOF:
                continue
            else:
                logger.error(f"Error in Kafka: {message.error()}")
                continue

        try:
            topic = message.topic()
            raw_value = message.value()
            
            if not raw_value:
                continue

            event = json.loads(raw_value.decode('utf-8'))
            
            # --- AQUI ESTÁ A MÁGICA NOVA ---
            # Ele vai tentar achar o 'after' de duas formas diferentes, para não perder NENHUMA mensagem
            if 'payload' in event:
                record = event['payload'].get('after') if isinstance(event['payload'], dict) else None
            else:
                record = event.get('after') if isinstance(event, dict) else None

            if record:
                buffer[topic].append(record)
                # Adicionamos um print para você VER o buffer enchendo
                print(f"✅ {topic}: record to buffer ({len(buffer[topic])}/{batch_size})")
            else:
                # Se mesmo assim não achar, ele imprime na tela para descobrirmos o motivo
                print(f"⚠️ Ignored: {str(event)[:150]}...")
            # -------------------------------

            if len(buffer.get(topic, [])) >= batch_size:
                write_to_minio(topic.split('.')[-1], buffer[topic])
                buffer[topic] = []
        except Exception:
            logger.exception('Error processing message, continuing')

except KeyboardInterrupt:
    logger.info('Interrupted by user, flushing buffers and exiting...')
except Exception:
    logger.exception('Fatal error in consumer loop')
finally:
    for topic, records in list(buffer.items()):
        if records:
            try:
                write_to_minio(topic.split('.')[-1], records)
            except Exception:
                logger.exception('Failed to flush buffer for %s', topic)
    try:
        consumer.close()
    except Exception:    
        pass
    logger.info('Consumer stopped')