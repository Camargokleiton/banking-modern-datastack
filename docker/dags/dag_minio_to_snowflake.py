import os
import boto3
import snowflake.connector
from airflow.decorators import dag, task
from datetime import datetime, timedelta
import tempfile
from dotenv import load_dotenv

# Load environment variables (If Airflow is in Docker, ensure .env is accessible)
load_dotenv()

TABLES = ["customers", "accounts", "transactions"]
STAGE_NAME = os.getenv("SNOWFLAKE_STAGE")  

# -------- Helper Connection Functions --------
def get_minio_client():
    return boto3.client(
        "s3",
        endpoint_url=os.getenv("MINIO_ENDPOINT"),
        aws_access_key_id=os.getenv("MINIO_ACCESS_KEY"),
        aws_secret_access_key=os.getenv("MINIO_SECRET_KEY")
    )

def get_snowflake_conn():
    return snowflake.connector.connect(
        user=os.getenv("SNOWFLAKE_USER"),
        password=os.getenv("SNOWFLAKE_PASSWORD"),
        account=os.getenv("SNOWFLAKE_ACCOUNT"),
        warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
        database=os.getenv("SNOWFLAKE_DB"),
        schema=os.getenv("SNOWFLAKE_SCHEMA"),
        role=os.getenv("SNOWFLAKE_ROLE") 
    )

TABLE_UNIQUE_KEYS = {
    "customers": "customer_id",
    "accounts": "account_id",
    "transactions": "transactions_id",
}


def get_merge_sql(table_name: str, source_name: str) -> str | None:
    unique_key = TABLE_UNIQUE_KEYS.get(table_name)
    if not unique_key:
        return None

    return f"""
    MERGE INTO {table_name} AS target
    USING (
        SELECT
            source.v:{unique_key}::STRING AS key_value,
            source.v AS row_variant
        FROM {source_name} AS source
    ) AS source
    ON target.v:{unique_key}::STRING = source.key_value
    WHEN MATCHED THEN UPDATE SET v = source.row_variant
    WHEN NOT MATCHED THEN INSERT (v) VALUES (source.row_variant)
    """

# -------- DAG Definition --------
default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}

@dag(
    dag_id="minio_to_snowflake_banking",
    default_args=default_args,
    description="Load MinIO parquet into Snowflake RAW tables iteratively using a Named Stage",
    schedule="*/1 * * * *",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["banking", "ingestion"]
)
def banking_ingestion_dag():

    @task()
    def process_table(table_name: str):
        """
        Downloads data for a given table from MinIO, executes PUT/COPY in Snowflake via a Named Stage, 
        and deletes from MinIO upon success to avoid duplication.
        """
        
        from dotenv import load_dotenv
        from pathlib import Path
        load_dotenv(Path(__file__).parent / ".env")
        
        
        s3 = get_minio_client()
        conn = get_snowflake_conn()
        bucket = os.getenv("MINIO_BUCKET")
        prefix = f"{table_name}/"

        # 1. List files in MinIO
        resp = s3.list_objects_v2(Bucket=bucket, Prefix=prefix)
        objects = resp.get("Contents", [])
        
        if not objects:
            print(f"📭 No new files found for table {table_name}.")
            return

        print(f"📦 Found {len(objects)} files for {table_name}.")

        with conn.cursor() as cur:
            db_name = os.getenv("SNOWFLAKE_DB") or os.getenv("SNOWFLAKE_DATABASE")
            cur.execute(f"USE DATABASE {db_name}")
            cur.execute(f"USE SCHEMA {os.getenv('SNOWFLAKE_SCHEMA')}")
            cur.execute(f"CREATE STAGE IF NOT EXISTS {STAGE_NAME.lower()}")
            cur.execute(f"CREATE TABLE IF NOT EXISTS {table_name.lower()} (v VARIANT)")
            
            resp = s3.list_objects_v2(Bucket=bucket, Prefix=prefix)
            objects = resp.get("Contents", [])
        
            if not objects:
                 print(f"📭 No new files found for table {table_name}.")
                 return

            for obj in objects:
                minio_key = obj["Key"]
                
                # Ignore empty directories or non-parquet files
                if not minio_key.endswith('.parquet'):
                    continue

                # 2. Use a secure temporary directory (cleans up the disk automatically afterwards)
                with tempfile.NamedTemporaryFile(suffix=".parquet", delete=False) as tmp:
                    local_file = tmp.name

                try:
                    # Download from MinIO
                    s3.download_file(bucket, minio_key, local_file)
                    safe_path = local_file.replace('\\', '/')

                    # 3. Upload to Snowflake (Explicit Named Stage inside a specific table folder)
                    stage_path = f"@{STAGE_NAME.lower()}/{table_name.lower()}"
                    cur.execute(f"PUT file://{safe_path} {stage_path} AUTO_COMPRESS=FALSE OVERWRITE=TRUE")
                    
                    temp_table = f"{table_name.lower()}__tmp"
                    cur.execute(f"CREATE OR REPLACE TEMPORARY TABLE {temp_table} (v VARIANT)")

                    # 4. Copy into the temporary table (Using $1 reading for VARIANT)
                    copy_sql = f"""
                    COPY INTO {temp_table}(v)
                    FROM (SELECT $1 FROM {stage_path})
                    FILE_FORMAT=(TYPE=PARQUET)
                    ON_ERROR='SKIP_FILE'
                    """
                    cur.execute(copy_sql)
                    print(f"✅ File loaded into temporary Snowflake table: {minio_key}")

                    merge_sql = get_merge_sql(table_name, temp_table)
                    if merge_sql:
                        try:
                            cur.execute(merge_sql)
                            print(f"🔄 Upsert completed for {table_name} from {minio_key}")
                        except Exception as merge_error:
                            print(f"⚠️ Merge failed for {table_name}: {merge_error}")
                            raise

                    # Clean the stage to avoid accumulating junk in Snowflake
                    cur.execute(f"REMOVE {stage_path} pattern='.*{os.path.basename(minio_key)}.*'")

                    # 5. DELETE from MinIO (Idempotency: ensures it won't be processed again in the next minute)
                    s3.delete_object(Bucket=bucket, Key=minio_key)
                    print(f"🗑️ File deleted from MinIO: {minio_key}")

                except Exception as e:
                    print(f"❌ Error processing {minio_key}: {e}")
                    raise
                finally:
                    # Ensure the local file is deleted from the Airflow machine
                    if os.path.exists(local_file):
                        os.remove(local_file)

        conn.close()

    # Dynamically create tasks for each table
    for table in TABLES:
        process_table(table)

# Instantiate the DAG
dag_instance = banking_ingestion_dag()