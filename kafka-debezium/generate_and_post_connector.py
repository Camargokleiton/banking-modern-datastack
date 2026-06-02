import os
import json
import requests
import time
from dotenv import load_dotenv

# -----------------------------
# Load environment variables
# -----------------------------
load_dotenv()

# -----------------------------
# Build connector JSON in memory
# -----------------------------
connector_name = "postgres-connector"

connector_config = {
    "name": connector_name,
    "config": {
        "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
        "database.hostname": os.getenv("POSTGRES_HOST"),
        "database.port": os.getenv("POSTGRES_PORT"),
        "database.user": os.getenv("POSTGRES_USER"),
        "database.password": os.getenv("POSTGRES_PASSWORD"),
        "database.dbname": os.getenv("POSTGRES_DB"),
        "topic.prefix": "banking_server",
        "table.include.list": "public.customers,public.accounts,public.transactions",
        "plugin.name": "pgoutput",
        "slot.name": "banking_slot",
        "publication.autocreate.mode": "filtered",
        "tombstones.on.delete": "false",
        "decimal.handling.mode": "double",
    },
}

# -----------------------------
# Send request to Debezium Connect
# -----------------------------
url = "http://localhost:8083/connectors"
headers = {"Content-Type": "application/json"}

print("Enviando configuração para o Kafka Connect...")
response = requests.post(url, headers=headers, data=json.dumps(connector_config))

# -----------------------------
# Debug/Output & Recreate Logic
# -----------------------------
if response.status_code == 201:
    print("✅ Connector created successfully!")
    
elif response.status_code == 409:
    print("⚠️ Connector already exists. deleting and re-creating...")
    
    # 1. Apaga o conector existente
    delete_url = f"{url}/{connector_name}"
    requests.delete(delete_url)
    

    time.sleep(2)
    
    # 3. Tenta criar de novo
    retry_response = requests.post(url, headers=headers, data=json.dumps(connector_config))
    
    if retry_response.status_code == 201:
        print("✅ Connector recriado com sucesso!")
    else:
        print(f"❌ Falha ao recriar o conector ({retry_response.status_code}): {retry_response.text}")
        
else:
    print(f"❌ Failed to create connector ({response.status_code}): {response.text}")