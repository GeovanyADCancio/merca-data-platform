# Databricks notebook source
# MAGIC %md
# MAGIC # Configuracao - ecommerce_rastreamento
# MAGIC Define credenciais, caminhos ADLS e helpers usados pelas camadas bronze, silver e gold.

# COMMAND ----------

# MAGIC %pip install python-dotenv

# COMMAND ----------

import os
import json
from datetime import datetime, timezone
from dotenv import load_dotenv

load_dotenv(".env")

container_raw = os.getenv("CONTAINER_RAW", "raw")
storage_account_name = os.getenv("STORAGE_ACCOUNT_NAME", "internshipdatalake")

client_id = os.getenv("ADLS_CLIENT_ID")
tenant_id = os.getenv("ADLS_TENANT_ID")
client_secret = os.getenv("ADLS_CLIENT_SECRET")

raw_base_path = f"abfss://{container_raw}@{storage_account_name}.dfs.core.windows.net/"
raw_input_path = raw_base_path + "real-time-data/"

bronze_path = raw_base_path + "squad2/bronze/ecommerce_rastreamento"
silver_path = raw_base_path + "squad2/silver/ecommerce_rastreamento"
gold_path = raw_base_path + "squad2/gold/ecommerce_rastreamento"
quarantine_path = raw_base_path + "squad2/quarantine/ecommerce_rastreamento_entregas"
control_base_path = raw_base_path + "control"

database_bronze = "bronze"
database_silver = "silver"
database_gold = "gold"
table_name = "ecommerce_rastreamento"
business_table_name = "ecommerce_rastreamento_entregas"
pedidos_silver_table = "silver.ecommerce_pedidos"
sla_entrega_dias = 7

required_tracking_columns = [
    "id_rastreamento",
    "id_pedido_ecommerce",
    "codigo_rastreio",
    "id_transportadora",
    "status_entrega",
    "dt_evento",
]

status_entrega_permitidos = [
    "aguardando coleta",
    "coletado",
    "em transporte",
    "em rota de entrega",
    "saiu para entrega",
    "entregue",
    "falha na entrega",
    "devolvido",
    "cancelado",
]

# COMMAND ----------

def configurar_adls_oauth():
    required = {
        "ADLS_CLIENT_ID": client_id,
        "ADLS_TENANT_ID": tenant_id,
        "ADLS_CLIENT_SECRET": client_secret,
        "STORAGE_ACCOUNT_NAME": storage_account_name,
    }
    missing = [name for name, value in required.items() if not value]
    if missing:
        raise ValueError(f"Variaveis ausentes: {', '.join(missing)}")

    account = f"{storage_account_name}.dfs.core.windows.net"
    spark.conf.set(f"fs.azure.account.auth.type.{account}", "OAuth")
    spark.conf.set(
        f"fs.azure.account.oauth.provider.type.{account}",
        "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
    )
    spark.conf.set(f"fs.azure.account.oauth2.client.id.{account}", client_id)
    spark.conf.set(f"fs.azure.account.oauth2.client.secret.{account}", client_secret)
    spark.conf.set(
        f"fs.azure.account.oauth2.client.endpoint.{account}",
        f"https://login.microsoftonline.com/{tenant_id}/oauth2/token",
    )


def criar_database(database_name: str):
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {database_name}")


def checkpoint_path(camada: str, tabela: str) -> str:
    return f"{control_base_path}/{camada}/{tabela}/checkpoint.json"


def carregar_checkpoint(camada: str, tabela: str) -> dict:
    path = checkpoint_path(camada, tabela)
    try:
        return json.loads(dbutils.fs.head(path))
    except Exception:
        return {
            "camada": camada,
            "tabela": tabela,
            "arquivos_processados": [],
            "ultima_execucao": None,
        }


def salvar_checkpoint(camada: str, tabela: str, checkpoint: dict):
    checkpoint["camada"] = camada
    checkpoint["tabela"] = tabela
    checkpoint["ultima_execucao"] = datetime.now(timezone.utc).isoformat()
    path = checkpoint_path(camada, tabela)
    dbutils.fs.mkdirs(path.rsplit("/", 1)[0])
    dbutils.fs.put(path, json.dumps(checkpoint, indent=2, ensure_ascii=False), overwrite=True)
    print(f"Checkpoint salvo em: {path}")


configurar_adls_oauth()
for db in [database_bronze, database_silver, database_gold]:
    criar_database(db)

print("raw_input_path:", raw_input_path)
print("bronze_path:", bronze_path)
print("silver_path:", silver_path)
print("gold_path:", gold_path)
print("quarantine_path:", quarantine_path)
print("control_base_path:", control_base_path)

