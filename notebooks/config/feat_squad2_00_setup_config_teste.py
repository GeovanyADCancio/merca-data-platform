# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "2"
# dependencies = [
#   "python-dotenv",
# ]
# ///
# MAGIC %md
# MAGIC ###Configuracao - ecommerce_rastreamento
# MAGIC Define credenciais, caminhos ADLS e helpers usados pelas camadas bronze, silver e gold.

# COMMAND ----------

# MAGIC %pip install python-dotenv

# COMMAND ----------

import os
import json
from datetime import datetime, timezone
from dotenv import load_dotenv

# Opcional: se voce usa Databricks Secrets, informe o scope no widget.
try:
    dbutils.widgets.text("adls_secret_scope", "")
    dbutils.widgets.text("env_path", "")
    adls_secret_scope = dbutils.widgets.get("adls_secret_scope").strip()
    env_path_widget = dbutils.widgets.get("env_path").strip()
except Exception:
    adls_secret_scope = ""
    env_path_widget = ""

env_paths = [
    env_path_widget,
    ".env",
    "/Workspace/Users/kalitamariano01@gmail.com/merca-data-platform/.env",
    "/Workspace/Users/kalitamariano01@gmail.com/.env",
]

env_carregado = None
for env_path in [path for path in env_paths if path]:
    if load_dotenv(env_path, override=True):
        env_carregado = env_path
        break


def get_config_value(env_name: str, secret_key: str | None = None):
    value = os.getenv(env_name)
    if value:
        return value

    if adls_secret_scope and secret_key:
        try:
            return dbutils.secrets.get(scope=adls_secret_scope, key=secret_key)
        except Exception:
            return None

    return None


container_raw = "raw"
raw_input_dir = "real-time-data"
storage_account_name = get_config_value("STORAGE_ACCOUNT_NAME", "storage-account-name") or "internshipdatalake"

client_id = get_config_value("ADLS_CLIENT_ID", "adls-client-id")
tenant_id = get_config_value("ADLS_TENANT_ID", "adls-tenant-id")
client_secret = get_config_value("ADLS_CLIENT_SECRET", "adls-client-secret")

raw_base_path = f"abfss://{container_raw}@{storage_account_name}.dfs.core.windows.net/"
raw_input_path = raw_base_path if not raw_input_dir else raw_base_path + raw_input_dir + "/"

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
        print(
            "Aviso: credenciais OAuth do ADLS nao encontradas: "
            + ", ".join(missing)
        )
        print(
            "A config vai continuar sem setar OAuth. "
            "Isso funciona se o cluster/Unity Catalog ja tiver permissao no storage."
        )
        return False

    account = f"{storage_account_name}.dfs.core.windows.net"
    configs = {
        f"fs.azure.account.auth.type.{account}": "OAuth",
        f"fs.azure.account.oauth.provider.type.{account}": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
        f"fs.azure.account.oauth2.client.id.{account}": client_id,
        f"fs.azure.account.oauth2.client.secret.{account}": client_secret,
        f"fs.azure.account.oauth2.client.endpoint.{account}": f"https://login.microsoftonline.com/{tenant_id}/oauth2/token",
    }

    try:
        for key, value in configs.items():
            spark.conf.set(key, value)
        return True
    except Exception as erro_spark_conf:
        print("Aviso: o ambiente nao permitiu configurar OAuth via spark.conf.set.")
        print(f"Detalhe: {erro_spark_conf}")
        print(
            "Vou tentar configurar via Hadoop Configuration. "
            "Se o ambiente tambem bloquear, a config vai seguir sem quebrar."
        )

    try:
        hadoop_conf = spark.sparkContext._jsc.hadoopConfiguration()
        for key, value in configs.items():
            hadoop_conf.set(key, value)
        return True
    except Exception as erro_hadoop_conf:
        print("Aviso: o ambiente tambem bloqueou Hadoop Configuration.")
        print(f"Detalhe: {erro_hadoop_conf}")
        print(
            "A config vai continuar. Se a leitura do ADLS falhar depois, "
            "sera necessario usar permissao do cluster/Unity Catalog ou Databricks Secrets."
        )
        return False


def criar_database(database_name: str):
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {database_name}")


def adls_oauth_options() -> dict:
    account = f"{storage_account_name}.dfs.core.windows.net"
    if not all([client_id, tenant_id, client_secret, storage_account_name]):
        return {}

    return {
        f"fs.azure.account.auth.type.{account}": "OAuth",
        f"fs.azure.account.oauth.provider.type.{account}": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
        f"fs.azure.account.oauth2.client.id.{account}": client_id,
        f"fs.azure.account.oauth2.client.secret.{account}": client_secret,
        f"fs.azure.account.oauth2.client.endpoint.{account}": f"https://login.microsoftonline.com/{tenant_id}/oauth2/token",
    }


def aplicar_options_adls(reader):
    for key, value in adls_oauth_options().items():
        reader = reader.option(key, value)
    return reader


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


adls_oauth_configurado = configurar_adls_oauth()
for db in [database_bronze, database_silver, database_gold]:
    criar_database(db)

print("raw_input_path:", raw_input_path)
print("bronze_path:", bronze_path)
print("silver_path:", silver_path)
print("gold_path:", gold_path)
print("quarantine_path:", quarantine_path)
print("control_base_path:", control_base_path)
print("adls_oauth_configurado:", adls_oauth_configurado)
print("env_carregado:", env_carregado)
