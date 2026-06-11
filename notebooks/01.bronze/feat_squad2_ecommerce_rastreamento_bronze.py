# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "2"
# dependencies = [
#   "azure-identity",
#   "azure-storage-file-datalake",
#   "deltalake",
#   "pyarrow",
#   "pandas",
#   "python-dotenv",
# ]
# ///
# MAGIC %md
# MAGIC ### Ingestão Bronze — ecommerce_rastreamento
# MAGIC
# MAGIC **Objetivo:** ler micro-lotes `.parquet` do container `raw`, adicionar colunas de auditoria e salvar os dados em formato Delta na camada Bronze do container da Squad 2.
# MAGIC
# MAGIC **Regras atendidas:**
# MAGIC
# MAGIC - Leitura dos arquivos em `raw/real-time-data/`;
# MAGIC - Adição da coluna `bronze_ingested_at`;
# MAGIC - Adição da coluna `bronze_source_file`;
# MAGIC - Preservação das colunas de negócio, sem transformação na Bronze;
# MAGIC - Escrita dos dados em formato Delta;
# MAGIC - Particionamento por ano, mês, dia e hora de ingestão;
# MAGIC - Controle de arquivos processados via checkpoint JSON em `control/bronze/ecommerce_rastreamento/checkpoint.json`.

# COMMAND ----------

# MAGIC %pip install azure-identity azure-storage-file-datalake deltalake pyarrow pandas

# COMMAND ----------

# MAGIC %run /Workspace/Users/kalitamariano01@gmail.com/merca-data-platform/notebooks/config/feat_squad2_00_setup_config_teste

# COMMAND ----------

from azure.identity import ClientSecretCredential
from azure.storage.filedatalake import DataLakeServiceClient
from deltalake.writer import write_deltalake
from deltalake import DeltaTable
from datetime import datetime, timezone
import io
import json
import pandas as pd
import pyarrow.parquet as pq

# COMMAND ----------

credential = ClientSecretCredential(
    tenant_id=tenant_id,
    client_id=client_id,
    client_secret=client_secret,
)

service_client = DataLakeServiceClient(
    account_url=f"https://{storage_account_name}.dfs.core.windows.net",
    credential=credential,
)

file_system_client = service_client.get_file_system_client(file_system=container_raw)

storage_options = {
    "AZURE_STORAGE_ACCOUNT_NAME": storage_account_name,
    "AZURE_CLIENT_ID": client_id,
    "AZURE_TENANT_ID": tenant_id,
    "AZURE_CLIENT_SECRET": client_secret,
}

bronze_delta_path = f"az://{container_raw}/squad2/bronze/ecommerce_rastreamento"
checkpoint_adls_path = f"control/bronze/{table_name}/checkpoint.json"

print("raw_input_dir:", raw_input_dir)
print("bronze_delta_path:", bronze_delta_path)
print("checkpoint_adls_path:", checkpoint_adls_path)

# COMMAND ----------

def carregar_checkpoint_bronze() -> dict:
    try:
        file_client = file_system_client.get_file_client(checkpoint_adls_path)
        conteudo = file_client.download_file().readall().decode("utf-8")
        return json.loads(conteudo)
    except Exception:
        return {
            "camada": "bronze",
            "tabela": table_name,
            "arquivos_processados": [],
            "ultima_execucao": None,
        }


def salvar_checkpoint_bronze(checkpoint: dict):
    checkpoint["camada"] = "bronze"
    checkpoint["tabela"] = table_name
    checkpoint["ultima_execucao"] = datetime.now(timezone.utc).isoformat()

    directory_client = file_system_client.get_directory_client(f"control/bronze/{table_name}")
    try:
        directory_client.create_directory()
    except Exception:
        pass

    file_client = file_system_client.get_file_client(checkpoint_adls_path)
    file_client.upload_data(
        json.dumps(checkpoint, indent=2, ensure_ascii=False),
        overwrite=True,
    )
    print(f"Checkpoint salvo em: az://{container_raw}/{checkpoint_adls_path}")


def ler_parquet_adls(caminho_arquivo: str) -> pd.DataFrame:
    file_client = file_system_client.get_file_client(caminho_arquivo)
    bytes_file = file_client.download_file().readall()
    table = pq.read_table(io.BytesIO(bytes_file))
    df = table.to_pandas()

    # Evita erro do Spark/Delta com timestamp Parquet em nanossegundos.
    if "dt_evento" in df.columns:
        df["dt_evento"] = df["dt_evento"].astype(str)

    return df


def adicionar_metadados_bronze(df: pd.DataFrame, caminho_arquivo: str) -> pd.DataFrame:
    agora = datetime.now(timezone.utc).replace(tzinfo=None)
    df["bronze_ingested_at"] = agora
    df["bronze_source_file"] = caminho_arquivo
    df["ano_ingestao"] = agora.year
    df["mes_ingestao"] = agora.month
    df["dia_ingestao"] = agora.day
    df["hora_ingestao"] = agora.hour
    return df

# COMMAND ----------

arquivos = []
paths = file_system_client.get_paths(path=raw_input_dir, recursive=True)

for path in paths:
    if (
        not path.is_directory
        and "ecommerce_rastreamento" in path.name
        and path.name.endswith(".parquet")
    ):
        arquivos.append(path.name)

checkpoint_bronze = carregar_checkpoint_bronze()
arquivos_processados = set(checkpoint_bronze.get("arquivos_processados", []))
arquivos_novos = [arquivo for arquivo in arquivos if arquivo not in arquivos_processados]

print(f"Arquivos encontrados: {len(arquivos)}")
print(f"Arquivos ja processados: {len(arquivos_processados)}")
print(f"Arquivos novos: {len(arquivos_novos)}")

for arquivo in arquivos_novos[:5]:
    print("Novo:", arquivo)

if not arquivos:
    raise ValueError(f"Nenhum arquivo ecommerce_rastreamento encontrado em {raw_input_dir}")

if not arquivos_novos:
    dbutils.notebook.exit("Nenhum arquivo novo para processar na bronze.")

# COMMAND ----------

lista_dfs = []

for arquivo in arquivos_novos:
    print(f"Lendo arquivo: {arquivo}")
    df_pandas = ler_parquet_adls(arquivo)
    df_bronze = adicionar_metadados_bronze(df_pandas, arquivo)
    lista_dfs.append(df_bronze)

df_bronze_final = pd.concat(lista_dfs, ignore_index=True)

colunas_ausentes = [coluna for coluna in required_tracking_columns if coluna not in df_bronze_final.columns]
if colunas_ausentes:
    raise ValueError(f"Schema incompleto no micro-lote. Colunas ausentes: {colunas_ausentes}")

print("Total de registros:", len(df_bronze_final))
display(spark.createDataFrame(df_bronze_final).limit(20))

# COMMAND ----------

import pyarrow as pa

tabela_arrow = pa.Table.from_pandas(df_bronze_final, preserve_index=False)

write_deltalake(
    bronze_delta_path,
    tabela_arrow,
    mode="append",
    partition_by=[
        "ano_ingestao",
        "mes_ingestao",
        "dia_ingestao",
        "hora_ingestao",
    ],
    storage_options=storage_options,
)

print("Carga Bronze salva com sucesso.")
print("Delta:", bronze_delta_path)

# COMMAND ----------

dt = DeltaTable(bronze_delta_path, storage_options=storage_options)
print("Versao Delta:", dt.version())

checkpoint_bronze["arquivos_processados"] = sorted(arquivos_processados.union(arquivos_novos))
checkpoint_bronze["total_arquivos_processados"] = len(checkpoint_bronze["arquivos_processados"])
checkpoint_bronze["ultimo_lote"] = arquivos_novos
checkpoint_bronze["destino"] = bronze_delta_path
salvar_checkpoint_bronze(checkpoint_bronze)

print("Checkpoint Bronze salvo com sucesso.")
