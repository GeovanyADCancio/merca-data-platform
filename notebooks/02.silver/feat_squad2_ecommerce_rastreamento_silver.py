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
# MAGIC # Silver - ecommerce_rastreamento
# MAGIC
# MAGIC Este notebook aplica validações e padronizações nos dados de rastreamento.
# MAGIC
# MAGIC Regras implementadas:
# MAGIC - Validar schema obrigatório da tabela.
# MAGIC - Validar status_entrega conforme fluxo logístico permitido.
# MAGIC - Remover ou separar registros com dt_evento nula ou futura.
# MAGIC - Deduplicar registros por id_rastreamento.
# MAGIC - Validar id_pedido_ecommerce com a base de pedidos quando disponível.
# MAGIC - Enviar registros inválidos para quarentena.

# COMMAND ----------

# MAGIC %pip install azure-identity azure-storage-file-datalake deltalake pyarrow pandas

# COMMAND ----------

# MAGIC %run /Workspace/Users/kalitamariano01@gmail.com/merca-data-platform/notebooks/config/feat_squad2_00_setup_config_teste

# COMMAND ----------

from azure.identity import ClientSecretCredential
from azure.storage.filedatalake import DataLakeServiceClient
from deltalake import DeltaTable
from deltalake.writer import write_deltalake
from datetime import datetime, timezone
import json
import pandas as pd
import pyarrow as pa
import io
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

container_squad = "squad2"

file_system_client_squad = service_client.get_file_system_client(
    file_system=container_squad
)

storage_options = {
    "AZURE_STORAGE_ACCOUNT_NAME": storage_account_name,
    "AZURE_CLIENT_ID": client_id,
    "AZURE_TENANT_ID": tenant_id,
    "AZURE_CLIENT_SECRET": client_secret,
}

bronze_delta_path = f"az://{container_squad}/bronze/ecommerce_rastreamento"
silver_delta_path = f"az://{container_squad}/silver/ecommerce_rastreamento"
quarantine_delta_path = f"az://{container_squad}/quarantine/ecommerce_rastreamento_entregas"
checkpoint_adls_path = f"control/silver/{table_name}/checkpoint.json"

print("bronze_delta_path:", bronze_delta_path)
print("silver_delta_path:", silver_delta_path)
print("quarantine_delta_path:", quarantine_delta_path)
print("checkpoint_adls_path:", checkpoint_adls_path)

# COMMAND ----------

def carregar_checkpoint_silver() -> dict:
    try:
        file_client = file_system_client_squad.get_file_client(checkpoint_adls_path)
        conteudo = file_client.download_file().readall().decode("utf-8")
        return json.loads(conteudo)
    except Exception:
        return {
            "camada": "silver",
            "tabela": table_name,
            "ultima_execucao": None,
        }


def salvar_checkpoint_silver(checkpoint: dict):
    checkpoint["camada"] = "silver"
    checkpoint["tabela"] = table_name
    checkpoint["ultima_execucao"] = datetime.now(timezone.utc).isoformat()

    directory_client = file_system_client_squad.get_directory_client(f"control/silver/{table_name}")
    try:
        directory_client.create_directory()
    except Exception:
        pass

    file_client = file_system_client_squad.get_file_client(checkpoint_adls_path)
    file_client.upload_data(
        json.dumps(checkpoint, indent=2, ensure_ascii=False),
        overwrite=True,
    )
print(f"Checkpoint salvo em: az://{container_squad}/{checkpoint_adls_path}")


def escrever_delta(path: str, df: pd.DataFrame, mode: str = "overwrite"):
    tabela_arrow = pa.Table.from_pandas(df, preserve_index=False)
    write_deltalake(
        path,
        tabela_arrow,
        mode=mode,
        storage_options=storage_options,
    )

# COMMAND ----------

arquivos_bronze_delta = []

paths = file_system_client_squad.get_paths(
    path="bronze/ecommerce_rastreamento",
    recursive=True

)

for path in paths:
    if (
        not path.is_directory
        and path.name.endswith(".parquet")
        and "_delta_log" not in path.name
    ):
        arquivos_bronze_delta.append(path.name)

print("Arquivos parquet da Bronze Delta:", len(arquivos_bronze_delta))

lista_bronze = []

for arquivo in arquivos_bronze_delta:
    file_client = file_system_client_squad.get_file_client(arquivo)
    bytes_file = file_client.download_file().readall()

    table = pq.read_table(io.BytesIO(bytes_file))
    pdf = table.to_pandas()
    lista_bronze.append(pdf)

df_bronze = pd.concat(lista_bronze, ignore_index=True)

print("Registros Bronze:", len(df_bronze))
display(spark.createDataFrame(df_bronze.head(20)))

# COMMAND ----------

df = df_bronze.copy()

df_silver_pre = pd.DataFrame({
    "id_rastreamento": pd.to_numeric(df["id_rastreamento"], errors="coerce").astype("Int64"),
    "id_pedido_ecommerce": pd.to_numeric(df["id_pedido_ecommerce"], errors="coerce").astype("Int64"),
    "codigo_rastreio": df["codigo_rastreio"].astype("string").str.strip(),
    "id_transportadora": pd.to_numeric(df["id_transportadora"], errors="coerce").astype("Int64"),
    "status_entrega": df["status_entrega"].astype("string").str.strip(),
    "dt_evento": pd.to_datetime(df["dt_evento"], errors="coerce"),
    "observacao": df["observacao"].astype("string").str.strip() if "observacao" in df.columns else pd.Series([pd.NA] * len(df), dtype="string"),
    "bronze_source_file": df["bronze_source_file"].astype("string"),
    "bronze_ingested_at": pd.to_datetime(df["bronze_ingested_at"], errors="coerce"),
})

df_silver_pre["status_entrega_normalizado"] = df_silver_pre["status_entrega"].str.lower().str.strip()

agora = pd.Timestamp.utcnow().tz_localize(None)

df_quarentena_status = df_silver_pre[
    ~df_silver_pre["status_entrega_normalizado"].isin(status_entrega_permitidos)
].copy()
df_quarentena_status["motivo_quarentena"] = "status_entrega fora do fluxo logistico permitido"

df_quarentena_data = df_silver_pre[
    df_silver_pre["dt_evento"].isna() | (df_silver_pre["dt_evento"] > agora)
].copy()
df_quarentena_data["motivo_quarentena"] = "dt_evento nula ou futura"

df_silver_valida = df_silver_pre[
    df_silver_pre["id_rastreamento"].notna()
    & df_silver_pre["id_pedido_ecommerce"].notna()
    & df_silver_pre["dt_evento"].notna()
    & (df_silver_pre["dt_evento"] <= agora)
    & df_silver_pre["status_entrega_normalizado"].isin(status_entrega_permitidos)
].copy()

print("Pre-validos:", len(df_silver_valida))
print("Quarentena status:", len(df_quarentena_status))
print("Quarentena data:", len(df_quarentena_data))

# COMMAND ----------

# Regra 5: validar pedido pai na Silver de pedidos, se a tabela existir.
# Se a tabela de pedidos ainda nao existir no ambiente, a validacao fica registrada como pendente
# para nao bloquear as outras regras da Silver de rastreamento.
df_quarentena_pedido_orfao = pd.DataFrame(columns=list(df_silver_valida.columns) + ["motivo_quarentena"])
validacao_pedidos_status = "pendente: tabela silver.ecommerce_pedidos nao encontrada"

try:
    if spark.catalog.tableExists(pedidos_silver_table):
        ids_pedidos = (
            spark.table(pedidos_silver_table)
            .select("id_pedido_ecommerce")
            .distinct()
            .toPandas()["id_pedido_ecommerce"]
        )
        ids_pedidos = set(pd.to_numeric(ids_pedidos, errors="coerce").dropna().astype("int64"))

        mask_pedido_existente = df_silver_valida["id_pedido_ecommerce"].astype("int64").isin(ids_pedidos)
        df_quarentena_pedido_orfao = df_silver_valida[~mask_pedido_existente].copy()
        df_quarentena_pedido_orfao["motivo_quarentena"] = "id_pedido_ecommerce sem correspondencia na Silver de pedidos"
        df_silver_valida = df_silver_valida[mask_pedido_existente].copy()
        validacao_pedidos_status = "executada contra silver.ecommerce_pedidos"
except Exception as erro_validacao_pedidos:
    validacao_pedidos_status = f"pendente por erro ao consultar pedidos: {erro_validacao_pedidos}"

print("Validacao pedidos:", validacao_pedidos_status)
print("Quarentena pedido orfao:", len(df_quarentena_pedido_orfao))

# COMMAND ----------

df_silver = (
    df_silver_valida
    .sort_values(["id_rastreamento", "dt_evento", "bronze_ingested_at"], ascending=[True, False, False])
    .drop_duplicates(subset=["id_rastreamento"], keep="first")
    .drop(columns=["status_entrega_normalizado"])
    .copy()
)

df_silver["silver_updated_at"] = agora

df_quarentena = pd.concat(
    [df_quarentena_status, df_quarentena_data, df_quarentena_pedido_orfao],
    ignore_index=True,
)

if len(df_quarentena) > 0:
    df_quarentena = df_quarentena.drop_duplicates(subset=["id_rastreamento", "motivo_quarentena"])
    df_quarentena["silver_updated_at"] = agora
    df_quarentena = df_quarentena.drop(columns=["status_entrega_normalizado"], errors="ignore")

print("Registros Silver:", len(df_silver))
print("Registros Quarentena:", len(df_quarentena))

display(spark.createDataFrame(df_silver.head(20)))

# COMMAND ----------

escrever_delta(silver_delta_path, df_silver, mode="overwrite")
print("Silver salva com sucesso:", silver_delta_path)

if len(df_quarentena) > 0:
    escrever_delta(quarantine_delta_path, df_quarentena, mode="overwrite")
    print("Quarentena salva com sucesso:", quarantine_delta_path)
else:
    print("Sem registros de quarentena para gravar.")

# COMMAND ----------

checkpoint_silver = carregar_checkpoint_silver()
checkpoint_silver["origem"] = bronze_delta_path
checkpoint_silver["destino"] = silver_delta_path
checkpoint_silver["quarentena"] = quarantine_delta_path
checkpoint_silver["registros_processados"] = int(len(df_silver))
checkpoint_silver["registros_quarentena"] = int(len(df_quarentena))
checkpoint_silver["regra_deduplicacao"] = "id_rastreamento ordenado por dt_evento e bronze_ingested_at desc"
checkpoint_silver["validacao_pedidos"] = validacao_pedidos_status
checkpoint_silver["validacoes"] = [
    "schema completo validado na bronze",
    "status_entrega dentro do fluxo logistico permitido",
    "dt_evento nao nula e nao futura",
    "id_rastreamento deduplicado",
    "id_pedido_ecommerce validado quando silver.ecommerce_pedidos existe",
]

salvar_checkpoint_silver(checkpoint_silver)
print("Checkpoint Silver salvo com sucesso.")

# COMMAND ----------

file_client = file_system_client_squad.get_file_client(
    "control/silver/ecommerce_rastreamento/checkpoint.json"
)
print(file_client.download_file().readall().decode("utf-8"))
