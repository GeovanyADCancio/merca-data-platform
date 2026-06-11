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
# ]
# ///
# MAGIC %md
# MAGIC # Gold - ecommerce_rastreamento
# MAGIC Cria KPIs e alertas de rastreamento lendo a Silver com Azure SDK/PyArrow e gravando Delta com `deltalake`.

# COMMAND ----------

# MAGIC %pip install azure-identity azure-storage-file-datalake deltalake pyarrow pandas

# COMMAND ----------

# MAGIC %run /Workspace/Users/kalitamariano01@gmail.com/merca-data-platform/notebooks/config/feat_squad2_00_setup_config_teste

# COMMAND ----------

from azure.identity import ClientSecretCredential
from azure.storage.filedatalake import DataLakeServiceClient
from deltalake.writer import write_deltalake
from datetime import datetime, timezone
import io
import json
import pandas as pd
import pyarrow as pa
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

silver_adls_dir = "squad2/silver/ecommerce_rastreamento"
gold_base_path = f"az://{container_raw}/squad2/gold/ecommerce_rastreamento"

print("silver_adls_dir:", silver_adls_dir)
print("gold_base_path:", gold_base_path)

# COMMAND ----------

def ler_parquets_adls(diretorio: str) -> pd.DataFrame:
    arquivos = []
    paths = file_system_client.get_paths(path=diretorio, recursive=True)

    for path in paths:
        if (
            not path.is_directory
            and path.name.endswith(".parquet")
            and "_delta_log" not in path.name
        ):
            arquivos.append(path.name)

    print(f"Arquivos parquet encontrados em {diretorio}: {len(arquivos)}")

    if not arquivos:
        raise ValueError(f"Nenhum parquet encontrado em {diretorio}")

    lista = []
    for arquivo in arquivos:
        file_client = file_system_client.get_file_client(arquivo)
        bytes_file = file_client.download_file().readall()
        table = pq.read_table(io.BytesIO(bytes_file))
        lista.append(table.to_pandas())

    return pd.concat(lista, ignore_index=True)


def escrever_delta(path: str, df: pd.DataFrame, mode: str = "overwrite"):
    tabela_arrow = pa.Table.from_pandas(df, preserve_index=False)
    write_deltalake(
        path,
        tabela_arrow,
        mode=mode,
        storage_options=storage_options,
    )
    print(f"Delta salvo em: {path} | registros: {len(df)}")


def salvar_checkpoint_gold(nome_tabela: str, destino: str, registros: int, detalhes: dict | None = None):
    checkpoint = {
        "camada": "gold",
        "tabela": nome_tabela,
        "origem": f"az://{container_raw}/{silver_adls_dir}",
        "destino": destino,
        "registros_processados": int(registros),
        "ultima_execucao": datetime.now(timezone.utc).isoformat(),
    }
    if detalhes:
        checkpoint.update(detalhes)

    checkpoint_path = f"control/gold/{nome_tabela}/checkpoint.json"
    directory_client = file_system_client.get_directory_client(f"control/gold/{nome_tabela}")
    try:
        directory_client.create_directory()
    except Exception:
        pass

    file_client = file_system_client.get_file_client(checkpoint_path)
    file_client.upload_data(
        json.dumps(checkpoint, indent=2, ensure_ascii=False),
        overwrite=True,
    )
    print(f"Checkpoint salvo em: az://{container_raw}/{checkpoint_path}")

# COMMAND ----------

df_silver = ler_parquets_adls(silver_adls_dir)

df_silver["dt_evento"] = pd.to_datetime(df_silver["dt_evento"], errors="coerce")
df_silver["bronze_ingested_at"] = pd.to_datetime(df_silver["bronze_ingested_at"], errors="coerce")
df_silver["status_entrega_normalizado"] = df_silver["status_entrega"].astype("string").str.lower().str.strip()

agora = pd.Timestamp.utcnow().tz_localize(None)

print("Registros Silver:", len(df_silver))
display(spark.createDataFrame(df_silver.head(20)))

# COMMAND ----------

df_gold_status_diario = (
    df_silver.assign(data_evento=df_silver["dt_evento"].dt.date)
    .groupby(["data_evento", "status_entrega"], dropna=False)
    .agg(
        qtd_eventos=("id_rastreamento", "count"),
        qtd_pedidos=("id_pedido_ecommerce", "nunique"),
        qtd_codigos_rastreio=("codigo_rastreio", "nunique"),
    )
    .reset_index()
)
df_gold_status_diario["gold_updated_at"] = agora

status_diario_path = gold_base_path + "/status_diario"
escrever_delta(status_diario_path, df_gold_status_diario)
salvar_checkpoint_gold(
    f"{table_name}_status_diario",
    status_diario_path,
    len(df_gold_status_diario),
    {"granularidade": "data_evento, status_entrega"},
)

display(spark.createDataFrame(df_gold_status_diario))

# COMMAND ----------

df_ultimo_evento = (
    df_silver.sort_values(["id_pedido_ecommerce", "dt_evento"], ascending=[True, False])
    .drop_duplicates(subset=["id_pedido_ecommerce"], keep="first")
    [["id_pedido_ecommerce", "dt_evento", "status_entrega", "codigo_rastreio"]]
    .rename(
        columns={
            "dt_evento": "ultima_data_evento",
            "status_entrega": "ultimo_status_entrega",
            "codigo_rastreio": "ultimo_codigo_rastreio",
        }
    )
)

df_eventos_por_pedido = (
    df_silver.groupby("id_pedido_ecommerce", dropna=False)
    .size()
    .reset_index(name="qtd_eventos_rastreamento")
)

df_gold_pedido_ultima_posicao = df_ultimo_evento.merge(
    df_eventos_por_pedido,
    on="id_pedido_ecommerce",
    how="left",
)
df_gold_pedido_ultima_posicao["gold_updated_at"] = agora

pedido_ultima_posicao_path = gold_base_path + "/pedido_ultima_posicao"
escrever_delta(pedido_ultima_posicao_path, df_gold_pedido_ultima_posicao)
salvar_checkpoint_gold(
    f"{table_name}_pedido_ultima_posicao",
    pedido_ultima_posicao_path,
    len(df_gold_pedido_ultima_posicao),
    {"granularidade": "id_pedido_ecommerce"},
)

display(spark.createDataFrame(df_gold_pedido_ultima_posicao.head(20)))

# COMMAND ----------

## Regra 6 - KPI: pedidos que entraram em "saiu para entrega" nas ultimas 2 horas
df_gold_saiu_entrega_2h = pd.DataFrame(
    [{
        "qtd_pedidos_saiu_para_entrega_2h": df_silver[
            (df_silver["status_entrega_normalizado"] == "saiu para entrega")
            & (df_silver["dt_evento"] >= agora - pd.Timedelta(hours=2))
        ]["id_pedido_ecommerce"].nunique(),
        "gold_updated_at": agora,
    }]
)

saiu_entrega_2h_path = gold_base_path + "/saiu_para_entrega_2h"
escrever_delta(saiu_entrega_2h_path, df_gold_saiu_entrega_2h)
salvar_checkpoint_gold(f"{table_name}_saiu_para_entrega_2h", saiu_entrega_2h_path, len(df_gold_saiu_entrega_2h))

display(spark.createDataFrame(df_gold_saiu_entrega_2h))

# COMMAND ----------

## Regra 8 - Alerta: mesmo pedido com evento "entregue" mais de uma vez
df_alerta_entrega_duplicada = (
    df_silver[df_silver["status_entrega_normalizado"] == "entregue"]
    .groupby("id_pedido_ecommerce", dropna=False)
    .size()
    .reset_index(name="qtd_eventos_entregue")
)
df_alerta_entrega_duplicada = df_alerta_entrega_duplicada[
    df_alerta_entrega_duplicada["qtd_eventos_entregue"] > 1
].copy()
df_alerta_entrega_duplicada["gold_updated_at"] = agora

alerta_entrega_duplicada_path = gold_base_path + "/alerta_entrega_duplicada"
escrever_delta(alerta_entrega_duplicada_path, df_alerta_entrega_duplicada)
salvar_checkpoint_gold(
    f"{table_name}_alerta_entrega_duplicada",
    alerta_entrega_duplicada_path,
    len(df_alerta_entrega_duplicada),
)

if len(df_alerta_entrega_duplicada) > 0:
    display(spark.createDataFrame(df_alerta_entrega_duplicada))
else:
    print("Sem alertas de entrega duplicada.")

# COMMAND ----------

## Regra 9 - KPI: top 3 transportadoras com mais eventos no micro-lote atual
ultima_ingestao_bronze = df_silver["bronze_ingested_at"].max()
df_micro_lote_atual = df_silver[df_silver["bronze_ingested_at"] == ultima_ingestao_bronze].copy()

df_gold_top3_transportadoras_micro_lote = (
    df_micro_lote_atual.groupby("id_transportadora", dropna=False)
    .size()
    .reset_index(name="qtd_eventos_micro_lote")
    .sort_values("qtd_eventos_micro_lote", ascending=False)
    .head(3)
)
df_gold_top3_transportadoras_micro_lote["gold_updated_at"] = agora

top3_transportadoras_path = gold_base_path + "/top3_transportadoras_micro_lote"
escrever_delta(top3_transportadoras_path, df_gold_top3_transportadoras_micro_lote)
salvar_checkpoint_gold(
    f"{table_name}_top3_transportadoras_micro_lote",
    top3_transportadoras_path,
    len(df_gold_top3_transportadoras_micro_lote),
)

display(spark.createDataFrame(df_gold_top3_transportadoras_micro_lote))

# COMMAND ----------

df_eventos_pedido = (
    df_silver.groupby("id_pedido_ecommerce", dropna=False)
    .agg(
        dt_ultimo_evento=("dt_evento", "max"),
        dt_ultima_coleta=(
            "dt_evento",
            lambda s: s[df_silver.loc[s.index, "status_entrega_normalizado"] == "coletado"].max(),
        ),
    )
    .reset_index()
)
## Regra 10 - Alerta: pedido sem novo evento por mais de 3 dias apos "coletado"
df_alerta_sem_evento_apos_coleta = df_eventos_pedido[
    df_eventos_pedido["dt_ultima_coleta"].notna()
    & (df_eventos_pedido["dt_ultimo_evento"] == df_eventos_pedido["dt_ultima_coleta"])
    & ((agora - df_eventos_pedido["dt_ultimo_evento"]).dt.days > 3)
].copy()

if len(df_alerta_sem_evento_apos_coleta) > 0:
    df_alerta_sem_evento_apos_coleta["dias_sem_evento"] = (
        agora - df_alerta_sem_evento_apos_coleta["dt_ultimo_evento"]
    ).dt.days
else:
    df_alerta_sem_evento_apos_coleta["dias_sem_evento"] = pd.Series(dtype="int64")

df_alerta_sem_evento_apos_coleta["gold_updated_at"] = agora

alerta_sem_evento_path = gold_base_path + "/alerta_sem_evento_apos_coleta"
escrever_delta(alerta_sem_evento_path, df_alerta_sem_evento_apos_coleta)
salvar_checkpoint_gold(
    f"{table_name}_alerta_sem_evento_apos_coleta",
    alerta_sem_evento_path,
    len(df_alerta_sem_evento_apos_coleta),
)

if len(df_alerta_sem_evento_apos_coleta) > 0:
    display(spark.createDataFrame(df_alerta_sem_evento_apos_coleta))
else:
    print("Sem alertas de pedido sem evento apos coleta.")

# COMMAND ----------

def ler_parquets_adls(diretorio: str, client=None) -> pd.DataFrame:
    if client is None:
        client = file_system_client

    arquivos = []
    paths = client.get_paths(path=diretorio, recursive=True)

    for path in paths:
        if (
            not path.is_directory
            and path.name.endswith(".parquet")
            and "_delta_log" not in path.name
        ):
            arquivos.append(path.name)

    print(f"Arquivos parquet encontrados em {diretorio}: {len(arquivos)}")

    if not arquivos:
        raise ValueError(f"Nenhum parquet encontrado em {diretorio}")

    lista = []
    for arquivo in arquivos:
        file_client = client.get_file_client(arquivo)
        bytes_file = file_client.download_file().readall()
        table = pq.read_table(io.BytesIO(bytes_file))
        lista.append(table.to_pandas())

    return pd.concat(lista, ignore_index=True)

# COMMAND ----------

df_pedidos = ler_parquets_adls(
    diretorio=f"silver/{TABELA_PEDIDOS}",
    client=pedidos_file_system_client
)

# COMMAND ----------

# Regra 7 - KPI: percentual de pedidos entregues no prazo
# SLA = 7 dias
# Cruzamento:
# rastreamento.id_pedido_ecommerce = pedidos.id_pedido

try:
    df_pedidos["dt_pedido"] = pd.to_datetime(
        df_pedidos["dt_pedido"],
        errors="coerce"
    )

    df_pedidos["id_pedido"] = pd.to_numeric(
        df_pedidos["id_pedido"],
        errors="coerce"
    )

    df_entregues = df_silver[
        df_silver["status_entrega_normalizado"] == "entregue"
    ].copy()

    df_entregues["id_pedido_ecommerce"] = pd.to_numeric(
        df_entregues["id_pedido_ecommerce"],
        errors="coerce"
    )

    df_sla_base = df_entregues.merge(
        df_pedidos[["id_pedido", "dt_pedido"]],
        left_on="id_pedido_ecommerce",
        right_on="id_pedido",
        how="inner"
    )

    df_sla_base["dias_ate_entrega"] = (
        df_sla_base["dt_evento"] - df_sla_base["dt_pedido"]
    ).dt.days

    qtd_entregues = df_sla_base["id_pedido_ecommerce"].nunique()

    qtd_no_prazo = df_sla_base[
        df_sla_base["dias_ate_entrega"] <= sla_entrega_dias
    ]["id_pedido_ecommerce"].nunique()

    percentual = round(
        (100.0 * qtd_no_prazo / qtd_entregues),
        2
    ) if qtd_entregues else 0.0

    if qtd_entregues == 0:
        status_sla = "calculado_sem_pedidos_entregues"
    else:
        status_sla = "calculado"

except Exception as erro_sla:
    qtd_entregues = 0
    qtd_no_prazo = 0
    percentual = 0.0
    status_sla = f"pendente por erro: {erro_sla}"

df_gold_sla_entrega = pd.DataFrame(
    [{
        "qtd_pedidos_entregues": qtd_entregues,
        "qtd_pedidos_no_prazo": qtd_no_prazo,
        "percentual_entregue_no_prazo": percentual,
        "status_calculo": status_sla,
        "gold_updated_at": agora,
    }]
)

sla_entrega_path = gold_base_path + "/sla_entrega"

escrever_delta(sla_entrega_path, df_gold_sla_entrega)

salvar_checkpoint_gold(
    f"{table_name}_sla_entrega",
    sla_entrega_path,
    len(df_gold_sla_entrega),
    {"status_calculo": status_sla},
)

display(spark.createDataFrame(df_gold_sla_entrega))

print("Gold finalizada com sucesso.")

# COMMAND ----------

print(df_pedidos.columns.tolist())

# COMMAND ----------

df_silver["status_entrega_normalizado"].value_counts()

# COMMAND ----------

df_entregues = df_silver[
    df_silver["status_entrega_normalizado"] == "entregue"
].copy()

print("Entregues no rastreamento:", len(df_entregues))

# COMMAND ----------

ids_rastreamento = set(df_entregues["id_pedido_ecommerce"].dropna().astype(int))
ids_pedidos = set(df_pedidos["id_pedido"].dropna().astype(int))

print("IDs entregues no rastreamento:", len(ids_rastreamento))
print("IDs na tabela pedidos:", len(ids_pedidos))
print("IDs em comum:", len(ids_rastreamento.intersection(ids_pedidos)))
