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
# MAGIC
# MAGIC **Responsável:** Kálita Ribeiro Boni  
# MAGIC **Data da ingestão/processamento:** 18/06/2026  
# MAGIC **Camada:** Gold  
# MAGIC **Tabela:** ecommerce_rastreamento  
# MAGIC
# MAGIC ### Origem e destino
# MAGIC
# MAGIC | Item | Valor |
# MAGIC |---|---|
# MAGIC | **Origem** | `squad2/silver/ecommerce_rastreamento` |
# MAGIC | **Destino** | `squad2/gold/ecommerce_rastreamento` |
# MAGIC | **control** | `squad2/control/gold/{nome_tabela}/checkpoint.json` |
# MAGIC | **Publicação SQL Server** | Schema `squad2` |
# MAGIC
# MAGIC ### Objetivo
# MAGIC
# MAGIC Gerar os indicadores e alertas analíticos da camada Gold a partir dos dados tratados na camada Silver, disponibilizando os resultados no Blob da Squad 2 e no SQL Server para consumo analítico.
# MAGIC
# MAGIC ### Regras/KPIs implementados
# MAGIC
# MAGIC - **Regra 6:** KPI de pedidos que saíram para entrega nas últimas 2 horas.
# MAGIC - **Regra 7:** KPI de percentual de pedidos entregues dentro do SLA.
# MAGIC - **Regra 8:** Alerta de entrega duplicada por pedido.
# MAGIC - **Regra 9:** Top 3 transportadoras do micro-lote.
# MAGIC - **Regra 10:** Alerta de pedido sem evento após coleta.
# MAGIC - **Publicação:** Gravação dos resultados no Blob e no SQL Server..

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

silver_adls_dir = "silver/ecommerce_rastreamento"
gold_base_path = f"az://{container_squad}/gold/ecommerce_rastreamento"

print("silver_adls_dir:", silver_adls_dir)
print("gold_base_path:", gold_base_path)

# COMMAND ----------

def ler_parquets_adls(diretorio: str, client=None) -> pd.DataFrame:
    if client is None:
        client = file_system_client_squad

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
        "origem": f"az://{container_squad}/{silver_adls_dir}",
        "destino": destino,
        "registros_processados": int(registros),
        "ultima_execucao": datetime.now(timezone.utc).isoformat(),
    }
    if detalhes:
        checkpoint.update(detalhes)

    checkpoint_path = f"control/gold/{nome_tabela}/checkpoint.json"
    directory_client = file_system_client_squad.get_directory_client(f"control/gold/{nome_tabela}")
    try:
        directory_client.create_directory()
    except Exception:
        pass

    file_client = file_system_client_squad.get_file_client(checkpoint_path)
    file_client.upload_data(
        json.dumps(checkpoint, indent=2, ensure_ascii=False),
        overwrite=True,
    )
    print(f"Checkpoint salvo em: az://{container_squad}/{checkpoint_path}")

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

SQUAD2_CONTAINER = "squad2"
TABELA_PEDIDOS = "ecommerce_pedidos"

pedidos_file_system_client = service_client.get_file_system_client(
    file_system=SQUAD2_CONTAINER
)

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

# COMMAND ----------

import os
from dotenv import load_dotenv

load_dotenv("/Workspace/Users/kalitamariano01@gmail.com/merca-data-platform/.env", override=True)

sql_host = os.getenv("SQL_HOST")
sql_database = os.getenv("SQL_DATABASE")
sql_username = os.getenv("SQL_USERNAME")
sql_password = os.getenv("SQL_PASSWORD")

print("sql_host:", sql_host)
print("sql_database:", sql_database)
print("sql_username:", sql_username)
print("sql_password carregado:", sql_password is not None)

# COMMAND ----------

# -------------------------------------------------------------------------
# # SINK 2: INGESTÃO NO SQL SERVER COM CRIAÇÃO E ALINHAMENTO AUTOMÁTICO
def gravar_gold_sql_server(df: pd.DataFrame, nome_tabela: str, mode: str = "overwrite"):
    if len(df) == 0:
        print(f"Sem registros para gravar no SQL Server: squad2.{nome_tabela}")
        return

    (
        spark.createDataFrame(df)
        .write
        .format("sqlserver")
        .mode(mode)
        .option("host", sql_host)
        .option("port", "1433")
        .option("database", sql_database)
        .option("user", sql_username)
        .option("password", sql_password)
        .option("dbtable", f"squad2.{nome_tabela}")
        .option("encrypt", "true")
        .option("trustServerCertificate", "false")
        .save()
    )

    print(f"Gold gravada no SQL Server: squad2.{nome_tabela}")

# COMMAND ----------

gravar_gold_sql_server(df_gold_status_diario, f"{table_name}_status_diario")
gravar_gold_sql_server(df_gold_pedido_ultima_posicao, f"{table_name}_pedido_ultima_posicao")
gravar_gold_sql_server(df_gold_saiu_entrega_2h, f"{table_name}_saiu_para_entrega_2h")
gravar_gold_sql_server(df_alerta_entrega_duplicada, f"{table_name}_alerta_entrega_duplicada")
gravar_gold_sql_server(df_gold_top3_transportadoras_micro_lote, f"{table_name}_top3_transportadoras_micro_lote")
gravar_gold_sql_server(df_alerta_sem_evento_apos_coleta, f"{table_name}_alerta_sem_evento_apos_coleta")
gravar_gold_sql_server(df_gold_sla_entrega, f"{table_name}_sla_entrega")

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC
# MAGIC
# MAGIC ##  Insights gráficos da camada Gold
# MAGIC
# MAGIC  Esta seção apresenta gráficos de apoio para análise dos KPIs e alertas gerados na camada Gold, com foco na quantidade de ocorrências por problema conforme as regras da planilha.
# MAGIC
# MAGIC

# COMMAND ----------

import matplotlib.pyplot as plt
from matplotlib.patches import Patch

def exibir_grafico_barras(df: pd.DataFrame, coluna_x: str, coluna_y: str, titulo: str, eixo_x: str, eixo_y: str):
    if df.empty:
        print(f"Sem dados para exibir: {titulo}")
        return

    plt.figure(figsize=(10, 5))
    plt.bar(df[coluna_x].astype(str), df[coluna_y])
    plt.title(titulo)
    plt.xlabel(eixo_x)
    plt.ylabel(eixo_y)
    plt.xticks(rotation=45, ha="right")
    plt.tight_layout()
    plt.show()

# COMMAND ----------

# DBTITLE 1,Consolida a quantidade de ocorrências encontradas nas regras de alerta e validação da camada GoldConsolida a quantidade de ocorrências encontradas nas regras de alerta e validação da camada Gold
def exibir_grafico_barras_insights(df: pd.DataFrame, titulo: str):
    if df.empty:
        print(f"Sem dados para exibir: {titulo}")
        return

    df_plot = df.copy()
    df_plot["codigo"] = df_plot["regra"].str.replace("Regra ", "R", regex=False)

    plt.figure(figsize=(8, 4))

    barras = plt.bar(
        df_plot["codigo"],
        df_plot["quantidade"],
        color=["#4C78A8", "#F58518", "#54A24B"][:len(df_plot)]
    )

    plt.ylim(0, max(df_plot["quantidade"].max() + 1, 1))
    plt.title(titulo)
    plt.xlabel("Regra")
    plt.ylabel("Quantidade")

    for barra, valor in zip(barras, df_plot["quantidade"]):
        plt.text(
            barra.get_x() + barra.get_width() / 2,
            valor + 0.03,
            str(int(valor)),
            ha="center"
        )

    plt.tight_layout()
    plt.show()

    print("Legenda:")
    for _, linha in df_plot.iterrows():
        print(f"{linha['codigo']} - {linha['problema']}")

# COMMAND ----------

# MAGIC %md
# MAGIC Este gráfico apresenta a quantidade de eventos por status de entrega. Todos os status previstos na regra são exibidos, mesmo quando a quantidade encontrada é zero.

# COMMAND ----------

# DBTITLE 1,Este gráfico apresenta a quantidade de eventos por status de entrega. Todos os status previstos na regra são exibidos, mesmo quando a quantidade encontrada é zero.
status_mapa = {
    "aguardando coleta": "Aguard. coleta",
    "saiu para entrega": "Saiu entrega",
    "em transporte": "Transporte",
    "entregue": "Entregue",
    "coletado": "Coletado",
}

df_status_base = (
    df_silver.groupby("status_entrega_normalizado", dropna=False)
    .size()
    .reset_index(name="quantidade")
)

df_status_todos = pd.DataFrame(
    {
        "status_entrega_normalizado": list(status_mapa.keys()),
        "status_curto": list(status_mapa.values()),
    }
)

df_status_plot = df_status_todos.merge(
    df_status_base,
    on="status_entrega_normalizado",
    how="left"
)

df_status_plot["quantidade"] = df_status_plot["quantidade"].fillna(0).astype(int)

cores = ["#4C78A8", "#F58518", "#54A24B", "#E45756", "#72B7B2"]

plt.figure(figsize=(10, 5))

barras = plt.bar(
    df_status_plot["status_curto"],
    df_status_plot["quantidade"],
    color=cores
)

maior_valor = df_status_plot["quantidade"].max()
plt.ylim(0, max(maior_valor + 50, 1))

plt.title("Quantidade de eventos por status de entrega")
plt.xlabel("Status de entrega")
plt.ylabel("Quantidade")
plt.xticks(rotation=0)

for barra, valor in zip(barras, df_status_plot["quantidade"]):
    plt.text(
        barra.get_x() + barra.get_width() / 2,
        valor + max(maior_valor * 0.01, 0.05),
        str(int(valor)),
        ha="center",
        va="bottom",
        fontsize=9,
    )

legenda = [
    Patch(
        facecolor=cor,
        label=f"{curto} = {completo}"
    )
    for cor, curto, completo in zip(
        cores,
        df_status_plot["status_curto"],
        df_status_plot["status_entrega_normalizado"],
    )
]

plt.legend(
    handles=legenda,
    title="Legenda",
    bbox_to_anchor=(1.02, 1),
    loc="upper left"
)

plt.tight_layout()
plt.show()

display(spark.createDataFrame(df_status_plot))

# COMMAND ----------

import matplotlib.pyplot as plt
from matplotlib.patches import Patch

df_saiu_entrega = df_silver[
    df_silver["status_entrega_normalizado"] == "saiu para entrega"
].copy()

df_saiu_entrega["horas_desde_evento"] = (
    (agora - df_saiu_entrega["dt_evento"]).dt.total_seconds() / 3600
)

ids_ate_2h = set(
    df_saiu_entrega[
        df_saiu_entrega["horas_desde_evento"] <= 2
    ]["id_pedido_ecommerce"].dropna()
)

ids_2h_8h = set(
    df_saiu_entrega[
        (df_saiu_entrega["horas_desde_evento"] > 2)
        & (df_saiu_entrega["horas_desde_evento"] <= 8)
    ]["id_pedido_ecommerce"].dropna()
)

ids_8h_24h = set(
    df_saiu_entrega[
        (df_saiu_entrega["horas_desde_evento"] > 8)
        & (df_saiu_entrega["horas_desde_evento"] <= 24)
    ]["id_pedido_ecommerce"].dropna()
)

ids_acima_24h = set(
    df_saiu_entrega[
        df_saiu_entrega["horas_desde_evento"] > 24
    ]["id_pedido_ecommerce"].dropna()
)

ids_com_saida = (
    ids_ate_2h
    | ids_2h_8h
    | ids_8h_24h
    | ids_acima_24h
)

ids_total = set(df_silver["id_pedido_ecommerce"].dropna())
ids_sem_saida = ids_total - ids_com_saida

df_grafico_saida_entrega_faixas = pd.DataFrame(
    [
        {
            "faixa": "Até 2h",
            "descricao": "Pedidos com saída para entrega registrada nas últimas 2 horas",
            "quantidade": len(ids_ate_2h),
        },
        {
            "faixa": "2h a 8h",
            "descricao": "Pedidos com saída para entrega registrada entre 2 e 8 horas",
            "quantidade": len(ids_2h_8h),
        },
        {
            "faixa": "8h a 24h",
            "descricao": "Pedidos com saída para entrega registrada entre 8 e 24 horas",
            "quantidade": len(ids_8h_24h),
        },
        {
            "faixa": "Acima de 24h",
            "descricao": "Pedidos com saída para entrega registrada há mais de 24 horas",
            "quantidade": len(ids_acima_24h),
        },
        {
            "faixa": "Sem saída registrada",
            "descricao": "Pedidos que ainda não possuem evento de saída para entrega",
            "quantidade": len(ids_sem_saida),
        },
    ]
)

display(spark.createDataFrame(df_grafico_saida_entrega_faixas))

cores = ["#4C78A8", "#72B7B2", "#F58518", "#E45756", "#BAB0AC"]

plt.figure(figsize=(10, 6))

barras = plt.bar(
    df_grafico_saida_entrega_faixas["faixa"],
    df_grafico_saida_entrega_faixas["quantidade"],
    color=cores,
    width=0.55,
)

maior_valor = df_grafico_saida_entrega_faixas["quantidade"].max()
plt.ylim(0, max(maior_valor + max(maior_valor * 0.15, 1), 1))

plt.title("Distribuição de pedidos por tempo desde saída para entrega")
plt.xlabel("Faixa de tempo")
plt.ylabel("Quantidade de pedidos")
plt.xticks(rotation=0)

for barra, valor in zip(barras, df_grafico_saida_entrega_faixas["quantidade"]):
    plt.text(
        barra.get_x() + barra.get_width() / 2,
        valor + max(maior_valor * 0.02, 0.05),
        str(int(valor)),
        ha="center",
        va="bottom",
        fontsize=10,
    )

legenda = [
    Patch(
        facecolor=cor,
        label=f"{linha['faixa']} = {linha['descricao']}"
    )
    for cor, (_, linha) in zip(cores, df_grafico_saida_entrega_faixas.iterrows())
]

plt.legend(
    handles=legenda,
    title="Legenda",
    loc="upper center",
    bbox_to_anchor=(0.5, -0.18),
    ncol=1,
    frameon=True,
)

plt.tight_layout()
plt.show()

# COMMAND ----------

import matplotlib.pyplot as plt
from matplotlib.patches import Patch

qtd_fora_sla = max(int(qtd_entregues) - int(qtd_no_prazo), 0)
                   
df_grafico_sla = pd.DataFrame(
    [
        {
            "situacao": "No prazo",
            "descricao": "Pedidos entregues dentro do SLA de 7 dias",
            "quantidade": int(qtd_no_prazo),
        },
        {
            "situacao": "Fora do prazo",
            "descricao": "Pedidos entregues acima do SLA de 7 dias",
            "quantidade": int(qtd_fora_sla),
        },
    ]
)

display(spark.createDataFrame(df_grafico_sla))

cores = ["#2E7D32", "#C62828"]

plt.figure(figsize=(8, 5))

barras = plt.bar(
    df_grafico_sla["situacao"],
    df_grafico_sla["quantidade"],
    color=cores,
    width=0.45,
)

maior_valor = df_grafico_sla["quantidade"].max()
plt.ylim(0, max(maior_valor + max(maior_valor * 0.15, 1), 1))

plt.title("SLA de entrega - pedidos no prazo x fora do prazo")
plt.xlabel("Situação do SLA")
plt.ylabel("Quantidade de pedidos")
plt.xticks(rotation=0)

for barra, valor in zip(barras, df_grafico_sla["quantidade"]):
    plt.text(
        barra.get_x() + barra.get_width() / 2,
        valor + max(maior_valor * 0.02, 0.03),
        str(int(valor)),
        ha="center",
        va="bottom",
        fontsize=10,
        fontweight="bold",
    )

if int(qtd_entregues) == 0:
    plt.text(
        0.5,
        0.55,
        "Nenhum pedido com status entregue foi encontrado\npara cálculo do SLA",
        transform=plt.gca().transAxes,
        ha="center",
        va="center",
        fontsize=11,
        color="#555555",
        bbox=dict(boxstyle="round,pad=0.4", facecolor="#F5F5F5", edgecolor="#CCCCCC"),
    )

legenda = [
    Patch(
        facecolor=cor,
        label=f"{linha['situacao']} = {linha['descricao']}"
    )
    for cor, (_, linha) in zip(cores, df_grafico_sla.iterrows())
]

plt.legend(
    handles=legenda,
    title="Legenda",
    loc="upper center",
    bbox_to_anchor=(0.5, -0.18),
    ncol=1,
    frameon=True,
)

plt.tight_layout()
plt.show()

# COMMAND ----------


import matplotlib.pyplot as plt
from matplotlib.patches import Patch

qtd_alerta_sem_evento = int(len(df_alerta_sem_evento_apos_coleta))

df_grafico_sem_evento = pd.DataFrame(
    [
        {
            "situacao": "Alerta",
            "descricao": "Pedidos com mais de 3 dias sem novo evento após coleta",
            "quantidade": qtd_alerta_sem_evento,
        }
    ]
)

display(spark.createDataFrame(df_grafico_sem_evento))

cor_alerta = "#C62828"

plt.figure(figsize=(7, 5))

barras = plt.bar(
    df_grafico_sem_evento["situacao"],
    df_grafico_sem_evento["quantidade"],
    color=[cor_alerta],
    width=0.35,
)

maior_valor = df_grafico_sem_evento["quantidade"].max()
plt.ylim(0, max(maior_valor + max(maior_valor * 0.15, 1), 1))

plt.title("Regra 10 - Pedidos sem evento após coleta")
plt.xlabel("Situação")
plt.ylabel("Quantidade de alertas")
plt.xticks(rotation=0)

for barra, valor in zip(barras, df_grafico_sem_evento["quantidade"]):
    plt.text(
        barra.get_x() + barra.get_width() / 2,
        valor + max(maior_valor * 0.02, 0.03),
        str(int(valor)),
        ha="center",
        va="bottom",
        fontsize=11,
        fontweight="bold",
    )

if qtd_alerta_sem_evento == 0:
    plt.text(
        0.5,
        0.55,
        "Nenhum alerta encontrado para esta regra",
        transform=plt.gca().transAxes,
        ha="center",
        va="center",
        fontsize=11,
        color="#555555",
        bbox=dict(
            boxstyle="round,pad=0.4",
            facecolor="#F5F5F5",
            edgecolor="#CCCCCC",
        ),
    )

plt.legend(
    handles=[
        Patch(
            facecolor=cor_alerta,
            label="Alerta = pedidos com mais de 3 dias sem novo evento após coleta"
        )
    ],
    title="Legenda",
    loc="upper center",
    bbox_to_anchor=(0.5, -0.18),
    frameon=True,
)

plt.tight_layout()
plt.show()
