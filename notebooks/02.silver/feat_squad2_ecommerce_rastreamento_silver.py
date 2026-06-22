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
# MAGIC
# MAGIC **Responsável:** Kálita Ribeiro Boni  
# MAGIC **Data da ingestão/processamento:** 18/06/2026  
# MAGIC **Camada:** Silver  
# MAGIC **Tabela:** ecommerce_rastreamento  
# MAGIC
# MAGIC ### Origem e destino
# MAGIC
# MAGIC | Item | Valor |
# MAGIC |---|---|
# MAGIC | **Origem** | `squad2/bronze/ecommerce_rastreamento` |
# MAGIC | **Destino** | `squad2/silver/ecommerce_rastreamento` |
# MAGIC | **Quarentena** | `squad2/quarantine/ecommerce_rastreamento_entregas` |
# MAGIC | **control** | `squad2/control/silver/ecommerce_rastreamento/checkpoint.json` |
# MAGIC
# MAGIC ### Objetivo
# MAGIC
# MAGIC Aplicar validações, padronizações e deduplicação nos dados de rastreamento recebidos da camada Bronze.
# MAGIC
# MAGIC ### Regras implementadas
# MAGIC
# MAGIC - Validar schema obrigatório da tabela.
# MAGIC - Validar `status_entrega` conforme fluxo logístico permitido.
# MAGIC - Remover ou separar registros com `dt_evento` nula ou futura.
# MAGIC - Deduplicar registros por `id_rastreamento`.
# MAGIC - Validar `id_pedido_ecommerce` com a base de pedidos quando disponível.
# MAGIC - Enviar registros inválidos para quarentena.

# COMMAND ----------

# MAGIC %run /Workspace/Users/kalitamariano01@gmail.com/merca-data-platform/notebooks/config/feat_squad2_00_setup_config

# COMMAND ----------

# MAGIC %md
# MAGIC ### Importação das Bibliotecas
# MAGIC  Importa bibliotecas para autenticação no Azure, leitura de arquivos Parquet, manipulação com Pandas/PyArrow e escrita em Delta Lake.
# MAGIC

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

# MAGIC %md
# MAGIC ### 3. Conexão com ADLS e Definição dos Paths
# MAGIC Cria o cliente do Azure Data Lake Storage e define os caminhos de origem, destino, quarentena e checkpoint usados nesta execução.
# MAGIC
# MAGIC

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

# MAGIC %md
# MAGIC ###  Funções de Checkpoint
# MAGIC Centraliza a leitura e a gravação do checkpoint para registrar a última execução, origem, destino e quantidade de registros processados.

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


    

# COMMAND ----------

# MAGIC %md
# MAGIC ###  Leitura dos Dados da Bronze
# MAGIC  Lista os arquivos Parquet da tabela Delta Bronze, ignora metadados do `_delta_log` e consolida os dados em um DataFrame Pandas.
# MAGIC
# MAGIC

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
# display usado apenas para análise manual
# display(spark.createDataFrame(df_bronze.head(20)))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Validação de Schema e Padronização
# MAGIC Confere as colunas obrigatórias, padroniza tipos e normaliza campos usados nas regras de qualidade da camada Silver.
# MAGIC
# MAGIC

# COMMAND ----------

print("Validando schema obrigatório da Bronze...")

# Valida se todas as colunas obrigatórias da Bronze estão presentes
# antes de iniciar as transformações da Silver.
colunas_esperadas = [
    "id_rastreamento",
    "id_pedido_ecommerce",
    "codigo_rastreio",
    "id_transportadora",
    "status_entrega",
    "dt_evento",
    "bronze_source_file",
    "bronze_ingested_at",
]

colunas_ausentes = [
    coluna for coluna in colunas_esperadas
    if coluna not in df_bronze.columns
]

if colunas_ausentes:
    raise ValueError(f"Schema inválido na Bronze. Colunas ausentes: {colunas_ausentes}")


df = df_bronze.copy()

# Padroniza os tipos das colunas e realiza limpeza básica
# para preparação das regras de negócio da Silver.
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

# Registros com status nulo ou fora do fluxo logístico
# permitido são enviados para quarentena.
df_quarentena_status = df_silver_pre[
    df_silver_pre["status_entrega_normalizado"].isna()
    | ~df_silver_pre["status_entrega_normalizado"].isin(status_entrega_permitidos)
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
    & df_silver_pre["status_entrega_normalizado"].notna()
    & df_silver_pre["status_entrega_normalizado"].isin(status_entrega_permitidos)
].copy()

print("Pre-validos:", len(df_silver_valida))
print("Quarentena status:", len(df_quarentena_status))
print("Quarentena data:", len(df_quarentena_data))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Validação do Pedido Pai
# MAGIC Verifica se `id_pedido_ecommerce` existe na tabela Silver de pedidos. Caso a tabela ainda não esteja disponível, a validação fica registrada como pendente e não bloqueia o pipeline.
# MAGIC

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

# MAGIC %md
# MAGIC ### Deduplicação e Montagem da Quarentena
# MAGIC Mantém o evento mais recente por `id_rastreamento`, cria colunas de particionamento e consolida os registros rejeitados pelas regras de qualidade.

# COMMAND ----------



df_silver = (
    df_silver_valida
    .sort_values(["id_rastreamento", "dt_evento", "bronze_ingested_at"], ascending=[True, False, False])
    .drop_duplicates(subset=["id_rastreamento"], keep="first")
    .drop(columns=["status_entrega_normalizado"])
    .copy()
)

df_silver["silver_updated_at"] = agora
df_silver["ano_processamento"] = pd.to_datetime(df_silver["silver_updated_at"]).dt.year
df_silver["mes_processamento"] = pd.to_datetime(df_silver["silver_updated_at"]).dt.month
df_silver["dia_processamento"] = pd.to_datetime(df_silver["silver_updated_at"]).dt.day
df_silver["hora_processamento"] = pd.to_datetime(df_silver["silver_updated_at"]).dt.hour

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
# 
# display(spark.createDataFrame(df_silver.head(20)))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Atualização do Checkpoint
# MAGIC Salva metadados da execução atual para auditoria: origem, destino, registros processados, quarentena e validações aplicadas.

# COMMAND ----------


def escrever_delta(path: str, df: pd.DataFrame, mode: str = "append", partition_by=None):
    tabela_arrow = pa.Table.from_pandas(df, preserve_index=False)

    opcoes = {
        "mode": mode,
        "storage_options": storage_options,
    }

    if partition_by:
        opcoes["partition_by"] = partition_by

    if mode == "overwrite":
        opcoes["schema_mode"] = "overwrite"

    write_deltalake(
        path,
        tabela_arrow,
        **opcoes,
    )

    print(f"Delta salvo em: {path} | registros: {len(df)}")

# utilizando particionamento por data/hora de processamento.

particoes_silver = [
    "ano_processamento",
    "mes_processamento",
    "dia_processamento",
    "hora_processamento",
]

modo_gravacao_silver = "append"
modo_gravacao_quarentena = "append"

print("Gravando dados validos na camada Silver...")
escrever_delta(
    silver_delta_path,
    df_silver,
    mode=modo_gravacao_silver,
    partition_by=particoes_silver,
)

print("Silver salva com sucesso:", silver_delta_path)

# Grava registros rejeitados pelas regras de qualidade
# na área de quarentena para análise posterior.
if len(df_quarentena) > 0:
    print("Gravando registros invalidos na quarentena...")

    df_quarentena["ano_processamento"] = pd.to_datetime(df_quarentena["silver_updated_at"]).dt.year
    df_quarentena["mes_processamento"] = pd.to_datetime(df_quarentena["silver_updated_at"]).dt.month
    df_quarentena["dia_processamento"] = pd.to_datetime(df_quarentena["silver_updated_at"]).dt.day
    df_quarentena["hora_processamento"] = pd.to_datetime(df_quarentena["silver_updated_at"]).dt.hour

    escrever_delta(
        quarantine_delta_path,
        df_quarentena,
        mode=modo_gravacao_quarentena,
        partition_by=particoes_silver,
    )

    print("Quarentena salva com sucesso:", quarantine_delta_path)
else:
    print("Sem registros de quarentena para gravar.")

# COMMAND ----------

# Atualiza o checkpoint da Silver com informações
# da execução atual para rastreabilidade do processamento.

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

# MAGIC %md
# MAGIC ## Insights gráficos da camada Silver
# MAGIC
# MAGIC Esta seção apresenta gráficos de apoio para análise da qualidade dos dados tratados na camada Silver, com foco nas validações aplicadas e registros enviados para quarentena.

# COMMAND ----------

# DBTITLE 1,Registros processados x quarentena Mostra se teve dado inválido.
import matplotlib.pyplot as plt
from matplotlib.patches import Patch

qtd_silver = int(len(df_silver))
qtd_quarentena = int(len(df_quarentena))

df_grafico_qualidade = pd.DataFrame(
    [
        {
            "situacao": "Válidos",
            "descricao": "Registros aprovados nas validações da Silver",
            "quantidade": qtd_silver,
        },
        {
            "situacao": "Quarentena",
            "descricao": "Registros rejeitados pelas validações da Silver",
            "quantidade": qtd_quarentena,
        },
    ]
)

# display(spark.createDataFrame(df_grafico_qualidade))

cores = ["#2E7D32", "#C62828"]

plt.figure(figsize=(8, 5))

barras = plt.bar(
    df_grafico_qualidade["situacao"],
    df_grafico_qualidade["quantidade"],
    color=cores,
    width=0.45,
)

maior_valor = df_grafico_qualidade["quantidade"].max()
plt.ylim(0, max(maior_valor + max(maior_valor * 0.15, 1), 1))

plt.title("Qualidade dos registros na camada Silver")
plt.xlabel("Situação")
plt.ylabel("Quantidade de registros")

for barra, valor in zip(barras, df_grafico_qualidade["quantidade"]):
    plt.text(
        barra.get_x() + barra.get_width() / 2,
        valor + max(maior_valor * 0.02, 0.05),
        str(int(valor)),
        ha="center",
        va="bottom",
        fontsize=10,
        fontweight="bold",
    )

plt.legend(
    handles=[
        Patch(facecolor=cores[0], label="Válidos = registros gravados na Silver"),
        Patch(facecolor=cores[1], label="Quarentena = registros inválidos separados"),
    ],
    title="Legenda",
    loc="upper center",
    bbox_to_anchor=(0.5, -0.18),
    frameon=True,
)

plt.tight_layout()
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC Status de entrega após normalização
# MAGIC Mostra como ficaram os status tratados.

# COMMAND ----------

import matplotlib.pyplot as plt

df_status_base = df_silver.copy()

df_status_base["status_entrega_tratado"] = (
    df_status_base["status_entrega"]
    .astype("string")
    .str.lower()
    .str.strip()
)

status_permitidos = [
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

status_mapa = {
    "aguardando coleta": "Aguard. coleta",
    "coletado": "Coletado",
    "em transporte": "Transporte",
    "em rota de entrega": "Rota entrega",
    "saiu para entrega": "Saiu entrega",
    "entregue": "Entregue",
    "falha na entrega": "Falha",
    "devolvido": "Devolvido",
    "cancelado": "Cancelado",
}

df_status_contagem = (
    df_status_base.groupby("status_entrega_tratado", dropna=False)
    .size()
    .reset_index(name="quantidade")
    .rename(columns={"status_entrega_tratado": "status_entrega"})
)

df_status_todos = pd.DataFrame(
    {
        "status_entrega": status_permitidos,
        "status_curto": [status_mapa[s] for s in status_permitidos],
    }
)

df_status_plot = df_status_todos.merge(
    df_status_contagem,
    on="status_entrega",
    how="left"
)

df_status_plot["quantidade"] = df_status_plot["quantidade"].fillna(0).astype(int)

# display(spark.createDataFrame(df_status_plot))

plt.figure(figsize=(12, 5))

barras = plt.bar(
    df_status_plot["status_curto"],
    df_status_plot["quantidade"],
    color="#4C78A8",
    width=0.55,
)

maior_valor = df_status_plot["quantidade"].max()
plt.ylim(0, max(maior_valor + max(maior_valor * 0.15, 1), 1))

plt.title("Distribuição dos status de entrega na Silver")
plt.xlabel("Status de entrega")
plt.ylabel("Quantidade de registros")
plt.xticks(rotation=25, ha="right")

for barra, valor in zip(barras, df_status_plot["quantidade"]):
    plt.text(
        barra.get_x() + barra.get_width() / 2,
        valor + max(maior_valor * 0.02, 0.05),
        str(int(valor)),
        ha="center",
        va="bottom",
        fontsize=9,
    )

plt.tight_layout()
plt.show()
