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
# MAGIC # Bronze - ecommerce_rastreamento
# MAGIC
# MAGIC **Responsável:** Kálita Ribeiro Boni  
# MAGIC **Data da ingestão/processamento:** 18/06/2026  
# MAGIC **Camada:** Bronze  
# MAGIC **Tabela:** ecommerce_rastreamento  
# MAGIC
# MAGIC ### Origem e destino
# MAGIC
# MAGIC | Item | Valor |
# MAGIC |---|---|
# MAGIC | **Origem** | `raw/real-time-data` |
# MAGIC | **Destino** | `squad2/bronze/ecommerce_rastreamento` |
# MAGIC | **Control** | `squad2/control/bronze/ecommerce_rastreamento/checkpoint.json` |
# MAGIC
# MAGIC ### Objetivo
# MAGIC
# MAGIC Realizar a ingestão incremental dos arquivos parquet da camada raw para a camada Bronze, adicionando metadados de auditoria e mantendo os dados de negócio sem transformação.
# MAGIC
# MAGIC ### Regras/objetivos implementados
# MAGIC
# MAGIC - Leitura dos arquivos parquet da origem `raw/real-time-data`.
# MAGIC - Controle de arquivos já processados via checkpoint JSON.
# MAGIC - Processamento apenas dos arquivos ainda não registrados no checkpoint.
# MAGIC - Adição da coluna `bronze_ingested_at`.
# MAGIC - Adição da coluna `bronze_source_file`.
# MAGIC - Preservação das colunas de negócio sem transformação.
# MAGIC - Escrita dos dados em formato Delta na camada Bronze.
# MAGIC - Particionamento por ano, mês, dia e hora de ingestão.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Instalação das dependências
# MAGIC
# MAGIC Instala as bibliotecas necessárias para autenticação no Azure, acesso ao ADLS Gen2, leitura de arquivos Parquet, manipulação de dados com Pandas e escrita em Delta Lake.

# COMMAND ----------

# MAGIC %pip install azure-identity azure-storage-file-datalake deltalake pyarrow pandas

# COMMAND ----------

# MAGIC %md
# MAGIC ### Carregamento das configurações do projeto
# MAGIC
# MAGIC Executa o notebook de configuração da Squad 2, responsável por disponibilizar variáveis, credenciais, caminhos e funções auxiliares utilizadas neste pipeline.

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

# MAGIC %md
# MAGIC ###  Conexão com o Azure Data Lake Storage
# MAGIC
# MAGIC Cria a autenticação com o Azure e instancia os clientes de acesso aos containers `raw` e `squad2`, que serão utilizados para leitura dos arquivos de origem e escrita dos dados processados..

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

container_raw = "raw"
container_squad = "squad2"

file_system_client_raw = service_client.get_file_system_client(file_system=container_raw)
file_system_client_squad = service_client.get_file_system_client(file_system=container_squad)

storage_options = {
    "AZURE_STORAGE_ACCOUNT_NAME": storage_account_name,
    "AZURE_CLIENT_ID": client_id,
    "AZURE_TENANT_ID": tenant_id,
    "AZURE_CLIENT_SECRET": client_secret,
}

bronze_delta_path = f"az://{container_squad}/bronze/ecommerce_rastreamento"
#Definindo o checkpoint(Para evitar processar tudo de novo.)
checkpoint_adls_path = f"control/bronze/{table_name}/checkpoint.json"

print("raw_input_dir:", raw_input_dir)
print("bronze_delta_path:", bronze_delta_path)
print("checkpoint_adls_path:", checkpoint_adls_path)

# COMMAND ----------

# MAGIC %md
# MAGIC ###  Funções auxiliares do pipeline Bronze
# MAGIC
# MAGIC Define funções para leitura e gravação do checkpoint, leitura dos arquivos Parquet no ADLS e inclusão dos metadados de ingestão da camada Bronze.

# COMMAND ----------

def carregar_checkpoint_bronze() -> dict:
    try:
        file_client = file_system_client_squad.get_file_client(checkpoint_adls_path)
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

    directory_client = file_system_client_squad.get_directory_client(f"control/bronze/{table_name}")
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


def ler_parquet_adls(caminho_arquivo: str) -> pd.DataFrame:
    file_client = file_system_client_raw.get_file_client(caminho_arquivo)
    bytes_file = file_client.download_file().readall()
    table = pq.read_table(io.BytesIO(bytes_file))
    df = table.to_pandas()

    # Evita erro do Spark/Delta com timestamp Parquet em nanossegundos.
    if "dt_evento" in df.columns:
        df["dt_evento"] = df["dt_evento"].astype(str)

    return df

#Adicionar metadados Bronze
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

# MAGIC %md
# MAGIC ### Identificação de arquivos novos
# MAGIC
# MAGIC Lista os arquivos `.parquet` da tabela `ecommerce_rastreamento` no container `raw`, compara com o checkpoint da Bronze e seleciona apenas os arquivos ainda não processados.

# COMMAND ----------

## Busca arquivos de ecommerce_rastreamento no RAW,
# compara com o checkpoint da Bronze e identifica
# apenas os arquivos ainda não processados.


arquivos = []
paths = file_system_client_raw.get_paths(path=raw_input_dir, recursive=True)

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

# MAGIC %md
# MAGIC ### Leitura, enriquecimento e validação do micro-lote
# MAGIC
# MAGIC Lê os arquivos novos, adiciona metadados de auditoria, consolida os dados em um único DataFrame e valida a presença das colunas obrigatórias da tabela.

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

# MAGIC %md
# MAGIC ### Escrita dos dados na camada Bronze
# MAGIC
# MAGIC Converte o DataFrame final para formato Arrow e grava os dados em Delta Lake no container da Squad 2, utilizando modo append e particionamento por data e hora de ingestão.

# COMMAND ----------

# DBTITLE 1,Gravação da camada Bronze (Delta Lake)
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

# DBTITLE 1,Atualização do controle de processamento (Checkpoint)
dt = DeltaTable(bronze_delta_path, storage_options=storage_options)
print("Versao Delta:", dt.version())

checkpoint_bronze["arquivos_processados"] = sorted(arquivos_processados.union(arquivos_novos))
checkpoint_bronze["total_arquivos_processados"] = len(checkpoint_bronze["arquivos_processados"])
checkpoint_bronze["ultimo_lote"] = arquivos_novos
checkpoint_bronze["destino"] = bronze_delta_path
salvar_checkpoint_bronze(checkpoint_bronze)

print("Checkpoint Bronze salvo com sucesso.")
