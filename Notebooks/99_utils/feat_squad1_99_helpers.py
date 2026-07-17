# Databricks notebook source
# MAGIC %md
# MAGIC # Funções Utilitárias Centralizadas (Squad 1)
# MAGIC **Projeto: Merca Data Platform — Data Quality em Tempo Real**
# MAGIC
# MAGIC Este notebook centraliza as funções reutilizáveis de acesso ao Data Lake da Squad 1: leitura e
# MAGIC gravação de tabelas Delta no container `squad1`. O padrão segue a mesma estrutura já validada
# MAGIC pela Squad 2, adaptando apenas o container de destino.
# MAGIC
# MAGIC **Decisões arquiteturais:**
# MAGIC * **Reaproveitamento de padrão:** as funções de leitura/gravação Delta (`ler_delta`, `gravar_delta`)
# MAGIC   replicam a lógica já testada pela Squad 2, incluindo o fallback via Azure SDK caso a engine
# MAGIC   `deltalake` encontre restrições no ambiente Databricks Free.
# MAGIC * **Escopo reduzido:** diferente do helper da Squad 2, este notebook não inclui funções de
# MAGIC   checkpoint/snapshot de ingestão. As camadas Bronze e Silver da Squad 1 já existem (confirmado
# MAGIC   no notebook de diagnóstico) e não são de responsabilidade deste card — o foco aqui é leitura de
# MAGIC   Silver e gravação de features na camada Gold.

# COMMAND ----------

import os
import logging
import pandas as pd
from dotenv import load_dotenv
from azure.identity import ClientSecretCredential
from azure.storage.filedatalake import DataLakeServiceClient

# ─────────────────────────────────────────────
# CONFIGURAÇÃO DE LOGS
# ─────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
log = logging.getLogger("squad1")

# Oculta logs HTTP verbosos das bibliotecas do Azure (azure-identity, azure-storage) —
# por padrão elas logam cada requisição/resposta em nível INFO, poluindo a saída do notebook.
logging.getLogger("azure").setLevel(logging.WARNING)

# ─────────────────────────────────────────────
# CARREGAMENTO DE CREDENCIAIS
# ─────────────────────────────────────────────
load_dotenv()

ADLS_CLIENT_ID = os.getenv("ADLS_CLIENT_ID")
ADLS_TENANT_ID = os.getenv("ADLS_TENANT_ID")
ADLS_CLIENT_SECRET = os.getenv("ADLS_CLIENT_SECRET")
ADLS_STORAGE_ACCOUNT = "internshipdatalake"  # mesmo valor usado no setup da Squad 2
SQUAD1_CONTAINER = "squad1"  # confirmado no notebook de diagnóstico

def _validar_credenciais() -> None:
    credenciais = {
        "ADLS_CLIENT_ID": ADLS_CLIENT_ID,
        "ADLS_TENANT_ID": ADLS_TENANT_ID,
        "ADLS_CLIENT_SECRET": ADLS_CLIENT_SECRET,
    }
    todas_ok = True
    for nome, valor in credenciais.items():
        if not valor:
            log.error(f"Credencial não encontrada: {nome}")
            todas_ok = False

    if todas_ok:
        log.info("Helpers da Squad 1 carregados. Todas as credenciais OK.")
    else:
        raise EnvironmentError("Credenciais ausentes. Verificar o arquivo .env")

_validar_credenciais()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Conexão com o Data Lake
# MAGIC
# MAGIC As funções abaixo centralizam a construção do caminho Delta (`get_delta_path`) e a criação do
# MAGIC cliente autenticado do ADLS (`get_squad1_client`), evitando repetição dessa lógica em cada notebook.

# COMMAND ----------

def get_storage_options() -> dict:
    return {
        "account_name": ADLS_STORAGE_ACCOUNT,
        "tenant_id": ADLS_TENANT_ID,
        "client_id": ADLS_CLIENT_ID,
        "client_secret": ADLS_CLIENT_SECRET,
    }

def get_delta_path(camada: str, tabela: str) -> str:
    return (
        f"abfss://{SQUAD1_CONTAINER}@{ADLS_STORAGE_ACCOUNT}"
        f".dfs.core.windows.net/{camada}/{tabela}"
    )

def get_squad1_client():
    credential = ClientSecretCredential(
        tenant_id=ADLS_TENANT_ID,
        client_id=ADLS_CLIENT_ID,
        client_secret=ADLS_CLIENT_SECRET,
    )
    service_client = DataLakeServiceClient(
        account_url=f"https://{ADLS_STORAGE_ACCOUNT}.dfs.core.windows.net",
        credential=credential,
    )
    return service_client.get_file_system_client(SQUAD1_CONTAINER)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Leitura e gravação de tabelas Delta
# MAGIC
# MAGIC `ler_delta` tenta primeiro a leitura nativa via biblioteca `deltalake`; caso essa via encontre
# MAGIC alguma restrição do ambiente, um fallback via Azure SDK garante a entrega do DataFrame mesmo assim.
# MAGIC `gravar_delta` grava um DataFrame PySpark como tabela Delta, decidindo automaticamente entre
# MAGIC `overwrite` (quando a tabela ainda não existe) e o modo solicitado (quando ela já existe).

# COMMAND ----------

def ler_delta(camada: str, tabela: str) -> "pyspark.sql.DataFrame":
    import io

    path = f"{camada}/{tabela}"
    storage_opts = get_storage_options()

    try:
        from deltalake import DeltaTable
        dt = DeltaTable(get_delta_path(camada, tabela), storage_options=storage_opts)
        pdf = dt.to_pandas()
        log.info(f"Lido via deltalake: {len(pdf)} linhas — {camada}/{tabela}")
        return spark.createDataFrame(pdf)

    except Exception as e1:
        log.warning(f"deltalake falhou ({str(e1)[:80]}), tentando via Azure SDK...")
        fs_client = get_squad1_client()
        paths = list(fs_client.get_paths(path=path, recursive=True))
        parquets = [
            p.name for p in paths
            if p.name.endswith(".parquet") and "_delta_log" not in p.name
        ]

        frames = []
        for p in parquets:
            file_client = fs_client.get_file_client(p)
            bytes_data = file_client.download_file().readall()
            frames.append(pd.read_parquet(io.BytesIO(bytes_data)))

        if not frames:
            raise Exception(f"Nenhum arquivo parquet encontrado em {path}")

        pdf_total = pd.concat(frames, ignore_index=True)
        log.info(f"Lido via Azure SDK: {len(pdf_total)} linhas — {camada}/{tabela}")
        return spark.createDataFrame(pdf_total)


def delta_existe(camada: str, tabela: str) -> bool:
    try:
        from deltalake import DeltaTable
        DeltaTable(get_delta_path(camada, tabela), storage_options=get_storage_options())
        return True
    except Exception:
        return False


def gravar_delta(
    df: "pyspark.sql.DataFrame",
    camada: str,
    tabela: str,
    mode: str = "overwrite",
    partition_by: list = None,
) -> bool:
    import pyarrow as pa
    from deltalake.writer import write_deltalake

    path = get_delta_path(camada, tabela)
    storage_opts = get_storage_options()
    modo_real = mode if delta_existe(camada, tabela) else "overwrite"

    try:
        pdf = df.toPandas()
        tabela_arrow = pa.Table.from_pandas(pdf)
        write_deltalake(
            table_or_uri=path,
            data=tabela_arrow,
            mode=modo_real,
            storage_options=storage_opts,
            partition_by=partition_by,
        )
        log.info(f"Gravado: {path} → {len(pdf)} linhas | modo: {modo_real}")
        return True

    except Exception as e:
        log.error(f"Erro ao gravar {path}: {str(e)}")
        return False
