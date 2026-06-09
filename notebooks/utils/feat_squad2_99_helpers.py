# Databricks notebook source
# MAGIC %md
# MAGIC # Projeto: Merca Data Platform
# MAGIC ## Squad 2 | Funções Utilitárias Centralizadas
# MAGIC > Este notebook centraliza todas as funções reutilizáveis do projeto. Deve ser chamado via %run pelos demais notebooks.

# COMMAND ----------

import os
import io
import time
import json
import logging
import pandas as pd
from datetime import datetime
from dotenv import load_dotenv
from azure.identity import ClientSecretCredential
from azure.storage.filedatalake import DataLakeServiceClient

# ─────────────────────────────────────────────
# CONFIGURAÇÃO DE LOGS
# ─────────────────────────────────────────────
logging.basicConfig(
    level  = logging.INFO,
    format = "%(asctime)s [%(levelname)s] %(message)s"
)
log = logging.getLogger("squad2")

# ─────────────────────────────────────────────
# CARREGAMENTO DE CREDENCIAIS
# ─────────────────────────────────────────────
load_dotenv()

# ADLS
ADLS_CLIENT_ID       = os.getenv("ADLS_CLIENT_ID")
ADLS_TENANT_ID       = os.getenv("ADLS_TENANT_ID")
ADLS_CLIENT_SECRET   = os.getenv("ADLS_CLIENT_SECRET")
ADLS_STORAGE_ACCOUNT = os.getenv("ADLS_STORAGE_ACCOUNT")
ADLS_CONTAINER       = os.getenv("ADLS_CONTAINER")
SQUAD2_CONTAINER     = os.getenv("SQUAD2_CONTAINER", "squad2")

# SQL Server
SQL_HOST     = os.getenv("SQL_HOST")
SQL_DATABASE = os.getenv("SQL_DATABASE")
SQL_USERNAME = os.getenv("SQL_USERNAME")
SQL_PASSWORD = os.getenv("SQL_PASSWORD")

# Opções SQL reutilizáveis
SQL_OPTIONS = {
    "host"    : SQL_HOST,
    "database": SQL_DATABASE,
    "user"    : SQL_USERNAME,
    "password": SQL_PASSWORD
}

# ─────────────────────────────────────────────
# CONFIGURAÇÕES DO PROJETO
# ─────────────────────────────────────────────

# Caminhos Medalhão
PATHS = {
    "raw"        : "real-time-data",
    "bronze"     : "squad2/bronze",
    "silver"     : "squad2/silver",
    "gold"       : "squad2/gold",
    "checkpoint" : "squad2/checkpoints"
}

# Tabelas sob responsabilidade do Squad 2
TABELAS_SQUAD2 = [
    "ecommerce_categorias",
    "ecommerce_itens_pedido",
    "ecommerce_produtos"
]

# Schema destino SQL Server
SQL_SCHEMA = "squad2"
SQL_PREFIX = ""

# ─────────────────────────────────────────────
# FUNÇÕES — ADLS
# ─────────────────────────────────────────────

def get_adls_client() -> DataLakeServiceClient:
    """
    Cria e retorna um cliente autenticado do ADLS Gen2
    via Service Principal.
    """
    credential = ClientSecretCredential(
        tenant_id     = ADLS_TENANT_ID,
        client_id     = ADLS_CLIENT_ID,
        client_secret = ADLS_CLIENT_SECRET
    )
    return DataLakeServiceClient(
        account_url = f"https://{ADLS_STORAGE_ACCOUNT}.dfs.core.windows.net",
        credential  = credential
    )

def get_container_client():
    """
    Retorna o cliente do container configurado.
    """
    return get_adls_client().get_file_system_client(ADLS_CONTAINER)

def get_squad2_client():
    """
    Retorna o cliente do container squad2 para escrita
    de dados nas camadas bronze, silver e gold.
    """
    return get_adls_client().get_file_system_client(SQUAD2_CONTAINER)

def listar_snapshots(base_path: str = None) -> set:
    """
    Lista todas as pastas de snapshot disponíveis no lake.
    Padrão esperado: vendas_raw/YYYY/MM/DD/HHMMSS

    Returns:
        set: conjunto de snapshot_ids no formato YYYY/MM/DD/HHMMSS
    """
    if base_path is None:
        base_path = PATHS["raw"]

    snapshots        = set()
    container_client = get_container_client()
    paths            = container_client.get_paths(
        path      = base_path,
        recursive = True
    )

    for item in paths:
        partes = item.name.replace(base_path + "/", "").split("/")
        if len(partes) == 4 and item.is_directory:
            snapshots.add("/".join(partes))

    return snapshots

def ler_parquet(snapshot_id: str, tabela: str) -> "pyspark.sql.DataFrame":
    """
    Lê um arquivo parquet de um snapshot específico do ADLS
    diretamente na memória e retorna um Spark DataFrame.
    """
    base_path        = PATHS["raw"]
    file_path        = f"{base_path}/{snapshot_id}/{tabela}.parquet"
    container_client = get_container_client()
    file_client      = container_client.get_file_client(file_path)

    bytes_data = file_client.download_file().readall()
    pdf        = pd.read_parquet(io.BytesIO(bytes_data))

    return spark.createDataFrame(pdf)

# ─────────────────────────────────────────────
# FUNÇÕES — SQL SERVER
# ─────────────────────────────────────────────

def get_destino_sql(tabela: str) -> str:
    """
    Retorna o nome completo da tabela destino no SQL Server.
    Formato: squad2.nome_tabela
    """
    return f"{SQL_SCHEMA}.{SQL_PREFIX}{tabela}"

def gravar_sql(
    df     : "pyspark.sql.DataFrame",
    tabela : str,
    mode   : str = "overwrite"
) -> bool:
    """
    Grava um Spark DataFrame no SQL Server.
    """
    destino = get_destino_sql(tabela)
    try:
        df.write \
            .format("sqlserver") \
            .options(**SQL_OPTIONS) \
            .option("dbtable", destino) \
            .mode(mode) \
            .save()

        log.info(f"Gravado: {destino} → {df.count()} linhas")
        return True

    except Exception as e:
        log.error(f"Erro ao gravar {destino}: {str(e)}")
        return False

def ler_sql(tabela: str) -> "pyspark.sql.DataFrame":
    """
    Lê uma tabela do SQL Server e retorna um Spark DataFrame.
    """
    destino = get_destino_sql(tabela)
    return spark.read \
        .format("sqlserver") \
        .options(**SQL_OPTIONS) \
        .option("dbtable", destino) \
        .load()

def validar_gravacao(tabela: str) -> bool:
    """
    Valida se uma tabela foi gravada corretamente no SQL Server.
    """
    try:
        df    = ler_sql(tabela)
        count = df.count()
        log.info(f"Validado: {get_destino_sql(tabela)} → {count} linhas")
        return count > 0
    except Exception as e:
        log.error(f"Erro ao validar {tabela}: {str(e)}")
        return False

# ─────────────────────────────────────────────
# FUNÇÕES — DELTA LAKE (ADLS via deltalake-python)
# ─────────────────────────────────────────────

def get_delta_path(camada: str, tabela: str) -> str:
    """
    Retorna a URI nativa exigida pelo storage_options do deltalake-python.
    """
    return f"az://{SQUAD2_CONTAINER}/{camada}/{tabela}"

def get_storage_options() -> dict:
    """
    Retorna as opções de autenticação para o deltalake-python.
    """
    return {
        "account_name"  : ADLS_STORAGE_ACCOUNT,
        "tenant_id"     : ADLS_TENANT_ID,
        "client_id"     : ADLS_CLIENT_ID,
        "client_secret" : ADLS_CLIENT_SECRET
    }

def delta_existe(camada: str, tabela: str) -> bool:
    """
    Verifica se uma Delta Table já existe no ADLS.
    """
    try:
        from deltalake import DeltaTable
        DeltaTable(
            get_delta_path(camada, tabela),
            storage_options=get_storage_options()
        )
        return True
    except Exception:
        return False

def gravar_delta(
    df          : "pyspark.sql.DataFrame",
    camada      : str,
    tabela      : str,
    mode        : str = "append",
    particionar : bool = True
) -> bool:
    """
    Grava um Spark DataFrame como Delta Table no ADLS via deltalake-python.
    Aplica correção automática de fuso horário e reestruturação de partições.
    """
    import pyarrow as pa
    from deltalake.writer import write_deltalake

    path         = get_delta_path(camada, tabela)
    storage_opts = get_storage_options()
    modo_real    = mode if delta_existe(camada, tabela) else "overwrite"

    try:
        pdf          = df.toPandas()
        
        # Correção interna: Remove timezones do Pandas/Arrow para compatibilidade Delta v7
        for col_name in pdf.columns:
            if pd.api.types.is_datetime64_any_dtype(pdf[col_name]):
                pdf[col_name] = pdf[col_name].dt.tz_localize(None)
                
        tabela_arrow = pa.Table.from_pandas(pdf)

        # Configuração nativa de particionamento
        partition_by = None
        if particionar and camada == "bronze":
            partition_by = [
                "ingestion_year",
                "ingestion_month",
                "ingestion_day",
                "ingestion_hour"
            ]
            colunas = pdf.columns.tolist()
            if not all(c in colunas for c in partition_by):
                partition_by = None

        write_deltalake(
            table_or_uri    = path,
            data            = tabela_arrow,
            mode            = modo_real,
            storage_options = storage_opts,
            partition_by    = partition_by
        )

        log.info(f"Gravado com sucesso: {path} → {len(pdf)} linhas. partições: {partition_by}")
        return True

    except Exception as e:
        # Auto-correção caso herde erros de layout não-particionado gerados em testes anteriores
        if "does not match table partitioning" in str(e):
            try:
                log.warning("Layout antigo em conflito. Forçando alinhamento das partições no Storage...")
                write_deltalake(
                    table_or_uri    = path,
                    data            = tabela_arrow,
                    mode            = "overwrite",
                    storage_options = storage_opts,
                    partition_by    = partition_by,
                    schema_mode     = "overwrite"
                )
                return True
            except Exception as e_inner:
                log.error(f"Falha ao reestruturar esquema Delta: {str(e_inner)}")
                return False
                
        log.error(f"Erro ao gravar {path}: {str(e)}")
        return False

def ler_delta(camada: str, tabela: str) -> "pyspark.sql.DataFrame":
    """
    Lê uma Delta Table do ADLS unificando fragmentos de partições em memória
    para mitigar exceções de ChunkedArray do PyArrow.
    """
    from deltalake import DeltaTable
    import pyarrow as pa

    path         = get_delta_path(camada, tabela)
    storage_opts = get_storage_options()

    try:
        dt = DeltaTable(path, storage_options=storage_opts)
        
        # Consolida blocos fragmentados de partições em um vetor contíguo
        arrow_table = dt.to_pyarrow_table().combine_chunks()
        pdf = arrow_table.to_pandas()
        
        log.info(f"Lido via deltalake-engine: {len(pdf)} linhas")
        return spark.createDataFrame(pdf)

    except Exception as e1:
        log.warning(f"Engine principal indisponível ({str(e1)[:50]}). Executando Fallback via Azure SDK...")
        
        import io
        import pandas as pd
        squad2_client = get_squad2_client()
        frames = []
        
        paths = list(squad2_client.get_paths(path=f"{camada}/{tabela}", recursive=True))
        parquets = [
            p.name for p in paths 
            if p.name.endswith(".parquet") and "_delta_log" not in p.name
        ]

        for p in parquets:
            file_client = squad2_client.get_file_client(p)
            bytes_data  = file_client.download_file().readall()
            pdf_part    = pd.read_parquet(io.BytesIO(bytes_data))
            frames.append(pdf_part)

        if frames:
            pdf_total = pd.concat(frames, ignore_index=True)
            log.info(f"Lido via Fallback Azure SDK: {len(pdf_total)} linhas")
            return spark.createDataFrame(pdf_total)
        else:
            raise Exception(f"Nenhum arquivo encontrado para a tabela {tabela} na camada {camada}")

def get_nome_delta(camada: str, tabela: str) -> str:
    """
    Retorna o path de compatibilidade da Delta Table.
    """
    return get_delta_path(camada, tabela)

# ─────────────────────────────────────────────
# FUNÇÕES — CHECKPOINT & CONTROLE (MIGRADO PARA JSON)
# ─────────────────────────────────────────────

def get_control_path(tabela: str) -> tuple:
    """
    Retorna o cliente e o caminho estruturado na nova pasta unificada 'control/'
    """
    container_client = get_container_client()
    control_file     = f"control/{tabela}/control_file.json"
    return container_client, control_file

def ler_checkpoint(camada: str, tabela: str) -> set:
    """
    Lê o arquivo de controle em formato JSON da pasta control/
    """
    container_client, control_file = get_control_path(tabela)
    processados                    = set()

    try:
        file_client = container_client.get_file_client(control_file)
        conteudo    = file_client.download_file().readall().decode("utf-8")
        dados       = json.loads(conteudo)
        
        processados = set(dados.get("processed_snapshots", []))
        log.info(f"{len(processados)} snapshot(s) recuperados do JSON de controle.")
    except Exception:
        log.info(f"Nenhum controle JSON localizado em {control_file}. Iniciando carga limpa.")

    return processados

def salvar_checkpoint(camada: str, tabela: str, processados: set) -> None:
    """
    Grava metadados e histórico estruturado em formato JSON dentro de control/
    """
    container_client, control_file = get_control_path(tabela)
    file_client                       = container_client.get_file_client(control_file)
    
    dados_controle = {
        "tabela": tabela,
        "camada": camada,
        "ultima_atualizacao": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "total_snapshots_processados": len(processados),
        "processed_snapshots": sorted(list(processados))
    }
    
    conteudo = json.dumps(dados_controle, indent=4).encode("utf-8")

    try:
        file_client.get_file_properties()
        file_client.upload_data(conteudo, overwrite=True)
    except Exception:
        file_client.create_file()
        file_client.upload_data(conteudo, overwrite=True)

    log.info(f"Governança unificada: Controle JSON atualizado em {control_file}")

# ─────────────────────────────────────────────
# FUNÇÕES — UTILITÁRIAS
# ─────────────────────────────────────────────

def get_snapshot_mais_recente(base_path: str = None) -> str:
    """
    Retorna o snapshot mais recente disponível no lake.
    """
    snapshots = listar_snapshots(base_path)
    if not snapshots:
        raise ValueError("Nenhum snapshot encontrado no lake.")
    return sorted(snapshots)[-1]

def log_inicio(notebook: str) -> datetime:
    """Loga o início da execução de um notebook."""
    inicio = datetime.now()
    log.info(f"{'='*50}")
    log.info(f"INÍCIO: {notebook}")
    log.info(f"Data  : {inicio.strftime('%Y-%m-%d %H:%M:%S')}")
    log.info(f"{'='*50}")
    return inicio

def log_fim(notebook: str, inicio: datetime) -> None:
    """Loga o fim da execução e o tempo total."""
    fim      = datetime.now()
    duracao  = (fim - inicio).seconds
    log.info(f"{'='*50}")
    log.info(f"FIM   : {notebook}")
    log.info(f"Tempo : {duracao}s")
    log.info(f"{'='*50}")

# ─────────────────────────────────────────────
# VALIDAÇÃO DO CARREGAMENTO
# ─────────────────────────────────────────────
def _validar_credenciais() -> None:
    credenciais = {
        "ADLS_CLIENT_ID"      : ADLS_CLIENT_ID,
        "ADLS_TENANT_ID"      : ADLS_TENANT_ID,
        "ADLS_CLIENT_SECRET"  : ADLS_CLIENT_SECRET,
        "ADLS_STORAGE_ACCOUNT": ADLS_STORAGE_ACCOUNT,
        "ADLS_CONTAINER"      : ADLS_CONTAINER,
        "SQL_HOST"            : SQL_HOST,
        "SQL_DATABASE"        : SQL_DATABASE,
        "SQL_USERNAME"        : SQL_USERNAME,
        "SQL_PASSWORD"        : SQL_PASSWORD
    }
    todas_ok = True
    for nome, valor in credenciais.items():
        if not valor:
            log.error(f"Credencial não encontrada: {nome}")
            todas_ok = False

    if todas_ok:
        log.info("Helpers carregados! Todas as credenciais OK.")
    else:
        raise EnvironmentError(
            "Credenciais ausentes. Verifique o arquivo .env"
        )

_validar_credenciais()

# COMMAND ----------

try:
    print("Iniciando a remoção da pasta...")
    squad2_client = get_squad2_client()
    squad2_client.delete_directory("ecommerce_categorias")
    print("✅ Sucesso! A pasta da raiz foi eliminada.")
except Exception as e:
    print(f"❌ Erro: {str(e)}")
