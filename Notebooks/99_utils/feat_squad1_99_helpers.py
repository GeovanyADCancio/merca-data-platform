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


# ─────────────────────────────────────────────
# FUNÇÕES — PERSISTÊNCIA DE MODELO
# ─────────────────────────────────────────────
def salvar_modelo(obj, caminho: str) -> bool:
    """Serializa um objeto Python (ex: modelo treinado) via pickle e grava no Data Lake.
    Usado para desacoplar o notebook de treino do notebook de score em dados novos —
    ambos devem reutilizar exatamente o mesmo modelo já ajustado, nunca retreinar."""
    import pickle
    try:
        dados = pickle.dumps(obj)
        fs_client = get_squad1_client()
        file_client = fs_client.get_file_client(caminho)
        file_client.upload_data(dados, overwrite=True)
        log.info(f"Modelo salvo em: {caminho} ({len(dados)} bytes)")
        return True
    except Exception as e:
        log.error(f"Erro ao salvar modelo em {caminho}: {str(e)}")
        return False


def carregar_modelo(caminho: str):
    """Lê um objeto serializado via pickle do Data Lake (contraparte de salvar_modelo)."""
    import pickle
    fs_client = get_squad1_client()
    file_client = fs_client.get_file_client(caminho)
    dados = file_client.download_file().readall()
    return pickle.loads(dados)


# ─────────────────────────────────────────────
# FUNÇÕES — ENGENHARIA DE FEATURES (compartilhada entre treino e score)
# ─────────────────────────────────────────────
def construir_features_pedidos(df_pedidos, df_itens, df_produtos):
    """
    Constrói a tabela de features em nível de pedido (1 linha por id_pedido), a partir das tabelas
    Silver de pedidos, itens_pedido e produtos. Compartilhada entre o notebook de treino
    (feat_squad1_03_treinamento_isolation_forest) e o de score em dados novos — garante que os dois
    calculem exatamente as mesmas colunas, do mesmo jeito, evitando divergência entre treino e uso
    real do modelo (training-serving skew).

    IMPORTANTE: df_pedidos deve conter o HISTÓRICO COMPLETO de pedidos, não um recorte só dos
    pedidos "novos" a pontuar — as janelas de histórico do cliente (ticket médio, desvio, dias desde
    a última compra) precisam enxergar todos os pedidos anteriores de cada cliente para calcular
    corretamente, mesmo que só um subconjunto do resultado final seja usado depois.
    """
    from pyspark.sql import functions as F
    from pyspark.sql.window import Window

    df_pedidos_tempo = (
        df_pedidos
        .withColumn("hora_do_pedido", F.hour("dt_pedido"))
        .withColumn("dia_semana_pedido", F.dayofweek("dt_pedido"))
    )

    janela_historico = (
        Window.partitionBy("id_cliente")
        .orderBy("dt_pedido")
        .rowsBetween(Window.unboundedPreceding, -1)
    )
    janela_pedido_anterior = Window.partitionBy("id_cliente").orderBy("dt_pedido")

    df_pedidos_ts = df_pedidos_tempo.withColumn("dt_pedido_unix", F.col("dt_pedido").cast("long"))
    janela_30_dias = (
        Window.partitionBy("id_cliente")
        .orderBy("dt_pedido_unix")
        .rangeBetween(-30 * 86400, -1)
    )

    df_pedidos_features = (
        df_pedidos_ts
        .withColumn("ticket_medio_historico_cliente", F.avg("valor_total").over(janela_historico))
        .withColumn("desvio_padrao_historico_cliente", F.stddev("valor_total").over(janela_historico))
        .withColumn("qtd_pedidos_historico_cliente", F.count("id_pedido").over(janela_historico))
        .withColumn("dt_pedido_anterior", F.lag("dt_pedido").over(janela_pedido_anterior))
        .withColumn("dias_desde_ultima_compra", F.datediff(F.col("dt_pedido"), F.col("dt_pedido_anterior")))
        .withColumn("qtd_pedidos_ultimos_30_dias", F.count("id_pedido").over(janela_30_dias))
        .withColumn(
            "desvio_valor_vs_historico",
            F.when(
                F.col("desvio_padrao_historico_cliente") > 0,
                (F.col("valor_total") - F.col("ticket_medio_historico_cliente"))
                / F.col("desvio_padrao_historico_cliente"),
            ).otherwise(F.lit(None).cast("double")),
        )
        .withColumn(
            "desvio_pct_vs_media_cliente",
            F.when(
                F.col("ticket_medio_historico_cliente") > 0,
                (F.col("valor_total") - F.col("ticket_medio_historico_cliente"))
                / F.col("ticket_medio_historico_cliente"),
            ).otherwise(F.lit(None).cast("double")),
        )
        .withColumn(
            "razao_frete_valor",
            F.when(F.col("valor_total") > 0, F.col("valor_frete") / F.col("valor_total")),
        )
    )

    df_itens_produtos = df_itens.join(df_produtos.select("sku", "id_categoria"), on="sku", how="left")

    df_composicao_pedido = (
        df_itens_produtos
        .groupBy("id_pedido")
        .agg(
            F.count("id_item_pedido").alias("qtd_linhas_itens"),
            F.sum("quantidade").alias("qtd_unidades_total"),
            F.countDistinct("id_categoria").alias("qtd_categorias_distintas"),
            F.sum(
                F.col("quantidade") * F.col("preco_unitario") - F.col("desconto_aplicado")
            ).alias("valor_itens_calculado"),
        )
    )

    df_final = (
        df_pedidos_features
        .join(df_composicao_pedido, on="id_pedido", how="left")
        .withColumn(
            "ticket_medio_por_unidade",
            F.when(F.col("qtd_unidades_total") > 0, F.col("valor_total") / F.col("qtd_unidades_total")),
        )
        .withColumn(
            "diferenca_valor_itens_vs_pedido",
            F.col("valor_total") - F.col("valor_itens_calculado"),
        )
    )

    colunas_features = [
        "id_pedido", "id_cliente", "dt_pedido", "valor_total", "valor_frete", "razao_frete_valor",
        "hora_do_pedido", "dia_semana_pedido",
        "ticket_medio_historico_cliente", "desvio_padrao_historico_cliente",
        "qtd_pedidos_historico_cliente", "dias_desde_ultima_compra",
        "qtd_pedidos_ultimos_30_dias", "desvio_valor_vs_historico", "desvio_pct_vs_media_cliente",
        "qtd_linhas_itens", "qtd_unidades_total", "qtd_categorias_distintas",
        "ticket_medio_por_unidade", "valor_itens_calculado", "diferenca_valor_itens_vs_pedido",
    ]

    return df_final.select(*colunas_features)


# Colunas efetivamente usadas como entrada do Isolation Forest (exclui identificadores e datas)
COLUNAS_MODELO_ANOMALIA = [
    "valor_total", "valor_frete", "razao_frete_valor",
    "hora_do_pedido", "dia_semana_pedido",
    "ticket_medio_historico_cliente", "desvio_padrao_historico_cliente",
    "qtd_pedidos_historico_cliente", "dias_desde_ultima_compra",
    "qtd_pedidos_ultimos_30_dias", "desvio_valor_vs_historico", "desvio_pct_vs_media_cliente",
    "qtd_linhas_itens", "qtd_unidades_total", "qtd_categorias_distintas",
    "ticket_medio_por_unidade", "valor_itens_calculado", "diferenca_valor_itens_vs_pedido",
]

# Preenchimento padrão para colunas que ficam nulas na primeira compra de cada cliente
# (ver decisão registrada no notebook de treino, Seção 2)
VALORES_NULOS_PADRAO_FEATURES = {
    "ticket_medio_historico_cliente": 0.0,
    "desvio_padrao_historico_cliente": 0.0,
    "dias_desde_ultima_compra": 0,
    "desvio_valor_vs_historico": 0.0,
    "desvio_pct_vs_media_cliente": 0.0,
    "qtd_linhas_itens": 0,
    "qtd_unidades_total": 0,
    "qtd_categorias_distintas": 0,
    "ticket_medio_por_unidade": 0.0,
    "valor_itens_calculado": 0.0,
    "diferenca_valor_itens_vs_pedido": 0.0,
}

